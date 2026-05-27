"""
Momentus AI — GEO Attack Map

Builds a per-competitor capability matrix (9 dimensions) so the agency can
attack the weakest axis instead of grinding the whole front. Also keeps a
time-series in capability_history and fires alerts when a competitor
suddenly gains in any axis.

Dimensions (each scored 0-100):
  schema_score, reddit_score, youtube_score, faq_depth_score,
  decision_support_score, review_score, entity_consistency_score,
  pr_score, local_authority_score
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from .claude_engine import ClaudeEngine
from .database import (
    execute, fetch_all, fetch_one, gen_id, to_json, from_json,
)
from .scraper import scrape_urls

logger = logging.getLogger("geo.attack_map")

AXES = (
    "schema_score", "reddit_score", "youtube_score", "faq_depth_score",
    "decision_support_score", "review_score", "entity_consistency_score",
    "pr_score", "local_authority_score",
)

# How big a delta between snapshots fires a "competitor moved" alert.
MOVEMENT_ALERT_THRESHOLD = 12.0


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def analyze_competitor(
    workspace_id: str,
    competitor_domain: str,
    sample_urls: int = 3,
) -> Dict[str, Any]:
    """Compute the 9 capability scores for one competitor and persist them."""
    competitor_domain = competitor_domain.lower().lstrip(".").lstrip("www.")
    if not competitor_domain:
        raise ValueError("competitor_domain required")

    # 1. Cited pages signal: top-citation URLs from Peec/sources
    page_rows = await fetch_all(
        "SELECT url, total_citation_count FROM sources WHERE project_id = ? "
        "AND url LIKE ? ORDER BY total_citation_count DESC LIMIT ?",
        (workspace_id, f"%{competitor_domain}%", sample_urls),
    )
    urls = [r["url"] for r in page_rows if r.get("url")]

    # 2. Reddit signal: how many reddit_intel rows mention this brand
    reddit_count = await _count(
        "SELECT COUNT(*) AS n FROM reddit_intel WHERE workspace_id = ? "
        "AND brands_mentioned LIKE ?",
        (workspace_id, f"%{competitor_domain}%"),
    )
    # 3. AIO presence
    aio_count = await _count(
        "SELECT COUNT(*) AS n FROM aio_tracking WHERE workspace_id = ? "
        "AND visible_brands LIKE ?",
        (workspace_id, f"%{competitor_domain}%"),
    )
    # 4. Prompt observation citation share
    obs_count = await _count(
        "SELECT COUNT(*) AS n FROM prompt_observations WHERE workspace_id = ? "
        "AND sources_cited LIKE ?",
        (workspace_id, f"%{competitor_domain}%"),
    )

    pages: List[Dict[str, Any]] = []
    if urls:
        try:
            scraped = await scrape_urls(urls, project_id=workspace_id)
            if isinstance(scraped, dict):
                pages = list(scraped.values()) if not scraped.get("pages") else scraped["pages"]
            elif isinstance(scraped, list):
                pages = scraped
        except Exception as e:
            logger.warning("scrape during attack-map analyze failed: %s", e)

    # How often this brand surfaces in prompt observations (CSV-derived).
    # Page-free fallback so the Attack Map is useful immediately after a CSV
    # import, before anything has been scraped.
    obs_brand_count = await _count(
        "SELECT COUNT(*) AS n FROM prompt_observations WHERE workspace_id = ? "
        "AND brands_appeared LIKE ?",
        (workspace_id, f"%{competitor_domain}%"),
    )

    scores = _compute_scores_local(pages, reddit_count, aio_count, obs_count)

    if not pages and obs_brand_count:
        scaled = min(100.0, obs_brand_count * 6.0)
        scores["decision_support_score"] = max(scores["decision_support_score"], scaled)
        scores["local_authority_score"] = max(scores["local_authority_score"], scaled)
        scores["entity_consistency_score"] = max(scores["entity_consistency_score"], scaled * 0.6)

    # Optional: ask Claude to refine the diagnosis + summary text. Cheap,
    # one call per competitor.
    summary, weakest_axis, weakest_score, claude_scores = await _claude_summary(
        competitor_domain, pages, reddit_count, aio_count, obs_count, scores,
    )
    if claude_scores:
        # Average local + Claude where both produced a score
        for axis, val in claude_scores.items():
            if axis in scores:
                scores[axis] = (scores[axis] + float(val)) / 2.0

    overall = sum(scores.values()) / len(scores) if scores else 0.0
    if not weakest_axis:
        weakest_axis, weakest_score = min(scores.items(), key=lambda kv: kv[1])

    await execute(
        """INSERT INTO competitor_capabilities
           (workspace_id, competitor_domain, schema_score, reddit_score,
            youtube_score, faq_depth_score, decision_support_score, review_score,
            entity_consistency_score, pr_score, local_authority_score,
            overall_strength, weakest_axis, weakest_axis_score, summary,
            analyzed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(workspace_id, competitor_domain) DO UPDATE SET
             schema_score=excluded.schema_score,
             reddit_score=excluded.reddit_score,
             youtube_score=excluded.youtube_score,
             faq_depth_score=excluded.faq_depth_score,
             decision_support_score=excluded.decision_support_score,
             review_score=excluded.review_score,
             entity_consistency_score=excluded.entity_consistency_score,
             pr_score=excluded.pr_score,
             local_authority_score=excluded.local_authority_score,
             overall_strength=excluded.overall_strength,
             weakest_axis=excluded.weakest_axis,
             weakest_axis_score=excluded.weakest_axis_score,
             summary=excluded.summary,
             analyzed_at=excluded.analyzed_at""",
        (
            workspace_id, competitor_domain,
            scores["schema_score"], scores["reddit_score"], scores["youtube_score"],
            scores["faq_depth_score"], scores["decision_support_score"],
            scores["review_score"], scores["entity_consistency_score"],
            scores["pr_score"], scores["local_authority_score"],
            overall, weakest_axis, weakest_score, summary,
        ),
    )
    await _record_history(workspace_id, competitor_domain, scores)
    return {
        "workspace_id": workspace_id,
        "competitor_domain": competitor_domain,
        "scores": scores,
        "overall_strength": overall,
        "weakest_axis": weakest_axis,
        "weakest_axis_score": weakest_score,
        "summary": summary,
        "analyzed_urls": urls,
    }


async def list_attack_map(workspace_id: str) -> List[Dict[str, Any]]:
    rows = await fetch_all(
        "SELECT * FROM competitor_capabilities WHERE workspace_id = ? "
        "ORDER BY overall_strength DESC",
        (workspace_id,),
    )
    # Phase 3: enrich each row with confidence + recommendation. Non-breaking:
    # extra keys are added; existing keys are preserved.
    for r in rows:
        try:
            r.update(await _enrich_capability_row(workspace_id, r))
        except Exception as e:
            logger.warning("attack-map enrich failed for %s: %s",
                           r.get("competitor_domain", "?"), e)
    return rows


async def analyze_all_known(workspace_id: str, max_competitors: int = 12) -> Dict[str, Any]:
    """One-click: re-analyze every competitor we've already seen for this
    workspace (either previously seeded by the CSV importer or added by
    the user). Returns per-domain results."""
    rows = await fetch_all(
        "SELECT competitor_domain FROM competitor_capabilities "
        "WHERE workspace_id = ? ORDER BY overall_strength DESC LIMIT ?",
        (workspace_id, max_competitors),
    )
    if not rows:
        # Bootstrap: pull the top brands from prompt_observations directly.
        obs = await fetch_all(
            "SELECT brands_appeared FROM prompt_observations WHERE workspace_id = ?",
            (workspace_id,),
        )
        counts: Dict[str, int] = {}
        for o in obs:
            try:
                import json as _json
                brands = _json.loads(o.get("brands_appeared") or "[]")
            except Exception:
                brands = []
            for b in brands or []:
                name = (b.get("name") if isinstance(b, dict) else str(b)) or ""
                name = name.strip().lower()
                if name:
                    counts[name] = counts.get(name, 0) + 1
        top = sorted(counts.items(), key=lambda kv: -kv[1])[:max_competitors]
        rows = [{"competitor_domain": d} for d, _ in top]

    results: List[Dict[str, Any]] = []
    for r in rows:
        try:
            results.append(await analyze_competitor(workspace_id, r["competitor_domain"]))
        except Exception as e:
            logger.warning("analyze %s failed: %s", r["competitor_domain"], e)
            results.append({"competitor_domain": r["competitor_domain"], "error": str(e)})
    return {"analyzed": len(results), "details": results}


async def list_movements(workspace_id: str, days: int = 14) -> List[Dict[str, Any]]:
    """Return capability movements > MOVEMENT_ALERT_THRESHOLD in the window."""
    rows = await fetch_all(
        "SELECT * FROM capability_history WHERE workspace_id = ? "
        "AND observed_at >= datetime('now', ? || ' days') "
        "AND ABS(delta_vs_prev) >= ? ORDER BY observed_at DESC",
        (workspace_id, f"-{int(days)}", MOVEMENT_ALERT_THRESHOLD),
    )
    return rows


# ═══════════════════════════════════════════════════════════════
# LOCAL SCORING
# ═══════════════════════════════════════════════════════════════

def _compute_scores_local(
    pages: List[Dict[str, Any]],
    reddit_count: int,
    aio_count: int,
    obs_count: int,
) -> Dict[str, float]:
    schema_types: List[str] = []
    faq_hits = 0
    review_hits = 0
    yt_hits = 0
    pr_hits = 0
    decision_hits = 0
    entity_hits = 0
    local_hits = 0
    word_count = 0

    for p in pages or []:
        body = (p.get("text") or p.get("body") or p.get("content") or "").lower()
        word_count += len(body.split())
        schema = p.get("schema") or p.get("jsonld") or []
        if not isinstance(schema, list):
            schema = [schema]
        for node in schema or []:
            t = _jsonld_type(node)
            if t:
                schema_types.append(t)
            if "FAQ" in t:
                faq_hits += 1
            if "Review" in t or "AggregateRating" in t:
                review_hits += 1
            if "Person" in t or "Physician" in t:
                entity_hits += 1
            if "LocalBusiness" in t or "MedicalOrganization" in t or "Hospital" in t:
                local_hits += 1
            if "VideoObject" in t:
                yt_hits += 1
        # Body-level signals
        if "youtube.com" in body or "youtu.be" in body:
            yt_hits += 1
        if "vs " in body or "compared to" in body or "comparison" in body:
            decision_hits += 1
        if "press" in body or "as featured in" in body or "interview" in body:
            pr_hits += 1
        if "reddit.com" in body:
            pass  # handled by reddit_count

    schema_breadth = len(set(schema_types))
    faq_depth = faq_hits + body_count_re(pages, r"\?\s*</h[2-4]>")
    return {
        "schema_score": _scale(schema_breadth, 0, 8) * 100,
        "reddit_score": _scale(reddit_count, 0, 12) * 100,
        "youtube_score": _scale(yt_hits, 0, 5) * 100,
        "faq_depth_score": _scale(faq_depth, 0, 8) * 100,
        "decision_support_score": _scale(decision_hits, 0, 5) * 100,
        "review_score": _scale(review_hits, 0, 4) * 100,
        "entity_consistency_score": _scale(entity_hits + (1 if "sameAs" in str(pages) else 0), 0, 5) * 100,
        "pr_score": _scale(pr_hits, 0, 4) * 100,
        "local_authority_score": _scale(local_hits + (aio_count // 5), 0, 6) * 100,
    }


def body_count_re(pages: List[Dict[str, Any]], pattern: str) -> int:
    n = 0
    rx = re.compile(pattern, re.I)
    for p in pages or []:
        body = (p.get("html") or p.get("text") or "")
        n += len(rx.findall(body))
    return n


def _scale(value: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    v = max(0.0, min(1.0, (float(value) - lo) / (hi - lo)))
    return v


# ═══════════════════════════════════════════════════════════════
# CLAUDE SUMMARY
# ═══════════════════════════════════════════════════════════════

CLAUDE_ATTACK_SYSTEM = """You assess a single competitor's GEO authority profile.

Given:
 - the competitor domain
 - a few of their top-cited pages (excerpts + detected schema types)
 - aggregate counts (reddit citations, AIO appearances, prompt observation
   citations)

Output STRICT JSON:
{
  "scores": {
    "schema_score": 0-100,
    "reddit_score": 0-100,
    "youtube_score": 0-100,
    "faq_depth_score": 0-100,
    "decision_support_score": 0-100,
    "review_score": 0-100,
    "entity_consistency_score": 0-100,
    "pr_score": 0-100,
    "local_authority_score": 0-100
  },
  "weakest_axis": "axis_name",
  "weakest_axis_score": 0-100,
  "summary": "ONE crisp paragraph: why this competitor is strong, where the open flank is."
}"""


async def _claude_summary(
    competitor_domain: str,
    pages: List[Dict[str, Any]],
    reddit_count: int,
    aio_count: int,
    obs_count: int,
    local_scores: Dict[str, float],
) -> Tuple[str, str, float, Dict[str, float]]:
    engine = ClaudeEngine()
    blocks = []
    for p in pages[:3]:
        body = (p.get("text") or "")[:3500]
        schema = p.get("schema") or p.get("jsonld") or []
        if not isinstance(schema, list):
            schema = [schema]
        schema_types = sorted({_jsonld_type(s) for s in schema if s})
        blocks.append(
            f"URL: {p.get('url','')}\nTitle: {p.get('title','')}\n"
            f"Schema: {schema_types}\nExcerpt:\n{body}\n---"
        )
    user = (
        f"Competitor: {competitor_domain}\n"
        f"Reddit citation count: {reddit_count}\n"
        f"AIO appearances: {aio_count}\n"
        f"Prompt-obs citations: {obs_count}\n"
        f"Local heuristic scores: {json.dumps(local_scores)}\n\n"
        + "\n\n".join(blocks)
    )
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": user}],
            system=CLAUDE_ATTACK_SYSTEM,
            max_tokens=900,
            temperature=0.1,
            operation="attack_map_summary",
            use_cache=True,
        )
        parsed = _safe_json(resp.get("content", "")) or {}
    except Exception as e:
        logger.warning("Claude attack summary failed: %s", e)
        parsed = {}
    return (
        (parsed.get("summary") or "")[:1200],
        parsed.get("weakest_axis", "") or "",
        float(parsed.get("weakest_axis_score") or 0.0),
        parsed.get("scores") or {},
    )


# ═══════════════════════════════════════════════════════════════
# HISTORY + DELTAS
# ═══════════════════════════════════════════════════════════════

async def _record_history(
    workspace_id: str, competitor_domain: str, scores: Dict[str, float],
) -> None:
    # Pull the previous snapshot (the one we are about to overwrite)
    prev = {}
    rows = await fetch_all(
        "SELECT axis, score FROM capability_history WHERE workspace_id = ? "
        "AND competitor_domain = ? ORDER BY observed_at DESC LIMIT 32",
        (workspace_id, competitor_domain),
    )
    seen = set()
    for r in rows:
        if r["axis"] in seen:
            continue
        seen.add(r["axis"])
        prev[r["axis"]] = float(r["score"] or 0)
    for axis, val in scores.items():
        delta = float(val) - float(prev.get(axis, 0.0))
        await execute(
            "INSERT INTO capability_history (id, workspace_id, competitor_domain, "
            "axis, score, delta_vs_prev) VALUES (?, ?, ?, ?, ?, ?)",
            (gen_id("ch-"), workspace_id, competitor_domain, axis, val, delta),
        )


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

def _jsonld_type(node: Any) -> str:
    if isinstance(node, dict):
        t = node.get("@type") or node.get("type") or ""
        if isinstance(t, list):
            return ",".join(str(x) for x in t)
        return str(t)
    return ""


async def _count(sql: str, args: Tuple) -> int:
    row = await fetch_one(sql, args)
    return int(row.get("n") or 0) if row else 0


def _safe_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        t = text.strip()
        if t.startswith("```"):
            t = t.strip("`")
            if t.lower().startswith("json"):
                t = t[4:]
        s, e = t.find("{"), t.rfind("}")
        if s == -1 or e == -1:
            return None
        return json.loads(t[s:e + 1])
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
# PHASE 3 — CAPABILITY ROW ENRICHMENT + ACTION PUSH
# Adds per-row {confidence, last_analyzed_at, data_source, open_flank,
# recommended_attack, effort, potential_impact}. Non-breaking — extra keys.
# ═══════════════════════════════════════════════════════════════

# Map "weakest axis" → recommended attack copy + effort/impact.
_ATTACK_BY_AXIS: Dict[str, Dict[str, str]] = {
    "schema_score": {
        "attack": "Install missing schema.org markup (Physician/MedicalOrg/FAQ/Review). Schema is their thinnest axis — own it.",
        "effort": "low", "impact": "medium",
    },
    "reddit_score": {
        "attack": "Seed credible Reddit answers + AMAs. Their absence from Reddit is the open flank.",
        "effort": "medium", "impact": "high",
    },
    "youtube_score": {
        "attack": "Publish expert YouTube videos with chapters + VideoObject schema, embed on key pages.",
        "effort": "medium", "impact": "medium",
    },
    "faq_depth_score": {
        "attack": "Add deep FAQ sections that mirror real AI prompts; mark up with FAQPage schema.",
        "effort": "low", "impact": "high",
    },
    "decision_support_score": {
        "attack": "Build comparison/decision pages + Physician schema + review proof.",
        "effort": "medium", "impact": "high",
    },
    "review_score": {
        "attack": "Aggregate verified reviews on-site + Review/AggregateRating schema.",
        "effort": "low", "impact": "medium",
    },
    "entity_consistency_score": {
        "attack": "Tighten brand entity: consistent NAP, sameAs links, Person bios across all pages.",
        "effort": "low", "impact": "medium",
    },
    "pr_score": {
        "attack": "Earn PR mentions in trusted publishers (interviews, expert quotes) and link back.",
        "effort": "high", "impact": "high",
    },
    "local_authority_score": {
        "attack": "Strengthen local signals: LocalBusiness schema, branch pages, local citations.",
        "effort": "medium", "impact": "medium",
    },
}

# Action-type each axis maps to (drives push_attack_to_actions).
_AXIS_TO_ACTION_TYPE: Dict[str, str] = {
    "schema_score": "schema_install",
    "reddit_score": "reddit_seed",
    "youtube_score": "content_brief",
    "faq_depth_score": "content_brief",
    "decision_support_score": "comparison_page",
    "review_score": "schema_install",
    "entity_consistency_score": "metadata_rewrite",
    "pr_score": "content_brief",
    "local_authority_score": "schema_install",
}


async def _enrich_capability_row(
    workspace_id: str, row: Dict[str, Any],
) -> Dict[str, Any]:
    """Derive {confidence, last_analyzed_at, data_source, open_flank,
    recommended_attack, effort, potential_impact} for a capability row."""
    domain = (row.get("competitor_domain") or "").lower()
    scores = {a: float(row.get(a) or 0.0) for a in AXES}
    # Strongest two axes + weakest one.
    ranked = sorted(scores.items(), key=lambda kv: -kv[1])
    strongest = [a for a, _ in ranked[:2]]
    weakest_axis = (row.get("weakest_axis") or ranked[-1][0]) if ranked else ""
    weakest_score = float(row.get("weakest_axis_score") or (ranked[-1][1] if ranked else 0.0))
    attack = _ATTACK_BY_AXIS.get(weakest_axis, {
        "attack": "Diagnose this competitor with `comparative_diagnose` to find the thinnest axis, then publish a stronger decision page.",
        "effort": "medium", "impact": "medium",
    })
    open_flank_text = ", ".join(strongest) + (f" (weakest: {weakest_axis})" if weakest_axis else "")

    # How many pages we actually scraped (sources rows on this domain).
    scraped_pages = 0
    try:
        r = await fetch_one(
            "SELECT COUNT(*) AS n FROM sources WHERE project_id = ? AND url LIKE ?",
            (workspace_id, f"%{domain}%"),
        )
        scraped_pages = int((r or {}).get("n", 0))
    except Exception:
        scraped_pages = 0
    # Observation counts (prompts where this domain was mentioned).
    obs_count = 0
    try:
        r = await fetch_one(
            "SELECT COUNT(*) AS n FROM prompt_observations WHERE workspace_id = ? "
            "AND (brands_appeared LIKE ? OR sources_cited LIKE ?)",
            (workspace_id, f"%{domain}%", f"%{domain}%"),
        )
        obs_count = int((r or {}).get("n", 0))
    except Exception:
        obs_count = 0

    has_summary = bool((row.get("summary") or "").strip())
    # Confidence ladder:
    #   verified         — scraped pages + Claude summary + observations
    #   claude_analyzed  — Claude summary present (may lack pages or obs)
    #   scraped          — scraped pages but no Claude refinement
    #   observation_only — only observations exist (CSV-seeded)
    #   seeded           — nothing but a row exists
    if scraped_pages and has_summary and obs_count:
        confidence = "verified"
        data_source = "scraped"
    elif has_summary:
        confidence = "claude_analyzed"
        data_source = "scraped" if scraped_pages else "observation_only"
    elif scraped_pages:
        confidence = "scraped"
        data_source = "scraped"
    elif obs_count:
        confidence = "observation_only"
        data_source = "observation_only"
    else:
        confidence = "seeded"
        data_source = "seeded"

    return {
        "confidence": confidence,
        "last_analyzed_at": row.get("analyzed_at") or "",
        "data_source": data_source,
        "scraped_pages": scraped_pages,
        "observation_count": obs_count,
        "open_flank": open_flank_text,
        "recommended_attack": attack["attack"],
        "effort": attack["effort"],
        "potential_impact": attack["impact"],
        "weakest_axis_resolved": weakest_axis,
        "weakest_axis_score_resolved": weakest_score,
    }


# Step text -> action_type fallback (mirrors action_engine._step_to_type).
def _step_to_action_type(step: str) -> str:
    s = (step or "").lower()
    if "schema" in s or "json-ld" in s or "jsonld" in s or "structured data" in s:
        return "schema_install"
    if "reddit" in s or "subreddit" in s or "forum" in s or "community" in s:
        return "reddit_seed"
    if "compare" in s or "comparison" in s or "vs " in s or "versus" in s or "alternative" in s:
        return "comparison_page"
    if "meta" in s or "title tag" in s or "snippet" in s or "description" in s:
        return "metadata_rewrite"
    if "rebut" in s or "respond" in s or "response" in s or "correct" in s or "factual" in s:
        return "citation_response"
    return "content_brief"


async def push_attack_to_actions(
    workspace_id: str, competitor_domain: str,
) -> Dict[str, Any]:
    """Turn the recommended_attack for one competitor into 2-4 geo_actions
    rows. Idempotent on (workspace_id, action_type, source_id)."""
    domain = (competitor_domain or "").lower().lstrip(".").lstrip("www.")
    if not domain:
        raise ValueError("competitor_domain required")
    row = await fetch_one(
        "SELECT * FROM competitor_capabilities WHERE workspace_id = ? AND competitor_domain = ?",
        (workspace_id, domain),
    )
    if not row:
        raise ValueError(f"no capability row for {domain}; analyze first")
    enrich = await _enrich_capability_row(workspace_id, row)
    weakest = enrich.get("weakest_axis_resolved") or ""

    # Lazy import action_engine to register schema if needed.
    from . import action_engine as _act
    try:
        await _act.init()
    except Exception as e:
        logger.warning("action_engine.init failed: %s", e)

    async def _insert(at: str, title: str, desc: str, sub_id: str,
                      priority: float = 65.0, impact: str = "") -> Optional[str]:
        if at not in _act.ACTION_TYPES:
            return None
        sid = f"am:{domain}:{sub_id}"
        existing = await fetch_one(
            "SELECT 1 FROM geo_actions WHERE workspace_id=? AND action_type=? AND source_id=? LIMIT 1",
            (workspace_id, at, sid),
        )
        if existing:
            return None
        aid = gen_id("act-")
        await execute(
            "INSERT INTO geo_actions "
            "(id, workspace_id, action_type, status, title, description, source_kind, "
            " source_id, target_url, target_prompt_id, priority, estimated_impact, payload) "
            "VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                aid, workspace_id, at, title[:300], desc[:2000],
                "competitor_capabilities", sid, "", "",
                float(priority), impact[:200],
                to_json({
                    "competitor_domain": domain,
                    "weakest_axis": weakest,
                    "open_flank": enrich.get("open_flank", ""),
                    "effort": enrich.get("effort", ""),
                    "potential_impact": enrich.get("potential_impact", ""),
                }),
            ),
        )
        return aid

    created: List[Dict[str, str]] = []
    skipped: List[str] = []

    # 1. Primary attack derived from weakest axis.
    primary_attack = enrich.get("recommended_attack") or ""
    if primary_attack:
        at_primary = _AXIS_TO_ACTION_TYPE.get(weakest, _step_to_action_type(primary_attack))
        aid = await _insert(
            at_primary,
            title=f"Attack {domain} via {weakest or 'weakest axis'}",
            desc=primary_attack,
            sub_id=f"axis:{weakest or 'primary'}",
            priority=80.0,
            impact=enrich.get("potential_impact", ""),
        )
        if aid:
            created.append({"action_id": aid, "type": at_primary, "category": "primary"})
        else:
            skipped.append("primary")

    # 2-3. Follow-ups against the 1-2 other lowest axes (so we get 2-4 rows).
    scores = sorted(
        ((a, float(row.get(a) or 0.0)) for a in AXES),
        key=lambda kv: kv[1],
    )
    for axis, score in scores[1:3]:
        if axis == weakest:
            continue
        ax = _ATTACK_BY_AXIS.get(axis)
        if not ax:
            continue
        at = _AXIS_TO_ACTION_TYPE.get(axis, "content_brief")
        aid = await _insert(
            at,
            title=f"Press {domain} on {axis} ({round(score)}/100)",
            desc=ax["attack"],
            sub_id=f"axis:{axis}",
            priority=60.0,
            impact=ax["impact"],
        )
        if aid:
            created.append({"action_id": aid, "type": at, "category": axis})
        else:
            skipped.append(axis)

    return {
        "workspace_id": workspace_id,
        "competitor_domain": domain,
        "weakest_axis": weakest,
        "created": len(created),
        "skipped": len(skipped),
        "actions": created,
        "skipped_details": skipped,
    }
