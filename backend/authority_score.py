"""
Momentus AI — GEO Authority Score™

Proprietary 0-100 score expressing how strong a brand is in the eyes of
generative AI systems. Composed of 7 sub-scores:

  citation_score, prompt_ownership_score, schema_score, offsite_score,
  reddit_score, entity_score, local_score

Each sub-score is normalised to 0-100 and weighted into the total. Scores
are persisted as time-series in `authority_scores` so the dashboard can
plot Month 1 → Month 2 → Month 3 trends.

Public API:
  - compute_for_workspace(workspace_id) — our brand's score, fresh
  - compute_for_competitor(workspace_id, domain) — comparable score
  - rebuild_all(workspace_id) — recompute us + every competitor seen
  - timeseries(workspace_id, subject_domain) — chart data
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from .database import (
    execute, fetch_all, fetch_one, gen_id, to_json, from_json,
)

logger = logging.getLogger("geo.authority")

WEIGHTS = {
    "citation_score": 0.18,
    "prompt_ownership_score": 0.22,
    "schema_score": 0.12,
    "offsite_score": 0.13,
    "reddit_score": 0.10,
    "entity_score": 0.10,
    "local_score": 0.15,
}


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def compute_for_workspace(workspace_id: str) -> Dict[str, Any]:
    """Our brand's GEO Authority Score."""
    ws = await fetch_one(
        "SELECT brand_name, name, domains FROM workspaces WHERE id = ?",
        (workspace_id,),
    )
    brand = ((ws or {}).get("brand_name") or (ws or {}).get("name") or "").lower()
    # workspaces.domains is stored as a JSON array; take the first if present.
    domain = ""
    try:
        import json as _json
        ds = (ws or {}).get("domains") or "[]"
        if isinstance(ds, str):
            ds = _json.loads(ds)
        if isinstance(ds, list) and ds:
            domain = str(ds[0]).lower().lstrip("www.")
    except Exception:
        domain = ""
    return await _compute(workspace_id, subject_domain=domain or brand,
                          is_us=True, brand_alias=brand)


async def compute_for_competitor(workspace_id: str, domain: str) -> Dict[str, Any]:
    return await _compute(workspace_id, subject_domain=domain.lower(),
                          is_us=False, brand_alias="")


async def rebuild_all(workspace_id: str) -> Dict[str, Any]:
    us = await compute_for_workspace(workspace_id)
    comp_rows = await fetch_all(
        "SELECT DISTINCT competitor_domain FROM competitor_capabilities "
        "WHERE workspace_id = ?",
        (workspace_id,),
    )
    competitors: List[Dict[str, Any]] = []
    for r in comp_rows:
        d = r["competitor_domain"]
        try:
            competitors.append(await compute_for_competitor(workspace_id, d))
        except Exception as e:
            logger.warning("authority compute failed for %s: %s", d, e)
    return {"us": us, "competitors": competitors}


async def timeseries(workspace_id: str, subject_domain: str, months: int = 12) -> List[Dict[str, Any]]:
    return await fetch_all(
        "SELECT * FROM authority_scores WHERE workspace_id = ? "
        "AND subject_domain = ? "
        "AND observed_at >= datetime('now', ? || ' months') "
        "ORDER BY observed_at ASC",
        (workspace_id, subject_domain.lower(), f"-{int(months)}"),
    )


async def latest(workspace_id: str) -> Dict[str, Any]:
    rows = await fetch_all(
        "SELECT * FROM authority_scores WHERE workspace_id = ? "
        "AND observed_at = (SELECT MAX(observed_at) FROM authority_scores "
        "                  WHERE workspace_id = ? AND subject_domain = authority_scores.subject_domain) "
        "ORDER BY total_score DESC",
        (workspace_id, workspace_id),
    )
    return {"scores": rows}


# Per-component presentation metadata for the breakdown view.
# (label, metric_dictionary key, recommended fix, qualitative effort/impact)
_COMPONENT_META = {
    "citation_score": (
        "Citation Score", "citation_score",
        "Refresh content + land citations on pages AI already pulls from.", "medium",
    ),
    "prompt_ownership_score": (
        "Prompt Ownership", "prompt_ownership_level",
        "Track top-revenue prompts and target the lost ones with decision content.", "high",
    ),
    "schema_score": (
        "Schema Depth", "faq_depth",
        "Implement Physician + Review + FAQ schema on decision pages.", "fast",
    ),
    "offsite_score": (
        "Off-site Authority", "source_quality",
        "Land PR mentions on trusted publisher domains.", "high",
    ),
    "reddit_score": (
        "Reddit Authority", "citation_quality",
        "Seed credible Reddit discussions in target subreddits.", "medium",
    ),
    "entity_score": (
        "Entity Consistency", "entity_score",
        "Tighten brand-entity consistency: Organization/Person schema, sameAs, NAP.", "fast",
    ),
    "local_score": (
        "Local Authority", "local_authority",
        "Strengthen LocalBusiness schema + Google Business signals.", "medium",
    ),
}

# Rough effort ranking: lower = quicker to move.
_EFFORT_RANK = {"fast": 0, "medium": 1, "high": 2}


async def authority_breakdown(workspace_id: str, subject_domain: str = "") -> Dict[str, Any]:
    """Per-component breakdown of our (or a given domain's) GEO Authority Score.

    Reads the latest authority_scores row, then for each of the 7 sub-scores
    returns its score, weight, weighted contribution, plain-English meaning
    (from metric_dictionary), confidence, a recommended fix, and a qualitative
    estimated impact. Also surfaces the biggest_lever / fastest_win / hardest_gap.
    """
    # Resolve our domain if none supplied (same logic as compute_for_workspace).
    if not subject_domain:
        ws = await fetch_one(
            "SELECT brand_name, name, domains FROM workspaces WHERE id = ?",
            (workspace_id,),
        )
        brand = ((ws or {}).get("brand_name") or (ws or {}).get("name") or "").lower()
        domain = ""
        try:
            ds = (ws or {}).get("domains") or "[]"
            if isinstance(ds, str):
                ds = json.loads(ds)
            if isinstance(ds, list) and ds:
                domain = str(ds[0]).lower().lstrip("www.")
        except Exception:
            domain = ""
        subject_domain = domain or brand

    row = None
    if subject_domain:
        row = await fetch_one(
            "SELECT * FROM authority_scores WHERE workspace_id = ? AND subject_domain = ? "
            "ORDER BY observed_at DESC LIMIT 1",
            (workspace_id, subject_domain.lower()),
        )
    if not row:
        # No authority computed yet — return an empty-but-shaped result.
        return {
            "workspace_id": workspace_id,
            "subject_domain": subject_domain,
            "found": False,
            "total": 0.0,
            "components": [],
            "biggest_lever": None,
            "fastest_win": None,
            "hardest_gap": None,
        }

    try:
        from . import metric_dictionary as _md
    except Exception:  # pragma: no cover - defensive
        _md = None

    components: List[Dict[str, Any]] = []
    for key, weight in WEIGHTS.items():
        score = float(row.get(key) or 0)
        contribution = round(score * weight, 4)
        label, md_key, fix, effort = _COMPONENT_META.get(
            key, (key, key, _why(key), "medium")
        )
        meaning = ""
        confidence = "estimated"
        if _md is not None:
            m = _md.get_metric(md_key)
            if m:
                meaning = m.get("definition", "")
                confidence = m.get("confidence", "estimated")
        # Estimated impact: how many total points we could gain if this
        # component went to 100 (i.e. headroom × weight).
        estimated_impact = round((100.0 - score) * weight, 2)
        components.append({
            "key": key,
            "label": label,
            "score": round(score, 2),
            "weight": weight,
            "contribution": contribution,
            "meaning": meaning,
            "confidence": confidence,
            "recommended_fix": fix,
            "estimated_impact": estimated_impact,
            "effort": effort,
        })

    total = round(float(row.get("total_score") or sum(c["contribution"] for c in components)), 2)

    # biggest_lever: most total points to gain (headroom × weight).
    biggest_lever = max(components, key=lambda c: c["estimated_impact"]) if components else None
    # fastest_win: best estimated_impact among the lowest-effort components.
    fastest_win = None
    if components:
        fastest_win = min(
            components,
            key=lambda c: (_EFFORT_RANK.get(c["effort"], 1), -c["estimated_impact"]),
        )
    # hardest_gap: lowest raw score (most behind), regardless of effort.
    hardest_gap = min(components, key=lambda c: c["score"]) if components else None

    return {
        "workspace_id": workspace_id,
        "subject_domain": subject_domain,
        "found": True,
        "total": total,
        "components": components,
        "biggest_lever": biggest_lever,
        "fastest_win": fastest_win,
        "hardest_gap": hardest_gap,
    }


# ═══════════════════════════════════════════════════════════════
# COMPUTATION
# ═══════════════════════════════════════════════════════════════

async def _compute(
    workspace_id: str, subject_domain: str, is_us: bool, brand_alias: str = "",
) -> Dict[str, Any]:
    if not subject_domain:
        raise ValueError("subject_domain required")

    citation = await _citation_score(workspace_id, subject_domain, is_us, brand_alias)
    ownership = await _ownership_score(workspace_id, subject_domain, is_us)
    schema = await _schema_score(workspace_id, subject_domain, is_us)
    offsite = await _offsite_score(workspace_id, subject_domain, is_us)
    reddit = await _reddit_score(workspace_id, subject_domain, is_us)
    entity = await _entity_score(workspace_id, subject_domain, is_us)
    local = await _local_score(workspace_id, subject_domain, is_us)

    sub = {
        "citation_score": citation, "prompt_ownership_score": ownership,
        "schema_score": schema, "offsite_score": offsite,
        "reddit_score": reddit, "entity_score": entity, "local_score": local,
    }
    total = sum(sub[k] * WEIGHTS[k] for k in WEIGHTS)
    biggest_lever = min(sub.items(), key=lambda kv: kv[1])
    rationale = {
        "biggest_lever": biggest_lever[0],
        "biggest_lever_score": biggest_lever[1],
        "summary": _why(biggest_lever[0]),
    }

    await execute(
        "INSERT INTO authority_scores "
        "(id, workspace_id, subject_domain, is_us, total_score, citation_score, "
        " prompt_ownership_score, schema_score, offsite_score, reddit_score, "
        " entity_score, local_score, rationale) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            gen_id("as-"), workspace_id, subject_domain, 1 if is_us else 0,
            total, citation, ownership, schema, offsite, reddit, entity, local,
            to_json(rationale),
        ),
    )
    return {
        "subject_domain": subject_domain, "is_us": is_us,
        "total_score": total, **sub, "rationale": rationale,
    }


# ═══════════════════════════════════════════════════════════════
# SUB-SCORES
# ═══════════════════════════════════════════════════════════════

async def _citation_score(ws_id: str, domain: str, is_us: bool, brand_alias: str) -> float:
    """Citation share across mention_events + prompt_observations."""
    total = await _scalar(
        "SELECT COUNT(*) AS n FROM mention_events WHERE project_id = ?",
        (ws_id,),
    )
    if not total:
        return 0.0
    ours_in = await _scalar(
        "SELECT COUNT(*) AS n FROM mention_events WHERE project_id = ? AND url LIKE ?",
        (ws_id, f"%{domain}%"),
    )
    share = ours_in / total
    return min(100.0, share * 200.0)  # 50% share = 100/100


async def _ownership_score(ws_id: str, domain: str, is_us: bool) -> float:
    if is_us:
        rows = await fetch_all(
            "SELECT our_score FROM prompt_ownership WHERE workspace_id = ?",
            (ws_id,),
        )
        if not rows:
            return 0.0
        return min(100.0, sum(float(r["our_score"] or 0) for r in rows) / len(rows))
    # competitor: pull their average score from competitor_scores JSON
    rows = await fetch_all(
        "SELECT competitor_scores FROM prompt_ownership WHERE workspace_id = ?",
        (ws_id,),
    )
    if not rows:
        return 0.0
    sums, n = 0.0, 0
    for r in rows:
        m = from_json(r["competitor_scores"] or "{}") or {}
        for k, v in m.items():
            if domain in k:
                sums += float(v or 0)
                n += 1
                break
    return min(100.0, (sums / n) if n else 0.0)


async def _schema_score(ws_id: str, domain: str, is_us: bool) -> float:
    rows = await fetch_all(
        "SELECT schema_depth_score FROM schema_audits WHERE workspace_id = ? "
        "AND page_url LIKE ? AND is_competitor = ?",
        (ws_id, f"%{domain}%", 0 if is_us else 1),
    )
    if not rows:
        return 0.0
    return min(100.0, sum(float(r["schema_depth_score"] or 0) for r in rows) / len(rows))


async def _offsite_score(ws_id: str, domain: str, is_us: bool) -> float:
    """PR + publisher mentions: count appearances in AIO publisher_domains
    + prompt_observations sources_cited where the domain is a third party."""
    aio = await _scalar(
        "SELECT COUNT(*) AS n FROM aio_tracking WHERE workspace_id = ? "
        "AND publisher_domains LIKE ?",
        (ws_id, f"%{domain}%"),
    )
    obs = await _scalar(
        "SELECT COUNT(*) AS n FROM prompt_observations WHERE workspace_id = ? "
        "AND sources_cited LIKE ?",
        (ws_id, f"%{domain}%"),
    )
    return min(100.0, (aio * 4) + (obs * 1.5))


async def _reddit_score(ws_id: str, domain: str, is_us: bool) -> float:
    if is_us:
        n = await _scalar(
            "SELECT COUNT(*) AS n FROM reddit_intel WHERE workspace_id = ? "
            "AND our_brand_mentioned = 1",
            (ws_id,),
        )
    else:
        n = await _scalar(
            "SELECT COUNT(*) AS n FROM reddit_intel WHERE workspace_id = ? "
            "AND brands_mentioned LIKE ?",
            (ws_id, f"%{domain}%"),
        )
    return min(100.0, float(n) * 8.0)


async def _entity_score(ws_id: str, domain: str, is_us: bool) -> float:
    """Entity consistency proxy: presence of Person/Organization schema +
    capability score."""
    cap = await fetch_one(
        "SELECT entity_consistency_score FROM competitor_capabilities "
        "WHERE workspace_id = ? AND competitor_domain = ?",
        (ws_id, domain),
    )
    if cap and cap.get("entity_consistency_score") is not None:
        return float(cap["entity_consistency_score"])
    schema_hit = await _scalar(
        "SELECT COUNT(*) AS n FROM schema_audits WHERE workspace_id = ? "
        "AND page_url LIKE ? AND (schema_types LIKE '%Person%' OR schema_types LIKE '%Organization%')",
        (ws_id, f"%{domain}%"),
    )
    return min(100.0, float(schema_hit) * 12.0)


async def _local_score(ws_id: str, domain: str, is_us: bool) -> float:
    cap = await fetch_one(
        "SELECT local_authority_score FROM competitor_capabilities "
        "WHERE workspace_id = ? AND competitor_domain = ?",
        (ws_id, domain),
    )
    if cap and cap.get("local_authority_score") is not None:
        return float(cap["local_authority_score"])
    aio = await _scalar(
        "SELECT COUNT(*) AS n FROM aio_tracking WHERE workspace_id = ? "
        "AND visible_brands LIKE ?",
        (ws_id, f"%{domain}%"),
    )
    return min(100.0, float(aio) * 6.0)


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

async def _scalar(sql: str, args: Tuple) -> int:
    row = await fetch_one(sql, args)
    return int((row or {}).get("n") or 0)


def _why(axis: str) -> str:
    return {
        "citation_score": "Improve raw citation share via Peec MCP sync + content refreshes.",
        "prompt_ownership_score": "Run prompt_tracker on top-revenue prompts and target the lost ones.",
        "schema_score": "Implement Physician + Review + FAQ schema on decision pages.",
        "offsite_score": "Land PR mentions on trusted publisher domains.",
        "reddit_score": "Seed credible Reddit discussions in target subreddits.",
        "entity_score": "Tighten brand-entity consistency: Person, sameAs, NAP across pages.",
        "local_score": "Strengthen LocalBusiness schema + Google Business signals.",
    }.get(axis, "")
