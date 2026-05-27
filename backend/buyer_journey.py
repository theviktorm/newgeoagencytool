"""
Momentus AI — Buyer Journey Coverage Map

Audits content + prompts across 7 decision stages. Tells the agency
exactly where the conversion gap is:

  awareness → problem → solution → comparison → trust → objection → decision

Outputs:
  - per-stage coverage_score with gap_severity and a one-line recommendation
  - per-page stage classification (lazy: heuristic first, Claude on demand)
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional

from .claude_engine import ClaudeEngine
from .database import (
    execute, fetch_all, fetch_one, gen_id, to_json,
)

logger = logging.getLogger("geo.buyer_journey")

STAGES = ("awareness", "problem", "solution", "comparison",
          "trust", "objection", "decision")

# ── Canonical 6-stage buyer-journey model (Phase 2 gap map) ──
# The product-facing model the dashboard renders. The 7 internal stages above
# are folded into these 6 so the gap map reads cleanly to an agency.
CANONICAL_STAGES = (
    "awareness", "consideration", "comparison", "trust", "decision", "purchase",
)

# Folding of the 7 internal stages -> 6 canonical stages.
#   awareness   -> awareness
#   problem     -> consideration   (recognising the problem = early consideration)
#   solution    -> consideration   (exploring solutions = consideration)
#   comparison  -> comparison
#   trust       -> trust
#   objection   -> trust           (handling objections is a trust activity)
#   decision    -> decision
# `purchase` has no internal equivalent — it is detected from prompt text that
# implies booking / price / consultation (see _is_purchase_prompt).
INTERNAL_TO_CANONICAL = {
    "awareness": "awareness",
    "problem": "consideration",
    "solution": "consideration",
    "comparison": "comparison",
    "trust": "trust",
    "objection": "trust",
    "decision": "decision",
}

# Prompt-text signals that a prompt is really purchase-stage (booking / price /
# consultation), which overrides the folded stage.
_PURCHASE_PATTERN = re.compile(
    r"\b(book|booking|appointment|consultation|consult|price|pricing|cost|"
    r"how much|quote|buy|order|deposit|deposit|reserve|schedule|sign up|"
    r"get a quote|free quote|fees?)\b",
    re.IGNORECASE,
)

# Benchmark distribution: a healthy GEO content stack should look roughly
# like this. Big deviations flag gaps.
BENCHMARK_RATIO = {
    "awareness": 0.18,
    "problem": 0.10,
    "solution": 0.14,
    "comparison": 0.18,
    "trust": 0.16,
    "objection": 0.10,
    "decision": 0.14,
}

# Title/URL heuristics for fast offline classification. Real strategic value
# comes from Claude — but heuristic fills the table the moment data lands.
_STAGE_RULES = [
    (r"\bbest\b|\btop\b|\b#1\b|\bsurgeon\b.*\bnear\b", "decision"),
    (r"\bvs\b|\bversus\b|\bcompare\b|\bcomparison\b|\bor\b\s+\w+", "comparison"),
    (r"\breview\b|\brated\b|\bratings?\b|\btestimonial\b", "trust"),
    (r"\brecovery\b|\bside effect\b|\brisk\b|\bcomplication\b|\bworth it\b|\bsafe\b", "objection"),
    (r"\bhow to choose\b|\bwhich one\b|\bshould I\b|\bguide\b", "decision"),
    (r"\btreatment\b|\boption\b|\balternative\b|\bsolution\b", "solution"),
    (r"\bsymptom\b|\bcause\b|\bsigns of\b|\bdiagnos\w*\b", "problem"),
    (r"\bwhat is\b|\bdefinition\b|\bmeaning\b|\bguide to\b", "awareness"),
]


# ═══════════════════════════════════════════════════════════════
# CLASSIFICATION
# ═══════════════════════════════════════════════════════════════

def classify_text_heuristic(text: str) -> str:
    if not text:
        return "awareness"
    t = text.lower()
    for pattern, stage in _STAGE_RULES:
        if re.search(pattern, t):
            return stage
    return "awareness"


async def classify_workspace(workspace_id: str, force: bool = False) -> Dict[str, Any]:
    """Classify every source URL into a stage. Heuristic-first, idempotent."""
    pages = await fetch_all(
        "SELECT url, title, topic, topics FROM sources WHERE project_id = ?",
        (workspace_id,),
    )
    n = 0
    for p in pages:
        text = " ".join(filter(None, [p.get("title", ""), p.get("topic", ""),
                                       p.get("url", "")]))
        stage = classify_text_heuristic(text)
        await execute(
            "INSERT OR REPLACE INTO page_stages "
            "(workspace_id, url, stage, confidence, classified_at) "
            "VALUES (?, ?, ?, ?, datetime('now'))",
            (workspace_id, p["url"], stage, 0.6),
        )
        n += 1
    return {"classified": n}


# ═══════════════════════════════════════════════════════════════
# COVERAGE
# ═══════════════════════════════════════════════════════════════

async def legacy_coverage_map(workspace_id: str, refresh: bool = True) -> Dict[str, Any]:
    """Compute per-stage coverage over the 7 internal stages. Persists into
    journey_coverage and returns the map for the legacy /api/journey dashboard.

    Kept under its own name so the new canonical 6-stage `coverage_map`
    (Phase 2) can own that name for the gap-map endpoint."""
    if refresh:
        await classify_workspace(workspace_id)

    page_counts = await fetch_all(
        "SELECT stage, COUNT(*) AS n FROM page_stages WHERE workspace_id = ? "
        "GROUP BY stage", (workspace_id,),
    )
    pages_by_stage = {r["stage"]: int(r["n"]) for r in page_counts}

    prompt_counts = await fetch_all(
        "SELECT buyer_stage AS stage, COUNT(*) AS n FROM prompts WHERE workspace_id = ? "
        "GROUP BY buyer_stage", (workspace_id,),
    )
    prompts_by_stage = {r["stage"]: int(r["n"]) for r in prompt_counts}

    # Citations per stage: join sources → page_stages on URL
    cite_rows = await fetch_all(
        """SELECT ps.stage AS stage,
                  SUM(s.total_citation_count) AS cites
           FROM page_stages ps
           JOIN sources s ON s.url = ps.url AND s.project_id = ps.workspace_id
           WHERE ps.workspace_id = ?
           GROUP BY ps.stage""",
        (workspace_id,),
    )
    cites_by_stage = {r["stage"]: int(r["cites"] or 0) for r in cite_rows}

    # Combine pages + prompts so journey coverage works on CSV-only
    # workspaces (no scraped pages yet).
    asset_by_stage = {
        s: pages_by_stage.get(s, 0) + prompts_by_stage.get(s, 0)
        for s in STAGES
    }
    total_assets = sum(asset_by_stage.values()) or 1
    out: Dict[str, Any] = {}
    for stage in STAGES:
        actual = asset_by_stage.get(stage, 0)
        actual_ratio = actual / total_assets if total_assets else 0.0
        target_ratio = BENCHMARK_RATIO[stage]
        coverage = min(100.0, (actual_ratio / target_ratio) * 100.0) if target_ratio else 100.0
        gap = "none"
        if coverage < 30:
            gap = "critical"
        elif coverage < 60:
            gap = "medium"
        elif coverage < 85:
            gap = "low"
        rec = _stage_recommendation(stage, gap, actual)
        await execute(
            "INSERT OR REPLACE INTO journey_coverage "
            "(workspace_id, stage, page_count, prompt_count, citation_count, "
            " coverage_score, gap_severity, recommendation, analyzed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
            (
                workspace_id, stage, actual,
                prompts_by_stage.get(stage, 0),
                cites_by_stage.get(stage, 0),
                coverage, gap, rec,
            ),
        )
        out[stage] = {
            "page_count": actual,
            "prompt_count": prompts_by_stage.get(stage, 0),
            "citation_count": cites_by_stage.get(stage, 0),
            "coverage_score": coverage,
            "gap_severity": gap,
            "recommendation": rec,
        }
    return {"workspace_id": workspace_id, "stages": out, "total_pages": total_assets}


def _stage_recommendation(stage: str, gap: str, count: int) -> str:
    if gap == "none":
        return f"{stage} coverage healthy ({count} pages). Maintain entity consistency."
    if stage == "comparison":
        return "Build provider/method comparison pages — high decision impact."
    if stage == "trust":
        return "Add Physician + Review schema and surgeon trust pages."
    if stage == "decision":
        return "Publish 'Best X in {city}' decision pages — highest revenue lift."
    if stage == "objection":
        return "Address recovery, risk, and 'is it worth it' content + Review schema."
    if stage == "solution":
        return "Cover treatment options, alternatives, and solution-exploration FAQs."
    if stage == "problem":
        return "Strengthen problem-recognition content (symptoms, signs, causes)."
    return "Add awareness-stage explainers + internal links to deeper pages."


# ═══════════════════════════════════════════════════════════════
# CANONICAL 6-STAGE GAP MAP (Phase 2)
# ═══════════════════════════════════════════════════════════════

def _is_purchase_prompt(text: str) -> bool:
    """True when prompt text implies booking / price / consultation."""
    return bool(text and _PURCHASE_PATTERN.search(text))


def canonical_stage_for_prompt(prompt_row: Dict[str, Any]) -> str:
    """Fold a prompt's internal buyer_stage into a canonical stage, promoting
    to 'purchase' when the prompt text signals booking/price/consultation."""
    text = (prompt_row.get("text") or "")
    if _is_purchase_prompt(text):
        return "purchase"
    internal = (prompt_row.get("buyer_stage") or "awareness").lower()
    return INTERNAL_TO_CANONICAL.get(internal, "awareness")


def _prompt_revenue_estimate(prompt_row: Dict[str, Any]) -> float:
    """Best-effort EUR estimate for a prompt. Mirrors the heuristic used across
    the engine: explicit estimated_value_eur wins, else revenue_score × 1000."""
    val = float(prompt_row.get("estimated_value_eur") or 0)
    if val > 0:
        return val
    return float(prompt_row.get("revenue_score") or 0) * 1000.0


async def coverage_map(workspace_id: str) -> Dict[str, Any]:
    """Canonical 6-stage buyer-journey gap map.

    For each of awareness / consideration / comparison / trust / decision /
    purchase return:
      - prompt_count        : prompts folded into this stage
      - owned_count         : prompts we own (our_score >= leader_score and >= 50)
      - lost_count          : prompts a competitor leads
      - existing_pages      : best-effort count of pages/sources mapped to the
                              stage (via page_stages -> canonical fold), else 0
      - existing_pages_confidence : 'estimated' (heuristic page classification)
      - missing_pages       : bool — stage has prompts but no mapped page
      - revenue_at_stake    : sum of per-prompt EUR estimates in the stage
      - priority            : revenue_at_stake × lost_ratio (lost/prompt_count)

    Defensive: a fresh workspace returns shaped zeros, never raises.
    """
    prompts = await fetch_all(
        "SELECT * FROM prompts WHERE workspace_id = ?", (workspace_id,)
    )
    ownership = {
        r["prompt_id"]: r
        for r in await fetch_all(
            "SELECT * FROM prompt_ownership WHERE workspace_id = ?", (workspace_id,)
        )
    }

    # Pages per canonical stage (best-effort from page_stages classification).
    page_rows = await fetch_all(
        "SELECT stage, COUNT(*) AS n FROM page_stages WHERE workspace_id = ? "
        "GROUP BY stage",
        (workspace_id,),
    )
    canonical_pages: Dict[str, int] = {s: 0 for s in CANONICAL_STAGES}
    for r in page_rows:
        internal = (r.get("stage") or "").lower()
        canon = INTERNAL_TO_CANONICAL.get(internal)
        if canon:
            canonical_pages[canon] += int(r.get("n") or 0)

    agg: Dict[str, Dict[str, float]] = {
        s: {"prompt_count": 0, "owned_count": 0, "lost_count": 0,
            "revenue_at_stake": 0.0}
        for s in CANONICAL_STAGES
    }

    for p in prompts:
        stage = canonical_stage_for_prompt(p)
        if stage not in agg:
            stage = "awareness"
        bucket = agg[stage]
        bucket["prompt_count"] += 1
        bucket["revenue_at_stake"] += _prompt_revenue_estimate(p)
        own = ownership.get(p["id"]) or {}
        our = float(own.get("our_score") or 0)
        leader = float(own.get("leader_score") or 0)
        if our >= 50 and our >= leader and our > 0:
            bucket["owned_count"] += 1
        elif leader > our and leader > 0:
            bucket["lost_count"] += 1

    stages_out: List[Dict[str, Any]] = []
    for stage in CANONICAL_STAGES:
        b = agg[stage]
        pc = int(b["prompt_count"])
        lost = int(b["lost_count"])
        existing_pages = int(canonical_pages.get(stage, 0))
        lost_ratio = (lost / pc) if pc else 0.0
        revenue = round(b["revenue_at_stake"], 2)
        stages_out.append({
            "stage": stage,
            "prompt_count": pc,
            "owned_count": int(b["owned_count"]),
            "lost_count": lost,
            "existing_pages": existing_pages,
            "existing_pages_confidence": "estimated",
            "missing_pages": bool(pc > 0 and existing_pages == 0),
            "revenue_at_stake": revenue,
            "priority": round(revenue * lost_ratio, 2),
            "confidence": "estimated",
        })

    return {
        "workspace_id": workspace_id,
        "model": "canonical_6_stage",
        "stages": stages_out,
        "total_prompts": len(prompts),
        "confidence": "estimated",
    }


async def coverage_insight(workspace_id: str) -> Dict[str, Any]:
    """Auto-generated plain-English summary of where coverage is over/under
    indexed vs an even baseline across the 6 canonical stages.

    Defensive: empty workspace returns a shaped, non-raising result.
    """
    cmap = await coverage_map(workspace_id)
    stages = cmap.get("stages") or []
    total_prompts = sum(int(s.get("prompt_count") or 0) for s in stages)

    if not total_prompts:
        return {
            "workspace_id": workspace_id,
            "summary": "No prompts tracked yet — import or add prompts to map "
                       "buyer-journey coverage.",
            "over_covered": [],
            "under_covered": [],
            "confidence": "estimated",
        }

    n_stages = len(CANONICAL_STAGES)
    baseline = 1.0 / n_stages  # even share per stage
    over: List[str] = []
    under: List[str] = []
    shares: Dict[str, float] = {}
    for s in stages:
        stage = s["stage"]
        share = (int(s.get("prompt_count") or 0)) / total_prompts
        shares[stage] = share
        # > 40% above even => over-covered; < 50% of even => under-covered.
        if share >= baseline * 1.4:
            over.append(stage)
        elif share <= baseline * 0.5:
            under.append(stage)

    # Where is the lost revenue concentrated? (decision/comparison/trust matter).
    losses = {s["stage"]: int(s.get("lost_count") or 0) for s in stages}
    worst_late = sorted(
        [st for st in ("comparison", "trust", "decision", "purchase")],
        key=lambda st: -losses.get(st, 0),
    )
    late_lost = [st for st in worst_late if losses.get(st, 0) > 0]

    parts: List[str] = []
    if over:
        parts.append("Over-indexed on " + _join(over))
    if under:
        parts.append("under-covered in " + _join(under))
    summary = ", ".join(parts) if parts else "Coverage is fairly even across stages"
    if late_lost:
        summary += (
            " — AI educates with your content but recommends competitors at "
            + _join(late_lost) + " time."
        )
    else:
        summary += "."

    return {
        "workspace_id": workspace_id,
        "summary": summary,
        "over_covered": over,
        "under_covered": under,
        "shares": {k: round(v, 3) for k, v in shares.items()},
        "confidence": "estimated",
    }


def _join(items: List[str]) -> str:
    items = [str(i) for i in items if i]
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"


# ═══════════════════════════════════════════════════════════════
# DEEP CLASSIFY (Claude, on demand)
# ═══════════════════════════════════════════════════════════════

async def deep_classify_pages(
    workspace_id: str, urls: List[str],
) -> Dict[str, Any]:
    """Use Claude to classify ambiguous pages into stages, with confidence."""
    if not urls:
        return {"classified": 0}
    rows = await fetch_all(
        "SELECT url, title, topic, scraped_word_count "
        "FROM sources WHERE project_id = ? AND url IN (" + ",".join("?" * len(urls)) + ")",
        (workspace_id, *urls),
    )
    engine = ClaudeEngine()
    n = 0
    for r in rows:
        sys = (
            "You classify a single web page into one of these GEO buyer-journey stages:\n"
            f"{list(STAGES)}\n"
            "Return STRICT JSON: "
            '{"stage":"...","confidence":0.0,"why":"one sentence"}'
        )
        try:
            resp = await engine.call(
                messages=[{
                    "role": "user",
                    "content": f"URL: {r['url']}\nTitle: {r.get('title','')}\nTopic: {r.get('topic','')}",
                }],
                system=sys, max_tokens=200, temperature=0.0,
                operation="journey_classify", reference_id=r["url"],
            )
        except Exception:
            continue
        parsed = _safe_json(resp.get("content", ""))
        if not parsed:
            continue
        stage = parsed.get("stage")
        if stage not in STAGES:
            continue
        await execute(
            "INSERT OR REPLACE INTO page_stages "
            "(workspace_id, url, stage, confidence, classified_at) "
            "VALUES (?, ?, ?, ?, datetime('now'))",
            (workspace_id, r["url"], stage, float(parsed.get("confidence") or 0)),
        )
        n += 1
    return {"classified": n}


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

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
