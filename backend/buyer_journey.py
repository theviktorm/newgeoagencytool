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

async def coverage_map(workspace_id: str, refresh: bool = True) -> Dict[str, Any]:
    """Compute per-stage coverage. Persists into journey_coverage and
    returns the map for the dashboard."""
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

    total_pages = sum(pages_by_stage.values()) or 1
    out: Dict[str, Any] = {}
    for stage in STAGES:
        actual = pages_by_stage.get(stage, 0)
        actual_ratio = actual / total_pages if total_pages else 0.0
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
    return {"workspace_id": workspace_id, "stages": out, "total_pages": total_pages}


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
