"""
Momentus AI — Revenue Priority Layer

Forces every recommendation, dashboard tile, and cluster ranking to lead
with EUR-equivalent business value, not citation counts. Wraps the prompt
engine's revenue_score + estimated_value_eur into rolled-up views:

  - revenue_summary(workspace_id) — top-line "$ at stake / $ won / $ lost"
  - revenue_priority_recs(workspace_id) — actionable "do X next" list,
    sorted by impact, that the dashboard surfaces above generic recs
  - score_cluster(cluster_id) — average revenue value of all prompts in a
    cluster, used to re-prioritize content production
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .database import execute, fetch_all, fetch_one, gen_id, to_json
from .automation import create_recommendation

logger = logging.getLogger("geo.revenue_priority")

# Default conversion-value heuristic per buyer_stage (EUR per converted lead).
# Workspaces can override via workspace meta later.
DEFAULT_VALUE_BY_STAGE = {
    "awareness": 50,
    "problem": 150,
    "solution": 400,
    "comparison": 1200,
    "trust": 2500,
    "objection": 2000,
    "decision": 4500,
}


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def revenue_summary(workspace_id: str) -> Dict[str, Any]:
    """High-level money view for the dashboard hero."""
    prompts = await fetch_all(
        "SELECT * FROM prompts WHERE workspace_id = ?", (workspace_id,)
    )
    own = {
        r["prompt_id"]: r
        for r in await fetch_all(
            "SELECT * FROM prompt_ownership WHERE workspace_id = ?", (workspace_id,)
        )
    }
    won_eur, lost_eur, at_stake_eur = 0.0, 0.0, 0.0
    won_count, lost_count = 0, 0
    by_stage: Dict[str, Dict[str, float]] = {}

    for p in prompts:
        eur = _value_eur(p)
        at_stake_eur += eur
        stage = p.get("buyer_stage") or "awareness"
        by_stage.setdefault(stage, {"won_eur": 0, "lost_eur": 0, "count": 0})
        by_stage[stage]["count"] += 1
        o = own.get(p["id"])
        if o and (o.get("our_score") or 0) >= 70:
            won_eur += eur
            won_count += 1
            by_stage[stage]["won_eur"] += eur
        elif o and (o.get("leader_score") or 0) > (o.get("our_score") or 0):
            lost_eur += eur
            lost_count += 1
            by_stage[stage]["lost_eur"] += eur

    return {
        "workspace_id": workspace_id,
        "total_prompts": len(prompts),
        "estimated_pipeline_eur": at_stake_eur,
        "won_eur": won_eur,
        "lost_eur": lost_eur,
        "won_count": won_count,
        "lost_count": lost_count,
        "by_stage": by_stage,
    }


async def revenue_priority_recs(
    workspace_id: str, top_n: int = 10,
) -> List[Dict[str, Any]]:
    """The 'do this next' list. Sorts open prompts by:

       priority = revenue_score × (1 - our_share) × stage_multiplier

    so high-revenue, lost prompts at decision/trust stage rise to the top.
    """
    rows = await fetch_all(
        """SELECT p.*,
                  o.our_score, o.leader_score, o.leader_domain
           FROM prompts p
           LEFT JOIN prompt_ownership o
             ON o.workspace_id = p.workspace_id AND o.prompt_id = p.id
           WHERE p.workspace_id = ? AND p.status != 'ignored'
        """,
        (workspace_id,),
    )
    scored = []
    for p in rows:
        rev = float(p.get("revenue_score") or 0)
        our = float(p.get("our_score") or 0)
        stage = p.get("buyer_stage") or "awareness"
        stage_mul = {
            "decision": 1.4, "trust": 1.3, "objection": 1.2,
            "comparison": 1.1, "solution": 1.0, "problem": 0.85,
            "awareness": 0.7,
        }.get(stage, 1.0)
        priority = rev * (1 - our / 100.0) * stage_mul
        scored.append({
            "prompt_id": p["id"],
            "text": p["text"],
            "revenue_score": rev,
            "estimated_value_eur": _value_eur(p),
            "buyer_stage": stage,
            "our_score": our,
            "leader_domain": p.get("leader_domain", "") or "",
            "priority": priority,
            "next_action": _next_action(p),
        })
    scored.sort(key=lambda r: -r["priority"])
    return scored[:top_n]


async def push_recommendations(
    workspace_id: str, top_n: int = 5,
) -> Dict[str, Any]:
    """Materialise the top revenue_priority_recs into the recommendations
    table so they show up in the existing /api/ops/recommendations UI."""
    recs = await revenue_priority_recs(workspace_id, top_n=top_n)
    n = 0
    for r in recs:
        title = f"Win '{r['text'][:60]}' (€{r['estimated_value_eur']:,.0f} at stake)"
        desc = (
            f"Buyer stage: {r['buyer_stage']}. "
            f"Current ownership: {r['our_score']:.0f}/100. "
            f"Leader: {r['leader_domain'] or 'none yet'}. "
            f"Next: {r['next_action']}"
        )
        try:
            await create_recommendation(
                workspace_id=workspace_id,
                rec_type="opportunity",
                title=title,
                description=desc,
                priority_score=min(100.0, r["priority"]),
                cluster_id="",
                source_data={"revenue": r, "from": "revenue_priority"},
            )
            n += 1
        except Exception as e:
            logger.warning("push rec failed: %s", e)
    return {"pushed": n, "top_n": top_n}


async def score_cluster(workspace_id: str, cluster_id: str) -> Dict[str, Any]:
    """Average revenue value of all prompts in a cluster — used to
    re-rank cluster cards on the Analysis page."""
    rows = await fetch_all(
        "SELECT revenue_score, buyer_stage, estimated_value_eur "
        "FROM prompts WHERE workspace_id = ? AND cluster_id = ?",
        (workspace_id, cluster_id),
    )
    if not rows:
        return {"cluster_id": cluster_id, "avg_revenue_score": 0,
                "estimated_value_eur": 0, "prompt_count": 0}
    avg_rev = sum(float(r["revenue_score"] or 0) for r in rows) / len(rows)
    total_eur = sum(_value_eur(r) for r in rows)
    return {
        "cluster_id": cluster_id,
        "avg_revenue_score": avg_rev,
        "estimated_value_eur": total_eur,
        "prompt_count": len(rows),
    }


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

def _value_eur(prompt_row: Dict[str, Any]) -> float:
    explicit = float(prompt_row.get("estimated_value_eur") or 0)
    if explicit:
        return explicit
    stage = prompt_row.get("buyer_stage") or "awareness"
    base = DEFAULT_VALUE_BY_STAGE.get(stage, 50)
    rev = float(prompt_row.get("revenue_score") or 0)
    # Scale base by revenue_score, so a 90/100 prompt is worth ~base × 0.9
    return base * (rev / 100.0)


def _next_action(p: Dict[str, Any]) -> str:
    stage = p.get("buyer_stage") or "awareness"
    leader = (p.get("leader_domain") or "").lower()
    if stage in ("decision", "trust"):
        if leader:
            return f"Run citation diagnosis on {leader} → publish a comparison/decision page that out-cites them"
        return "Publish a decision page (FAQ + Physician schema + reviews)"
    if stage == "comparison":
        return "Generate a comparison page; embed FAQ schema; seed Reddit"
    if stage == "objection":
        return "Add an objection-handling section + Review schema"
    if stage == "solution":
        return "Strengthen solution-exploration content + internal authority links"
    return "Add educational content + entity-consistent internal links"
