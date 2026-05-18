"""
Momentus AI — Google AI Overview Opportunity Engine

A purpose-built tracker for Google AI Overview separate from generic
prompt_observations. Surfaces:

  - which prompts trigger AIO right now
  - which brands are visible (and which are missing)
  - which publisher domains dominate the source set
  - what changed vs the previous snapshot ("AIO appeared", "competitor entered")
  - a triage queue of "we are losing here, fix this"

Most heavy lifting (live SERP fetch) is done by prompt_tracker._query_google_aio
which writes into aio_tracking. This module aggregates + diffs.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from .database import (
    execute, fetch_all, fetch_one, gen_id, to_json, from_json,
)
from .automation import create_recommendation, create_notification

logger = logging.getLogger("geo.aio")


# ═══════════════════════════════════════════════════════════════
# AGGREGATION
# ═══════════════════════════════════════════════════════════════

async def overview(workspace_id: str) -> Dict[str, Any]:
    """Top-level: how many of our tracked prompts have AIO; how many include
    our brand; which publishers dominate; which prompts to fix."""
    rows = await fetch_all(
        "SELECT * FROM aio_tracking WHERE workspace_id = ? "
        "AND observed_at >= datetime('now', '-30 days') "
        "ORDER BY observed_at DESC",
        (workspace_id,),
    )
    # Latest per prompt_id
    latest: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        if r["prompt_id"] not in latest:
            latest[r["prompt_id"]] = r

    aio_count = sum(1 for r in latest.values() if r.get("aio_present"))
    we_in = sum(1 for r in latest.values() if r.get("our_brand_in_aio"))
    publisher_count: Dict[str, int] = {}
    visible_brands: Dict[str, int] = {}
    for r in latest.values():
        for d in (from_json(r.get("publisher_domains") or "[]") or []):
            if not d:
                continue
            publisher_count[d] = publisher_count.get(d, 0) + 1
        for b in (from_json(r.get("visible_brands") or "[]") or []):
            if not b:
                continue
            visible_brands[b] = visible_brands.get(b, 0) + 1
    top_publishers = sorted(publisher_count.items(), key=lambda kv: -kv[1])[:10]
    top_brands = sorted(visible_brands.items(), key=lambda kv: -kv[1])[:10]

    # Detect tracker readiness so the UI can show a clear setup hint
    # instead of an empty page when no SerpAPI key is configured.
    import os
    try:
        from .config import settings as _s
        has_serpapi = bool(os.environ.get("SERPAPI_KEY")
                           or getattr(_s, "serpapi_key", ""))
    except Exception:
        has_serpapi = bool(os.environ.get("SERPAPI_KEY"))

    return {
        "workspace_id": workspace_id,
        "tracked_prompts": len(latest),
        "with_aio": aio_count,
        "with_our_brand": we_in,
        "without_us": aio_count - we_in,
        "top_publishers": [{"domain": d, "appearances": n} for d, n in top_publishers],
        "top_brands": [{"brand": b, "appearances": n} for b, n in top_brands],
        "tracker_configured": has_serpapi,
        "tracker_hint": (
            "Set SERPAPI_KEY in Railway → Variables to start live AIO tracking. "
            "Without it, AIO observations only come from manual prompt tracks."
        ) if not has_serpapi else "",
    }


async def list_losses(workspace_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Prompts where AIO is present but our brand is missing."""
    rows = await fetch_all(
        """SELECT p.text AS prompt_text, p.revenue_score, p.buyer_stage,
                  a.*
           FROM aio_tracking a
           JOIN prompts p ON p.id = a.prompt_id
           WHERE a.workspace_id = ? AND a.aio_present = 1
             AND a.our_brand_in_aio = 0
           ORDER BY a.observed_at DESC LIMIT ?""",
        (workspace_id, limit),
    )
    return rows


async def detect_movements(workspace_id: str) -> Dict[str, Any]:
    """Compare today's snapshot to the prior one; emit alerts when AIO
    appeared, competitor entered, or our brand dropped out."""
    # Group by prompt; compare latest two snapshots.
    rows = await fetch_all(
        "SELECT * FROM aio_tracking WHERE workspace_id = ? "
        "ORDER BY prompt_id, observed_at DESC",
        (workspace_id,),
    )
    by_prompt: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_prompt.setdefault(r["prompt_id"], []).append(r)

    movements: List[Dict[str, Any]] = []
    for pid, snaps in by_prompt.items():
        if len(snaps) < 2:
            continue
        cur, prev = snaps[0], snaps[1]
        cur_brands = set(from_json(cur.get("visible_brands") or "[]") or [])
        prev_brands = set(from_json(prev.get("visible_brands") or "[]") or [])
        msg = ""
        if cur.get("aio_present") and not prev.get("aio_present"):
            msg = "Google AI Overview appeared for this prompt"
        elif prev.get("aio_present") and not cur.get("aio_present"):
            msg = "AI Overview disappeared for this prompt"
        elif cur_brands - prev_brands:
            new = ", ".join(sorted(cur_brands - prev_brands))[:200]
            msg = f"New brand(s) entered the AIO: {new}"
        elif prev.get("our_brand_in_aio") and not cur.get("our_brand_in_aio"):
            msg = "Our brand DROPPED OUT of the AIO"
        elif cur.get("our_brand_in_aio") and not prev.get("our_brand_in_aio"):
            msg = "We just entered the AIO"
        if msg:
            await execute(
                "UPDATE aio_tracking SET delta_vs_prev = ? WHERE id = ?",
                (msg, cur["id"]),
            )
            movements.append({"prompt_id": pid, "delta": msg, "observed_at": cur["observed_at"]})
            # Push as recommendation only when bad news
            if "DROPPED" in msg or "entered the AIO:" in msg:
                p = await fetch_one("SELECT text FROM prompts WHERE id = ?", (pid,))
                if p:
                    try:
                        await create_recommendation(
                            workspace_id=workspace_id,
                            rec_type="competitor_alert",
                            title=f"AIO movement: '{p['text'][:60]}'",
                            description=msg,
                            priority_score=75.0,
                            cluster_id="",
                            source_data={"prompt_id": pid, "movement": msg},
                        )
                    except Exception as e:
                        logger.debug("AIO rec push failed: %s", e)
    return {"movements": len(movements), "details": movements[:50]}
