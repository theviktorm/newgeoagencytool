"""
Momentus AI — Competitor Analysis Profile (Phase 3)

Per-competitor intelligence profile built from existing GEO data:
  prompt_observations, prompt_ownership, competitor_capabilities,
  mention_events. Plus a `tracked` flag on competitor_capabilities so the
  agency can pin a set of competitors to monitor.

Pure SQLite + stdlib. No new dependencies, no network calls. Tolerates
empty/missing tables — every read is defensively wrapped.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .database import execute, fetch_all, fetch_one, from_json

logger = logging.getLogger("geo.competitor_profile")


def _norm_domain(d: str) -> str:
    return (d or "").strip().lower().lstrip(".").lstrip("www.")


def _src_domain(url: str) -> str:
    try:
        return (urlparse(url or "").hostname or "").lower().lstrip("www.")
    except Exception:
        return ""


async def _safe_fetch_all(sql: str, params: tuple) -> List[Dict[str, Any]]:
    try:
        return await fetch_all(sql, params)
    except Exception as e:
        logger.warning("safe_fetch_all failed: %s", e)
        return []


async def _safe_fetch_one(sql: str, params: tuple) -> Optional[Dict[str, Any]]:
    try:
        return await fetch_one(sql, params)
    except Exception as e:
        logger.warning("safe_fetch_one failed: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════
# competitor_profile — full intelligence dossier for ONE competitor
# ═══════════════════════════════════════════════════════════════

async def competitor_profile(workspace_id: str, domain: str) -> Dict[str, Any]:
    """Build the full intelligence profile for a competitor domain."""
    domain = _norm_domain(domain)
    if not domain:
        raise ValueError("domain required")

    # 1. Ownership-derived stats: prompts_won / high_value / est_revenue /
    #    strongest_stage / strongest_topic.
    ownership_rows = await _safe_fetch_all(
        "SELECT prompt_id, leader_domain, leader_score, our_score "
        "FROM prompt_ownership WHERE workspace_id = ?",
        (workspace_id,),
    )
    prompts_by_id: Dict[str, Dict[str, Any]] = {}
    if ownership_rows:
        ids = [r.get("prompt_id") for r in ownership_rows if r.get("prompt_id")]
        if ids:
            placeholders = ",".join("?" * len(ids))
            for p in await _safe_fetch_all(
                f"SELECT id, text, buyer_stage, prompt_type, revenue_score, "
                f"estimated_value_eur, cluster_id, classifier_meta "
                f"FROM prompts WHERE id IN ({placeholders})",
                tuple(ids),
            ):
                prompts_by_id[p["id"]] = p

    prompts_won = 0
    high_value_prompts_won = 0
    est_revenue_captured = 0.0
    stage_count: Dict[str, int] = {}
    topic_count: Dict[str, int] = {}
    for own in ownership_rows:
        leader = _norm_domain(own.get("leader_domain") or "")
        if leader != domain:
            continue
        our = float(own.get("our_score") or 0)
        leader_score = float(own.get("leader_score") or 0)
        if leader_score <= our:
            continue
        p = prompts_by_id.get(own.get("prompt_id"))
        if not p:
            continue
        prompts_won += 1
        if float(p.get("revenue_score") or 0) >= 60:
            high_value_prompts_won += 1
        rev = float(p.get("estimated_value_eur") or 0) or \
              float(p.get("revenue_score") or 0) * 1000.0
        est_revenue_captured += rev
        stage = (p.get("buyer_stage") or "awareness")
        stage_count[stage] = stage_count.get(stage, 0) + 1
        meta = from_json(p.get("classifier_meta") or "{}") or {}
        topic = meta.get("topic") or (p.get("cluster_id") or "")
        if topic:
            topic_count[topic] = topic_count.get(topic, 0) + 1

    strongest_stage = max(stage_count.items(), key=lambda kv: kv[1])[0] if stage_count else ""
    strongest_topic = max(topic_count.items(), key=lambda kv: kv[1])[0] if topic_count else ""

    # 2. Capability row — provides weakest_factor + suggested_attack.
    cap = await _safe_fetch_one(
        "SELECT * FROM competitor_capabilities WHERE workspace_id = ? AND competitor_domain = ?",
        (workspace_id, domain),
    )
    weakest_factor = ""
    suggested_attack = ""
    confidence = "estimated"
    tracked = 0
    if cap:
        weakest_factor = cap.get("weakest_axis") or ""
        tracked = int(cap.get("tracked") or 0)
        # Reuse attack_map enrichment (it returns recommended_attack).
        try:
            from . import attack_map
            enrich = await attack_map._enrich_capability_row(workspace_id, cap)
            suggested_attack = enrich.get("recommended_attack") or ""
            confidence = enrich.get("confidence") or confidence
        except Exception as e:
            logger.warning("competitor_profile enrich failed: %s", e)
    elif prompts_won:
        confidence = "observation_only"
    else:
        confidence = "seeded"

    # 3. Citation sources: top source-type breakdown for their wins.
    citation_sources = await _competitor_citation_sources(workspace_id, domain)

    # 4. Trends: prompt-flips toward this domain in last 30 days.
    trend_row = await _safe_fetch_one(
        "SELECT COUNT(*) AS n FROM prompt_observations WHERE workspace_id = ? "
        "AND observed_at >= datetime('now','-30 days') "
        "AND (brands_appeared LIKE ? OR sources_cited LIKE ?)",
        (workspace_id, f"%{domain}%", f"%{domain}%"),
    )
    recent_30d = int((trend_row or {}).get("n", 0) or 0)

    # 5. mention_events: last_seen + sentiment, if Tier-2 MCP data is present.
    me_row = await _safe_fetch_one(
        "SELECT COUNT(*) AS n, MAX(observed_at) AS last_seen, "
        " AVG(sentiment_score) AS avg_sent "
        "FROM mention_events WHERE project_id = ? AND url LIKE ?",
        (workspace_id, f"%{domain}%"),
    )
    mention_events_count = int((me_row or {}).get("n", 0) or 0)
    last_mention_at = (me_row or {}).get("last_seen") or ""

    return {
        "workspace_id": workspace_id,
        "domain": domain,
        "tracked": bool(tracked),
        "prompts_won": prompts_won,
        "high_value_prompts_won": high_value_prompts_won,
        "est_revenue_captured": round(est_revenue_captured, 2),
        "strongest_stage": strongest_stage,
        "strongest_topic": strongest_topic,
        "weakest_factor": weakest_factor,
        "citation_sources": citation_sources,
        "trends": {
            "recent_30d_movement": recent_30d,
            "mention_events_30d_or_total": mention_events_count,
            "last_mention_at": last_mention_at,
        },
        "suggested_attack": suggested_attack,
        "confidence": confidence,
        "capability_row": cap or {},
    }


async def _competitor_citation_sources(
    workspace_id: str, domain: str,
) -> Dict[str, Any]:
    """Breakdown of where this competitor gets cited from (top hosts +
    rough source-type buckets: own_domain / reddit / youtube / publisher)."""
    out_buckets = {"own_domain": 0, "reddit": 0, "youtube": 0, "publisher": 0}
    host_count: Dict[str, int] = {}

    obs = await _safe_fetch_all(
        "SELECT sources_cited FROM prompt_observations WHERE workspace_id = ? "
        "AND sources_cited LIKE ? LIMIT 1500",
        (workspace_id, f"%{domain}%"),
    )
    for o in obs:
        srcs = from_json(o.get("sources_cited") or "[]", []) or []
        for s in srcs:
            url = s.get("url") if isinstance(s, dict) else ""
            host = _src_domain(url) or ""
            if not host:
                continue
            host_count[host] = host_count.get(host, 0) + 1
            if domain in host:
                out_buckets["own_domain"] += 1
            elif "reddit.com" in host:
                out_buckets["reddit"] += 1
            elif "youtube.com" in host or "youtu.be" in host:
                out_buckets["youtube"] += 1
            else:
                out_buckets["publisher"] += 1
    top_hosts = sorted(host_count.items(), key=lambda kv: -kv[1])[:10]
    return {
        "buckets": out_buckets,
        "top_hosts": [{"host": h, "count": c} for h, c in top_hosts],
    }


# ═══════════════════════════════════════════════════════════════
# tracked competitors
# ═══════════════════════════════════════════════════════════════

async def track_competitor(workspace_id: str, domain: str) -> Dict[str, Any]:
    """Mark a competitor as tracked. Idempotent. Creates a stub capability
    row if none exists, so the tracked flag can persist."""
    domain = _norm_domain(domain)
    if not domain:
        raise ValueError("domain required")
    existing = await _safe_fetch_one(
        "SELECT 1 FROM competitor_capabilities WHERE workspace_id = ? AND competitor_domain = ?",
        (workspace_id, domain),
    )
    if not existing:
        # Seed a stub row so the FK + tracked flag persist.
        try:
            await execute(
                "INSERT INTO competitor_capabilities (workspace_id, competitor_domain, tracked) "
                "VALUES (?, ?, 1)",
                (workspace_id, domain),
            )
        except Exception as e:
            logger.warning("track_competitor seed failed: %s", e)
    else:
        try:
            await execute(
                "UPDATE competitor_capabilities SET tracked = 1 "
                "WHERE workspace_id = ? AND competitor_domain = ?",
                (workspace_id, domain),
            )
        except Exception as e:
            logger.warning("track_competitor update failed: %s", e)
    return {"workspace_id": workspace_id, "domain": domain, "tracked": True}


async def untrack_competitor(workspace_id: str, domain: str) -> Dict[str, Any]:
    """Unset the tracked flag. No row deletion."""
    domain = _norm_domain(domain)
    if not domain:
        raise ValueError("domain required")
    try:
        await execute(
            "UPDATE competitor_capabilities SET tracked = 0 "
            "WHERE workspace_id = ? AND competitor_domain = ?",
            (workspace_id, domain),
        )
    except Exception as e:
        logger.warning("untrack_competitor failed: %s", e)
    return {"workspace_id": workspace_id, "domain": domain, "tracked": False}


async def list_tracked_competitors(workspace_id: str) -> List[Dict[str, Any]]:
    """Return the profile for every tracked competitor."""
    rows = await _safe_fetch_all(
        "SELECT competitor_domain FROM competitor_capabilities "
        "WHERE workspace_id = ? AND tracked = 1 ORDER BY overall_strength DESC",
        (workspace_id,),
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            out.append(await competitor_profile(workspace_id, r["competitor_domain"]))
        except Exception as e:
            logger.warning("list_tracked_competitors %s failed: %s",
                           r.get("competitor_domain", "?"), e)
    return out


__all__ = [
    "competitor_profile", "track_competitor", "untrack_competitor",
    "list_tracked_competitors",
]
