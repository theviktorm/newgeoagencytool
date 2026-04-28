"""
Momentus AI — Insights Engine (Tier 2)

Turns raw mention/competitor data into actionable signals:

  • sentiment-drop detector — flags clusters/URLs whose average sentiment
    has degraded since the previous sync window
  • competitor-movement detector — flags competitors that gained/lost
    notable share of voice between snapshots
  • emerging-topics detector — surfaces topics where our citations
    spiked but we have no published draft yet

Outputs flow into:
  • `recommendations` rows (visible in the Recommendations page)
  • `notifications` rows for every workspace admin

Designed to run idempotently and cheaply on every Peec MCP sync, plus
on demand via `POST /api/insights/scan/{ws_id}`.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .automation import create_notification, create_recommendation
from .database import execute, fetch_all, fetch_one, gen_id, from_json, to_json

logger = logging.getLogger("geo.insights")


# ═══════════════════════════════════════════════════════════════
# THRESHOLDS — tuneable, kept as constants for now
# ═══════════════════════════════════════════════════════════════

SENTIMENT_DROP_THRESHOLD = 0.20      # average score drop > 0.2 → alert
SENTIMENT_NEGATIVE_RATIO = 0.30      # >30% negative mentions → alert
COMPETITOR_MOVE_THRESHOLD = 0.05     # share-of-voice change > 5pp → alert
COMPETITOR_MIN_CITATIONS = 3         # ignore noise below this
EMERGING_TOPIC_MIN_CITATIONS = 5
RECENT_WINDOW_HOURS = 168            # 7 days


# ═══════════════════════════════════════════════════════════════
# PUBLIC ENTRY POINT
# ═══════════════════════════════════════════════════════════════

async def scan_workspace_insights(workspace_id: str) -> Dict[str, Any]:
    """Run every detector and write recommendations + notifications.

    Returns a summary describing what was found and surfaced.
    """
    summary = {
        "workspace_id": workspace_id,
        "sentiment_alerts": 0,
        "competitor_alerts": 0,
        "emerging_topics": 0,
        "recommendations_created": 0,
        "notifications_created": 0,
    }

    # Each detector returns a list of "findings" — generic dicts that we
    # then turn into recommendations + notifications uniformly.
    findings: List[Dict[str, Any]] = []
    findings += await _detect_sentiment_drops(workspace_id)
    findings += await _detect_competitor_moves(workspace_id)
    findings += await _detect_emerging_topics(workspace_id)

    summary["sentiment_alerts"] = sum(1 for f in findings if f["kind"] == "sentiment")
    summary["competitor_alerts"] = sum(1 for f in findings if f["kind"] == "competitor")
    summary["emerging_topics"] = sum(1 for f in findings if f["kind"] == "emerging")

    # De-dupe against existing open recs (avoid spam on repeated scans)
    existing = await fetch_all(
        "SELECT title FROM recommendations WHERE workspace_id = ? AND status IN ('new','accepted')",
        (workspace_id,),
    )
    open_titles = {r["title"] for r in existing}

    # Find admin user_ids to notify
    admins = await fetch_all(
        "SELECT user_id FROM workspace_members WHERE workspace_id = ? "
        "AND role IN ('admin','editor')",
        (workspace_id,),
    )
    admin_ids = [a["user_id"] for a in admins]

    for f in findings:
        if f["title"] in open_titles:
            continue
        await create_recommendation(
            workspace_id=workspace_id,
            rec_type=f["rec_type"],
            title=f["title"],
            description=f["description"],
            priority_score=float(f["priority"]),
            cluster_id=f.get("cluster_id", ""),
            source_data=f.get("source_data") or {},
        )
        summary["recommendations_created"] += 1
        for uid in admin_ids:
            await create_notification(
                user_id=uid, workspace_id=workspace_id,
                ntype="alert", title=f["title"], message=f["description"],
                link=f.get("link", ""),
            )
            summary["notifications_created"] += 1

    return summary


# ═══════════════════════════════════════════════════════════════
# DETECTOR 1 — sentiment drops
# ═══════════════════════════════════════════════════════════════

async def _detect_sentiment_drops(workspace_id: str) -> List[Dict[str, Any]]:
    """Compare sentiment for each URL between the recent window and the
    previous window. Flag drops > SENTIMENT_DROP_THRESHOLD or high
    negative ratios."""
    project_id = workspace_id
    now = datetime.utcnow()
    recent_start = (now - timedelta(hours=RECENT_WINDOW_HOURS)).isoformat()
    prior_start = (now - timedelta(hours=RECENT_WINDOW_HOURS * 2)).isoformat()

    # Recent window
    recent = await fetch_all(
        """SELECT url,
                  AVG(sentiment_score) AS avg_score,
                  COUNT(*) AS n,
                  SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS neg
           FROM mention_events
           WHERE project_id = ? AND observed_at >= ?
           GROUP BY url
           HAVING n >= 3""",
        (project_id, recent_start),
    )
    # Prior window (one before the recent one)
    prior = await fetch_all(
        """SELECT url, AVG(sentiment_score) AS avg_score, COUNT(*) AS n
           FROM mention_events
           WHERE project_id = ? AND observed_at >= ? AND observed_at < ?
           GROUP BY url""",
        (project_id, prior_start, recent_start),
    )
    prior_by_url = {r["url"]: r for r in prior}

    findings: List[Dict[str, Any]] = []
    for r in recent:
        url = r["url"]
        cur_score = r["avg_score"] or 0.0
        n = r["n"]
        neg = r["neg"]
        neg_ratio = neg / n if n else 0
        prev = prior_by_url.get(url)
        prev_score = prev["avg_score"] if prev else None

        drop = (prev_score - cur_score) if prev_score is not None else 0
        if (prev_score is not None and drop >= SENTIMENT_DROP_THRESHOLD) or (
            neg_ratio >= SENTIMENT_NEGATIVE_RATIO and cur_score < 0
        ):
            priority = min(100.0, 50 + (drop * 100) + (neg_ratio * 30))
            findings.append({
                "kind": "sentiment",
                "rec_type": "gap",
                "title": f"Sentiment drop on {_short_url(url)}",
                "description": (
                    f"Avg sentiment {cur_score:+.2f} across {n} recent mentions; "
                    f"{neg} negative ({neg_ratio:.0%}). "
                    + (f"Previous window: {prev_score:+.2f}." if prev_score is not None else "")
                    + " Refresh or rebut this page."
                ),
                "priority": priority,
                "source_data": {
                    "url": url, "avg_score": cur_score, "prev_score": prev_score,
                    "negative_ratio": neg_ratio, "mentions": n,
                },
                "link": f"/sources?url={url}",
            })
    return findings


# ═══════════════════════════════════════════════════════════════
# DETECTOR 2 — competitor movement
# ═══════════════════════════════════════════════════════════════

async def _detect_competitor_moves(workspace_id: str) -> List[Dict[str, Any]]:
    """Compare each competitor's most recent two snapshots from
    competitor_history. Flag share-of-voice swings."""
    snapshots = await fetch_all(
        """SELECT domain, share_of_voice, citation_count, avg_position, observed_at
           FROM competitor_history
           WHERE workspace_id = ?
           ORDER BY domain, observed_at DESC""",
        (workspace_id,),
    )
    by_domain: Dict[str, List[Dict[str, Any]]] = {}
    for s in snapshots:
        by_domain.setdefault(s["domain"], []).append(s)

    findings: List[Dict[str, Any]] = []
    for domain, rows in by_domain.items():
        if len(rows) < 2:
            continue
        cur, prev = rows[0], rows[1]
        cur_sov = cur["share_of_voice"] or 0.0
        prev_sov = prev["share_of_voice"] or 0.0
        delta = cur_sov - prev_sov
        if abs(delta) < COMPETITOR_MOVE_THRESHOLD:
            continue
        if (cur["citation_count"] or 0) < COMPETITOR_MIN_CITATIONS:
            continue

        direction = "gained" if delta > 0 else "lost"
        priority = min(100.0, 60 + abs(delta) * 200)
        findings.append({
            "kind": "competitor",
            "rec_type": "competitor_alert",
            "title": f"Competitor {domain} {direction} {abs(delta):.1%} share",
            "description": (
                f"{domain} share-of-voice moved from {prev_sov:.1%} to {cur_sov:.1%} "
                f"({direction} {abs(delta):.1%} pp). "
                f"Cited {cur['citation_count']} times in this window. "
                f"Investigate which prompts they took and refresh the relevant cluster."
            ),
            "priority": priority,
            "source_data": {
                "domain": domain, "current_sov": cur_sov, "previous_sov": prev_sov,
                "delta": delta, "citation_count": cur["citation_count"],
            },
            "link": f"/competitors?domain={domain}",
        })
    return findings


# ═══════════════════════════════════════════════════════════════
# DETECTOR 3 — emerging topics (we get cited but have no draft)
# ═══════════════════════════════════════════════════════════════

async def _detect_emerging_topics(workspace_id: str) -> List[Dict[str, Any]]:
    """Topics with rising citations and zero approved drafts."""
    project_id = workspace_id
    rows = await fetch_all(
        """SELECT c.id, c.name, c.total_citations, c.url_count,
                  (SELECT COUNT(*) FROM drafts d
                   WHERE d.cluster_id = c.id AND d.status = 'approved') AS approved
           FROM clusters c
           WHERE c.project_id = ?
           ORDER BY c.total_citations DESC""",
        (project_id,),
    )
    findings: List[Dict[str, Any]] = []
    for r in rows:
        if (r["total_citations"] or 0) < EMERGING_TOPIC_MIN_CITATIONS:
            continue
        if (r["approved"] or 0) > 0:
            continue
        priority = min(95.0, 40 + (r["total_citations"] or 0))
        findings.append({
            "kind": "emerging",
            "rec_type": "opportunity",
            "title": f"Untapped: {r['name']} ({r['total_citations']} citations)",
            "description": (
                f"Cluster '{r['name']}' has {r['total_citations']} citations across "
                f"{r['url_count']} URLs but no approved draft yet. "
                f"Generate a brief and produce content while interest is high."
            ),
            "priority": priority,
            "cluster_id": r["id"],
            "source_data": {
                "cluster_id": r["id"], "name": r["name"],
                "total_citations": r["total_citations"],
            },
            "link": f"/analysis?cluster={r['id']}",
        })
    return findings


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

def _short_url(url: str, max_len: int = 50) -> str:
    if len(url) <= max_len:
        return url
    return url[:max_len - 3] + "..."
