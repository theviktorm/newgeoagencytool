"""
Momentus AI — Review Intelligence (Phase 4)

Reviews are a top-tier GEO authority signal. This module holds the data and
the rule-based strategy recommendations — it does NOT scrape platforms and
NEVER suggests fake reviews. Data lands here either via UI manual entry
(`record_signal`) or via future scrapers.

Tables:
  review_signals — one row per (workspace, brand, platform) observation.

Public API:
  init(), record_signal(...), workspace_review_summary(ws),
  ethical_disclaimer()
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .database import (
    execute, fetch_all, fetch_one, from_json, gen_id, get_db, to_json,
)

logger = logging.getLogger("geo.review_intelligence")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS review_signals (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    brand               TEXT DEFAULT '',
    platform            TEXT DEFAULT '',
    review_count        INTEGER DEFAULT 0,
    avg_rating          REAL DEFAULT 0.0,
    latest_review_at    TEXT,
    sentiment_score     REAL DEFAULT 0.0,
    keyword_coverage    TEXT DEFAULT '[]',
    source_url          TEXT DEFAULT '',
    confidence          TEXT DEFAULT 'estimated',
    observed_at         TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_review_ws ON review_signals(workspace_id);
CREATE INDEX IF NOT EXISTS idx_review_brand ON review_signals(workspace_id, brand);
CREATE INDEX IF NOT EXISTS idx_review_platform ON review_signals(workspace_id, platform);
"""


async def init() -> None:
    """Create review_signals. Idempotent."""
    db = await get_db()
    await db.executescript(_SCHEMA)
    await db.commit()


def ethical_disclaimer() -> str:
    """Wire this into any UI surface that touches review data. Used by the
    API responses too so the disclaimer is unmissable."""
    return (
        "Review intelligence is for measurement and strategy only. NEVER buy "
        "or fabricate reviews — that violates platform terms and damages "
        "trust. Encourage real, recent first-person reviews and respond to "
        "negative feedback transparently."
    )


_ALLOWED_FIELDS = (
    "review_count", "avg_rating", "latest_review_at", "sentiment_score",
    "keyword_coverage", "source_url", "confidence",
)


async def record_signal(
    workspace_id: str, brand: str, platform: str, **fields: Any,
) -> Dict[str, Any]:
    """Manual record helper (UI-driven). Inserts a new observation row so the
    summary view can compute trend / freshness."""
    if not brand or not platform:
        raise ValueError("brand and platform are required")
    row = {k: fields.get(k) for k in _ALLOWED_FIELDS}
    rid = gen_id("rv-")
    keyword_coverage = row.get("keyword_coverage") or []
    if isinstance(keyword_coverage, str):
        kc_blob = keyword_coverage
    else:
        kc_blob = to_json(keyword_coverage or [])
    await execute(
        "INSERT INTO review_signals (id, workspace_id, brand, platform, "
        "review_count, avg_rating, latest_review_at, sentiment_score, "
        "keyword_coverage, source_url, confidence) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            rid, workspace_id, (brand or "").strip(), (platform or "").strip(),
            int(row.get("review_count") or 0),
            float(row.get("avg_rating") or 0.0),
            row.get("latest_review_at"),
            float(row.get("sentiment_score") or 0.0),
            kc_blob,
            (row.get("source_url") or ""),
            (row.get("confidence") or "estimated"),
        ),
    )
    return await fetch_one(
        "SELECT * FROM review_signals WHERE id = ?", (rid,),
    ) or {}


# ─────────────────────────────────────────────────────────────────
# WORKSPACE SUMMARY
# ─────────────────────────────────────────────────────────────────

def _days_since(iso: Optional[str]) -> Optional[int]:
    if not iso:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(str(iso)[:19], fmt)
            return max(0, (datetime.utcnow() - dt).days)
        except (ValueError, TypeError):
            continue
    return None


async def workspace_review_summary(workspace_id: str) -> Dict[str, Any]:
    """Per-brand aggregates from review_signals + competitor_capabilities
    review_score. Always returns shaped output."""
    try:
        rows = await fetch_all(
            "SELECT * FROM review_signals WHERE workspace_id = ? "
            "ORDER BY observed_at DESC LIMIT 1000",
            (workspace_id,),
        )
    except Exception:
        rows = []

    # Aggregate to (brand, platform) using the LATEST observation per pair.
    latest_by_pair: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = f"{r.get('brand','').lower()}|{r.get('platform','').lower()}"
        if key not in latest_by_pair:
            latest_by_pair[key] = r

    brands: Dict[str, Dict[str, Any]] = {}
    for r in latest_by_pair.values():
        brand = (r.get("brand") or "").strip()
        if not brand:
            continue
        b = brands.setdefault(brand, {
            "brand": brand,
            "platforms": [],
            "total_review_count": 0,
            "avg_rating": 0.0,
            "latest_review_age_days": None,
            "sentiment_score": 0.0,
            "platform_count": 0,
            "recent_freshness_days": None,
        })
        platform_entry = {
            "platform": r.get("platform"),
            "review_count": int(r.get("review_count") or 0),
            "avg_rating": float(r.get("avg_rating") or 0.0),
            "latest_review_at": r.get("latest_review_at"),
            "sentiment_score": float(r.get("sentiment_score") or 0.0),
            "keyword_coverage": from_json(r.get("keyword_coverage") or "[]", []),
            "source_url": r.get("source_url") or "",
            "confidence": r.get("confidence") or "estimated",
        }
        b["platforms"].append(platform_entry)

    for b in brands.values():
        plats = b["platforms"]
        b["platform_count"] = len(plats)
        b["total_review_count"] = sum(p["review_count"] for p in plats)
        ratings = [p["avg_rating"] for p in plats if p["avg_rating"] > 0]
        b["avg_rating"] = round(sum(ratings) / len(ratings), 2) if ratings else 0.0
        sentiments = [p["sentiment_score"] for p in plats if p["sentiment_score"]]
        b["sentiment_score"] = round(sum(sentiments) / len(sentiments), 3) if sentiments else 0.0
        ages = [_days_since(p["latest_review_at"]) for p in plats]
        ages = [a for a in ages if a is not None]
        b["latest_review_age_days"] = min(ages) if ages else None
        b["recent_freshness_days"] = b["latest_review_age_days"]

    # Capability review_score lookup for competitor benchmarking.
    cap_review: Dict[str, float] = {}
    try:
        cap_rows = await fetch_all(
            "SELECT competitor_domain, review_score FROM competitor_capabilities "
            "WHERE workspace_id = ?",
            (workspace_id,),
        )
        for c in cap_rows:
            d = (c.get("competitor_domain") or "").lower()
            if d:
                cap_review[d] = float(c.get("review_score") or 0)
    except Exception:
        cap_review = {}

    # Recommendation strategy (rule-based, ethical).
    recommendations: List[Dict[str, Any]] = []
    for brand_name, b in brands.items():
        freshness = b["latest_review_age_days"]
        plat_count = b["platform_count"]
        notes: List[str] = []
        if freshness is None:
            notes.append("No recent review data on file.")
        elif freshness > 30:
            notes.append(
                f"Latest review is {freshness} days old — freshness signal weak."
            )
        if plat_count < 3:
            notes.append(
                f"Only {plat_count} platform(s) covered — expand to Google, "
                "Trustpilot, industry-specific platforms."
            )
        if b["avg_rating"] and b["avg_rating"] < 4.0:
            notes.append(
                f"Average rating {b['avg_rating']:.1f} below the 4.0 trust "
                "threshold — investigate negative themes."
            )
        if b["sentiment_score"] and b["sentiment_score"] < 0:
            notes.append("Sentiment is net-negative — prioritise service recovery.")
        if not notes:
            notes.append("Healthy review footprint — maintain freshness with "
                         "ongoing post-visit ask flow.")

        # Cap review score for context (lower-cased domain match best-effort).
        cap_score = None
        for d, sc in cap_review.items():
            if d in brand_name.lower() or brand_name.lower() in d:
                cap_score = sc
                break

        strategy = "Encourage real, recent reviews on under-covered platforms."
        if freshness is not None and freshness < 30 and plat_count >= 3:
            strategy = "Maintain — expand coverage to 1 new niche platform."
        elif freshness is not None and freshness >= 30:
            strategy = "Focus on collecting recent reviews on platforms with stale data."
        recommendations.append({
            "brand": brand_name,
            "strategy": strategy,
            "notes": notes,
            "competitor_review_score_context": cap_score,
            "confidence": "estimated",
        })

    if not brands:
        recommendations.append({
            "brand": "",
            "strategy": "Add review signals for each brand you track to unlock "
                       "recommendations.",
            "notes": ["No review signals recorded yet."],
            "competitor_review_score_context": None,
            "confidence": "needs_review",
        })

    return {
        "workspace_id": workspace_id,
        "brands": list(brands.values()),
        "competitor_review_scores": cap_review,
        "recommended_review_strategy": recommendations,
        "ethical_disclaimer": ethical_disclaimer(),
        "confidence": "estimated",
    }


__all__ = [
    "init", "record_signal", "workspace_review_summary",
    "ethical_disclaimer",
]
