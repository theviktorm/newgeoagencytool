"""
Momentus AI — Citation Score Breakdown (Explainability Phase 2)

Decomposes WHO gets cited, WHERE, and HOW WELL for a workspace, computed from
the two citation streams the engine already produces:

  - prompt_observations.sources_cited  (live model queries / Peec replay)
  - mention_events                     (Peec MCP sync)

Every dimension carries an honest `confidence` label. Where a dimension cannot
be computed from the available data we return null + confidence 'needs_review'
rather than fabricating a number.

`confidence` values used here:
    estimated     — heuristic / formula-derived (default for most dimensions)
    imported      — came straight from a Peec import (mention_events)
    needs_review  — could not be computed from available data (value is null)
    verified      — confirmed against a live model query
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from .database import fetch_all, fetch_one, from_json

logger = logging.getLogger("geo.citation_breakdown")

# Platforms we report coverage for.
PLATFORMS = ("chatgpt", "gemini", "claude", "perplexity", "google_aio")

# Source-type taxonomy. Documented domain lists drive classify_source_type.
SOURCE_TYPES = (
    "own_website", "competitor_website", "review_site", "reddit", "youtube",
    "local_directory", "media", "medical_directory", "gbp", "product_feed",
    "unknown",
)

# ── Domain lists for classify_source_type (documented) ──
# Reddit
_REDDIT_DOMAINS = {"reddit.com", "redd.it"}
# YouTube / video
_YOUTUBE_DOMAINS = {"youtube.com", "youtu.be", "m.youtube.com"}
# Google Business / Maps surfaces (GBP)
_GBP_DOMAINS = {
    "google.com/maps", "maps.google.com", "g.page", "business.google.com",
    "goo.gl/maps", "maps.app.goo.gl",
}
# Review aggregators / consumer review platforms
_REVIEW_DOMAINS = {
    "trustpilot.com", "yelp.com", "g2.com", "capterra.com", "tripadvisor.com",
    "glassdoor.com", "sitejabber.com", "reviews.io", "feefo.com",
    "consumeraffairs.com", "bbb.org", "productreview.com.au", "trustradius.com",
}
# Medical / health directories (this engine is used in healthcare verticals)
_MEDICAL_DIRECTORY_DOMAINS = {
    "healthgrades.com", "zocdoc.com", "webmd.com", "vitals.com", "ratemds.com",
    "doctolib.com", "practo.com", "nhs.uk", "dr.hu", "foglalorvost.hu",
    "wedmd.com", "realself.com", "whatclinic.com", "topdoctors.co.uk",
}
# General local / business directories
_LOCAL_DIRECTORY_DOMAINS = {
    "yellowpages.com", "yell.com", "foursquare.com", "manta.com",
    "bbb.org", "angi.com", "thomsonlocal.com", "cylex.com", "tupalo.com",
}
# Media / news publishers
_MEDIA_DOMAINS = {
    "nytimes.com", "theguardian.com", "bbc.com", "bbc.co.uk", "forbes.com",
    "cnn.com", "reuters.com", "bloomberg.com", "techcrunch.com", "wired.com",
    "businessinsider.com", "washingtonpost.com", "wsj.com", "telegraph.co.uk",
    "huffpost.com", "vox.com", "theverge.com", "medium.com",
}
# Product feeds / marketplaces
_PRODUCT_FEED_DOMAINS = {
    "amazon.com", "ebay.com", "etsy.com", "shopify.com", "walmart.com",
    "aliexpress.com", "google.com/shopping", "shopping.google.com",
}


def _norm_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    if d.startswith("http"):
        try:
            d = (urlparse(d).hostname or "").lower()
        except Exception:
            d = ""
    return d.lstrip("www.")


def classify_source_type(domain: str) -> str:
    """Map a cited domain to one of SOURCE_TYPES.

    Matching is suffix/substring based so subdomains (e.g. old.reddit.com)
    and path-bearing GBP/shopping URLs still classify correctly. Order matters:
    the most specific buckets (reddit/youtube/gbp) are checked before the
    broader review/media/directory buckets, with 'unknown' as the fallback.
    The caller decides own_website vs competitor_website (we cannot know the
    brand's domain here), so this function never returns those two — see
    `_classify_with_brand` in citation_breakdown for that overlay.
    """
    d = _norm_domain(domain)
    if not d:
        return "unknown"

    def hit(pool: set) -> bool:
        return any(d == p or d.endswith("." + p) or p in d for p in pool)

    if hit(_REDDIT_DOMAINS):
        return "reddit"
    if hit(_YOUTUBE_DOMAINS):
        return "youtube"
    if hit(_GBP_DOMAINS):
        return "gbp"
    if hit(_PRODUCT_FEED_DOMAINS):
        return "product_feed"
    if hit(_MEDICAL_DIRECTORY_DOMAINS):
        return "medical_directory"
    if hit(_REVIEW_DOMAINS):
        return "review_site"
    if hit(_LOCAL_DIRECTORY_DOMAINS):
        return "local_directory"
    if hit(_MEDIA_DOMAINS):
        return "media"
    return "unknown"


# ── Authority tiers for the citation_quality heuristic (documented) ──
# Tier weights are a coarse, honest proxy — NOT a measured domain authority.
_TIER_BY_SOURCE_TYPE = {
    "media": 90,
    "medical_directory": 80,
    "review_site": 70,
    "gbp": 65,
    "youtube": 55,
    "local_directory": 50,
    "reddit": 45,
    "product_feed": 40,
    "competitor_website": 50,
    "own_website": 60,
    "unknown": 35,
}


async def _resolve_our_domain(workspace_id: str) -> str:
    """Resolve the workspace's primary domain (first of workspaces.domains),
    falling back to brand_name. Empty string if nothing is set."""
    ws = await fetch_one(
        "SELECT brand_name, name, domains FROM workspaces WHERE id = ?",
        (workspace_id,),
    )
    if not ws:
        return ""
    ds = ws.get("domains") or "[]"
    parsed = from_json(ds, []) if isinstance(ds, str) else ds
    if isinstance(parsed, list) and parsed:
        return _norm_domain(str(parsed[0]))
    return _norm_domain(ws.get("brand_name") or ws.get("name") or "")


def _classify_with_brand(domain: str, our_domain: str) -> str:
    """Overlay own/competitor on top of the base source-type classification."""
    d = _norm_domain(domain)
    if not d:
        return "unknown"
    if our_domain and (d == our_domain or d.endswith("." + our_domain) or our_domain in d):
        return "own_website"
    base = classify_source_type(d)
    if base == "unknown":
        # A plain website that isn't ours and isn't a known platform: treat as a
        # competitor/other website only if it looks like a real site domain.
        if "." in d:
            return "competitor_website"
    return base


def _empty_breakdown(workspace_id: str, reason: str) -> Dict[str, Any]:
    nr = {"value": None, "confidence": "needs_review"}
    return {
        "workspace_id": workspace_id,
        "owned_citation_share": dict(nr),
        "third_party_share": dict(nr),
        "citation_quality": dict(nr),
        "citation_position": dict(nr),
        "citation_sentiment": dict(nr),
        "citation_freshness": dict(nr),
        "platform_coverage": {p: 0 for p in PLATFORMS},
        "platform_coverage_confidence": "needs_review",
        "source_type_breakdown": {t: 0 for t in SOURCE_TYPES},
        "source_type_breakdown_confidence": "needs_review",
        "citation_gap": [],
        "citation_gap_confidence": "needs_review",
        "total_citations": 0,
        "note": reason,
        "confidence": "needs_review",
    }


async def citation_breakdown(workspace_id: str) -> Dict[str, Any]:
    """Decompose citations for a workspace. Defensive: empty data returns a
    fully-shaped result with null values + 'needs_review' confidence, never
    raises."""
    our_domain = await _resolve_our_domain(workspace_id)

    # ── Pull cited sources from prompt_observations ──
    obs = await fetch_all(
        "SELECT model, sources_cited, our_brand_position, sentiment_score, "
        "observed_at FROM prompt_observations WHERE workspace_id = ? "
        "ORDER BY observed_at DESC",
        (workspace_id,),
    )

    # ── Counters ──
    platform_coverage = {p: 0 for p in PLATFORMS}
    source_type_counts = {t: 0 for t in SOURCE_TYPES}
    our_cites = 0
    third_party_cites = 0
    total_cites = 0
    quality_weight_sum = 0.0
    position_values: List[float] = []
    sentiment_values: List[float] = []
    latest_observed: Optional[str] = None
    # Source types where competitors are cited (for the gap).
    competitor_source_types: set = set()
    our_source_types: set = set()

    for o in obs:
        model = (o.get("model") or "").lower()
        if model in platform_coverage:
            platform_coverage[model] += 1
        if o.get("observed_at") and (latest_observed is None or o["observed_at"] > latest_observed):
            latest_observed = o["observed_at"]
        pos = o.get("our_brand_position")
        if pos and int(pos) > 0:
            position_values.append(float(pos))
        sent = o.get("sentiment_score")
        if sent is not None:
            try:
                sv = float(sent)
                if sv != 0.0:
                    sentiment_values.append(sv)
            except (TypeError, ValueError):
                pass

        sources = from_json(o.get("sources_cited") or "[]", []) or []
        if not isinstance(sources, list):
            continue
        for s in sources:
            if not isinstance(s, dict):
                continue
            dom = s.get("domain") or _norm_domain(s.get("url") or "")
            if not dom:
                continue
            total_cites += 1
            st = _classify_with_brand(dom, our_domain)
            source_type_counts[st] = source_type_counts.get(st, 0) + 1
            quality_weight_sum += _TIER_BY_SOURCE_TYPE.get(st, 35)
            if st == "own_website":
                our_cites += 1
                our_source_types.add(st)
            else:
                third_party_cites += 1
                competitor_source_types.add(st)

    # ── mention_events (Peec import) augments source-type + freshness ──
    mention_rows = await fetch_all(
        "SELECT url, position, sentiment_score, observed_at, model_source "
        "FROM mention_events WHERE project_id = ? ORDER BY observed_at DESC LIMIT 2000",
        (workspace_id,),
    )
    imported_any = False
    for m in mention_rows:
        dom = _norm_domain(m.get("url") or "")
        if not dom:
            continue
        imported_any = True
        total_cites += 1
        st = _classify_with_brand(dom, our_domain)
        source_type_counts[st] = source_type_counts.get(st, 0) + 1
        quality_weight_sum += _TIER_BY_SOURCE_TYPE.get(st, 35)
        if st == "own_website":
            our_cites += 1
            our_source_types.add(st)
        else:
            third_party_cites += 1
            competitor_source_types.add(st)
        if m.get("position") and float(m["position"]) > 0:
            position_values.append(float(m["position"]))
        if m.get("sentiment_score") is not None:
            try:
                sv = float(m["sentiment_score"])
                if sv != 0.0:
                    sentiment_values.append(sv)
            except (TypeError, ValueError):
                pass
        if m.get("observed_at") and (latest_observed is None or m["observed_at"] > latest_observed):
            latest_observed = m["observed_at"]

    if total_cites == 0:
        return _empty_breakdown(workspace_id, "No citations found in "
                                "prompt_observations or mention_events yet.")

    base_conf = "imported" if (imported_any and not obs) else "estimated"

    # ── owned vs third-party share ──
    owned_share = round(our_cites / total_cites, 4)
    third_share = round(third_party_cites / total_cites, 4)

    # ── citation_quality: avg authority tier normalised 0..100 ──
    citation_quality = round(quality_weight_sum / total_cites, 2)

    # ── citation_position: avg position of OUR citations ──
    if position_values:
        citation_position = {
            "value": round(sum(position_values) / len(position_values), 2),
            "confidence": base_conf,
        }
    else:
        citation_position = {"value": None, "confidence": "needs_review"}

    # ── citation_sentiment ──
    if sentiment_values:
        citation_sentiment = {
            "value": round(sum(sentiment_values) / len(sentiment_values), 3),
            "confidence": base_conf,
        }
    else:
        citation_sentiment = {"value": None, "confidence": "needs_review"}

    # ── citation_freshness: days since most recent citation ──
    freshness = _freshness(latest_observed)

    # ── citation_gap: source types where competitors are cited but we are not ──
    gap_types = sorted(
        st for st in competitor_source_types
        if st not in ("competitor_website",) and source_type_counts.get(st, 0) > 0
    )
    # We are "absent" from a source type if we have no own_website citation that
    # also appears in that channel — we only mark a gap on third-party channels.
    citation_gap = [
        {"source_type": st, "competitor_citations": source_type_counts.get(st, 0)}
        for st in gap_types
    ]

    return {
        "workspace_id": workspace_id,
        "our_domain": our_domain,
        "owned_citation_share": {"value": owned_share, "confidence": base_conf},
        "third_party_share": {"value": third_share, "confidence": base_conf},
        "citation_quality": {
            "value": citation_quality,
            "confidence": "estimated",
            "note": "Heuristic by source-domain authority tier — not a measured DA.",
        },
        "citation_position": citation_position,
        "citation_sentiment": citation_sentiment,
        "citation_freshness": freshness,
        "platform_coverage": platform_coverage,
        "platform_coverage_confidence": base_conf,
        "source_type_breakdown": source_type_counts,
        "source_type_breakdown_confidence": base_conf,
        "citation_gap": citation_gap,
        "citation_gap_confidence": "estimated",
        "total_citations": total_cites,
        "confidence": base_conf,
    }


def _freshness(latest_observed: Optional[str]) -> Dict[str, Any]:
    """Recency of the most recent citation, in days. needs_review if unknown."""
    if not latest_observed:
        return {"value": None, "confidence": "needs_review"}
    from datetime import datetime, timezone
    parsed = None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(latest_observed[:19], fmt)
            break
        except (ValueError, TypeError):
            continue
    if parsed is None:
        return {"value": None, "confidence": "needs_review",
                "last_observed_at": latest_observed}
    days = (datetime.utcnow() - parsed).days
    return {
        "value": max(0, days),
        "unit": "days_since_last_citation",
        "last_observed_at": latest_observed,
        "confidence": "estimated",
    }
