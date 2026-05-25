"""
Momentus AI — Metric Dictionary

Single source of truth for what every metric in the GEO engine MEANS, how it's
ACTUALLY calculated in THIS codebase (honestly labelled estimated/heuristic
where that's the truth), and what a user should do about a low score.

This is read-only reference data: there is no DB table and no I/O. The War Room,
tooltips, and the explainability endpoints all read from METRICS so the product
never shows a number without being able to explain it.

`confidence` is one of:
    seeded         — placeholder/demo value, not from real data
    imported       — came from an external export (e.g. Peec Prompts CSV)
    scraped        — derived from scraped competitor/own pages
    claude_analyzed— produced by a Claude analysis pass
    verified       — confirmed against a live model query
    estimated      — heuristic / formula-derived, not directly measured
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# ═══════════════════════════════════════════════════════════════
# METRICS — keyed by metric_key
# ═══════════════════════════════════════════════════════════════

METRICS: Dict[str, Dict[str, Any]] = {
    # ── Ownership statuses (per-prompt outcome buckets) ──
    "owned": {
        "name": "Owned",
        "definition": "Our brand is the answer AI gives — cited first / recommended on this prompt.",
        "formula": (
            "Heuristic: prompt_ownership.our_score >= 70 AND our_score >= leader_score. "
            "our_score is computed in prompt_engine.recompute_ownership() as the per-model "
            "average of 100/position for our brand across the latest prompt_observations."
        ),
        "data_sources": ["prompt_observations", "prompt_ownership"],
        "refresh_frequency": "On every prompt track / CSV import (recompute_ownership).",
        "confidence": "estimated",
        "low_score_meaning": "We are not the AI's default answer here yet.",
        "recommended_action": "Maintain: keep schema, reviews, and citations fresh so we don't lose the slot.",
    },
    "lost": {
        "name": "Lost",
        "definition": "A competitor owns this prompt and we are behind them in the AI answer.",
        "formula": (
            "Heuristic: prompt_ownership.leader_score > our_score. The leader_domain is the "
            "highest-scoring competitor in competitor_scores (recompute_ownership)."
        ),
        "data_sources": ["prompt_observations", "prompt_ownership"],
        "refresh_frequency": "On every prompt track / CSV import.",
        "confidence": "estimated",
        "low_score_meaning": "A rival is the AI's default answer; we lose this pipeline.",
        "recommended_action": "Run citation diagnosis on the leader and publish a page that out-cites them.",
    },
    "co_owned": {
        "name": "Co-owned",
        "definition": "We share the top of the answer with one or more competitors — no clear winner.",
        "formula": (
            "Heuristic (ownership.status_from_ownership): our_score within ~15 pts of leader_score "
            "while our ownership_level >= 3 (Top 3). Contested at the top rather than lost."
        ),
        "data_sources": ["prompt_observations", "prompt_ownership"],
        "refresh_frequency": "On every prompt track / CSV import.",
        "confidence": "estimated",
        "low_score_meaning": "We're tied, not winning — vulnerable to being edged out.",
        "recommended_action": "Add a differentiator (reviews, decision content) to break the tie in our favour.",
    },
    "mentioned": {
        "name": "Mentioned",
        "definition": "Our brand name appears in the AI answer but not prominently — buried in prose.",
        "formula": (
            "ownership.level_from_position -> level 1: brand present but position > 3 and not listed. "
            "Derived from our_brand_present / our_brand_position in prompt_observations."
        ),
        "data_sources": ["prompt_observations"],
        "refresh_frequency": "On every prompt track / CSV import.",
        "confidence": "estimated",
        "low_score_meaning": "AI knows we exist but doesn't rank us.",
        "recommended_action": "Strengthen topical depth + schema so the model promotes us into the shortlist.",
    },
    "listed": {
        "name": "Listed",
        "definition": "We appear in the AI's list/comparison of options but not in the top 3.",
        "formula": "ownership.level_from_position -> level 2: present in a list, position > 3.",
        "data_sources": ["prompt_observations"],
        "refresh_frequency": "On every prompt track / CSV import.",
        "confidence": "estimated",
        "low_score_meaning": "We're an also-ran option, not a frontrunner.",
        "recommended_action": "Improve decision-support content and reviews to climb into Top 3.",
    },
    "recommended": {
        "name": "Recommended",
        "definition": "AI actively recommends our brand, but as one of a few near-equal picks.",
        "formula": (
            "ownership.level_from_position -> level 4: position == 1 but signal is weak / shared. "
            "Status 'recommended' when our_score is high but not dominant."
        ),
        "data_sources": ["prompt_observations", "prompt_ownership"],
        "refresh_frequency": "On every prompt track / CSV import.",
        "confidence": "estimated",
        "low_score_meaning": "We're recommended but not the sole answer — share of voice leaks to rivals.",
        "recommended_action": "Push to full ownership with stronger authority + entity consistency.",
    },
    "prompt_ownership_level": {
        "name": "Prompt Ownership Level",
        "definition": "A 0-5 ladder describing how strongly we own a single prompt's answer.",
        "formula": (
            "ownership.level_from_position(present, position, total_brands): "
            "0 Not visible, 1 Mentioned, 2 Listed, 3 Top 3, 4 Recommended, 5 Owned."
        ),
        "data_sources": ["prompt_observations"],
        "refresh_frequency": "On every prompt track / CSV import.",
        "confidence": "estimated",
        "low_score_meaning": "Low level = AI doesn't surface us for this question.",
        "recommended_action": "Target the specific gap at the current level (e.g. listed -> top3 needs reviews + decision content).",
    },
    "emerging": {
        "name": "Emerging",
        "definition": "A prompt where no one strongly owns the answer yet — an open opportunity.",
        "formula": (
            "Heuristic: there is no dominant leader (low leader_score) and our_score is low but "
            "non-zero / trending. battlefield_summary buckets non-owned/non-lost high-value prompts here."
        ),
        "data_sources": ["prompt_observations", "prompt_ownership", "prompts"],
        "refresh_frequency": "On every prompt track / CSV import.",
        "confidence": "estimated",
        "low_score_meaning": "Land-grab available; first strong content can take it.",
        "recommended_action": "Publish authoritative content fast to claim the prompt before a rival does.",
    },
    "prompt_revenue": {
        "name": "Prompt Revenue (estimated)",
        "definition": "Estimated EUR business value of owning a single prompt.",
        "formula": (
            "ESTIMATED. revenue_priority._value_eur(): if estimated_value_eur is set, use it; "
            "else DEFAULT_VALUE_BY_STAGE[buyer_stage] * (revenue_score/100). i.e. "
            "stage_value × revenue_score/100. Pure heuristic, not measured conversions."
        ),
        "data_sources": ["prompts"],
        "refresh_frequency": "Recomputed on read; depends on classification + stage.",
        "confidence": "estimated",
        "low_score_meaning": "Low estimated value — deprioritise vs higher-intent prompts.",
        "recommended_action": "Focus effort on decision/purchase-stage prompts with higher estimated value.",
    },
    "pipeline_at_stake": {
        "name": "Pipeline at Stake (estimated)",
        "definition": "Total estimated EUR across all tracked prompts — the revenue we're competing for.",
        "formula": (
            "ESTIMATED. revenue_priority.revenue_summary(): sum of _value_eur(prompt) over all prompts "
            "in the workspace. Aggregate of the per-prompt heuristic."
        ),
        "data_sources": ["prompts", "prompt_ownership"],
        "refresh_frequency": "On read (summary endpoint).",
        "confidence": "estimated",
        "low_score_meaning": "Small modelled pipeline — add more high-intent prompts to track.",
        "recommended_action": "Import/track more decision-stage prompts to size the real opportunity.",
    },
    "citation_score": {
        "name": "Citation Score",
        "definition": "Our share of brand citations across tracked AI mentions.",
        "formula": (
            "authority_score._citation_score(): count of mention_events whose url matches our domain "
            "/ total mention_events, scaled so 50% share = 100. mention_events come from the Peec MCP sync."
        ),
        "data_sources": ["mention_events"],
        "refresh_frequency": "On Peec MCP sync; recomputed when authority is rebuilt.",
        "confidence": "imported",
        "low_score_meaning": "We rarely get cited relative to total citation volume.",
        "recommended_action": "Refresh content + land citations on pages AI already pulls from.",
    },
    "authority_score": {
        "name": "GEO Authority Score",
        "definition": "0-100 composite of how strong our brand is in the eyes of generative AI.",
        "formula": (
            "authority_score._compute(): weighted sum of 7 sub-scores using WEIGHTS "
            "(citation 0.18, prompt_ownership 0.22, schema 0.12, offsite 0.13, reddit 0.10, "
            "entity 0.10, local 0.15). Each sub-score 0-100. Mix of imported + scraped + heuristic."
        ),
        "data_sources": [
            "mention_events", "prompt_ownership", "schema_audits",
            "aio_tracking", "reddit_intel", "competitor_capabilities",
        ],
        "refresh_frequency": "On rebuild_all / compute_for_workspace; persisted as time-series.",
        "confidence": "estimated",
        "low_score_meaning": "Weak overall AI authority — multiple levers underperforming.",
        "recommended_action": "Fix the lowest-weighted-contribution sub-score first (see authority breakdown).",
    },
    "entity_score": {
        "name": "Entity Score",
        "definition": "How consistent and machine-recognisable our brand entity is across the web.",
        "formula": (
            "authority_score._entity_score(): uses competitor_capabilities.entity_consistency_score if "
            "present; else counts schema_audits pages with Person/Organization schema × 12 (capped 100). Heuristic proxy."
        ),
        "data_sources": ["competitor_capabilities", "schema_audits"],
        "refresh_frequency": "On authority rebuild / schema audit.",
        "confidence": "scraped",
        "low_score_meaning": "AI can't reliably tie our pages to one consistent entity.",
        "recommended_action": "Add Organization/Person schema + sameAs + consistent NAP across pages.",
    },
    "local_authority": {
        "name": "Local Authority",
        "definition": "Strength of our local/geographic signals for location-based prompts.",
        "formula": (
            "authority_score._local_score(): competitor_capabilities.local_authority_score if present; "
            "else count of aio_tracking rows where visible_brands matches us × 6 (capped 100). Heuristic proxy."
        ),
        "data_sources": ["competitor_capabilities", "aio_tracking"],
        "refresh_frequency": "On authority rebuild / AIO tracking.",
        "confidence": "scraped",
        "low_score_meaning": "We're weak on 'near me' / city-specific answers.",
        "recommended_action": "Strengthen LocalBusiness schema + Google Business + location pages.",
    },
    "decision_support": {
        "name": "Decision Support",
        "definition": "How much our content helps a buyer decide (comparisons, pros/cons, criteria).",
        "formula": (
            "competitor_capabilities.decision_support_score. Seeded from prompt-observation position in "
            "prompt_csv_importer; refined by attack_map scraping. Heuristic until scraped."
        ),
        "data_sources": ["competitor_capabilities", "prompt_observations"],
        "refresh_frequency": "On CSV seed + attack_map analysis.",
        "confidence": "estimated",
        "low_score_meaning": "We lack comparison/decision content AI cites at the buying moment.",
        "recommended_action": "Publish comparison + decision pages with criteria tables and FAQs.",
    },
    "faq_depth": {
        "name": "FAQ Depth",
        "definition": "Breadth and depth of FAQ content AI can lift answers from.",
        "formula": (
            "competitor_capabilities.faq_depth_score. 0 until a scrape pass (attack_map) measures "
            "FAQ coverage on the page. Scraped metric."
        ),
        "data_sources": ["competitor_capabilities", "scraped_content"],
        "refresh_frequency": "On attack_map scrape.",
        "confidence": "scraped",
        "low_score_meaning": "Thin FAQ coverage — AI has little to quote from us.",
        "recommended_action": "Add FAQ schema and answer the real prompts users ask, verbatim.",
    },
    "dominator": {
        "name": "Dominator",
        "definition": "A competitor domain that owns a large number of our high-value lost prompts.",
        "formula": (
            "prompt_engine.battlefield_summary(): for lost high-value prompts, count by leader_domain; "
            "top domains by lost-prompt count are dominators."
        ),
        "data_sources": ["prompt_ownership", "prompts"],
        "refresh_frequency": "On battlefield read.",
        "confidence": "estimated",
        "low_score_meaning": "n/a (higher = a rival controls more of our battlefield).",
        "recommended_action": "Prioritise diagnosing and attacking the top dominator's weakest axis.",
    },
    "ownership_gap": {
        "name": "Ownership Gap",
        "definition": "How far we are from owning a prompt — drives the revenue opportunity weighting.",
        "formula": (
            "ESTIMATED. revenue_priority.revenue_breakdown ownership_gap table keyed by status: "
            "not_visible 1.0, mentioned 0.8, listed 0.6, top3 0.4, recommended 0.2, owned 0.0, "
            "co_owned 0.3, lost 1.0. Larger gap = more revenue recoverable."
        ),
        "data_sources": ["prompt_ownership"],
        "refresh_frequency": "On revenue breakdown read.",
        "confidence": "estimated",
        "low_score_meaning": "Small gap = little left to win here (we're near/at ownership).",
        "recommended_action": "Spend effort where gap × estimated value is highest.",
    },
    "ai_visibility": {
        "name": "AI Visibility",
        "definition": "Whether and how often our brand shows up at all in AI answers.",
        "formula": (
            "Derived from prompt_observations.our_brand_present across models / Peec visibility field. "
            "Imported when from peec_aggregate, otherwise heuristic from tracking."
        ),
        "data_sources": ["prompt_observations"],
        "refresh_frequency": "On prompt track / CSV import.",
        "confidence": "imported",
        "low_score_meaning": "AI mostly answers these prompts without us in the room.",
        "recommended_action": "Get cited on source pages the models retrieve for these prompts.",
    },
    "prompt_priority": {
        "name": "Prompt Priority",
        "definition": "Ranking score for which prompt to work on next.",
        "formula": (
            "ESTIMATED. revenue_priority.revenue_priority_recs(): "
            "priority = revenue_score × (1 - our_score/100) × stage_multiplier. "
            "High-revenue, lost, late-stage prompts float to the top."
        ),
        "data_sources": ["prompts", "prompt_ownership"],
        "refresh_frequency": "On priority read.",
        "confidence": "estimated",
        "low_score_meaning": "Low priority — either already won or low value.",
        "recommended_action": "Work top-priority prompts first; they have the best value×gap.",
    },
    "data_confidence": {
        "name": "Data Confidence",
        "definition": "How trustworthy the underlying data for a metric is (its provenance).",
        "formula": (
            "Set on the record's source/confidence columns. peec_import -> imported; manual -> estimated; "
            "live model query -> claude_analyzed/verified; peec-replay -> estimated. Not numeric."
        ),
        "data_sources": ["prompts.confidence", "prompt_observations.model"],
        "refresh_frequency": "Set at write time (import / tracking).",
        "confidence": "estimated",
        "low_score_meaning": "Low confidence = treat the number as indicative, not exact.",
        "recommended_action": "Run a live prompt track to upgrade confidence from estimated to verified.",
    },
    "source_quality": {
        "name": "Source Quality",
        "definition": "Quality/authority of the pages AI cites for a prompt.",
        "formula": (
            "Heuristic over prompt_observations.sources_cited + sources table (citation counts, "
            "scrape status). Estimated; not an independent domain-authority score."
        ),
        "data_sources": ["prompt_observations", "sources"],
        "refresh_frequency": "On prompt track / scrape.",
        "confidence": "estimated",
        "low_score_meaning": "AI is citing weak/irrelevant sources — opening for stronger content.",
        "recommended_action": "Publish higher-authority content than the currently-cited sources.",
    },
    "citation_quality": {
        "name": "Citation Quality",
        "definition": "Whether our citations are prominent and positive, not just present.",
        "formula": (
            "Heuristic combining our_brand_position (prompt_observations) with sentiment_score / "
            "mention_events sentiment. Estimated."
        ),
        "data_sources": ["prompt_observations", "mention_events"],
        "refresh_frequency": "On prompt track / MCP sync.",
        "confidence": "estimated",
        "low_score_meaning": "We're cited late or with weak sentiment.",
        "recommended_action": "Earn earlier, more positive mentions via reviews + authoritative coverage.",
    },
    "volatility": {
        "name": "Volatility",
        "definition": "How much a prompt's ownership swings between observations — instability of the answer.",
        "formula": (
            "Heuristic: variance/spread of our_brand_position across recent prompt_observations for the "
            "prompt. High spread => volatile status. Estimated."
        ),
        "data_sources": ["prompt_observations"],
        "refresh_frequency": "On prompt track.",
        "confidence": "estimated",
        "low_score_meaning": "Low volatility = stable answer; high volatility = unstable / contested.",
        "recommended_action": "Lock in volatile-but-winnable prompts with consistent, fresh signals.",
    },
    "buyer_journey_stage": {
        "name": "Buyer Journey Stage",
        "definition": "Where a prompt sits in the buying funnel — drives its revenue weight.",
        "formula": (
            "prompt_engine.classify_prompt_heuristic / reclassify_prompt (Claude). "
            "One of: awareness, problem, solution, comparison, trust, objection, decision "
            "(+ purchase/booking variants used in revenue weighting)."
        ),
        "data_sources": ["prompts.buyer_stage"],
        "refresh_frequency": "On upsert + reclassify.",
        "confidence": "claude_analyzed",
        "low_score_meaning": "Early-stage prompts convert less; weight them lower.",
        "recommended_action": "Ensure decision/purchase-stage prompts are tracked and owned first.",
    },
}


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

def all_metrics() -> List[Dict[str, Any]]:
    """Return every metric as a flat list, each with its `key` inlined."""
    return [{"key": k, **v} for k, v in METRICS.items()]


def get_metric(key: str) -> Optional[Dict[str, Any]]:
    """Return one metric dict (with `key` inlined) or None if unknown."""
    v = METRICS.get(key)
    if v is None:
        return None
    return {"key": key, **v}
