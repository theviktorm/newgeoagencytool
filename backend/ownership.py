"""
Momentus AI — Nuanced Ownership Model

The old battlefield bucketed prompts into owned / lost / emerging only. This
module adds a richer 0-5 ownership ladder plus a finer status taxonomy
(owned / recommended / co_owned / lost / volatile / emerging / contested) so the
War Room can show *how* we own (or don't own) a prompt, not just a binary.

It reads the same `prompt_observations` + `prompt_ownership` data the prompt
engine already produces — it does not run any model calls itself. All functions
are defensive on empty data and return plain dicts ready for ApiResponse.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .database import fetch_all, fetch_one, from_json

logger = logging.getLogger("geo.ownership")

# 0-5 ladder: index == level.
OWNERSHIP_LEVELS = [
    (0, "Not visible"),
    (1, "Mentioned"),
    (2, "Listed"),
    (3, "Top 3"),
    (4, "Recommended"),
    (5, "Owned"),
]

_LEVEL_LABEL = {lvl: label for lvl, label in OWNERSHIP_LEVELS}

# Models whose observations are *imported aggregates* vs *live model queries*.
_IMPORTED_MODELS = {"peec_aggregate"}
_REPLAY_MODELS = {"peec-replay", "peec_replay"}


# ═══════════════════════════════════════════════════════════════
# LEVEL + STATUS DERIVATION
# ═══════════════════════════════════════════════════════════════

def level_from_position(present: bool, position: int, total_brands: int) -> int:
    """Map our position in an AI answer to a 0-5 ownership level.

    Thresholds (documented):
      - not present at all                -> 0  (Not visible)
      - present but no usable position     -> 1  (Mentioned) — we're in the prose
      - position == 1 AND only us / clear  -> 5  (Owned)     — sole/clear winner
            "clear" = total_brands <= 1 OR we're the single named brand
      - position == 1 but shared field     -> 4  (Recommended) — first but among peers
      - 2 <= position <= 3                 -> 3  (Top 3)
      - position == 4 or 5 in a list       -> 2  (Listed)
      - position > 5                       -> 1  (Mentioned)
    `total_brands` is the count of brands the AI named in the answer; a small
    field at position 1 means we genuinely own it, a crowded field means we're
    merely the first of many (recommended, not owned).
    """
    if not present:
        return 0
    try:
        pos = int(position or 0)
    except (TypeError, ValueError):
        pos = 0
    try:
        total = int(total_brands or 0)
    except (TypeError, ValueError):
        total = 0

    if pos <= 0:
        # Present, but position unknown -> we know we're mentioned at least.
        return 1
    if pos == 1:
        # Sole or near-sole winner = Owned; first-among-many = Recommended.
        if total <= 1:
            return 5
        if total <= 3:
            return 5  # small field, position 1 -> still a clear win
        return 4      # crowded field, first but shared
    if pos <= 3:
        return 3
    if pos <= 5:
        return 2
    return 1


def status_from_ownership(
    our_score: float,
    leader_score: float,
    our_level: int,
    num_competitors_strong: int,
    volatility: Optional[float] = None,
) -> str:
    """Classify a prompt into a status bucket.

    Returns one of:
      owned | recommended | co_owned | lost | volatile | emerging | contested

    Rules (evaluated in order):
      1. volatile   — volatility is high (>= 0.5 on a 0..1 scale): the answer
                      swings too much to trust any single status.
      2. owned      — our_level == 5, or our_score >= 70 and our_score > leader.
      3. recommended— our_level == 4, or our_score >= 55 and our_score >= leader.
      4. co_owned   — our_score and leader_score within 15 pts of each other AND
                      our_level >= 3 (we're tied at/near the top).
      5. lost       — leader_score > our_score and our_level <= 2 (a rival clearly
                      ahead while we're only listed/mentioned).
      6. contested  — multiple strong competitors (>=2) and no clear owner; a
                      crowded fight we're in but not winning.
      7. emerging   — nobody strong yet (low leader_score) — open opportunity.
    """
    our = float(our_score or 0)
    leader = float(leader_score or 0)
    lvl = int(our_level or 0)
    strong = int(num_competitors_strong or 0)

    if volatility is not None and float(volatility) >= 0.5:
        return "volatile"
    if lvl >= 5 or (our >= 70 and our > leader):
        return "owned"
    if lvl >= 4 or (our >= 55 and our >= leader):
        return "recommended"
    if abs(our - leader) <= 15 and lvl >= 3:
        return "co_owned"
    if leader > our and lvl <= 2:
        return "lost"
    if strong >= 2:
        return "contested"
    if leader < 40:
        return "emerging"
    # Fallback: behind a single leader but still in the game.
    return "lost" if leader > our else "emerging"


def _confidence_from_models(models: List[str]) -> str:
    """Provenance of a prompt's ownership data, from the observation models seen.

    imported        — only Peec aggregate import (peec_aggregate)
    estimated       — peec-replay / synthetic replay observations
    claude_analyzed — claude model query present
    verified        — a real external model query (chatgpt/gemini/perplexity/google_aio)
    """
    ms = {(m or "").lower() for m in models if m}
    if not ms:
        return "estimated"
    live = ms - _IMPORTED_MODELS - _REPLAY_MODELS - {"claude"}
    if live:
        return "verified"
    if "claude" in ms:
        return "claude_analyzed"
    if ms & _IMPORTED_MODELS:
        return "imported"
    if ms & _REPLAY_MODELS:
        return "estimated"
    return "estimated"


# ═══════════════════════════════════════════════════════════════
# PER-PROMPT OWNERSHIP
# ═══════════════════════════════════════════════════════════════

async def compute_prompt_ownership(workspace_id: str, prompt_id: str) -> Dict[str, Any]:
    """Derive the nuanced ownership view for a single prompt from its latest
    observations. Defensive: returns level 0 / not_visible when there's no data."""
    empty = {
        "workspace_id": workspace_id,
        "prompt_id": prompt_id,
        "ownership_level": 0,
        "level_label": _LEVEL_LABEL[0],
        "status": "not_visible",
        "our_best_position": 0,
        "winning_competitor": "",
        "platforms_visible": [],
        "confidence": "estimated",
        "last_tracked": None,
    }

    obs = await fetch_all(
        "SELECT * FROM prompt_observations WHERE prompt_id = ? "
        "ORDER BY observed_at DESC",
        (prompt_id,),
    )
    if not obs:
        return empty

    # Latest observation per model.
    latest_by_model: Dict[str, Dict[str, Any]] = {}
    for o in obs:
        m = o.get("model") or "unknown"
        if m not in latest_by_model:
            latest_by_model[m] = o

    our_best_position = 0
    platforms_visible: List[str] = []
    max_brands_in_answer = 0
    any_present = False

    for m, o in latest_by_model.items():
        present = bool(o.get("our_brand_present"))
        pos = int(o.get("our_brand_position") or 0)
        brands = from_json(o.get("brands_appeared") or "[]") or []
        if isinstance(brands, list):
            max_brands_in_answer = max(max_brands_in_answer, len(brands))
        if present:
            any_present = True
            platforms_visible.append(m)
            if pos > 0 and (our_best_position == 0 or pos < our_best_position):
                our_best_position = pos

    ownership_level = level_from_position(
        present=any_present,
        position=our_best_position,
        total_brands=max_brands_in_answer,
    )

    # Pull the persisted rollup for our_score / leader info.
    roll = await fetch_one(
        "SELECT * FROM prompt_ownership WHERE workspace_id = ? AND prompt_id = ?",
        (workspace_id, prompt_id),
    )
    our_score = float((roll or {}).get("our_score") or 0)
    leader_score = float((roll or {}).get("leader_score") or 0)
    winning_competitor = (roll or {}).get("leader_domain") or ""
    competitor_scores = from_json((roll or {}).get("competitor_scores") or "{}") or {}
    num_strong = sum(1 for v in competitor_scores.values() if float(v or 0) >= 50)

    status = status_from_ownership(
        our_score=our_score,
        leader_score=leader_score,
        our_level=ownership_level,
        num_competitors_strong=num_strong,
    )

    last_tracked = max((o.get("observed_at") for o in obs if o.get("observed_at")), default=None)
    confidence = _confidence_from_models(list(latest_by_model.keys()))

    return {
        "workspace_id": workspace_id,
        "prompt_id": prompt_id,
        "ownership_level": ownership_level,
        "level_label": _LEVEL_LABEL.get(ownership_level, ""),
        "status": status,
        "our_best_position": our_best_position,
        "our_score": our_score,
        "leader_score": leader_score,
        "winning_competitor": winning_competitor,
        "platforms_visible": platforms_visible,
        "confidence": confidence,
        "last_tracked": last_tracked,
    }


# ═══════════════════════════════════════════════════════════════
# WORKSPACE DISTRIBUTION
# ═══════════════════════════════════════════════════════════════

async def ownership_distribution(workspace_id: str) -> Dict[str, Any]:
    """Count prompts by nuanced status for the War Room breakdown.

    Buckets: not_visible, mentioned, listed, top3, recommended, owned, co_owned,
    lost, volatile, emerging, contested.
    """
    buckets = {
        "not_visible": 0, "mentioned": 0, "listed": 0, "top3": 0,
        "recommended": 0, "owned": 0, "co_owned": 0, "lost": 0,
        "volatile": 0, "emerging": 0, "contested": 0,
    }
    # Map a (status, level) into a display bucket. Status wins; for the bare
    # ladder levels we also fill mentioned/listed/top3 from the level.
    prompts = await fetch_all(
        "SELECT id FROM prompts WHERE workspace_id = ?", (workspace_id,)
    )
    total = len(prompts)
    if not prompts:
        return {"workspace_id": workspace_id, "total_prompts": 0, "distribution": buckets}

    for p in prompts:
        view = await compute_prompt_ownership(workspace_id, p["id"])
        status = view.get("status") or "not_visible"
        level = int(view.get("ownership_level") or 0)

        if status in buckets:
            buckets[status] += 1
            continue
        # Fallback by level when the status is one of the ladder-only labels.
        if level == 0:
            buckets["not_visible"] += 1
        elif level == 1:
            buckets["mentioned"] += 1
        elif level == 2:
            buckets["listed"] += 1
        elif level == 3:
            buckets["top3"] += 1
        elif level == 4:
            buckets["recommended"] += 1
        else:
            buckets["owned"] += 1

    return {
        "workspace_id": workspace_id,
        "total_prompts": total,
        "distribution": buckets,
    }


# ═══════════════════════════════════════════════════════════════
# EMERGING LOGIC (Explainability Phase 2)
# ═══════════════════════════════════════════════════════════════

# Sub-types of "emerging" — a prompt that's in flux / up for grabs.
EMERGING_SUBTYPES = (
    "new_prompt", "rising_intent", "no_clear_winner",
    "aio_emerging", "competitor_emerging", "revenue_emerging",
)


def _recent(created_at: Optional[str], days: int = 14) -> bool:
    """True if created_at is within the last `days` days (best-effort parse)."""
    if not created_at:
        return False
    from datetime import datetime
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(str(created_at)[:19], fmt)
            return (datetime.utcnow() - dt).days <= days
        except (ValueError, TypeError):
            continue
    return False


def emerging_subtype(
    prompt_row: Dict[str, Any],
    ownership_row: Optional[Dict[str, Any]],
    volatility: Optional[float] = None,
) -> Optional[str]:
    """Classify the flavour of "emerging" for a prompt, or None if it is not
    emerging at all.

    Rules (evaluated in order; first match wins):

      new_prompt          — created within 14 days AND fewer than 2 observations
                            recorded (we simply haven't measured it yet).
      revenue_emerging    — high revenue_score (>= 60) but ownership is unstable:
                            volatility >= 0.5 OR (our_score < 50 AND leader_score
                            < 50). High-value land grab.
      no_clear_winner     — leader_score < 40 AND our_score < 40 (nobody owns it).
      aio_emerging        — a Google AI Overview is present for the prompt but
                            our_brand is not yet in it (aio_observations > 0 and
                            our AIO presence is 0).
      rising_intent       — observation count increased recently (>=2 obs in the
                            last 14 days) while ownership is still low (our_score
                            < 50). Demand is climbing before anyone owns it.
      competitor_emerging — a competitor is gaining (leader_score between 40 and
                            70, climbing) while we stay low (our_score < 40).
      None                — prompt is settled (clearly owned or clearly lost).
    """
    our = float((ownership_row or {}).get("our_score") or 0)
    leader = float((ownership_row or {}).get("leader_score") or 0)
    rev = float(prompt_row.get("revenue_score") or 0)
    obs_count = int(prompt_row.get("_obs_count") or 0)
    recent_obs = int(prompt_row.get("_recent_obs_count") or 0)
    aio_obs = int(prompt_row.get("_aio_obs_count") or 0)
    aio_ours = int(prompt_row.get("_aio_our_count") or 0)
    vol = float(volatility) if volatility is not None else 0.0

    if _recent(prompt_row.get("created_at")) and obs_count < 2:
        return "new_prompt"
    if rev >= 60 and (vol >= 0.5 or (our < 50 and leader < 50)):
        return "revenue_emerging"
    if leader < 40 and our < 40:
        return "no_clear_winner"
    if aio_obs > 0 and aio_ours == 0:
        return "aio_emerging"
    if recent_obs >= 2 and our < 50:
        return "rising_intent"
    if 40 <= leader < 70 and our < 40:
        return "competitor_emerging"
    return None


def _why_emerging(subtype: str, prompt_row: Dict[str, Any],
                  ownership_row: Optional[Dict[str, Any]]) -> str:
    leader = (ownership_row or {}).get("leader_domain") or "a competitor"
    rev = float(prompt_row.get("revenue_score") or 0)
    return {
        "new_prompt": "Freshly added and barely measured — track it to learn the field.",
        "rising_intent": "Demand is climbing across recent observations while no one owns the answer yet.",
        "no_clear_winner": "Neither we nor any competitor strongly owns this — open land grab.",
        "aio_emerging": "A Google AI Overview is forming for this prompt and we are not in it yet.",
        "competitor_emerging": f"{leader} is gaining ground here while we stay low — act before they lock it in.",
        "revenue_emerging": f"High estimated value (revenue_score {round(rev)}) with unstable ownership — prioritise.",
    }.get(subtype, "Emerging opportunity.")


async def _prompt_volatility(prompt_id: str) -> float:
    """Spread of our_brand_position across recent observations, normalised 0..1.
    0 = stable, 1 = wildly swinging. Best-effort; 0 when too little data."""
    rows = await fetch_all(
        "SELECT our_brand_position FROM prompt_observations WHERE prompt_id = ? "
        "ORDER BY observed_at DESC LIMIT 12",
        (prompt_id,),
    )
    positions = [int(r.get("our_brand_position") or 0) for r in rows]
    positions = [p for p in positions if p > 0]
    if len(positions) < 2:
        return 0.0
    spread = max(positions) - min(positions)
    # Normalise: a spread of 5 positions or more -> fully volatile.
    return min(1.0, spread / 5.0)


async def emerging_prompts(workspace_id: str) -> List[Dict[str, Any]]:
    """Return every prompt that qualifies as emerging, with its subtype + why.

    Defensive: empty workspace -> []. Never raises.
    """
    prompts = await fetch_all(
        "SELECT * FROM prompts WHERE workspace_id = ?", (workspace_id,)
    )
    if not prompts:
        return []
    ownership = {
        r["prompt_id"]: r
        for r in await fetch_all(
            "SELECT * FROM prompt_ownership WHERE workspace_id = ?", (workspace_id,)
        )
    }
    # Observation counts per prompt (total, recent, AIO) in one pass each.
    obs_counts = {
        r["prompt_id"]: int(r["n"])
        for r in await fetch_all(
            "SELECT prompt_id, COUNT(*) AS n FROM prompt_observations "
            "WHERE workspace_id = ? GROUP BY prompt_id",
            (workspace_id,),
        )
    }
    recent_counts = {
        r["prompt_id"]: int(r["n"])
        for r in await fetch_all(
            "SELECT prompt_id, COUNT(*) AS n FROM prompt_observations "
            "WHERE workspace_id = ? AND observed_at >= datetime('now', '-14 days') "
            "GROUP BY prompt_id",
            (workspace_id,),
        )
    }
    aio_total = {
        r["prompt_id"]: int(r["n"])
        for r in await fetch_all(
            "SELECT prompt_id, COUNT(*) AS n FROM aio_tracking "
            "WHERE workspace_id = ? AND aio_present = 1 GROUP BY prompt_id",
            (workspace_id,),
        )
    }
    aio_ours = {
        r["prompt_id"]: int(r["n"])
        for r in await fetch_all(
            "SELECT prompt_id, COUNT(*) AS n FROM aio_tracking "
            "WHERE workspace_id = ? AND our_brand_in_aio = 1 GROUP BY prompt_id",
            (workspace_id,),
        )
    }

    out: List[Dict[str, Any]] = []
    for p in prompts:
        pid = p["id"]
        p["_obs_count"] = obs_counts.get(pid, 0)
        p["_recent_obs_count"] = recent_counts.get(pid, 0)
        p["_aio_obs_count"] = aio_total.get(pid, 0)
        p["_aio_our_count"] = aio_ours.get(pid, 0)
        vol = await _prompt_volatility(pid)
        own = ownership.get(pid)
        subtype = emerging_subtype(p, own, vol)
        if not subtype:
            continue
        out.append({
            "prompt_id": pid,
            "text": p.get("text", ""),
            "subtype": subtype,
            "why": _why_emerging(subtype, p, own),
            "revenue_score": float(p.get("revenue_score") or 0),
            "confidence": "estimated",
        })
    # Highest revenue first.
    out.sort(key=lambda r: -r["revenue_score"])
    return out
