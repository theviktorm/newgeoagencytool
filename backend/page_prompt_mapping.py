"""
Momentus AI — Page ↔ Prompt Mapping (Phase 4)

For every prompt the workspace tracks, decide:

  - what TYPE of page should win the answer (awareness_blog | comparison |
    decision | service | faq | review_trust | location | expert_profile |
    pricing | recovery_safety)
  - which of OUR pages is the current best candidate (token-overlap match
    against `sources`)
  - which competitor URL is winning (from prompt_ownership.leader_domain +
    recent prompt_observations citations)
  - whether we are missing a page entirely

Outputs land in `page_prompt_map` so the dashboard can show the brief gap
without recomputing on every page load.

Pure stdlib + existing deps. Defensive: empty workspace returns shaped zeros,
never raises. Every row carries a `confidence` label ('estimated' by default,
'needs_review' when we had nothing to work with).
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from .database import (
    execute, fetch_all, fetch_one, from_json, gen_id, get_db, to_json,
)

logger = logging.getLogger("geo.page_prompt_mapping")


PAGE_TYPES = (
    "awareness_blog", "comparison", "decision", "service", "faq",
    "review_trust", "location", "expert_profile", "pricing",
    "recovery_safety",
)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS page_prompt_map (
    id                          TEXT PRIMARY KEY,
    workspace_id                TEXT NOT NULL,
    prompt_id                   TEXT NOT NULL,
    target_url                  TEXT DEFAULT '',
    current_best_url            TEXT DEFAULT '',
    missing_page                INTEGER DEFAULT 0,
    competitor_winning_url      TEXT DEFAULT '',
    page_type                   TEXT DEFAULT '',
    confidence                  TEXT DEFAULT 'estimated',
    updated_at                  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_ppm_ws_prompt
    ON page_prompt_map(workspace_id, prompt_id);
CREATE INDEX IF NOT EXISTS idx_ppm_missing
    ON page_prompt_map(workspace_id, missing_page);
"""


async def init() -> None:
    """Create page_prompt_map. Idempotent. Same pattern as brand_resolver."""
    db = await get_db()
    await db.executescript(_SCHEMA)
    await db.commit()


# ─────────────────────────────────────────────────────────────────
# PAGE-TYPE RULES
# ─────────────────────────────────────────────────────────────────
# Documented mapping (text + buyer_stage → page_type):
#   buyer_stage == purchase OR text matches (price|book|cost|how much|quote)
#     → pricing
#   buyer_stage == purchase OR buyer_stage == decision
#     → decision (when no price signal)
#   buyer_stage == comparison OR text matches (vs|versus|compare|alternative)
#     → comparison
#   buyer_stage == trust AND text matches (review|safety|risk|complication)
#     → review_trust (or recovery_safety if recovery/risk/complication)
#   buyer_stage == consideration / solution / problem
#     → service (default consideration page) or faq when text starts with
#       what/how/why/can
#   buyer_stage == awareness
#     → awareness_blog (or location/expert_profile when matching local /
#       expert intent)
# Location intent: "near me", " in <city>", "budapest", "london", ...
# Expert intent:   "doctor", "dr", "surgeon", "expert", "specialist"
_PRICE_RE = re.compile(
    r"\b(price|pricing|cost|costs|how much|quote|fee|fees|book|booking|"
    r"deposit|reserve)\b", re.IGNORECASE,
)
_COMPARISON_RE = re.compile(
    r"\b(vs|versus|compare|comparison|alternative|alternatives|or)\b",
    re.IGNORECASE,
)
_REVIEW_RE = re.compile(
    r"\b(review|reviews|rating|ratings|testimonial|reddit|trustpilot)\b",
    re.IGNORECASE,
)
_RECOVERY_RE = re.compile(
    r"\b(recovery|side effect|side\-effect|risk|risks|complication|"
    r"complications|safe|safety|aftercare|downtime)\b", re.IGNORECASE,
)
_FAQ_RE = re.compile(
    r"^(what|how|why|can|does|do|is|are|should|when|will)\b", re.IGNORECASE,
)
_LOCATION_RE = re.compile(
    r"\b(near me|nearest|in [a-z]{3,}|budapest|london|new york|nyc|"
    r"madrid|paris|berlin|munich|warsaw|prague|hungary|magyarorszag)\b",
    re.IGNORECASE,
)
_EXPERT_RE = re.compile(
    r"\b(doctor|doctors|dr\.?|surgeon|expert|experts|specialist|"
    r"specialists|consultant|practitioner)\b", re.IGNORECASE,
)


async def recommend_page_type(prompt_text: str, buyer_stage: str) -> str:
    """Deterministic rule that picks a recommended page_type for a prompt.

    Documented above. Stable across calls so the dashboard never thrashes."""
    t = (prompt_text or "")
    stage = (buyer_stage or "").lower()

    has_price = bool(_PRICE_RE.search(t))
    has_comp = bool(_COMPARISON_RE.search(t))
    has_review = bool(_REVIEW_RE.search(t))
    has_recovery = bool(_RECOVERY_RE.search(t))
    has_location = bool(_LOCATION_RE.search(t))
    has_expert = bool(_EXPERT_RE.search(t))

    # Strongest signals first.
    if stage in ("purchase", "decision") and has_price:
        return "pricing"
    if stage == "purchase":
        return "pricing"
    if stage == "decision":
        return "decision"
    if stage == "comparison" or has_comp:
        return "comparison"
    if stage == "trust":
        if has_recovery:
            return "recovery_safety"
        if has_review:
            return "review_trust"
        return "review_trust"
    if stage in ("consideration", "solution", "problem"):
        # FAQ-style wording → faq, else service.
        if _FAQ_RE.match(t.strip()):
            return "faq"
        return "service"
    # Awareness / fallback.
    if has_location:
        return "location"
    if has_expert:
        return "expert_profile"
    if has_recovery:
        return "recovery_safety"
    return "awareness_blog"


# ─────────────────────────────────────────────────────────────────
# MAPPING
# ─────────────────────────────────────────────────────────────────

_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "for", "in", "on", "at",
    "is", "are", "what", "how", "why", "can", "do", "does", "near", "me",
    "my", "your", "our", "us", "with", "vs", "versus", "best", "top",
}


def _tokens(text: str) -> set:
    """Lowercased word tokens (>=3 chars), stripped of stopwords."""
    if not text:
        return set()
    raw = re.findall(r"[a-z0-9]{3,}", (text or "").lower())
    return {w for w in raw if w not in _STOPWORDS}


def _overlap(a: str, b: str) -> int:
    return len(_tokens(a) & _tokens(b))


def _domain(url: str) -> str:
    if not url:
        return ""
    try:
        return (urlparse(url).hostname or "").lower().lstrip("www.")
    except Exception:
        return ""


async def _pick_our_best_url(
    workspace_id: str, prompt: Dict[str, Any],
) -> str:
    """Best-effort token overlap against `sources`. Falls back to the
    prompt's explicit target_url if no source matches."""
    tgt = (prompt.get("target_url") or "").strip()
    text = (prompt.get("text") or "")
    rows: List[Dict[str, Any]] = []
    try:
        rows = await fetch_all(
            "SELECT url, title, topics, total_citation_count FROM sources "
            "WHERE project_id = ? ORDER BY total_citation_count DESC LIMIT 200",
            (workspace_id,),
        )
    except Exception:
        rows = []
    if not rows:
        return tgt
    best_url = tgt
    best_score = 0
    for r in rows:
        url = r.get("url") or ""
        if not url:
            continue
        title = r.get("title") or ""
        topics_blob = r.get("topics") or "[]"
        topics = from_json(topics_blob, []) if isinstance(topics_blob, str) else (topics_blob or [])
        haystack = " ".join([title, " ".join(topics or []), url])
        score = _overlap(text, haystack)
        # Soft bonus if the URL slug itself contains a prompt token.
        if score == 0:
            continue
        # Tie-break by citation count (preferred when already cited).
        bias = int(r.get("total_citation_count") or 0) // 10
        total = score * 10 + bias
        if total > best_score:
            best_score = total
            best_url = url
    return best_url or tgt


async def _pick_competitor_url(
    workspace_id: str, prompt_id: str, leader_domain: str,
) -> str:
    """Find a competitor URL that the leader_domain has been observed citing
    on this prompt. Falls back to "" when no observation matches."""
    if not leader_domain:
        return ""
    try:
        obs = await fetch_all(
            "SELECT sources_cited FROM prompt_observations "
            "WHERE prompt_id = ? AND workspace_id = ? "
            "ORDER BY observed_at DESC LIMIT 20",
            (prompt_id, workspace_id),
        )
    except Exception:
        obs = []
    for o in obs:
        srcs = from_json(o.get("sources_cited") or "[]", []) or []
        for s in srcs:
            if not isinstance(s, dict):
                continue
            url = s.get("url") or ""
            if not url:
                continue
            if leader_domain.lower() in _domain(url):
                return url
            if leader_domain.lower() in url.lower():
                return url
    # Fallback: search peec sources where url matches the leader domain.
    try:
        row = await fetch_one(
            "SELECT url FROM sources WHERE project_id = ? AND url LIKE ? "
            "ORDER BY total_citation_count DESC LIMIT 1",
            (workspace_id, f"%{leader_domain}%"),
        )
        if row and row.get("url"):
            return row["url"]
    except Exception:
        pass
    return ""


async def map_prompt(workspace_id: str, prompt_id: str) -> Dict[str, Any]:
    """Map ONE prompt to a recommended page type + best URLs.

    Upserts into page_prompt_map and returns the resulting row.
    """
    prompt = await fetch_one("SELECT * FROM prompts WHERE id = ?", (prompt_id,))
    if not prompt:
        raise ValueError(f"unknown prompt {prompt_id}")

    page_type = await recommend_page_type(
        prompt.get("text") or "", prompt.get("buyer_stage") or "",
    )
    target_url = (prompt.get("target_url") or "").strip()
    our_best = await _pick_our_best_url(workspace_id, prompt)
    leader_domain = ""
    try:
        own = await fetch_one(
            "SELECT leader_domain FROM prompt_ownership "
            "WHERE workspace_id = ? AND prompt_id = ?",
            (workspace_id, prompt_id),
        )
        leader_domain = ((own or {}).get("leader_domain") or "").lower()
    except Exception:
        leader_domain = ""
    competitor_url = await _pick_competitor_url(workspace_id, prompt_id, leader_domain)

    missing_page = 1 if not (our_best or target_url) else 0
    confidence = "estimated" if (our_best or competitor_url) else "needs_review"

    # Upsert: delete-then-insert keeps the row count of 1-per-(ws,prompt).
    existing = await fetch_one(
        "SELECT id FROM page_prompt_map WHERE workspace_id = ? AND prompt_id = ?",
        (workspace_id, prompt_id),
    )
    if existing:
        await execute(
            "UPDATE page_prompt_map SET target_url = ?, current_best_url = ?, "
            "missing_page = ?, competitor_winning_url = ?, page_type = ?, "
            "confidence = ?, updated_at = datetime('now') WHERE id = ?",
            (target_url, our_best or "", missing_page, competitor_url, page_type,
             confidence, existing["id"]),
        )
        row_id = existing["id"]
    else:
        row_id = gen_id("ppm-")
        await execute(
            "INSERT INTO page_prompt_map (id, workspace_id, prompt_id, "
            "target_url, current_best_url, missing_page, competitor_winning_url, "
            "page_type, confidence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (row_id, workspace_id, prompt_id, target_url, our_best or "",
             missing_page, competitor_url, page_type, confidence),
        )
    return await fetch_one(
        "SELECT * FROM page_prompt_map WHERE id = ?", (row_id,),
    ) or {}


async def map_workspace(workspace_id: str) -> Dict[str, Any]:
    """Map every prompt in the workspace.

    Returns {mapped, missing_pages}. Defensive: empty workspace returns zeros.
    """
    try:
        prompts = await fetch_all(
            "SELECT id FROM prompts WHERE workspace_id = ?", (workspace_id,),
        )
    except Exception:
        prompts = []
    mapped = 0
    missing = 0
    for p in prompts:
        try:
            row = await map_prompt(workspace_id, p["id"])
            mapped += 1
            if int(row.get("missing_page") or 0) == 1:
                missing += 1
        except Exception as e:
            logger.warning("map_prompt %s failed: %s", p.get("id"), e)
    return {
        "workspace_id": workspace_id,
        "mapped": mapped,
        "missing_pages": missing,
        "confidence": "estimated",
    }


async def list_mappings(
    workspace_id: str, only_missing: bool = False,
) -> List[Dict[str, Any]]:
    """Query helper. Newest first."""
    sql = "SELECT * FROM page_prompt_map WHERE workspace_id = ?"
    args: List[Any] = [workspace_id]
    if only_missing:
        sql += " AND missing_page = 1"
    sql += " ORDER BY updated_at DESC LIMIT 500"
    try:
        return await fetch_all(sql, tuple(args))
    except Exception:
        return []


__all__ = [
    "init", "recommend_page_type", "map_prompt", "map_workspace",
    "list_mappings", "PAGE_TYPES",
]
