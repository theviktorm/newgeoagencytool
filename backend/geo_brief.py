"""
Momentus AI — GEO Brief Generator (Phase 4)

Reads everything we know about a prompt (text, buyer_stage, page mapping,
latest citation diagnostics, competitor profile) and asks Claude for a
structured GEO content brief. Persists into `geo_briefs` so the dashboard
can list them, push them into the action queue, or pipe them into the
Content Studio later.

Defensive: no Claude key → returns a fully-shaped stub with
confidence='needs_review' rather than raising. Empty workspace → empty list.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from .claude_engine import ClaudeEngine
from .config import settings
from .database import (
    execute, fetch_all, fetch_one, from_json, gen_id, get_db, to_json,
)

logger = logging.getLogger("geo.geo_brief")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS geo_briefs (
    id                          TEXT PRIMARY KEY,
    workspace_id                TEXT NOT NULL,
    prompt_id                   TEXT NOT NULL,
    brief_json                  TEXT DEFAULT '{}',
    created_at                  TEXT DEFAULT (datetime('now')),
    created_from_module         TEXT DEFAULT 'geo_brief',
    published_to_content_studio INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_geob_workspace ON geo_briefs(workspace_id);
CREATE INDEX IF NOT EXISTS idx_geob_prompt ON geo_briefs(prompt_id);
"""


async def init() -> None:
    """Create geo_briefs. Idempotent."""
    db = await get_db()
    await db.executescript(_SCHEMA)
    await db.commit()


_BRIEF_KEYS = (
    "target_prompt", "buyer_stage", "search_intent", "recommended_page_type",
    "suggested_h1", "suggested_h2_outline", "direct_answer_block",
    "decision_support_sections", "comparison_table", "faq_questions",
    "schema_type", "trust_proof_needed", "internal_link_suggestions",
    "external_proof_suggestions", "cta_angle", "competitor_pages_to_study",
    "success_metric", "re_track_schedule_days", "confidence",
)


def _empty_brief(
    prompt: Dict[str, Any], page_type: str = "", reason: str = "needs_review",
) -> Dict[str, Any]:
    """Fully-shaped stub returned when Claude can't be called or returns junk."""
    return {
        "target_prompt": prompt.get("text", "") or "",
        "buyer_stage": prompt.get("buyer_stage", "") or "",
        "search_intent": "",
        "recommended_page_type": page_type or "",
        "suggested_h1": "",
        "suggested_h2_outline": [],
        "direct_answer_block": "",
        "decision_support_sections": [],
        "comparison_table": [],
        "faq_questions": [],
        "schema_type": [],
        "trust_proof_needed": [],
        "internal_link_suggestions": [],
        "external_proof_suggestions": [],
        "cta_angle": "",
        "competitor_pages_to_study": [],
        "success_metric": "",
        "re_track_schedule_days": 7,
        "confidence": "needs_review",
        "_reason": reason,
    }


def _safe_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.lower().startswith("json"):
            t = t[4:]
    s, e = t.find("{"), t.rfind("}")
    if s == -1 or e == -1:
        return None
    try:
        return json.loads(t[s:e + 1])
    except Exception:
        return None


def _normalize_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        return [value] if value.strip() else []
    return [value]


def _normalize_brief(raw: Dict[str, Any], fallback: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure every required key is present with a sane type."""
    out: Dict[str, Any] = {**fallback}
    for k in _BRIEF_KEYS:
        if k in raw:
            out[k] = raw[k]
    # Coerce list-shaped keys to actual lists.
    for k in (
        "suggested_h2_outline", "decision_support_sections", "comparison_table",
        "faq_questions", "schema_type", "trust_proof_needed",
        "internal_link_suggestions", "external_proof_suggestions",
        "competitor_pages_to_study",
    ):
        out[k] = _normalize_list(out.get(k))
    # Default re-track horizon.
    try:
        out["re_track_schedule_days"] = int(out.get("re_track_schedule_days") or 7)
    except (TypeError, ValueError):
        out["re_track_schedule_days"] = 7
    # Always present.
    if not out.get("confidence"):
        out["confidence"] = "estimated"
    return out


async def _gather_context(workspace_id: str, prompt_id: str) -> Dict[str, Any]:
    """Read every table we want Claude to consider — never raises."""
    ctx: Dict[str, Any] = {}
    ctx["prompt"] = await fetch_one(
        "SELECT * FROM prompts WHERE id = ?", (prompt_id,),
    ) or {}
    try:
        ctx["page_mapping"] = await fetch_one(
            "SELECT * FROM page_prompt_map WHERE workspace_id = ? AND prompt_id = ?",
            (workspace_id, prompt_id),
        ) or {}
    except Exception:
        ctx["page_mapping"] = {}
    try:
        ctx["ownership"] = await fetch_one(
            "SELECT * FROM prompt_ownership WHERE workspace_id = ? AND prompt_id = ?",
            (workspace_id, prompt_id),
        ) or {}
    except Exception:
        ctx["ownership"] = {}
    try:
        ctx["diagnostics"] = await fetch_all(
            "SELECT id, competitor_domain, analyzed_url, diagnosis, "
            "recommended_action, expected_impact, our_url, confidence "
            "FROM citation_diagnostics WHERE workspace_id = ? AND prompt_id = ? "
            "ORDER BY analyzed_at DESC LIMIT 3",
            (workspace_id, prompt_id),
        )
    except Exception:
        ctx["diagnostics"] = []
    leader = ((ctx.get("ownership") or {}).get("leader_domain") or "").lower()
    if leader:
        try:
            ctx["competitor"] = await fetch_one(
                "SELECT competitor_domain, schema_score, reddit_score, "
                "youtube_score, faq_depth_score, decision_support_score, "
                "review_score, entity_consistency_score, pr_score, "
                "local_authority_score, overall_strength, weakest_axis, "
                "weakest_axis_score, summary "
                "FROM competitor_capabilities WHERE workspace_id = ? AND "
                "competitor_domain = ?",
                (workspace_id, leader),
            ) or {}
        except Exception:
            ctx["competitor"] = {}
    else:
        ctx["competitor"] = {}
    return ctx


_BRIEF_SYSTEM = (
    "You are a senior GEO (Generative Engine Optimization) content strategist. "
    "Given a search prompt and its diagnostic context, produce a structured "
    "JSON brief that tells a writer EXACTLY what to publish to win the answer "
    "in ChatGPT / Gemini / Perplexity / Claude / Google AI Overview.\n\n"
    "Return STRICT JSON with these keys (omit none):\n"
    "  target_prompt, buyer_stage, search_intent, recommended_page_type,\n"
    "  suggested_h1, suggested_h2_outline (array of strings),\n"
    "  direct_answer_block (string — the 1-3 sentence answer the AI should\n"
    "                       lift verbatim),\n"
    "  decision_support_sections (array of strings),\n"
    "  comparison_table (array of {\"column\":\"...\",\"our\":\"...\",\n"
    "                              \"competitor\":\"...\"}),\n"
    "  faq_questions (array of strings),\n"
    "  schema_type (array of schema.org type names),\n"
    "  trust_proof_needed (array of strings),\n"
    "  internal_link_suggestions (array of strings),\n"
    "  external_proof_suggestions (array of strings),\n"
    "  cta_angle (string), competitor_pages_to_study (array of URLs),\n"
    "  success_metric (string), re_track_schedule_days (integer, default 7),\n"
    "  confidence (one of 'estimated' | 'verified' | 'needs_review').\n"
    "Do not include any prose outside the JSON. Do not invent URLs."
)


async def _claude_brief(ctx: Dict[str, Any], page_type: str) -> Optional[Dict[str, Any]]:
    """Ask Claude. Returns parsed JSON or None on failure."""
    prompt = ctx.get("prompt") or {}
    user = (
        f"Prompt: {prompt.get('text','')}\n"
        f"Buyer stage: {prompt.get('buyer_stage','')}\n"
        f"Prompt type: {prompt.get('prompt_type','')}\n"
        f"Revenue score: {prompt.get('revenue_score','')}\n"
        f"Recommended page type: {page_type or 'auto'}\n"
        f"Page mapping: {json.dumps(ctx.get('page_mapping') or {}, default=str)}\n"
        f"Ownership: {json.dumps(ctx.get('ownership') or {}, default=str)}\n"
        f"Top diagnostics: {json.dumps(ctx.get('diagnostics') or [], default=str)[:3000]}\n"
        f"Competitor capabilities: {json.dumps(ctx.get('competitor') or {}, default=str)[:1500]}\n\n"
        "Produce the brief JSON now."
    )
    engine = ClaudeEngine()
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": user}],
            system=_BRIEF_SYSTEM,
            max_tokens=1600,
            temperature=0.25,
            operation="geo_brief_generate",
            use_cache=True,
        )
    except Exception as e:
        logger.warning("Claude brief call failed: %s", e)
        return None
    return _safe_json(resp.get("content", ""))


async def _push_geo_action(brief_id: str, brief: Dict[str, Any],
                           workspace_id: str, prompt_id: str) -> Optional[str]:
    """Create a content_brief geo_actions row referencing the brief. Idempotent
    by (workspace_id, action_type='content_brief', source_id='geob:{brief_id}')."""
    # Lazy import to avoid circular import + ensure schema migration ran.
    from . import action_engine as _act
    try:
        await _act.init()
    except Exception as e:  # pragma: no cover
        logger.warning("action_engine.init failed: %s", e)
    source_id = f"geob:{brief_id}"
    existing = await fetch_one(
        "SELECT id FROM geo_actions WHERE workspace_id = ? "
        "AND action_type = 'content_brief' AND source_id = ? LIMIT 1",
        (workspace_id, source_id),
    )
    if existing:
        return existing.get("id")
    aid = gen_id("act-")
    title = (brief.get("suggested_h1") or brief.get("target_prompt") or "GEO brief")[:300]
    descr = brief.get("direct_answer_block") or "Generated GEO content brief — review and assign."
    await execute(
        "INSERT INTO geo_actions (id, workspace_id, action_type, status, "
        "title, description, source_kind, source_id, target_prompt_id, "
        "priority, estimated_impact, payload, workflow_stage) "
        "VALUES (?, ?, 'content_brief', 'recommended', ?, ?, 'geo_brief', ?, ?, ?, ?, ?, 'detect')",
        (aid, workspace_id, title, descr[:2000], source_id, prompt_id or "",
         75.0, brief.get("success_metric", "")[:200],
         to_json({"brief_id": brief_id, "brief": brief})),
    )
    return aid


async def generate_brief(
    workspace_id: str, prompt_id: str, push_to_action: bool = False,
) -> Dict[str, Any]:
    """Generate (and persist) a GEO brief for ONE prompt.

    push_to_action=True also drops a content_brief row into geo_actions so it
    enters the Do-It queue immediately."""
    ctx = await _gather_context(workspace_id, prompt_id)
    prompt = ctx.get("prompt") or {}
    if not prompt:
        raise ValueError(f"unknown prompt {prompt_id}")
    page_type = ((ctx.get("page_mapping") or {}).get("page_type") or "").strip()
    if not page_type:
        # Fall back to the deterministic rule (avoids a hard dep when
        # page_prompt_mapping hasn't been initialized yet for the ws).
        try:
            from . import page_prompt_mapping as _ppm
            page_type = await _ppm.recommend_page_type(
                prompt.get("text") or "", prompt.get("buyer_stage") or "",
            )
        except Exception:
            page_type = ""

    fallback = _empty_brief(prompt, page_type=page_type, reason="stub")
    if not settings.has_claude_api:
        brief = _normalize_brief({"recommended_page_type": page_type}, fallback)
        brief["confidence"] = "needs_review"
        brief["_reason"] = "no_anthropic_key"
    else:
        parsed = await _claude_brief(ctx, page_type)
        if not parsed:
            brief = _normalize_brief({"recommended_page_type": page_type}, fallback)
            brief["confidence"] = "needs_review"
            brief["_reason"] = "claude_returned_no_json"
        else:
            brief = _normalize_brief(parsed, fallback)
            if not brief.get("recommended_page_type"):
                brief["recommended_page_type"] = page_type

    brief_id = gen_id("gb-")
    try:
        await execute(
            "INSERT INTO geo_briefs (id, workspace_id, prompt_id, brief_json, "
            "created_from_module) VALUES (?, ?, ?, ?, 'geo_brief')",
            (brief_id, workspace_id, prompt_id, to_json(brief)),
        )
    except Exception as e:
        logger.warning("geo_briefs insert failed: %s", e)

    pushed_action_id: Optional[str] = None
    if push_to_action:
        try:
            pushed_action_id = await _push_geo_action(
                brief_id, brief, workspace_id, prompt_id,
            )
        except Exception as e:
            logger.warning("push_to_action failed: %s", e)

    return {
        "id": brief_id,
        "workspace_id": workspace_id,
        "prompt_id": prompt_id,
        "brief": brief,
        "pushed_action_id": pushed_action_id,
        "confidence": brief.get("confidence", "estimated"),
    }


async def list_briefs(workspace_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Recent briefs (newest first). Decodes brief_json."""
    try:
        rows = await fetch_all(
            "SELECT * FROM geo_briefs WHERE workspace_id = ? "
            "ORDER BY created_at DESC LIMIT ?",
            (workspace_id, int(limit)),
        )
    except Exception:
        return []
    for r in rows:
        r["brief"] = from_json(r.get("brief_json") or "{}", {})
    return rows


async def push_brief_to_actions(brief_id: str) -> Dict[str, Any]:
    """Create a content_brief geo_actions row for an already-generated brief.
    Idempotent — re-running returns the same action."""
    row = await fetch_one(
        "SELECT * FROM geo_briefs WHERE id = ?", (brief_id,),
    )
    if not row:
        raise ValueError(f"unknown brief {brief_id}")
    brief = from_json(row.get("brief_json") or "{}", {})
    aid = await _push_geo_action(
        brief_id, brief, row.get("workspace_id") or "", row.get("prompt_id") or "",
    )
    return {
        "brief_id": brief_id, "action_id": aid,
        "workspace_id": row.get("workspace_id"),
        "prompt_id": row.get("prompt_id"),
        "confidence": brief.get("confidence", "estimated"),
    }


__all__ = [
    "init", "generate_brief", "list_briefs", "push_brief_to_actions",
]
