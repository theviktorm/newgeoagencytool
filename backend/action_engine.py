"""
Momentus AI — GEO Action Engine

Closes the diagnosis→action loop. Other modules DIAGNOSE GEO problems
(citation_diagnostics, schema_audits, reddit_intel gaps, aio_tracking
losses); this module turns those findings into a queue of concrete,
approvable actions and — where safe — generates the deliverable with Claude.

  harvest_actions(ws)  — scan diagnostic tables, materialize pending actions
  generate_action(id)  — produce the deliverable for ONE action via Claude
  list_actions / update_status / queue_summary — operate the queue

action_type ∈ {schema_install, content_brief, reddit_seed, metadata_rewrite,
               citation_response, comparison_page}
status      ∈ {pending, generated, approved, dismissed, done}

Self-registers schema via init() (brand_resolver / entity_graph pattern).
Defensive: every diagnostic-table read is wrapped so a fresh workspace with
empty/missing tables still works.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from .claude_engine import ClaudeEngine
from .config import settings
from .database import execute, fetch_all, fetch_one, from_json, gen_id, get_db, to_json

try:  # optional — used by metadata_rewrite
    from . import metadata_engine
except Exception:  # pragma: no cover
    metadata_engine = None  # type: ignore

logger = logging.getLogger("geo.action_engine")

ACTION_TYPES = {
    "schema_install", "content_brief", "reddit_seed",
    "metadata_rewrite", "citation_response", "comparison_page",
}
STATUSES = {"pending", "generated", "approved", "dismissed", "done"}


_SCHEMA = """
CREATE TABLE IF NOT EXISTS geo_actions (
    id                TEXT PRIMARY KEY,
    workspace_id      TEXT,
    action_type       TEXT,
    status            TEXT DEFAULT 'pending',
    title             TEXT,
    description       TEXT,
    source_kind       TEXT,
    source_id         TEXT,
    target_url        TEXT DEFAULT '',
    target_prompt_id  TEXT DEFAULT '',
    priority          REAL DEFAULT 50,
    estimated_impact  TEXT DEFAULT '',
    payload           TEXT DEFAULT '{}',
    generated_output  TEXT DEFAULT '',
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT DEFAULT (datetime('now')),
    completed_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_geoact_workspace ON geo_actions(workspace_id);
CREATE INDEX IF NOT EXISTS idx_geoact_status ON geo_actions(status);
"""


async def init() -> None:
    """Create the geo_actions table + indexes. Idempotent."""
    db = await get_db()
    await db.executescript(_SCHEMA)
    await db.commit()


# ── HARVEST: materialize pending actions from diagnostic tables ──
_PRIORITY_RANK = {"high": 80.0, "med": 60.0, "medium": 60.0, "low": 40.0}


def _step_to_type(step: str) -> str:
    """Map a free-text remediation step to an action_type."""
    s = (step or "").lower()
    if "schema" in s or "json-ld" in s or "jsonld" in s or "structured data" in s:
        return "schema_install"
    if "reddit" in s or "subreddit" in s or "forum" in s or "community" in s:
        return "reddit_seed"
    if "compare" in s or "comparison" in s or "vs " in s or "versus" in s or "alternative" in s:
        return "comparison_page"
    if "meta" in s or "title tag" in s or "snippet" in s or "description" in s:
        return "metadata_rewrite"
    if "rebut" in s or "respond" in s or "response" in s or "correct" in s or "factual" in s:
        return "citation_response"
    return "content_brief"


def _prio(value: Any, fallback: float = 50.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    return _PRIORITY_RANK.get(str(value or "").lower(), fallback)


async def _exists(workspace_id: str, action_type: str, source_id: str) -> bool:
    """Dedupe key: (workspace_id, action_type, source_id)."""
    row = await fetch_one(
        "SELECT 1 FROM geo_actions WHERE workspace_id=? AND action_type=? AND source_id=? LIMIT 1",
        (workspace_id, action_type, source_id),
    )
    return bool(row)


async def _insert(
    workspace_id: str, action_type: str, title: str, description: str,
    source_kind: str, source_id: str, *, target_url: str = "",
    target_prompt_id: str = "", priority: float = 50.0,
    estimated_impact: str = "", payload: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    if action_type not in ACTION_TYPES:
        return None
    if await _exists(workspace_id, action_type, source_id):
        return None
    aid = gen_id("act-")
    await execute(
        "INSERT INTO geo_actions "
        "(id, workspace_id, action_type, status, title, description, source_kind, "
        " source_id, target_url, target_prompt_id, priority, estimated_impact, payload) "
        "VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            aid, workspace_id, action_type, title[:300], description[:2000],
            source_kind, source_id, target_url, target_prompt_id,
            float(priority), estimated_impact[:200], to_json(payload or {}),
        ),
    )
    return aid


async def harvest_actions(workspace_id: str) -> Dict[str, Any]:
    """Scan diagnostic tables, materialize pending geo_actions. Idempotent —
    deduped by (workspace_id, action_type, source_id). Returns counts/type."""
    await init()
    counts: Dict[str, int] = {t: 0 for t in ACTION_TYPES}

    def _bump(aid: Optional[str], action_type: str) -> None:
        if aid:
            counts[action_type] = counts.get(action_type, 0) + 1

    # ── citation_diagnostics: explode each diagnosis' actions list ──
    try:
        diags = await fetch_all(
            "SELECT d.id, d.prompt_id, d.competitor_domain, d.analyzed_url, d.diagnosis, "
            "d.actions, p.revenue_score, p.text AS prompt_text "
            "FROM citation_diagnostics d "
            "LEFT JOIN prompts p ON p.id = d.prompt_id "
            "WHERE d.workspace_id = ? ORDER BY d.analyzed_at DESC",
            (workspace_id,),
        )
    except Exception as e:
        logger.warning("harvest: citation_diagnostics read failed: %s", e)
        diags = []
    for d in diags:
        rev = _prio(d.get("revenue_score"), 50.0)
        steps = from_json(d.get("actions") or "[]", []) or []
        for i, step in enumerate(steps):
            if isinstance(step, dict):
                text = step.get("step") or step.get("action") or ""
                impact = str(step.get("impact") or "")
                pr = _prio(step.get("priority"), rev)
            else:
                text, impact, pr = str(step), "", rev
            if not text:
                continue
            at = _step_to_type(text)
            sid = f"cd:{d['id']}:{i}"
            aid = await _insert(
                workspace_id, at,
                title=text[:120],
                description=text,
                source_kind="citation_diagnostics", source_id=sid,
                target_url=d.get("analyzed_url") or "",
                target_prompt_id=d.get("prompt_id") or "",
                priority=max(pr, rev * 0.6),
                estimated_impact=impact or (f"Win prompt: {d.get('prompt_text','')}"[:200]),
                payload={"diagnosis": d.get("diagnosis") or "", "competitor_domain": d.get("competitor_domain") or "",
                         "prompt_text": d.get("prompt_text") or "", "step": text},
            )
            _bump(aid, at)

    # ── schema_audits.missing_critical: one schema_install per high-sev type ──
    try:
        audits = await fetch_all(
            "SELECT id, page_url, missing_critical, diagnosis FROM schema_audits "
            "WHERE workspace_id = ? AND is_competitor = 0 ORDER BY analyzed_at DESC",
            (workspace_id,),
        )
    except Exception as e:
        logger.warning("harvest: schema_audits read failed: %s", e)
        audits = []
    for a in audits:
        for m in from_json(a.get("missing_critical") or "[]", []) or []:
            if not isinstance(m, dict):
                continue
            if str(m.get("severity") or "").lower() != "high":
                continue
            stype = m.get("type") or ""
            if not stype:
                continue
            sid = f"sa:{a['id']}:{stype}"
            aid = await _insert(
                workspace_id, "schema_install",
                title=f"Install {stype} schema on {a.get('page_url','')}"[:120],
                description=m.get("why_it_hurts") or f"Add missing {stype} JSON-LD.",
                source_kind="schema_audits", source_id=sid,
                target_url=a.get("page_url") or "",
                priority=min(100.0, 50.0 + _prio(m.get("weight"), 4.0) * 8),  # weight5→90, weight4→82
                estimated_impact=m.get("why_it_hurts") or "",
                payload={"schema_type": stype, "missing_fields": m.get("missing_fields") or [],
                         "page_url": a.get("page_url") or "", "diagnosis": a.get("diagnosis") or ""},
            )
            _bump(aid, "schema_install")

    # ── reddit_intel opportunity gaps: one reddit_seed each ──
    try:
        gaps = await fetch_all(
            "SELECT id, subreddit, thread_url, thread_title, discussion_type, "
            "suggested_action, sentiment_label FROM reddit_intel "
            "WHERE workspace_id = ? AND is_opportunity_gap = 1 ORDER BY observed_at DESC",
            (workspace_id,),
        )
    except Exception as e:
        logger.warning("harvest: reddit_intel read failed: %s", e)
        gaps = []
    for g in gaps:
        sid = f"ri:{g['id']}"
        aid = await _insert(
            workspace_id, "reddit_seed",
            title=f"Reddit gap in r/{g.get('subreddit','')}: {g.get('thread_title','')}"[:120],
            description=g.get("suggested_action") or "Seed a credible brand mention in this thread.",
            source_kind="reddit_intel", source_id=sid,
            target_url=g.get("thread_url") or "",
            priority=60.0,
            estimated_impact="Capture Reddit authority where our brand is absent",
            payload={"subreddit": g.get("subreddit") or "", "thread_url": g.get("thread_url") or "",
                     "thread_title": g.get("thread_title") or "", "discussion_type": g.get("discussion_type") or "",
                     "suggested_action": g.get("suggested_action") or ""},
        )
        _bump(aid, "reddit_seed")

    # ── aio_tracking losses (AIO present, our brand absent) ──
    try:
        losses = await fetch_all(
            "SELECT a.id, a.prompt_id, a.source_urls, a.visible_brands, "
            "p.text AS prompt_text, p.target_url, p.revenue_score, p.prompt_type "
            "FROM aio_tracking a "
            "LEFT JOIN prompts p ON p.id = a.prompt_id "
            "WHERE a.workspace_id = ? AND a.aio_present = 1 AND a.our_brand_in_aio = 0 "
            "ORDER BY a.observed_at DESC",
            (workspace_id,),
        )
    except Exception as e:
        logger.warning("harvest: aio_tracking read failed: %s", e)
        losses = []
    for L in losses:
        rev = _prio(L.get("revenue_score"), 55.0)
        # Comparative prompts → comparison_page; otherwise metadata_rewrite.
        at = "comparison_page" if str(L.get("prompt_type") or "") in ("comparative", "decision") else "metadata_rewrite"
        sid = f"aio:{L['id']}"
        via = "a comparison page" if at == "comparison_page" else "metadata/snippet rewrite"
        aid = await _insert(
            workspace_id, at,
            title=f"Recover AI Overview: {L.get('prompt_text','')}"[:120],
            description=f"Google AI Overview shows for this prompt but our brand is absent. Win it back via {via}.",
            source_kind="aio_tracking", source_id=sid,
            target_url=L.get("target_url") or "",
            target_prompt_id=L.get("prompt_id") or "",
            priority=max(rev, 55.0),
            estimated_impact="Enter Google AI Overview for a revenue prompt",
            payload={"prompt_text": L.get("prompt_text") or "",
                     "visible_brands": from_json(L.get("visible_brands") or "[]", []),
                     "source_urls": from_json(L.get("source_urls") or "[]", [])},
        )
        _bump(aid, at)

    total = sum(counts.values())
    logger.info("harvest ws=%s created=%d %s", workspace_id, total, counts)
    return {"workspace_id": workspace_id, "created": total, "by_type": counts}


# ── GENERATE: produce the deliverable for ONE action via Claude ──
_SYS = {
    "schema_install": "You are a schema.org structured-data engineer. Emit VALID schema.org JSON-LD (a single ld+json object or @graph) for the requested type, tailored to the page/brand. Use [BRACKET] placeholders the user replaces. Output ONLY the JSON-LD.",
    "content_brief": "You are a GEO content strategist. Produce an actionable content brief in Markdown: H1, H2/H3 outline, target prompts to win, an FAQ block (question + 1-sentence answer guide), and 3-6 internal links to add.",
    "comparison_page": "You are a GEO content strategist. Produce a comparison-page brief in Markdown: H1, the comparison table columns/rows to build, decision criteria, an FAQ block, and the prompts this page should win.",
    "reddit_seed": "You write credible, non-spammy Reddit contributions. Draft a comment/post that genuinely helps the thread and surfaces the brand only where warranted. End with a one-line DISCLAIMER to disclose affiliation and follow each subreddit's self-promotion rules.",
    "citation_response": "You draft factual, non-defensive responses/rebuttals to inaccurate or unfavorable claims an AI is citing. Be specific, cite verifiable facts, keep a calm professional tone. Output the response draft only.",
    "metadata_rewrite": "You are a GEO + AI-snippet engineer. Produce an SEO title (<=60 chars), meta description (<=155 chars), an OG title/description, and the exact question this page should win in AI answers. Output as a small JSON object.",
}


def _stub(action: Dict[str, Any]) -> Dict[str, Any]:
    """Structured soft-fail when no anthropic key is configured."""
    return {
        "id": action["id"],
        "action_type": action["action_type"],
        "status": action.get("status", "pending"),
        "generated_output": "",
        "stub": True,
        "reason": "no_anthropic_key",
        "how_to_enable": "Set ANTHROPIC_API_KEY (config.settings.anthropic_api_key) to let the Action Engine generate this deliverable with Claude.",
    }


async def generate_action(action_id: str) -> Dict[str, Any]:
    """Produce the deliverable for ONE action via Claude; store in
    generated_output and set status='generated'."""
    action = await fetch_one("SELECT * FROM geo_actions WHERE id = ?", (action_id,))
    if not action:
        raise ValueError(f"unknown action {action_id}")
    at = action["action_type"]
    ws = action.get("workspace_id") or "default"
    payload = from_json(action.get("payload") or "{}", {}) or {}

    if not settings.has_claude_api:
        return _stub(action)

    # metadata_rewrite for an existing URL delegates to metadata_engine.
    if at == "metadata_rewrite" and action.get("target_url") and metadata_engine is not None:
        try:
            pkg = await metadata_engine.generate_for_url(ws, action["target_url"])
            if pkg:
                output = to_json(pkg)
                await _store_output(action_id, output)
                return {**action, "status": "generated", "generated_output": output}
        except Exception as e:
            logger.warning("metadata_engine delegation failed (%s); falling back inline", e)

    user = _build_user_prompt(at, action, payload)
    engine = ClaudeEngine()
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": user}],
            system=_SYS.get(at, _SYS["content_brief"]),
            max_tokens=1800,
            temperature=0.4 if at in ("reddit_seed", "content_brief", "comparison_page") else 0.2,
            project_id=ws,
            workspace_id=ws,
            operation=f"action_{at}",
            reference_id=action_id,
            use_cache=True,
        )
    except Exception as e:
        logger.warning("generate_action %s failed: %s", action_id, e)
        return {
            "id": action_id, "action_type": at, "status": action.get("status"),
            "generated_output": "", "error": str(e),
        }

    output = (resp.get("content") or "").strip()
    await _store_output(action_id, output)
    return {
        **action, "status": "generated", "generated_output": output,
        "cost": resp.get("cost"), "model": resp.get("model"),
    }


def _build_user_prompt(at: str, action: Dict[str, Any], payload: Dict[str, Any]) -> str:
    desc = action.get("description") or ""
    url = action.get("target_url") or ""
    if at == "schema_install":
        return (
            f"Page URL: {url}\n"
            f"Schema type to install: {payload.get('schema_type','')}\n"
            f"Missing fields: {json.dumps(payload.get('missing_fields') or [])}\n"
            f"Why it matters: {desc}\n"
            "Emit the JSON-LD now."
        )
    if at in ("content_brief", "comparison_page"):
        return (
            f"Target prompt: {payload.get('prompt_text','')}\n"
            f"Competitor leader: {payload.get('competitor_domain','')}\n"
            f"Diagnosis of why we lose: {payload.get('diagnosis','')}\n"
            f"Task: {desc}\n"
            f"Target URL (if rewriting an existing page): {url}\n"
            "Write the brief now."
        )
    if at == "reddit_seed":
        return (
            f"Subreddit: r/{payload.get('subreddit','')}\n"
            f"Thread: {payload.get('thread_title','')} ({payload.get('thread_url', url)})\n"
            f"Discussion type: {payload.get('discussion_type','')}\n"
            f"Suggested action: {payload.get('suggested_action', desc)}\n"
            "Draft the comment/post now (include the disclaimer line)."
        )
    if at == "citation_response":
        return (
            f"Prompt under contention: {payload.get('prompt_text','')}\n"
            f"What the AI is citing / claim to address: {payload.get('diagnosis', desc)}\n"
            f"Competitor: {payload.get('competitor_domain','')}\n"
            "Draft the factual response now."
        )
    if at == "metadata_rewrite":
        return (
            f"Page URL: {url}\n"
            f"Prompt to win: {payload.get('prompt_text', desc)}\n"
            f"Brands already visible in AI Overview: {json.dumps(payload.get('visible_brands') or [])}\n"
            "Produce the metadata package JSON now."
        )
    return desc


async def _store_output(action_id: str, output: str) -> None:
    await execute(
        "UPDATE geo_actions SET generated_output = ?, status = 'generated', "
        "updated_at = datetime('now') WHERE id = ?",
        (output, action_id),
    )


# ── QUEUE OPS ──
async def list_actions(
    workspace_id: str, status: str = "", action_type: str = "", limit: int = 200,
) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM geo_actions WHERE workspace_id = ?"
    params: List[Any] = [workspace_id]
    if status:
        sql += " AND status = ?"
        params.append(status)
    if action_type:
        sql += " AND action_type = ?"
        params.append(action_type)
    sql += " ORDER BY priority DESC, created_at DESC LIMIT ?"
    params.append(int(limit))
    rows = await fetch_all(sql, tuple(params))
    for r in rows:
        r["payload"] = from_json(r.get("payload") or "{}", {})
    return rows


async def update_status(action_id: str, status: str) -> Dict[str, Any]:
    """Approve / dismiss / mark done. Sets completed_at when status='done'."""
    if status not in STATUSES:
        raise ValueError(f"invalid status '{status}' (allowed: {sorted(STATUSES)})")
    action = await fetch_one("SELECT * FROM geo_actions WHERE id = ?", (action_id,))
    if not action:
        raise ValueError(f"unknown action {action_id}")
    if status == "done":
        await execute(
            "UPDATE geo_actions SET status = ?, completed_at = datetime('now'), "
            "updated_at = datetime('now') WHERE id = ?",
            (status, action_id),
        )
    else:
        await execute(
            "UPDATE geo_actions SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, action_id),
        )
    return await fetch_one("SELECT * FROM geo_actions WHERE id = ?", (action_id,)) or {}


async def queue_summary(workspace_id: str) -> Dict[str, Any]:
    """Counts by status + by type, plus a rough pipeline value (sum of
    priority across open actions, used as a relative value proxy)."""
    by_status = await fetch_all(
        "SELECT status, COUNT(*) c FROM geo_actions WHERE workspace_id = ? GROUP BY status",
        (workspace_id,),
    )
    by_type = await fetch_all(
        "SELECT action_type, COUNT(*) c FROM geo_actions WHERE workspace_id = ? GROUP BY action_type",
        (workspace_id,),
    )
    open_row = await fetch_one(
        "SELECT COUNT(*) c, COALESCE(SUM(priority), 0) p FROM geo_actions "
        "WHERE workspace_id = ? AND status IN ('pending','generated','approved')",
        (workspace_id,),
    )
    total_row = await fetch_one(
        "SELECT COUNT(*) c FROM geo_actions WHERE workspace_id = ?", (workspace_id,),
    )
    return {
        "workspace_id": workspace_id,
        "total": (total_row or {}).get("c", 0),
        "by_status": {r["status"]: r["c"] for r in by_status},
        "by_type": {r["action_type"]: r["c"] for r in by_type},
        "open_actions": (open_row or {}).get("c", 0),
        "pipeline_value_score": round(float((open_row or {}).get("p", 0) or 0), 2),
    }


__all__ = [
    "init", "harvest_actions", "generate_action", "list_actions",
    "update_status", "queue_summary", "ACTION_TYPES", "STATUSES",
]
