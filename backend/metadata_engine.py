"""
Momentus AI — Metadata + Snippet Optimization Engine

Generates and audits the full metadata package for every draft / page so
GEO citation infrastructure stays intact, not just body content.

Outputs per page:
  seo_title, meta_description, og_title, og_description, canonical_url,
  snippet_target (the question this page should win), faq_extractions,
  internal_links suggestions, aio_compatibility_score, audit_findings.

Two entry points:
  - generate_for_draft(draft_id) — runs after Content Studio finishes
  - audit_url(workspace_id, url)  — for legacy pages already published

Both persist into metadata_packages and are surfaced on the dashboard.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional

from .claude_engine import ClaudeEngine
from .database import (
    execute, fetch_all, fetch_one, gen_id, to_json, from_json,
)

logger = logging.getLogger("geo.metadata")

GEN_SYSTEM = """You are a GEO + AI-snippet engineer.
Given a page (its title, content, and target prompts), produce a strict
JSON metadata package optimised for both:
  (a) AI Overview / ChatGPT / Perplexity citation extraction
  (b) classic CTR via Google blue links

Required JSON shape:
{
  "seo_title": "≤60 chars, contains primary entity + intent",
  "meta_description": "≤155 chars, one decision-supporting sentence",
  "og_title": "≤55 chars",
  "og_description": "≤125 chars",
  "canonical_url": "absolute URL or '' to keep current",
  "snippet_target": "the exact question this page should win in AI answers",
  "faq_extractions": [{"q":"...","a":"≤300 chars"}],
  "internal_links": [{"anchor":"...","reason":"why link this"}],
  "aio_compatibility_score": 0-100,
  "rationale": "ONE sentence on the strongest GEO move you're making"
}

Style: punchy, decision-oriented, no clickbait, no emoji, no all-caps."""


AUDIT_SYSTEM = """You audit an existing page's metadata for GEO weakness.

Given (current title, current meta description, page body excerpt,
target prompts), output:
{
  "audit_findings": [
    {"field":"seo_title|meta_description|...","issue":"...","severity":"high|medium|low"}
  ],
  "rewrite": { ...same shape as the generator... }
}"""


# ═══════════════════════════════════════════════════════════════
# GENERATE
# ═══════════════════════════════════════════════════════════════

async def generate_for_draft(draft_id: str) -> Dict[str, Any]:
    draft = await fetch_one("SELECT * FROM drafts WHERE id = ?", (draft_id,))
    if not draft:
        raise ValueError(f"unknown draft {draft_id}")
    workspace_id = draft.get("workspace_id") or draft.get("project_id") or ""
    target_prompts = await _prompts_for_cluster(
        workspace_id, draft.get("cluster_id", "") or "",
    )
    pkg = await _claude_generate(
        title=draft.get("title", ""),
        body=(draft.get("content") or draft.get("body") or "")[:6000],
        target_prompts=target_prompts,
    )
    if not pkg:
        return {}
    return await _persist_package(workspace_id, "", draft_id, pkg)


async def generate_for_url(workspace_id: str, url: str) -> Dict[str, Any]:
    page = await fetch_one(
        "SELECT * FROM scraped_content WHERE project_id = ? AND url = ? "
        "ORDER BY scraped_at DESC LIMIT 1",
        (workspace_id, url),
    )
    title = (page or {}).get("title") or ""
    body = (page or {}).get("text_content") or (page or {}).get("body") or ""
    target_prompts = await _target_prompts_for_url(workspace_id, url)
    pkg = await _claude_generate(title=title, body=body[:6000],
                                 target_prompts=target_prompts)
    if not pkg:
        return {}
    return await _persist_package(workspace_id, url, "", pkg)


# ═══════════════════════════════════════════════════════════════
# AUDIT
# ═══════════════════════════════════════════════════════════════

async def audit_url(workspace_id: str, url: str) -> Dict[str, Any]:
    page = await fetch_one(
        "SELECT * FROM scraped_content WHERE project_id = ? AND url = ? "
        "ORDER BY scraped_at DESC LIMIT 1",
        (workspace_id, url),
    )
    if not page:
        raise ValueError(f"no scraped page for {url}; scrape it first")
    target_prompts = await _target_prompts_for_url(workspace_id, url)
    engine = ClaudeEngine()
    user = (
        f"URL: {url}\n"
        f"Current title: {page.get('title','')}\n"
        f"Current meta description: {page.get('meta_description','')}\n"
        f"Body excerpt:\n{(page.get('text_content') or page.get('body') or '')[:5000]}\n"
        f"Target prompts: {json.dumps([p['text'] for p in target_prompts])}"
    )
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": user}],
            system=AUDIT_SYSTEM, max_tokens=900, temperature=0.1,
            operation="metadata_audit", use_cache=True,
        )
    except Exception as e:
        logger.warning("metadata audit failed: %s", e)
        return {}
    parsed = _safe_json(resp.get("content", "")) or {}
    rewrite = parsed.get("rewrite") or {}
    findings = parsed.get("audit_findings") or []
    if rewrite:
        rewrite["audit_findings"] = findings
        return await _persist_package(workspace_id, url, "", rewrite)
    return {"audit_findings": findings, "rewrite": {}}


# ═══════════════════════════════════════════════════════════════
# PERSIST
# ═══════════════════════════════════════════════════════════════

async def _persist_package(
    workspace_id: str, url: str, draft_id: str, pkg: Dict[str, Any],
) -> Dict[str, Any]:
    pid = gen_id("md-")
    await execute(
        "INSERT INTO metadata_packages "
        "(id, workspace_id, page_url, draft_id, seo_title, meta_description, "
        " og_title, og_description, canonical_url, snippet_target, "
        " faq_extractions, internal_links, aio_compatibility_score, "
        " audit_findings) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            pid, workspace_id, url, draft_id,
            pkg.get("seo_title", "")[:200], pkg.get("meta_description", "")[:300],
            pkg.get("og_title", "")[:200], pkg.get("og_description", "")[:300],
            pkg.get("canonical_url", "")[:500],
            pkg.get("snippet_target", "")[:300],
            to_json(pkg.get("faq_extractions") or []),
            to_json(pkg.get("internal_links") or []),
            float(pkg.get("aio_compatibility_score") or 0),
            to_json(pkg.get("audit_findings") or []),
        ),
    )
    pkg["id"] = pid
    return pkg


async def list_packages(workspace_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    return await fetch_all(
        "SELECT * FROM metadata_packages WHERE workspace_id = ? "
        "ORDER BY generated_at DESC LIMIT ?",
        (workspace_id, limit),
    )


# ═══════════════════════════════════════════════════════════════
# CLAUDE
# ═══════════════════════════════════════════════════════════════

async def _claude_generate(
    title: str, body: str, target_prompts: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    engine = ClaudeEngine()
    user = (
        f"Title: {title}\n"
        f"Body:\n{body}\n\n"
        f"Target prompts: "
        + json.dumps([p["text"] for p in target_prompts])
    )
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": user}],
            system=GEN_SYSTEM, max_tokens=900, temperature=0.2,
            operation="metadata_generate", use_cache=True,
        )
    except Exception as e:
        logger.warning("metadata generate failed: %s", e)
        return None
    return _safe_json(resp.get("content", ""))


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

async def _prompts_for_cluster(workspace_id: str, cluster_id: str) -> List[Dict[str, Any]]:
    if not cluster_id:
        return []
    return await fetch_all(
        "SELECT id, text, prompt_type, buyer_stage FROM prompts "
        "WHERE workspace_id = ? AND cluster_id = ? "
        "ORDER BY revenue_score DESC LIMIT 8",
        (workspace_id, cluster_id),
    )


async def _target_prompts_for_url(workspace_id: str, url: str) -> List[Dict[str, Any]]:
    """Heuristic: prompts whose target_url matches OR whose cluster contains
    this URL (via cluster_sources)."""
    rows = await fetch_all(
        "SELECT id, text, prompt_type, buyer_stage FROM prompts "
        "WHERE workspace_id = ? AND target_url = ? LIMIT 8",
        (workspace_id, url),
    )
    if rows:
        return rows
    cluster = await fetch_one(
        "SELECT cluster_id FROM cluster_sources cs "
        "JOIN sources s ON s.id = cs.source_id "
        "WHERE s.url = ? LIMIT 1", (url,),
    )
    if not cluster:
        return []
    return await fetch_all(
        "SELECT id, text, prompt_type, buyer_stage FROM prompts "
        "WHERE workspace_id = ? AND cluster_id = ? "
        "ORDER BY revenue_score DESC LIMIT 8",
        (workspace_id, cluster["cluster_id"]),
    )


def _safe_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        t = text.strip()
        if t.startswith("```"):
            t = t.strip("`")
            if t.lower().startswith("json"):
                t = t[4:]
        s, e = t.find("{"), t.rfind("}")
        if s == -1 or e == -1:
            return None
        return json.loads(t[s:e + 1])
    except Exception:
        return None
