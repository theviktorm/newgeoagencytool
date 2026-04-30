"""
Momentus AI — YouTube GEO Optimizer

Generates an authority-engineered package for a video — title,
description, chapter timestamps, FAQ extractions, embedding strategy,
VideoObject schema recommendation — so videos function as AI authority
assets, not "content uploaded somewhere".

Two flows:
  - generate(workspace_id, input)  — fresh package from a strategic brief
  - audit(workspace_id, video_url) — diagnose an existing video

Both persist into youtube_assets.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from .claude_engine import ClaudeEngine
from .database import (
    execute, fetch_all, fetch_one, gen_id, to_json, from_json,
)

logger = logging.getLogger("geo.youtube")

GEN_SYSTEM = """You produce GEO-optimised YouTube publish packages so AI
systems (ChatGPT, Perplexity, Gemini, Google AI Overview) can confidently
cite the video as an authority signal.

Inputs:
  - topic, expert name, connected service, target prompts, goal
  - optional: existing transcript / summary

Output STRICT JSON only:
{
  "optimized_title": "≤70 chars, entity + intent, no clickbait",
  "description": "5-9 lines, first 2 lines self-contained for AI extraction.
                  Include service link, expert link, comparison page link,
                  contact CTA — placeholders OK like {{service_url}}",
  "chapters": [{"ts":"00:00","title":"..."}],
  "faq_extractions": [{"q":"...","a":"≤300 chars"}],
  "embed_strategy": "1-paragraph plan: which page(s) to embed it on and why",
  "schema_recommendation": "JSON-LD VideoObject template as a STRING (escaped)",
  "rationale": "ONE sentence on the strongest GEO move"
}"""

AUDIT_SYSTEM = """You audit a YouTube video for GEO authority weakness.

Given title + description + (optional) transcript + target prompts, output:
{
  "audit_findings": [{"field":"title|description|chapters|...","issue":"...","severity":"high|medium|low"}],
  "rewrite": { ...full publish package, same shape as the generator... }
}"""


# ═══════════════════════════════════════════════════════════════
# GENERATE
# ═══════════════════════════════════════════════════════════════

async def generate(
    workspace_id: str,
    topic: str,
    expert_name: str = "",
    connected_service: str = "",
    target_prompt_ids: Optional[List[str]] = None,
    goal: str = "trust",
    transcript: str = "",
    video_url: str = "",
) -> Dict[str, Any]:
    target_prompt_ids = target_prompt_ids or []
    target_prompts = await _load_prompts(workspace_id, target_prompt_ids)
    pkg = await _claude_generate(
        topic=topic, expert=expert_name, service=connected_service,
        prompts=target_prompts, goal=goal, transcript=transcript,
    )
    if not pkg:
        return {}
    return await _persist(
        workspace_id, video_url, topic, expert_name, connected_service,
        target_prompt_ids, goal, pkg, audit_findings=[],
    )


# ═══════════════════════════════════════════════════════════════
# AUDIT
# ═══════════════════════════════════════════════════════════════

async def audit(
    workspace_id: str, video_url: str,
    title: str = "", description: str = "", transcript: str = "",
    target_prompt_ids: Optional[List[str]] = None,
    expert_name: str = "", connected_service: str = "", goal: str = "trust",
) -> Dict[str, Any]:
    target_prompt_ids = target_prompt_ids or []
    target_prompts = await _load_prompts(workspace_id, target_prompt_ids)
    engine = ClaudeEngine()
    user = (
        f"Video URL: {video_url}\n"
        f"Title: {title}\nDescription:\n{description}\n\n"
        f"Transcript excerpt:\n{(transcript or '')[:5000]}\n\n"
        f"Target prompts: {json.dumps([p['text'] for p in target_prompts])}"
    )
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": user}],
            system=AUDIT_SYSTEM, max_tokens=1100, temperature=0.1,
            operation="youtube_audit", use_cache=True,
        )
    except Exception as e:
        logger.warning("youtube audit failed: %s", e)
        return {}
    parsed = _safe_json(resp.get("content", "")) or {}
    rewrite = parsed.get("rewrite") or {}
    findings = parsed.get("audit_findings") or []
    if not rewrite:
        return {"audit_findings": findings, "rewrite": {}}
    return await _persist(
        workspace_id, video_url, title or rewrite.get("optimized_title", ""),
        expert_name, connected_service, target_prompt_ids, goal,
        rewrite, audit_findings=findings,
    )


async def list_assets(workspace_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    return await fetch_all(
        "SELECT * FROM youtube_assets WHERE workspace_id = ? "
        "ORDER BY generated_at DESC LIMIT ?",
        (workspace_id, limit),
    )


async def export_publish_package(asset_id: str) -> Dict[str, Any]:
    """Return a flat publish-ready bundle the content team can paste straight
    into YouTube + their CMS."""
    a = await fetch_one("SELECT * FROM youtube_assets WHERE id = ?", (asset_id,))
    if not a:
        raise ValueError(f"unknown youtube asset {asset_id}")
    return {
        "title": a.get("optimized_title", ""),
        "description": a.get("description", ""),
        "chapters_block": _format_chapters(from_json(a.get("chapters") or "[]")),
        "faq_block": from_json(a.get("faq_extractions") or "[]"),
        "embed_strategy": a.get("embed_strategy", ""),
        "schema_jsonld": a.get("schema_recommendation", ""),
    }


# ═══════════════════════════════════════════════════════════════
# CLAUDE + PERSIST
# ═══════════════════════════════════════════════════════════════

async def _claude_generate(
    topic: str, expert: str, service: str,
    prompts: List[Dict[str, Any]], goal: str, transcript: str,
) -> Optional[Dict[str, Any]]:
    engine = ClaudeEngine()
    user = (
        f"Topic: {topic}\nExpert: {expert}\nConnected service: {service}\n"
        f"Goal: {goal}\n"
        f"Target prompts: {json.dumps([p['text'] for p in prompts])}\n\n"
        f"Transcript / summary:\n{(transcript or '')[:5000]}"
    )
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": user}],
            system=GEN_SYSTEM, max_tokens=1100, temperature=0.2,
            operation="youtube_generate", use_cache=True,
        )
    except Exception as e:
        logger.warning("youtube generate failed: %s", e)
        return None
    return _safe_json(resp.get("content", ""))


async def _persist(
    workspace_id: str, video_url: str, topic: str, expert: str, service: str,
    target_prompt_ids: List[str], goal: str, pkg: Dict[str, Any],
    audit_findings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    aid = gen_id("yt-")
    await execute(
        "INSERT INTO youtube_assets "
        "(id, workspace_id, video_url, topic, expert_name, connected_service, "
        " target_prompts, goal, optimized_title, description, chapters, "
        " faq_extractions, embed_strategy, schema_recommendation, "
        " audit_findings) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            aid, workspace_id, video_url, topic, expert, service,
            to_json(target_prompt_ids), goal,
            pkg.get("optimized_title", "")[:300],
            pkg.get("description", "")[:8000],
            to_json(pkg.get("chapters") or []),
            to_json(pkg.get("faq_extractions") or []),
            pkg.get("embed_strategy", "")[:2000],
            pkg.get("schema_recommendation", "")[:8000],
            to_json(audit_findings),
        ),
    )
    pkg["id"] = aid
    pkg["audit_findings"] = audit_findings
    return pkg


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

async def _load_prompts(workspace_id: str, prompt_ids: List[str]) -> List[Dict[str, Any]]:
    if not prompt_ids:
        return []
    return await fetch_all(
        "SELECT id, text, prompt_type, buyer_stage FROM prompts "
        "WHERE workspace_id = ? AND id IN ("
        + ",".join("?" * len(prompt_ids)) + ")",
        (workspace_id, *prompt_ids),
    )


def _format_chapters(chapters: List[Dict[str, Any]]) -> str:
    return "\n".join(
        f"{c.get('ts','00:00')} {c.get('title','').strip()}"
        for c in (chapters or [])
        if isinstance(c, dict)
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
