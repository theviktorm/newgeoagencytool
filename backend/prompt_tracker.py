"""
Momentus AI — Prompt Tracker

The AI SERP tracker. For each prompt in a workspace, query each AI model
(ChatGPT, Gemini, Perplexity, Claude, Google AI Overview) and persist a
prompt_observations row capturing:

  - which brands appeared in the answer + their order
  - which sources the AI cited
  - whether OUR target brand was present + at what position
  - whether Google AI Overview surfaced (and what it contained)
  - the raw answer text + a sentiment hint

Live model querying is plug-in shaped. For models we have direct API access
to (Claude via existing engine; OpenAI / Gemini / Perplexity if their keys
are set in .env) we run real calls. For models we don't have keys for, we
fall back to "Peec replay": if Peec MCP has cited that prompt recently,
we synthesise an observation from the cached mention_events. Either way,
a row lands in prompt_observations and the ownership rollup is updated.

This same module also feeds the Google AI Overview tracker (aio_tracking).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

from .claude_engine import ClaudeEngine
from .config import settings
from .database import (
    execute, fetch_all, fetch_one, gen_id, to_json, from_json,
)
from . import prompt_engine

logger = logging.getLogger("geo.prompt_tracker")

SUPPORTED_MODELS = ("claude", "chatgpt", "gemini", "perplexity", "google_aio")


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def track_prompt(
    workspace_id: str,
    prompt_id: str,
    models: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run a prompt against each requested model, persist observations,
    refresh the ownership rollup. Returns a summary."""
    prompt = await fetch_one("SELECT * FROM prompts WHERE id = ?", (prompt_id,))
    if not prompt:
        raise ValueError(f"unknown prompt {prompt_id}")
    text = prompt["text"]
    target_brand = (prompt.get("target_brand") or "").lower()
    models = [m for m in (models or list(SUPPORTED_MODELS)) if m in SUPPORTED_MODELS]

    batch_id = gen_id("trk-")
    summary = {
        "prompt_id": prompt_id, "batch_id": batch_id,
        "models_run": [], "errors": {}, "our_brand_hits": 0,
    }

    for m in models:
        try:
            obs = await _query_model(m, text, workspace_id, prompt_id, target_brand)
            if obs is None:
                continue
            summary["models_run"].append(m)
            if obs.get("our_brand_present"):
                summary["our_brand_hits"] += 1
            await _persist_observation(workspace_id, prompt_id, m, obs, batch_id)
            if m == "google_aio":
                await _persist_aio(workspace_id, prompt_id, obs)
        except Exception as e:
            logger.warning("track %s/%s failed: %s", m, prompt_id, e)
            summary["errors"][m] = str(e)

    rollup = await prompt_engine.recompute_ownership(workspace_id, prompt_id)
    summary["ownership"] = rollup
    return summary


async def track_workspace(
    workspace_id: str,
    only_high_value: bool = True,
    max_prompts: int = 50,
    models: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Bulk-track a workspace AND log a tracking_runs row (start + finish with
    deltas vs the previous run). Never fails silently: even a pure Peec-replay
    run (no live model keys) records a row with data_source='peec_replay',
    confidence='estimated', and a clear note.

    The actual tracking work lives in `_track_workspace_impl`; this wrapper only
    adds run accounting.
    """
    import time as _time

    started_ms = _time.monotonic()
    run_id = gen_id("trkr-")
    models_used = [m for m in (models or list(SUPPORTED_MODELS)) if m in SUPPORTED_MODELS]
    data_source = _data_source_for_models(models_used)

    # Snapshot ownership BEFORE the run so we can compute win/loss deltas.
    before = await _ownership_snapshot(workspace_id)
    before_citations = await _citation_count(workspace_id)
    before_aio = await _aio_count(workspace_id)

    # Open the run row immediately (proves the engine started).
    try:
        await execute(
            "INSERT INTO tracking_runs (id, workspace_id, started_at, "
            "models_checked, data_source, confidence, notes) "
            "VALUES (?, ?, datetime('now'), ?, ?, ?, ?)",
            (run_id, workspace_id, to_json(models_used), data_source,
             "estimated", "run started"),
        )
    except Exception as e:  # pragma: no cover - logging table best-effort
        logger.warning("tracking_runs insert failed: %s", e)

    errors = 0
    result: Dict[str, Any] = {}
    try:
        result = await _track_workspace_impl(
            workspace_id, only_high_value=only_high_value,
            max_prompts=max_prompts, models=models,
        )
        for d in (result.get("details") or []):
            if not d.get("ok"):
                errors += 1
    except Exception as e:
        errors += 1
        result = {"tracked": 0, "with_our_brand": 0, "details": [],
                  "error": str(e)}
        logger.warning("track_workspace impl failed: %s", e)

    # Snapshot AFTER + compute deltas (best-effort; 0 + note on failure).
    after = await _ownership_snapshot(workspace_id)
    after_citations = await _citation_count(workspace_id)
    after_aio = await _aio_count(workspace_id)
    new_wins, new_losses, delta_note = _ownership_deltas(before, after)
    citation_changes = abs(after_citations - before_citations)
    aio_changes = abs(after_aio - before_aio)

    duration_ms = int((_time.monotonic() - started_ms) * 1000)
    prompts_checked = int(result.get("tracked") or 0)

    # Confidence: verified if any non-replay live model key drove the run.
    confidence = "verified" if data_source == "live" else (
        "estimated" if data_source == "peec_replay" else "estimated"
    )
    note_bits = [delta_note] if delta_note else []
    if data_source == "peec_replay":
        note_bits.append(
            "No live model API keys configured — results synthesised from Peec "
            "MCP replay (mention_events). Treat as estimated."
        )
    elif data_source == "mixed":
        note_bits.append("Mixed live + Peec-replay observations.")
    if result.get("error"):
        note_bits.append("impl error: " + str(result["error"])[:200])
    note = " ".join(note_bits) or "ok"

    try:
        await execute(
            "UPDATE tracking_runs SET finished_at = datetime('now'), "
            "prompts_checked = ?, new_losses = ?, new_wins = ?, "
            "citation_changes = ?, aio_changes = ?, errors = ?, "
            "duration_ms = ?, data_source = ?, confidence = ?, notes = ? "
            "WHERE id = ?",
            (prompts_checked, new_losses, new_wins, citation_changes,
             aio_changes, errors, duration_ms, data_source, confidence,
             note, run_id),
        )
    except Exception as e:  # pragma: no cover
        logger.warning("tracking_runs update failed: %s", e)

    result["run_id"] = run_id
    result["data_source"] = data_source
    result["confidence"] = confidence
    return result


async def _track_workspace_impl(
    workspace_id: str,
    only_high_value: bool = True,
    max_prompts: int = 50,
    models: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Bulk-track every (or every high-value) prompt for a workspace."""
    where = "WHERE workspace_id = ? AND status != 'ignored'"
    args: List[Any] = [workspace_id]
    if only_high_value:
        where += " AND revenue_score >= 50"
    rows = await fetch_all(
        f"SELECT id FROM prompts {where} ORDER BY revenue_score DESC LIMIT ?",
        tuple(args + [max_prompts]),
    )
    summaries: List[Dict[str, Any]] = []
    for r in rows:
        try:
            s = await track_prompt(workspace_id, r["id"], models=models)
            summaries.append({"prompt_id": r["id"], "ok": True,
                              "our_brand_hits": s["our_brand_hits"]})
        except Exception as e:
            summaries.append({"prompt_id": r["id"], "ok": False, "error": str(e)})
    won = sum(1 for s in summaries if s.get("ok") and s.get("our_brand_hits", 0) > 0)
    return {
        "tracked": len(summaries),
        "with_our_brand": won,
        "details": summaries[:25],  # cap response size
    }


# ═══════════════════════════════════════════════════════════════
# MODEL ADAPTERS
#
# Each adapter returns a dict shaped like:
#   {
#     "answer_text": str,
#     "brands": [{"name", "position", "snippet"}, ...],
#     "sources": [{"url", "domain", "title", "position"}, ...],
#     "our_brand_present": bool,
#     "our_brand_position": int,
#     "ai_overview_present": bool,
#     "sentiment_score": float,
#   }
# ═══════════════════════════════════════════════════════════════

async def _query_model(
    model: str, text: str, workspace_id: str, prompt_id: str, target_brand: str,
) -> Optional[Dict[str, Any]]:
    if model == "claude":
        return await _query_claude(text, target_brand)
    if model == "chatgpt":
        return await _query_openai(text, target_brand)
    if model == "perplexity":
        return await _query_perplexity(text, target_brand)
    if model == "gemini":
        return await _query_gemini(text, target_brand)
    if model == "google_aio":
        return await _query_google_aio(text, target_brand)
    return None


# ── Claude (always available if anthropic key set) ──────────────

async def _query_claude(text: str, target_brand: str) -> Optional[Dict[str, Any]]:
    if not settings.anthropic_api_key:
        return None
    engine = ClaudeEngine()
    sys = (
        "You answer the user as if you were a generic AI assistant making a "
        "concrete recommendation. Then on the LAST line emit STRICT JSON "
        "summarising your own answer:\n"
        '{"brands":[{"name":"...","position":1,"snippet":"..."}],'
        '"sources":[{"url":"...","title":"..."}]}'
        " — brands listed in the order you mentioned them, sources in the "
        "order you'd cite them. Do not invent URLs you don't actually "
        "believe exist."
    )
    resp = await engine.call(
        messages=[{"role": "user", "content": text}],
        system=sys, max_tokens=900, temperature=0.2,
        operation="prompt_track_claude",
    )
    return _parse_dual_response(resp.get("content", ""), target_brand)


# ── OpenAI / ChatGPT ────────────────────────────────────────────

async def _query_openai(text: str, target_brand: str) -> Optional[Dict[str, Any]]:
    api_key = getattr(settings, "openai_api_key", "") or _env("OPENAI_API_KEY")
    if not api_key:
        return await _peec_replay(text, target_brand, "chatgpt")
    body = {
        "model": getattr(settings, "openai_default_model", "gpt-4o-mini"),
        "messages": [
            {"role": "system", "content": _ai_system_prompt()},
            {"role": "user", "content": text},
        ],
        "temperature": 0.2,
    }
    async with httpx.AsyncClient(timeout=30.0) as http:
        r = await http.post(
            "https://api.openai.com/v1/chat/completions",
            json=body,
            headers={"Authorization": f"Bearer {api_key}"},
        )
        if r.status_code != 200:
            logger.warning("OpenAI %s: %s", r.status_code, r.text[:200])
            return None
        data = r.json()
    content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
    return _parse_dual_response(content, target_brand)


# ── Perplexity (returns inline citations) ───────────────────────

async def _query_perplexity(text: str, target_brand: str) -> Optional[Dict[str, Any]]:
    api_key = getattr(settings, "perplexity_api_key", "") or _env("PERPLEXITY_API_KEY")
    if not api_key:
        return await _peec_replay(text, target_brand, "perplexity")
    body = {
        "model": "sonar",
        "messages": [
            {"role": "system", "content": _ai_system_prompt()},
            {"role": "user", "content": text},
        ],
    }
    async with httpx.AsyncClient(timeout=30.0) as http:
        r = await http.post(
            "https://api.perplexity.ai/chat/completions",
            json=body,
            headers={"Authorization": f"Bearer {api_key}"},
        )
        if r.status_code != 200:
            return None
        data = r.json()
    msg = (data.get("choices") or [{}])[0].get("message", {})
    content = msg.get("content", "")
    cits = data.get("citations") or []
    obs = _parse_dual_response(content, target_brand) or {
        "answer_text": content, "brands": [], "sources": [],
        "our_brand_present": False, "our_brand_position": 0,
        "ai_overview_present": False, "sentiment_score": 0.0,
    }
    # Augment sources with Perplexity's structured citations
    for i, c in enumerate(cits):
        url = c if isinstance(c, str) else (c.get("url") if isinstance(c, dict) else "")
        if url and not any(s.get("url") == url for s in obs["sources"]):
            obs["sources"].append({"url": url, "title": "", "position": i + 1,
                                   "domain": _domain(url)})
    return obs


# ── Gemini ──────────────────────────────────────────────────────

async def _query_gemini(text: str, target_brand: str) -> Optional[Dict[str, Any]]:
    api_key = getattr(settings, "google_api_key", "") or _env("GOOGLE_API_KEY") or _env("GEMINI_API_KEY")
    if not api_key:
        return await _peec_replay(text, target_brand, "gemini")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
    body = {
        "system_instruction": {"parts": [{"text": _ai_system_prompt()}]},
        "contents": [{"parts": [{"text": text}]}],
    }
    async with httpx.AsyncClient(timeout=30.0) as http:
        r = await http.post(url, json=body)
        if r.status_code != 200:
            return None
        data = r.json()
    parts = (data.get("candidates") or [{}])[0].get("content", {}).get("parts", [])
    content = " ".join(p.get("text", "") for p in parts)
    return _parse_dual_response(content, target_brand)


# ── Google AI Overview (best-effort: SerpAPI if configured) ─────

async def _query_google_aio(text: str, target_brand: str) -> Optional[Dict[str, Any]]:
    serp_key = getattr(settings, "serpapi_key", "") or _env("SERPAPI_KEY")
    if not serp_key:
        return await _peec_replay(text, target_brand, "google_aio")
    async with httpx.AsyncClient(timeout=30.0) as http:
        r = await http.get(
            "https://serpapi.com/search",
            params={"q": text, "engine": "google", "api_key": serp_key},
        )
        if r.status_code != 200:
            return None
        data = r.json()
    aio = data.get("ai_overview") or data.get("ai_overview_inline") or {}
    if not aio:
        return {
            "answer_text": "", "brands": [], "sources": [],
            "our_brand_present": False, "our_brand_position": 0,
            "ai_overview_present": False, "sentiment_score": 0.0,
        }
    text_blocks = aio.get("text_blocks") or []
    answer = " ".join(b.get("snippet", "") for b in text_blocks if isinstance(b, dict))
    refs = aio.get("references") or []
    sources = [
        {"url": ref.get("link") or ref.get("url") or "",
         "title": ref.get("title", ""),
         "domain": _domain(ref.get("link") or ref.get("url") or ""),
         "position": i + 1}
        for i, ref in enumerate(refs) if isinstance(ref, dict)
    ]
    obs = _parse_dual_response(answer, target_brand) or {
        "answer_text": answer, "brands": [], "sources": [],
        "our_brand_present": False, "our_brand_position": 0,
        "sentiment_score": 0.0,
    }
    obs["ai_overview_present"] = True
    if sources:
        obs["sources"] = sources
    return obs


# ── Peec replay fallback ────────────────────────────────────────

async def _peec_replay(text: str, target_brand: str, model: str) -> Optional[Dict[str, Any]]:
    """If we can't hit the model directly, use the most recent Peec MCP
    mention_events that match this prompt to synthesise an observation."""
    rows = await fetch_all(
        "SELECT * FROM mention_events WHERE prompt = ? "
        "AND model_source = ? ORDER BY observed_at DESC LIMIT 25",
        (text, _model_alias(model)),
    )
    if not rows:
        return None
    sources = []
    brands_set: List[str] = []
    for i, r in enumerate(rows[:10]):
        url = r.get("url") or ""
        d = _domain(url)
        if not d:
            continue
        sources.append({"url": url, "domain": d, "title": "", "position": i + 1})
        if d not in brands_set:
            brands_set.append(d)
    brands = [{"name": b, "position": i + 1, "snippet": ""} for i, b in enumerate(brands_set)]
    our_present = bool(target_brand and any(target_brand in b["name"] for b in brands))
    our_pos = next((b["position"] for b in brands if target_brand and target_brand in b["name"]), 0)
    return {
        "answer_text": "", "brands": brands, "sources": sources,
        "our_brand_present": our_present, "our_brand_position": our_pos,
        "ai_overview_present": False, "sentiment_score": 0.0,
    }


def _model_alias(model: str) -> str:
    return {
        "chatgpt": "ChatGPT",
        "gemini": "Gemini",
        "perplexity": "Perplexity",
        "claude": "Claude",
        "google_aio": "Google",
    }.get(model, model.title())


# ═══════════════════════════════════════════════════════════════
# PARSING
# ═══════════════════════════════════════════════════════════════

_BRAND_BLOCKLIST = {"i", "the", "you", "we", "they", "them"}


def _parse_dual_response(content: str, target_brand: str) -> Dict[str, Any]:
    """The AI is asked to follow free-form answer with a JSON tail. This
    parses both halves; if no JSON is found, falls back to regex extraction."""
    if not content:
        return {
            "answer_text": "", "brands": [], "sources": [],
            "our_brand_present": False, "our_brand_position": 0,
            "ai_overview_present": False, "sentiment_score": 0.0,
        }
    json_blob: Dict[str, Any] = {}
    answer_text = content
    s, e = content.rfind("{"), content.rfind("}")
    if 0 <= s < e:
        try:
            json_blob = json.loads(content[s:e + 1])
            answer_text = content[:s].strip()
        except Exception:
            json_blob = {}

    brands_raw = json_blob.get("brands") or []
    if not brands_raw:
        brands_raw = _regex_brands(answer_text or content)
    brands: List[Dict[str, Any]] = []
    for i, b in enumerate(brands_raw[:20]):
        if isinstance(b, str):
            name = b.strip()
            brands.append({"name": name, "position": i + 1, "snippet": ""})
        elif isinstance(b, dict):
            n = (b.get("name") or "").strip()
            if not n:
                continue
            brands.append({
                "name": n,
                "position": int(b.get("position") or i + 1),
                "snippet": b.get("snippet", ""),
            })
    sources_raw = json_blob.get("sources") or []
    sources: List[Dict[str, Any]] = []
    for i, s in enumerate(sources_raw[:20]):
        if isinstance(s, dict):
            url = s.get("url") or ""
            sources.append({
                "url": url, "domain": _domain(url),
                "title": s.get("title", ""), "position": i + 1,
            })

    target = target_brand.lower()
    our_present, our_pos = False, 0
    if target:
        for b in brands:
            if target in (b.get("name") or "").lower():
                our_present = True
                our_pos = b.get("position", 0)
                break

    return {
        "answer_text": (answer_text or "")[:4000],
        "brands": brands,
        "sources": sources,
        "our_brand_present": our_present,
        "our_brand_position": our_pos,
        "ai_overview_present": False,
        "sentiment_score": 0.0,
    }


def _regex_brands(text: str) -> List[str]:
    """Capitalised multi-word phrases as a brand fallback."""
    if not text:
        return []
    candidates = re.findall(r"\b[A-Z][\w&]+(?:\s+[A-Z][\w&]+){0,3}\b", text)
    seen, out = set(), []
    for c in candidates:
        if c.lower() in _BRAND_BLOCKLIST:
            continue
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out[:10]


# ═══════════════════════════════════════════════════════════════
# PERSISTENCE
# ═══════════════════════════════════════════════════════════════

async def _persist_observation(
    workspace_id: str, prompt_id: str, model: str,
    obs: Dict[str, Any], batch_id: str,
) -> None:
    await execute(
        "INSERT INTO prompt_observations "
        "(id, prompt_id, workspace_id, model, brands_appeared, sources_cited, "
        " our_brand_present, our_brand_position, ai_overview_present, "
        " answer_text, sentiment_score, sync_batch_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            gen_id("po-"), prompt_id, workspace_id, model,
            to_json(obs.get("brands") or []),
            to_json(obs.get("sources") or []),
            1 if obs.get("our_brand_present") else 0,
            int(obs.get("our_brand_position") or 0),
            1 if obs.get("ai_overview_present") else 0,
            (obs.get("answer_text") or "")[:4000],
            float(obs.get("sentiment_score") or 0.0),
            batch_id,
        ),
    )


async def _persist_aio(workspace_id: str, prompt_id: str, obs: Dict[str, Any]) -> None:
    sources = obs.get("sources") or []
    publishers = sorted({s.get("domain") for s in sources if s.get("domain")})
    await execute(
        "INSERT INTO aio_tracking (id, workspace_id, prompt_id, aio_present, "
        "our_brand_in_aio, visible_brands, source_urls, publisher_domains, "
        "snapshot_html) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            gen_id("aio-"), workspace_id, prompt_id,
            1 if obs.get("ai_overview_present") else 0,
            1 if obs.get("our_brand_present") else 0,
            to_json([b.get("name") for b in (obs.get("brands") or [])]),
            to_json([s.get("url") for s in sources]),
            to_json(list(publishers)),
            (obs.get("answer_text") or "")[:4000],
        ),
    )


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

def _ai_system_prompt() -> str:
    return (
        "Answer the user's question concretely as if you were a generic AI "
        "assistant in 2026. After your answer, on the LAST line, emit STRICT "
        "JSON summarising the brands and sources you mentioned, in mention "
        "order:\n"
        '{"brands":[{"name":"...","position":1,"snippet":"..."}],'
        '"sources":[{"url":"...","title":"..."}]}'
    )


def _domain(url: str) -> str:
    if not url:
        return ""
    try:
        return (urlparse(url).hostname or "").lower().lstrip("www.")
    except Exception:
        return ""


def _env(name: str) -> str:
    import os
    return os.environ.get(name, "")


# ═══════════════════════════════════════════════════════════════
# TRACK-ALL LOGGING (Explainability Phase 2)
# ═══════════════════════════════════════════════════════════════

def _data_source_for_models(models: List[str]) -> str:
    """Decide whether a run is 'live', 'peec_replay', or 'mixed' based on which
    model API keys are actually configured.

    - claude is live iff anthropic_api_key is set.
    - chatgpt/perplexity/gemini/google_aio are live iff their respective key is
      configured (settings attr or env var); otherwise they fall back to Peec
      replay inside the adapters.
    """
    live, replay = 0, 0
    for m in models:
        if _model_has_live_key(m):
            live += 1
        else:
            replay += 1
    if live and replay:
        return "mixed"
    if live:
        return "live"
    return "peec_replay"


def _model_has_live_key(model: str) -> bool:
    if model == "claude":
        return bool(settings.anthropic_api_key)
    if model == "chatgpt":
        return bool(getattr(settings, "openai_api_key", "") or _env("OPENAI_API_KEY"))
    if model == "perplexity":
        return bool(getattr(settings, "perplexity_api_key", "") or _env("PERPLEXITY_API_KEY"))
    if model == "gemini":
        return bool(getattr(settings, "google_api_key", "") or _env("GOOGLE_API_KEY") or _env("GEMINI_API_KEY"))
    if model == "google_aio":
        return bool(getattr(settings, "serpapi_key", "") or _env("SERPAPI_KEY"))
    return False


async def _ownership_snapshot(workspace_id: str) -> Dict[str, Dict[str, float]]:
    """{prompt_id: {our, leader}} from prompt_ownership for delta computation."""
    rows = await fetch_all(
        "SELECT prompt_id, our_score, leader_score FROM prompt_ownership "
        "WHERE workspace_id = ?",
        (workspace_id,),
    )
    return {
        r["prompt_id"]: {
            "our": float(r.get("our_score") or 0),
            "leader": float(r.get("leader_score") or 0),
        }
        for r in rows
    }


def _is_won(state: Dict[str, float]) -> bool:
    return state["our"] >= 50 and state["our"] >= state["leader"] and state["our"] > 0


def _is_lost(state: Dict[str, float]) -> bool:
    return state["leader"] > state["our"] and state["leader"] > 0


def _ownership_deltas(
    before: Dict[str, Dict[str, float]],
    after: Dict[str, Dict[str, float]],
) -> Tuple[int, int, str]:
    """Return (new_wins, new_losses, note) comparing before/after ownership.

    A new win = a prompt that flipped into 'won' state this run; a new loss =
    flipped into 'lost'. Prompts with no prior snapshot count if they land
    directly in won/lost. Best-effort: returns (0, 0, note) if data is missing.
    """
    if not after:
        return 0, 0, "no ownership data after run (delta=0)"
    new_wins, new_losses = 0, 0
    for pid, aft in after.items():
        bef = before.get(pid)
        won_now, lost_now = _is_won(aft), _is_lost(aft)
        if bef is None:
            if won_now:
                new_wins += 1
            elif lost_now:
                new_losses += 1
            continue
        if won_now and not _is_won(bef):
            new_wins += 1
        if lost_now and not _is_lost(bef):
            new_losses += 1
    if not before:
        return new_wins, new_losses, "first run for workspace (no prior baseline)"
    return new_wins, new_losses, ""


async def _citation_count(workspace_id: str) -> int:
    row = await fetch_one(
        "SELECT COUNT(*) AS n FROM prompt_observations WHERE workspace_id = ?",
        (workspace_id,),
    )
    return int((row or {}).get("n") or 0)


async def _aio_count(workspace_id: str) -> int:
    row = await fetch_one(
        "SELECT COUNT(*) AS n FROM aio_tracking WHERE workspace_id = ? "
        "AND aio_present = 1",
        (workspace_id,),
    )
    return int((row or {}).get("n") or 0)


async def list_tracking_runs(workspace_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Recent tracking runs for the workspace, newest first. Decodes the
    models_checked JSON for the dashboard. Defensive: returns [] on empty."""
    rows = await fetch_all(
        "SELECT * FROM tracking_runs WHERE workspace_id = ? "
        "ORDER BY started_at DESC LIMIT ?",
        (workspace_id, int(limit)),
    )
    for r in rows:
        r["models_checked"] = from_json(r.get("models_checked") or "[]", [])
    return rows
