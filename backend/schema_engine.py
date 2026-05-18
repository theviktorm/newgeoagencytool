"""
Momentus AI — Schema Opportunity Engine

Strategic schema diagnosis (NOT a JSON-LD generator). Audits both our pages
and competitor pages, scores schema breadth + depth, then maps that against
target prompts to say:

  "You are losing decision-stage prompts because your Physician schema
   is missing surgeon-specific authority signals — fix this first."

Pipeline:
  1. Read scraped JSON-LD from sources / scraped_content for given URLs.
  2. Detect schema types present.
  3. Compare against the prompt-type-specific "ideal" schema set.
  4. Compute schema_depth_score + missing_critical list.
  5. Ask Claude for the diagnosis paragraph + ordered remediations.
  6. Persist into schema_audits.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from .claude_engine import ClaudeEngine
from .database import (
    execute, fetch_all, fetch_one, gen_id, to_json, from_json,
)
from .scraper import scrape_urls

logger = logging.getLogger("geo.schema_engine")

# Prompt-type → schema priority. Higher values matter more for that intent.
SCHEMA_PRIORITY = {
    "decision": {
        "Physician": 5, "Person": 4, "Review": 4, "AggregateRating": 3,
        "MedicalOrganization": 4, "FAQPage": 3, "LocalBusiness": 3,
    },
    "trust": {
        "Review": 5, "AggregateRating": 5, "Person": 4, "Physician": 4,
        "MedicalOrganization": 3, "Article": 2,
    },
    "comparison": {
        "FAQPage": 5, "MedicalWebPage": 4, "Article": 3, "Service": 3,
        "ItemList": 3,
    },
    "objection": {
        "FAQPage": 5, "Review": 4, "MedicalWebPage": 3,
    },
    "purchase": {
        "Service": 5, "LocalBusiness": 5, "MedicalOrganization": 4,
        "PriceSpecification": 3, "Offer": 3,
    },
    "informational": {
        "Article": 4, "MedicalWebPage": 3, "FAQPage": 3,
    },
}

# Critical schema by vertical-agnostic prompt-stage
DEFAULT_REQUIRED = ["Organization", "WebSite", "BreadcrumbList"]


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def audit_url(
    workspace_id: str,
    url: str,
    target_prompts: Optional[List[str]] = None,
    is_competitor: bool = False,
    use_cache: bool = True,
) -> Dict[str, Any]:
    """Audit one URL: detect schemas, score against the target prompts'
    ideal sets, generate diagnosis + remediations."""
    target_prompts = target_prompts or []
    schemas = await _load_schemas_for_url(workspace_id, url)
    if not schemas:
        # Not yet scraped (or scraped but no JSON-LD captured) — pull on demand
        try:
            scraped = await scrape_urls([url], project_id=workspace_id)
            schemas = _extract_schemas_from_scrape(scraped, url) or []
        except Exception as e:
            logger.warning("schema audit auto-scrape %s failed: %s", url, e)
            schemas = []

    types_present = sorted({_jsonld_type(s) for s in schemas if s} - {""})

    prompt_rows: List[Dict[str, Any]] = []
    if target_prompts:
        prompt_rows = await fetch_all(
            "SELECT id, text, prompt_type, buyer_stage FROM prompts "
            "WHERE workspace_id = ? AND id IN (" + ",".join("?" * len(target_prompts)) + ")",
            (workspace_id, *target_prompts),
        )

    ideal: Dict[str, int] = {}
    for t in DEFAULT_REQUIRED:
        ideal[t] = ideal.get(t, 0) + 2
    for p in prompt_rows:
        for t, w in SCHEMA_PRIORITY.get(p.get("prompt_type", "informational"), {}).items():
            ideal[t] = max(ideal.get(t, 0), w)

    have_set = set(types_present)
    missing_critical = [
        {
            "type": t,
            "weight": w,
            "severity": "high" if w >= 4 else ("medium" if w >= 3 else "low"),
            "why_it_hurts": _why(t),
        }
        for t, w in sorted(ideal.items(), key=lambda kv: -kv[1])
        if t not in have_set
    ]

    # Depth: how many fields per node, average across schemas. Cap at 100.
    depth = _avg_depth(schemas)
    coverage = (len(have_set & set(ideal.keys())) / max(1, len(ideal))) * 100
    # Combine breadth (coverage) and field depth.
    depth_score = (coverage * 0.7) + (depth * 0.3)

    diagnosis, actions = "", []
    if use_cache:
        diagnosis, actions = await _claude_diagnose(
            url=url, types_present=types_present,
            missing_critical=missing_critical, prompts=prompt_rows,
        )

    audit_id = gen_id("sa-")
    await execute(
        "INSERT INTO schema_audits "
        "(id, workspace_id, page_url, is_competitor, schema_types, "
        " schema_depth_score, missing_critical, target_prompts, diagnosis, "
        " recommendations, raw_jsonld) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            audit_id, workspace_id, url, 1 if is_competitor else 0,
            to_json(types_present), depth_score,
            to_json(missing_critical), to_json(target_prompts),
            diagnosis, to_json(actions),
            to_json(schemas)[:60000],  # protect SQLite row size
        ),
    )
    return {
        "id": audit_id, "url": url, "schema_types": types_present,
        "schema_depth_score": depth_score, "missing_critical": missing_critical,
        "diagnosis": diagnosis, "recommendations": actions,
    }


async def audit_workspace(workspace_id: str, top_n: int = 20) -> Dict[str, Any]:
    """Audit our top-cited URLs + the leading competitor's top URLs."""
    own_urls = await fetch_all(
        "SELECT url FROM sources WHERE project_id = ? "
        "ORDER BY total_citation_count DESC LIMIT ?",
        (workspace_id, top_n),
    )
    out: List[Dict[str, Any]] = []
    for r in own_urls:
        try:
            out.append(await audit_url(workspace_id, r["url"], use_cache=True))
        except Exception as e:
            logger.warning("audit_url %s failed: %s", r.get("url"), e)
    return {"audited": len(out), "details": out[:25]}


async def list_audits(workspace_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    return await fetch_all(
        "SELECT * FROM schema_audits WHERE workspace_id = ? "
        "ORDER BY analyzed_at DESC LIMIT ?",
        (workspace_id, limit),
    )


# ═══════════════════════════════════════════════════════════════
# SCHEMA EXTRACTION
# ═══════════════════════════════════════════════════════════════

async def _load_schemas_for_url(workspace_id: str, url: str) -> Optional[List[Any]]:
    """If we already scraped this URL, use the cached JSON-LD."""
    row = await fetch_one(
        "SELECT json_ld, json_ld_data FROM scraped_content WHERE project_id = ? AND url = ? "
        "ORDER BY scraped_at DESC LIMIT 1",
        (workspace_id, url),
    )
    if not row:
        return None
    raw = row.get("json_ld") or row.get("json_ld_data") or "[]"
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except Exception:
            data = []
    else:
        data = raw or []
    if isinstance(data, dict):
        data = [data]
    return data or []


def _extract_schemas_from_scrape(scrape: Any, url: str) -> List[Any]:
    if not scrape:
        return []
    pages = scrape
    if isinstance(scrape, dict):
        pages = scrape.get("pages") or scrape.get("results") or list(scrape.values())
    for p in pages or []:
        if not isinstance(p, dict):
            continue
        if p.get("url") == url:
            schema = p.get("schema") or p.get("jsonld") or p.get("json_ld") or []
            if isinstance(schema, dict):
                schema = [schema]
            return schema or []
    return []


def _jsonld_type(node: Any) -> str:
    if isinstance(node, dict):
        t = node.get("@type") or node.get("type") or ""
        if isinstance(t, list):
            return ",".join(str(x) for x in t)
        return str(t)
    return ""


def _avg_depth(schemas: List[Any]) -> float:
    if not schemas:
        return 0.0
    total, n = 0, 0
    for s in schemas:
        if isinstance(s, dict):
            total += min(20, _count_keys(s))
            n += 1
    if not n:
        return 0.0
    avg = total / n
    return min(100.0, (avg / 20.0) * 100.0)


def _count_keys(d: Dict[str, Any], depth: int = 0) -> int:
    if depth > 4:
        return 0
    n = len(d)
    for v in d.values():
        if isinstance(v, dict):
            n += _count_keys(v, depth + 1)
        elif isinstance(v, list):
            for el in v:
                if isinstance(el, dict):
                    n += _count_keys(el, depth + 1)
    return n


# ═══════════════════════════════════════════════════════════════
# CLAUDE DIAGNOSIS
# ═══════════════════════════════════════════════════════════════

SCHEMA_DIAG_SYSTEM = """You diagnose schema.org structured-data gaps from a GEO standpoint.

Given a URL, the schema types currently present, the missing critical types
(with weights and why_it_hurts), and the target prompts the page should win,
output STRICT JSON:

{
  "diagnosis": "ONE crisp paragraph: which missing schema is hurting which
                prompts most, and the biggest leverage if fixed",
  "actions": [
     {"step":"...","priority":"high|medium|low","impact":"prompt-stage to lift"}
  ]
}

Be specific. Don't recommend 'add schema' generically."""


async def _claude_diagnose(
    url: str,
    types_present: List[str],
    missing_critical: List[Dict[str, Any]],
    prompts: List[Dict[str, Any]],
) -> Tuple[str, List[Dict[str, Any]]]:
    engine = ClaudeEngine()
    user = (
        f"URL: {url}\n"
        f"Schema types present: {types_present}\n"
        f"Missing critical: {json.dumps(missing_critical)}\n"
        f"Target prompts: "
        + json.dumps([{"text": p["text"], "stage": p.get("buyer_stage", ""),
                        "type": p.get("prompt_type", "")} for p in prompts])
    )
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": user}],
            system=SCHEMA_DIAG_SYSTEM, max_tokens=600, temperature=0.1,
            operation="schema_diagnose", use_cache=True,
        )
    except Exception as e:
        logger.warning("schema diagnose failed: %s", e)
        return "", []
    parsed = _safe_json(resp.get("content", "")) or {}
    return parsed.get("diagnosis", "")[:1500], parsed.get("actions") or []


def _why(t: str) -> str:
    return {
        "Physician": "AI cannot confidently link your doctors to specialties without it.",
        "Review": "Decision-stage prompts weight third-party trust signals heavily.",
        "AggregateRating": "Star/aggregate ratings extract well into AI Overview answers.",
        "FAQPage": "FAQ extraction is the #1 path into featured-snippet/AIO citation.",
        "MedicalOrganization": "Establishes the institution as an entity AI can cite.",
        "LocalBusiness": "Required for local pack + Google Business connection.",
        "Service": "Anchors the offering AI is asked to compare.",
        "Person": "Surgeon/expert authority requires Person + sameAs links.",
        "Organization": "Foundation entity. Without it the brand is fuzzy to LLMs.",
        "WebSite": "Canonical URL + searchAction signals; baseline GEO hygiene.",
        "BreadcrumbList": "Helps AI interpret topical hierarchy.",
    }.get(t, "Helps AI interpret entity relationships and context.")


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
