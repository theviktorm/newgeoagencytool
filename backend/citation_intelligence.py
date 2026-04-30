"""
Momentus AI — Citation Intelligence Layer

For every prompt where a competitor outranks us in AI answers, this module
finds the competitor's "winning" page, scrapes it, and asks Claude:

  "Why does AI cite THIS page over ours?"

Output is a structured diagnosis: content_score, schema_score,
authority_score, reddit_score, youtube_score, entity_score, plus a free-text
diagnosis and an ordered list of remediation actions. Persisted into
citation_diagnostics so the dashboard can show "WHY are we losing?" — not
just "we are losing".
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .claude_engine import ClaudeEngine
from .database import (
    execute, fetch_all, fetch_one, gen_id, to_json, from_json,
)
from .scraper import scrape_urls

logger = logging.getLogger("geo.citation_intel")


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def diagnose_prompt(
    workspace_id: str,
    prompt_id: str,
    competitor_domain: str = "",
    max_pages: int = 2,
) -> Dict[str, Any]:
    """Run the full diagnosis for a single prompt.

    1. From prompt_observations, identify the URL(s) that the AI cited for
       `competitor_domain` (or the leader if none specified).
    2. Scrape those pages.
    3. Pull schema breadth from the scraped JSON-LD.
    4. Ask Claude to grade content/schema/authority/reddit/youtube/entity
       and produce diagnosis + remediation actions.
    5. Persist into citation_diagnostics.
    """
    prompt = await fetch_one("SELECT * FROM prompts WHERE id = ?", (prompt_id,))
    if not prompt:
        raise ValueError(f"unknown prompt {prompt_id}")

    leader = competitor_domain.lower()
    if not leader:
        own = await fetch_one(
            "SELECT leader_domain FROM prompt_ownership WHERE workspace_id = ? AND prompt_id = ?",
            (workspace_id, prompt_id),
        )
        leader = (own or {}).get("leader_domain", "") if own else ""
        if not leader:
            raise ValueError("no competitor leader available — pass competitor_domain explicitly")

    urls = await _winning_urls_for(prompt_id, leader, max_pages)
    if not urls:
        # last-ditch: search peec sources for anything on that domain
        rows = await fetch_all(
            "SELECT url FROM sources WHERE project_id = ? AND url LIKE ? "
            "ORDER BY total_citation_count DESC LIMIT ?",
            (workspace_id, f"%{leader}%", max_pages),
        )
        urls = [r["url"] for r in rows if r.get("url")]
    if not urls:
        raise ValueError(f"no cited URLs found for {leader} on prompt {prompt_id}")

    scraped = await scrape_urls(urls, project_id=workspace_id)
    pages = scraped.get("pages") or scraped.get("results") or scraped or []
    # Normalise scraper output (scrape_urls returns a dict)
    if isinstance(pages, dict):
        pages = list(pages.values())

    diag = await _claude_diagnose(
        prompt_text=prompt["text"],
        prompt_type=prompt.get("prompt_type", ""),
        buyer_stage=prompt.get("buyer_stage", ""),
        competitor_domain=leader,
        pages=pages,
    )
    if not diag:
        raise RuntimeError("Claude diagnosis returned no usable JSON")

    diag_id = gen_id("cd-")
    await execute(
        "INSERT INTO citation_diagnostics "
        "(id, workspace_id, prompt_id, cluster_id, competitor_domain, analyzed_url, "
        " content_score, schema_score, authority_score, reddit_score, youtube_score, "
        " entity_score, diagnosis, actions, raw_evidence) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            diag_id, workspace_id, prompt_id, prompt.get("cluster_id", ""),
            leader, urls[0] if urls else "",
            float(diag.get("content_score") or 0),
            float(diag.get("schema_score") or 0),
            float(diag.get("authority_score") or 0),
            float(diag.get("reddit_score") or 0),
            float(diag.get("youtube_score") or 0),
            float(diag.get("entity_score") or 0),
            diag.get("diagnosis") or "",
            to_json(diag.get("actions") or []),
            to_json({"urls": urls, "pages": [_evidence_for(p) for p in pages]}),
        ),
    )
    diag["id"] = diag_id
    diag["competitor_domain"] = leader
    diag["analyzed_urls"] = urls
    return diag


async def diagnose_workspace(
    workspace_id: str, top_n: int = 10,
) -> Dict[str, Any]:
    """Run diagnosis for the top-N high-revenue prompts where we are losing."""
    losers = await fetch_all(
        """SELECT p.id AS prompt_id, p.text, p.revenue_score,
                  o.our_score, o.leader_score, o.leader_domain
           FROM prompts p
           LEFT JOIN prompt_ownership o
             ON o.workspace_id = p.workspace_id AND o.prompt_id = p.id
           WHERE p.workspace_id = ?
             AND COALESCE(o.leader_score, 0) > COALESCE(o.our_score, 0)
             AND p.revenue_score >= 50
           ORDER BY p.revenue_score DESC, COALESCE(o.leader_score, 0) DESC
           LIMIT ?""",
        (workspace_id, top_n),
    )
    out: List[Dict[str, Any]] = []
    for row in losers:
        try:
            d = await diagnose_prompt(
                workspace_id, row["prompt_id"],
                competitor_domain=row.get("leader_domain") or "",
            )
            out.append({"prompt_id": row["prompt_id"], "ok": True, "diag": d})
        except Exception as e:
            out.append({"prompt_id": row["prompt_id"], "ok": False, "error": str(e)})
    return {"diagnosed": len(out), "details": out}


async def list_diagnostics(workspace_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    return await fetch_all(
        "SELECT * FROM citation_diagnostics WHERE workspace_id = ? "
        "ORDER BY analyzed_at DESC LIMIT ?",
        (workspace_id, limit),
    )


# ═══════════════════════════════════════════════════════════════
# WINNING URL DISCOVERY
# ═══════════════════════════════════════════════════════════════

async def _winning_urls_for(
    prompt_id: str, competitor_domain: str, max_pages: int,
) -> List[str]:
    """From recent prompt_observations, return the top URLs whose domain
    matches the competitor."""
    obs = await fetch_all(
        "SELECT sources_cited FROM prompt_observations WHERE prompt_id = ? "
        "AND observed_at >= datetime('now','-30 days') ORDER BY observed_at DESC LIMIT 20",
        (prompt_id,),
    )
    counts: Dict[str, int] = {}
    for o in obs:
        sources = from_json(o.get("sources_cited") or "[]") or []
        for s in sources or []:
            url = (s.get("url") if isinstance(s, dict) else "") or ""
            d = _domain(url)
            if not d:
                continue
            if competitor_domain in d or d in competitor_domain:
                counts[url] = counts.get(url, 0) + 1
    ranked = sorted(counts.items(), key=lambda kv: -kv[1])
    return [url for url, _ in ranked[:max_pages]]


# ═══════════════════════════════════════════════════════════════
# CLAUDE DIAGNOSIS
# ═══════════════════════════════════════════════════════════════

DIAGNOSIS_SYSTEM = """You are an expert in Generative Engine Optimization (GEO).
For each prompt + a set of scraped competitor pages, score WHY the AI cites
them on a 0-100 scale across these axes:

  content_score:     topical depth, FAQ presence, comparison tables,
                     decision-supporting structure, recovery/objection content
  schema_score:      breadth + depth of schema.org JSON-LD (Organization,
                     MedicalOrganization, Physician, FAQPage, Review, Service,
                     LocalBusiness — entity relationships matter)
  authority_score:   visible PR / publisher mentions / professional voices /
                     external trust signals on the page
  reddit_score:      hints that this brand has Reddit traction (mentions
                     of subreddits / community pages / forum threads)
  youtube_score:     embedded expert videos / video schema / YouTube links
  entity_score:      how cleanly the page identifies the brand-entity
                     relationship (consistent NAP, sameAs, person bios)

Also produce:
  diagnosis:  ONE punchy paragraph explaining WHY AI prefers this competitor
  actions:    ordered list of {step, priority (high/med/low), impact} —
              concrete remediations the user should run

Return STRICT JSON only:
{"content_score":0,"schema_score":0,"authority_score":0,"reddit_score":0,
 "youtube_score":0,"entity_score":0,"diagnosis":"...","actions":[...]}"""


async def _claude_diagnose(
    prompt_text: str,
    prompt_type: str,
    buyer_stage: str,
    competitor_domain: str,
    pages: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not pages:
        return None
    engine = ClaudeEngine()
    page_blocks = []
    for p in pages[:3]:
        body = (p.get("text") or p.get("content") or p.get("body") or "")[:6000]
        title = p.get("title") or ""
        url = p.get("url") or ""
        schema = p.get("schema") or p.get("jsonld") or []
        if not isinstance(schema, list):
            schema = [schema]
        schema_types = sorted({_jsonld_type(s) for s in schema if s})
        page_blocks.append(
            f"URL: {url}\nTitle: {title}\nSchema types found: {schema_types}\n"
            f"Body excerpt:\n{body}\n---"
        )
    user = (
        f"Prompt: {prompt_text}\n"
        f"Prompt type: {prompt_type}\nBuyer stage: {buyer_stage}\n"
        f"Competitor domain: {competitor_domain}\n\n"
        + "\n\n".join(page_blocks)
    )
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": user}],
            system=DIAGNOSIS_SYSTEM,
            max_tokens=900,
            temperature=0.1,
            operation="citation_diagnose",
            use_cache=True,
        )
    except Exception as e:
        logger.warning("Claude diagnose call failed: %s", e)
        return None
    return _safe_json(resp.get("content", ""))


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

def _jsonld_type(node: Any) -> str:
    if isinstance(node, dict):
        t = node.get("@type") or node.get("type") or ""
        if isinstance(t, list):
            return ",".join(str(x) for x in t)
        return str(t)
    return ""


def _evidence_for(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "url": p.get("url", ""),
        "title": p.get("title", ""),
        "schema_count": len(p.get("schema") or p.get("jsonld") or [] or []),
        "word_count": len((p.get("text") or "").split()),
    }


def _domain(url: str) -> str:
    if not url:
        return ""
    try:
        return (urlparse(url).hostname or "").lower().lstrip("www.")
    except Exception:
        return ""


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
