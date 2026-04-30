"""
Momentus AI — Reddit GEO Engine

Pulls Reddit citations out of the generic source/citation stream and treats
them as authority infrastructure. Detects:

  - which subreddits surface for our prompts
  - which threads exist; what was the discussion type (recommendation / review / Q&A)
  - which brands were mentioned, with sentiment
  - which prompts have Reddit citations but our brand is absent (opportunity gap)
  - tactical recommendations: "seed a discussion in r/X about Y because the
    competitor's dominance is built on three threads here"

Signal sources:
  1. mention_events / sources where the URL is reddit.com → extract subreddit
  2. prompt_observations.sources_cited where domain == reddit.com
  3. Optional: live JSON fetch of /r/{sub}/comments/{id}.json (no auth needed)
     to enrich a thread row with title + brand mentions.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

from .claude_engine import ClaudeEngine
from .database import (
    execute, fetch_all, fetch_one, gen_id, to_json, from_json,
)

logger = logging.getLogger("geo.reddit")

REDDIT_HEADERS = {"User-Agent": "MomentusAI/1.0 (+https://momentus.ai)"}


# ═══════════════════════════════════════════════════════════════
# DETECTION
# ═══════════════════════════════════════════════════════════════

async def harvest_workspace(workspace_id: str) -> Dict[str, Any]:
    """Scan every recent citation source for Reddit URLs and persist
    one reddit_intel row per unique thread with the brands we can detect."""
    target_brand = ""
    ws = await fetch_one(
        "SELECT brand_name FROM workspaces WHERE id = ?", (workspace_id,),
    )
    if ws and ws.get("brand_name"):
        target_brand = ws["brand_name"].lower()

    threads: Dict[str, Dict[str, Any]] = {}

    # 1. Mention events
    rows = await fetch_all(
        "SELECT url, prompt FROM mention_events WHERE project_id = ? "
        "AND url LIKE '%reddit.com%' ORDER BY observed_at DESC LIMIT 500",
        (workspace_id,),
    )
    for r in rows:
        url = r["url"]
        sub = _extract_subreddit(url)
        if not sub:
            continue
        threads.setdefault(url, {
            "subreddit": sub, "thread_url": url, "prompts": set(),
        })
        if r.get("prompt"):
            threads[url]["prompts"].add(r["prompt"])

    # 2. Prompt observations
    obs = await fetch_all(
        "SELECT prompt_id, sources_cited FROM prompt_observations "
        "WHERE workspace_id = ? AND sources_cited LIKE '%reddit.com%' LIMIT 500",
        (workspace_id,),
    )
    for o in obs:
        srcs = from_json(o.get("sources_cited") or "[]") or []
        for s in srcs:
            url = (s.get("url") if isinstance(s, dict) else "") or ""
            if "reddit.com" not in url:
                continue
            sub = _extract_subreddit(url)
            if not sub:
                continue
            threads.setdefault(url, {
                "subreddit": sub, "thread_url": url, "prompts": set(),
            })
            threads[url].setdefault("prompt_ids", set()).add(o["prompt_id"])

    # Enrich each thread (best-effort, throttled)
    n_persisted = 0
    n_gaps = 0
    async with httpx.AsyncClient(headers=REDDIT_HEADERS, timeout=15.0,
                                 follow_redirects=True) as http:
        for url, t in list(threads.items())[:50]:
            try:
                title, body, mentions = await _fetch_thread_brands(http, url)
            except Exception as e:
                logger.debug("thread fetch %s failed: %s", url, e)
                title, body, mentions = "", "", []
            our_present = bool(target_brand) and any(target_brand in m for m in mentions)
            is_gap = (not our_present) and bool(mentions)
            if is_gap:
                n_gaps += 1
            disc_type = _discussion_type(title + " " + body)
            sentiment_label, sentiment_score = _sentiment(title + " " + body)
            await execute(
                "INSERT INTO reddit_intel "
                "(id, workspace_id, subreddit, thread_url, thread_title, "
                " prompt_id, discussion_type, brands_mentioned, our_brand_mentioned, "
                " sentiment_score, sentiment_label, is_opportunity_gap, "
                " suggested_action) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    gen_id("ri-"), workspace_id, t["subreddit"], url, title[:300],
                    next(iter(t.get("prompt_ids") or []), ""),
                    disc_type,
                    to_json(mentions),
                    1 if our_present else 0,
                    sentiment_score, sentiment_label,
                    1 if is_gap else 0,
                    _suggest_action(t["subreddit"], disc_type, is_gap, our_present),
                ),
            )
            n_persisted += 1
    return {"threads": n_persisted, "opportunity_gaps": n_gaps}


async def list_intel(workspace_id: str, only_gaps: bool = False) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM reddit_intel WHERE workspace_id = ?"
    args: List[Any] = [workspace_id]
    if only_gaps:
        sql += " AND is_opportunity_gap = 1"
    sql += " ORDER BY observed_at DESC LIMIT 200"
    return await fetch_all(sql, tuple(args))


# ═══════════════════════════════════════════════════════════════
# COMMAND CENTER SUMMARY
# ═══════════════════════════════════════════════════════════════

async def command_center(workspace_id: str) -> Dict[str, Any]:
    rows = await fetch_all(
        "SELECT * FROM reddit_intel WHERE workspace_id = ? ORDER BY observed_at DESC LIMIT 500",
        (workspace_id,),
    )
    by_sub: Dict[str, Dict[str, Any]] = {}
    gaps: List[Dict[str, Any]] = []
    brand_count: Dict[str, int] = {}
    for r in rows:
        sub = r["subreddit"]
        s = by_sub.setdefault(sub, {"threads": 0, "gaps": 0, "our_present": 0})
        s["threads"] += 1
        if r["is_opportunity_gap"]:
            s["gaps"] += 1
            gaps.append({
                "subreddit": sub, "thread_url": r["thread_url"],
                "thread_title": r["thread_title"],
                "suggested_action": r["suggested_action"],
            })
        if r["our_brand_mentioned"]:
            s["our_present"] += 1
        for b in (from_json(r["brands_mentioned"] or "[]") or []):
            if not b:
                continue
            brand_count[b] = brand_count.get(b, 0) + 1

    top_brands = sorted(brand_count.items(), key=lambda kv: -kv[1])[:8]
    return {
        "workspace_id": workspace_id,
        "subreddits": by_sub,
        "opportunity_gaps": gaps[:20],
        "top_brands": [{"brand": b, "mentions": n} for b, n in top_brands],
        "thread_count": len(rows),
    }


# ═══════════════════════════════════════════════════════════════
# FETCH + ANALYZE
# ═══════════════════════════════════════════════════════════════

async def _fetch_thread_brands(http: httpx.AsyncClient, url: str) -> Tuple[str, str, List[str]]:
    api_url = url.rstrip("/")
    if not api_url.endswith(".json"):
        api_url = api_url + ".json"
    r = await http.get(api_url)
    if r.status_code != 200:
        return "", "", []
    data = r.json()
    title = ""
    body_parts: List[str] = []
    if isinstance(data, list) and data:
        post = (data[0].get("data", {}).get("children") or [{}])[0].get("data", {})
        title = post.get("title", "") or ""
        body_parts.append(post.get("selftext", "") or "")
        comments_listing = (data[1].get("data", {}).get("children") or []) if len(data) > 1 else []
        for c in comments_listing[:30]:
            body = (c.get("data", {}) or {}).get("body", "") or ""
            if body:
                body_parts.append(body)
    body = "\n".join(body_parts)
    mentions = _extract_brand_mentions(title + " " + body)
    return title, body, mentions


def _extract_brand_mentions(text: str) -> List[str]:
    if not text:
        return []
    candidates = re.findall(r"\b[A-Z][\w&]+(?:\s+[A-Z][\w&]+){0,2}\b", text)
    seen = []
    for c in candidates:
        c2 = c.strip().lower()
        if len(c2) < 3 or c2 in {"the", "and", "you", "we", "they", "edit"}:
            continue
        if c2 not in seen:
            seen.append(c2)
    return seen[:25]


def _discussion_type(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ("recommend", "best", "go to", "should i go")):
        return "recommendation"
    if any(k in t for k in ("review", "experience", "how was")):
        return "review"
    if any(k in t for k in ("vs ", "or ", "compare", "compared")):
        return "comparison"
    if any(k in t for k in ("?", "anyone know", "help")):
        return "question"
    return "other"


def _sentiment(text: str) -> Tuple[str, float]:
    t = (text or "").lower()
    pos = sum(1 for w in ("great", "amazing", "best", "loved", "recommend", "fantastic", "excellent") if w in t)
    neg = sum(1 for w in ("worst", "terrible", "bad", "scam", "avoid", "awful", "disappointed") if w in t)
    if pos == neg == 0:
        return "neutral", 0.0
    score = (pos - neg) / max(1, pos + neg)
    label = "positive" if score > 0.15 else ("negative" if score < -0.15 else "neutral")
    return label, score


def _suggest_action(sub: str, disc_type: str, is_gap: bool, our_present: bool) -> str:
    if not is_gap and our_present:
        return f"Maintain presence in r/{sub} — already cited."
    if disc_type == "recommendation":
        return f"Seed a credible recommendation reply in r/{sub} citing your expert."
    if disc_type == "comparison":
        return f"Add a balanced comparison comment in r/{sub} linking to your comparison page."
    if disc_type == "question":
        return f"Answer the question in r/{sub} with concrete patient-journey detail."
    if disc_type == "review":
        return f"Encourage real patient reviews in r/{sub} — AI weights recent first-person experience."
    return f"Open a discussion in r/{sub} aligned to the underserved topic."


def _extract_subreddit(url: str) -> str:
    m = re.search(r"reddit\.com/r/([^/]+)", url or "")
    return m.group(1).lower() if m else ""
