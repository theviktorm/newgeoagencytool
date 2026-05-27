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
from .config import settings
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
    manual_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run the full diagnosis for a single prompt.

    1. Find the URL(s) AI cited for `competitor_domain`; if none are known
       use `manual_urls` from the caller; if none of those either, guess
       the competitor's homepage from the brand/domain string.
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

    urls = list(manual_urls or [])
    if not urls:
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
        # Final fallback: guess a homepage. If the leader string already
        # looks like a domain, hit it directly; otherwise build a slug.
        guessed = _guess_homepage(leader)
        if guessed:
            urls = [guessed]
    if not urls:
        raise ValueError(
            f"No cited URLs known for '{leader}' on this prompt and no "
            "homepage could be guessed. Pass `manual_urls` explicitly."
        )

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


def _guess_homepage(brand_or_domain: str) -> str:
    """If the string already has a dot, treat as a domain and prepend https.
    Otherwise build a slug + .com — best-effort, gives Claude something to
    chew on when the brand is just a name like 'Pasarét Klinika'."""
    s = (brand_or_domain or "").strip().lower()
    if not s:
        return ""
    if "." in s and " " not in s:
        if s.startswith(("http://", "https://")):
            return s
        return "https://" + s.lstrip("/")
    # Hungarian/German often uses ´á´ etc — strip accents then dash
    import unicodedata
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9]+", "", s)
    if not s:
        return ""
    return f"https://www.{s}.com"


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


# ═══════════════════════════════════════════════════════════════
# PHASE 3 — COMPARATIVE CITATION INTELLIGENCE
# Builds an "us vs. competitor" structured diagnosis. Persists into
# citation_diagnostics with the extra Phase-3 columns (our_url,
# recommended_action, expected_impact, difficulty, confidence,
# source_label).
# ═══════════════════════════════════════════════════════════════

# Light stop-words for token-overlap. Intentionally tiny — we only want to
# drop the highest-frequency noise. EN + HU coverage.
_OVERLAP_STOP = {
    # English noise
    "the", "a", "an", "of", "to", "in", "for", "on", "and", "or", "is",
    "are", "be", "with", "by", "as", "at", "this", "that", "it", "from",
    "what", "how", "who", "where", "when", "why", "vs", "versus", "best",
    "top", "near", "me", "i", "we", "our", "you", "your",
    # Hungarian noise (kept small + lowercase)
    "a", "az", "egy", "és", "vagy", "mi", "mit", "hol", "hogyan", "ki",
    "kik", "mely", "milyen", "van", "vannak", "is", "így", "csak",
}


def _tokens(text: str) -> List[str]:
    if not text:
        return []
    import unicodedata
    s = unicodedata.normalize("NFKD", str(text)).lower()
    s = re.sub(r"[^a-z0-9áéíóöőúüű]+", " ", s)
    return [t for t in s.split() if t and t not in _OVERLAP_STOP and len(t) > 1]


def _overlap_score(prompt_text: str, candidate_text: str) -> float:
    """Symmetric token overlap (Jaccard-ish) between prompt and candidate."""
    pt = set(_tokens(prompt_text))
    ct = set(_tokens(candidate_text))
    if not pt or not ct:
        return 0.0
    inter = pt & ct
    if not inter:
        return 0.0
    return len(inter) / (len(pt) + len(ct) - len(inter) + 1e-6)


async def _best_our_url(
    workspace_id: str, prompt: Dict[str, Any], our_url_hint: str = "",
) -> str:
    """Pick OUR best matching page for a prompt.

    Strategy:
      1. If `our_url_hint` is non-empty, use it.
      2. If prompt has target_url, use it.
      3. Else scan workspace sources + scraped_content, score by token
         overlap with prompt text. Bias toward same cluster_id when set.
    """
    hint = (our_url_hint or "").strip()
    if hint:
        return hint
    target = (prompt.get("target_url") or "").strip()
    if target:
        return target

    prompt_text = prompt.get("text", "") or ""
    cluster_id = (prompt.get("cluster_id") or "").strip()

    # Candidate URLs from sources (with title) and scraped_content (body).
    candidates: Dict[str, Dict[str, Any]] = {}
    try:
        for r in await fetch_all(
            "SELECT url, title, topics FROM sources WHERE project_id = ? LIMIT 500",
            (workspace_id,),
        ):
            url = r.get("url") or ""
            if not url:
                continue
            candidates[url] = {
                "url": url,
                "title": r.get("title") or "",
                "body": "",
                "topics": r.get("topics") or "[]",
            }
    except Exception as e:
        logger.warning("_best_our_url sources lookup failed: %s", e)
    # Pull bodies from scraped_content for any candidate we already have.
    if candidates:
        try:
            placeholders = ",".join("?" * len(candidates))
            rows = await fetch_all(
                f"SELECT url, body_text, title FROM scraped_content "
                f"WHERE url IN ({placeholders}) LIMIT 500",
                tuple(candidates.keys()),
            )
            for r in rows:
                url = r.get("url") or ""
                if url in candidates:
                    candidates[url]["body"] = (r.get("body_text") or "")[:4000]
                    if not candidates[url]["title"]:
                        candidates[url]["title"] = r.get("title") or ""
        except Exception as e:
            logger.warning("_best_our_url scraped lookup failed: %s", e)

    if not candidates:
        return ""

    best_url, best_score = "", 0.0
    for url, c in candidates.items():
        combined = f"{c.get('title', '')} {c.get('body', '')}"
        score = _overlap_score(prompt_text, combined)
        # Small cluster_id bias
        if cluster_id and cluster_id in (c.get("topics") or ""):
            score += 0.05
        if score > best_score:
            best_score, best_url = score, url
    # Only return a URL if it actually overlaps non-trivially with the prompt.
    if best_score < 0.05:
        return ""
    return best_url


async def _pick_competitor_url(
    workspace_id: str, prompt_id: str, competitor_domain: str, max_pages: int = 2,
) -> List[str]:
    """Pick competitor winning page(s). Reuses logic from diagnose_prompt."""
    urls = await _winning_urls_for(prompt_id, competitor_domain, max_pages)
    if urls:
        return urls
    try:
        rows = await fetch_all(
            "SELECT url FROM sources WHERE project_id = ? AND url LIKE ? "
            "ORDER BY total_citation_count DESC LIMIT ?",
            (workspace_id, f"%{competitor_domain}%", max_pages),
        )
        urls = [r["url"] for r in rows if r.get("url")]
    except Exception as e:
        logger.warning("_pick_competitor_url sources lookup failed: %s", e)
    if urls:
        return urls
    guessed = _guess_homepage(competitor_domain)
    return [guessed] if guessed else []


COMPARATIVE_SYSTEM = """You are a GEO (Generative Engine Optimization) analyst.
You will be given a prompt, the page on OUR site that we believe should win
that prompt, and the page on the COMPETITOR's site that currently wins it in
AI answers. Diagnose precisely WHY they win and what we must do.

Output STRICT JSON only — no prose around it — with these exact keys:
{
  "why_they_win": "1-2 sentences explaining the decisive advantage",
  "what_they_have_we_lack": "concrete content/structure they have we do not",
  "content_gap": "what topical/structural content we are missing",
  "schema_gap": "schema.org markup they have we lack (FAQ, Review, Physician, etc.)",
  "trust_gap": "PR / authority / expert bios they have we lack",
  "offsite_gap": "external mentions / publisher pickup we lack",
  "citation_gap": "patterns in how AI cites them we do not match",
  "decision_support_gap": "decision-support elements (comparison tables, pricing transparency, recovery info) we lack",
  "recommended_action": "ONE primary action to close the gap — concrete and specific",
  "expected_impact": "what we should expect to win (e.g. 'enter top 3 on this prompt in 2-4 weeks')",
  "implementation_difficulty": "low | medium | high"
}"""


def _comparative_stub(
    prompt: Dict[str, Any],
    competitor_url: str,
    our_url: str,
    reason: str = "no_anthropic_key",
) -> Dict[str, Any]:
    """Structured stub when Claude isn't available — never fabricated."""
    explanation = (
        "Claude API is not configured, so this comparison is a structured "
        "placeholder, not an AI-derived diagnosis. Connect ANTHROPIC_API_KEY "
        "and re-run to get the real comparison."
    ) if reason == "no_anthropic_key" else (
        f"Comparison could not be produced: {reason}"
    )
    return {
        "why_they_win": "needs_review",
        "what_they_have_we_lack": "needs_review",
        "content_gap": "needs_review",
        "schema_gap": "needs_review",
        "trust_gap": "needs_review",
        "offsite_gap": "needs_review",
        "citation_gap": "needs_review",
        "decision_support_gap": "needs_review",
        "recommended_action": (
            "Manual review required: compare the two pages above side-by-side"
            f" and identify the dominant decision factor."
        ),
        "expected_impact": "unknown",
        "implementation_difficulty": "medium",
        "confidence": "needs_review",
        "explanation": explanation,
    }


async def _claude_compare(
    prompt: Dict[str, Any],
    competitor_domain: str,
    competitor_pages: List[Dict[str, Any]],
    our_pages: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Ask Claude for the structured comparison. Returns the parsed dict
    or None on failure."""
    engine = ClaudeEngine()

    def _block(page: Dict[str, Any], label: str) -> str:
        body = (page.get("text") or page.get("body") or page.get("content") or "")[:5000]
        title = page.get("title") or ""
        url = page.get("url") or ""
        schema = page.get("schema") or page.get("jsonld") or []
        if not isinstance(schema, list):
            schema = [schema]
        schema_types = sorted({_jsonld_type(s) for s in schema if s})
        return (
            f"[{label}]\nURL: {url}\nTitle: {title}\n"
            f"Schema types: {schema_types}\nExcerpt:\n{body}\n---"
        )

    comp_blocks = [
        _block(p, f"COMPETITOR PAGE {i + 1}") for i, p in enumerate(competitor_pages[:2])
    ] or ["[COMPETITOR PAGE] (scrape failed or no URL available)"]
    our_blocks = [
        _block(p, f"OUR PAGE {i + 1}") for i, p in enumerate(our_pages[:2])
    ] or ["[OUR PAGE] (no matching page in our workspace yet — gap=missing_page)"]

    user = (
        f"Prompt: {prompt.get('text', '')}\n"
        f"Prompt type: {prompt.get('prompt_type', '')}\n"
        f"Buyer stage: {prompt.get('buyer_stage', '')}\n"
        f"Competitor domain: {competitor_domain}\n\n"
        + "\n\n".join(comp_blocks)
        + "\n\n"
        + "\n\n".join(our_blocks)
    )
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": user}],
            system=COMPARATIVE_SYSTEM,
            max_tokens=1100,
            temperature=0.15,
            operation="citation_comparative_diagnose",
            use_cache=True,
        )
    except Exception as e:
        logger.warning("Claude comparative call failed: %s", e)
        return None
    return _safe_json(resp.get("content", ""))


async def comparative_diagnose(
    workspace_id: str,
    prompt_id: str,
    competitor_domain: str = "",
    our_url: str = "",
    manual_competitor_url: str = "",
) -> Dict[str, Any]:
    """Phase 3 comparative diagnosis: us vs. competitor for ONE prompt.

    Persists a row into citation_diagnostics including Phase-3 columns and
    returns the diagnosis dict (always — even on degraded paths)."""
    prompt = await fetch_one("SELECT * FROM prompts WHERE id = ?", (prompt_id,))
    if not prompt:
        raise ValueError(f"unknown prompt {prompt_id}")

    # 1) Resolve competitor leader.
    leader = (competitor_domain or "").lower()
    if not leader:
        try:
            own = await fetch_one(
                "SELECT leader_domain FROM prompt_ownership "
                "WHERE workspace_id = ? AND prompt_id = ?",
                (workspace_id, prompt_id),
            )
            leader = ((own or {}).get("leader_domain") or "").lower()
        except Exception:
            leader = ""
    if not leader and not manual_competitor_url:
        raise ValueError(
            "no competitor leader available — pass competitor_domain or manual_competitor_url"
        )

    # 2) Competitor URL(s)
    comp_urls: List[str] = []
    if manual_competitor_url:
        comp_urls = [manual_competitor_url]
    elif leader:
        comp_urls = await _pick_competitor_url(workspace_id, prompt_id, leader, max_pages=2)

    # 3) Our best matching page.
    our_picked = await _best_our_url(workspace_id, prompt, our_url_hint=our_url)
    gap = "" if our_picked else "missing_page"

    # 4) Scrape both sides (tolerate failure).
    competitor_pages: List[Dict[str, Any]] = []
    if comp_urls:
        try:
            scraped = await scrape_urls(comp_urls, project_id=workspace_id)
            pages = scraped.get("details") or scraped.get("pages") or scraped.get("results") or []
            if isinstance(pages, dict):
                pages = list(pages.values())
            competitor_pages = pages if isinstance(pages, list) else []
        except Exception as e:
            logger.warning("comparative_diagnose: competitor scrape failed: %s", e)

    our_pages: List[Dict[str, Any]] = []
    if our_picked:
        try:
            scraped = await scrape_urls([our_picked], project_id=workspace_id)
            pages = scraped.get("details") or scraped.get("pages") or scraped.get("results") or []
            if isinstance(pages, dict):
                pages = list(pages.values())
            our_pages = pages if isinstance(pages, list) else []
        except Exception as e:
            logger.warning("comparative_diagnose: our scrape failed: %s", e)

    # 5) Claude or structured stub.
    if not settings.has_claude_api:
        diag = _comparative_stub(prompt, comp_urls[0] if comp_urls else "", our_picked,
                                 reason="no_anthropic_key")
        source_label = "needs_review"
        confidence = "needs_review"
    else:
        parsed = await _claude_compare(prompt, leader, competitor_pages, our_pages)
        if not parsed:
            diag = _comparative_stub(
                prompt, comp_urls[0] if comp_urls else "", our_picked,
                reason="claude_returned_no_json",
            )
            source_label = "needs_review"
            confidence = "needs_review"
        else:
            diag = parsed
            source_label = "claude_analyzed"
            confidence = "estimated"

    # 6) Persist into citation_diagnostics with Phase-3 columns.
    diag_id = gen_id("cd-")
    diagnosis_text = (
        f"{diag.get('why_they_win', '')} "
        f"They have: {diag.get('what_they_have_we_lack', '')}. "
        f"Action: {diag.get('recommended_action', '')}"
    ).strip()
    actions_payload = [
        {
            "step": diag.get("recommended_action", "") or "Manual review",
            "priority": "high",
            "impact": diag.get("expected_impact", "") or "",
            "category": "primary",
        },
    ]
    # Derive sub-actions from gap fields so the harvester can split them up.
    for key, label in (
        ("schema_gap", "Install missing schema"),
        ("decision_support_gap", "Add decision-support content"),
        ("trust_gap", "Add trust signals"),
        ("offsite_gap", "Seed offsite mentions"),
    ):
        v = (diag.get(key) or "").strip()
        if v and v.lower() not in ("none", "n/a", "needs_review"):
            actions_payload.append(
                {"step": f"{label}: {v}", "priority": "med", "impact": "", "category": key}
            )

    try:
        await execute(
            "INSERT INTO citation_diagnostics "
            "(id, workspace_id, prompt_id, cluster_id, competitor_domain, analyzed_url, "
            " content_score, schema_score, authority_score, reddit_score, youtube_score, "
            " entity_score, diagnosis, actions, raw_evidence, "
            " our_url, recommended_action, expected_impact, difficulty, confidence, source_label) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                diag_id, workspace_id, prompt_id, prompt.get("cluster_id", ""),
                leader, comp_urls[0] if comp_urls else "",
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                diagnosis_text[:2000],
                to_json(actions_payload),
                to_json({
                    "gap": gap,
                    "competitor_urls": comp_urls,
                    "our_url": our_picked,
                    "competitor_pages": [_evidence_for(p) for p in competitor_pages],
                    "our_pages": [_evidence_for(p) for p in our_pages],
                    "structured": diag,
                }),
                our_picked or "",
                diag.get("recommended_action", "")[:500],
                diag.get("expected_impact", "")[:500],
                diag.get("implementation_difficulty", "")[:64],
                confidence,
                source_label,
            ),
        )
    except Exception as e:
        logger.warning("comparative_diagnose: persist failed: %s", e)

    return {
        "id": diag_id,
        "workspace_id": workspace_id,
        "prompt_id": prompt_id,
        "competitor_domain": leader,
        "competitor_urls": comp_urls,
        "our_url": our_picked,
        "gap": gap,
        "structured": diag,
        "actions": actions_payload,
        "confidence": confidence,
        "source_label": source_label,
    }


# ═══════════════════════════════════════════════════════════════
# PHASE 3 — DIAGNOSIS → ACTIONS BRIDGE
# Reads a citation_diagnostics row and creates geo_actions rows (one per
# concrete sub-step), idempotently. Mirrors action_engine._insert.
# ═══════════════════════════════════════════════════════════════

# Inline duplicate-resistant inserter so we don't import action_engine at
# module import time (it self-registers via init() in main.startup()).
async def _insert_geo_action(
    workspace_id: str,
    action_type: str,
    title: str,
    description: str,
    source_kind: str,
    source_id: str,
    *,
    target_url: str = "",
    target_prompt_id: str = "",
    priority: float = 50.0,
    estimated_impact: str = "",
    payload: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    # Lazy import — action_engine.init() may not have run yet on a brand-new DB.
    from . import action_engine as _act
    try:
        await _act.init()
    except Exception as e:  # never fatal
        logger.warning("action_engine.init failed: %s", e)
    if action_type not in _act.ACTION_TYPES:
        return None
    row = await fetch_one(
        "SELECT 1 FROM geo_actions WHERE workspace_id=? AND action_type=? AND source_id=? LIMIT 1",
        (workspace_id, action_type, source_id),
    )
    if row:
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


_GAP_TO_ACTION = {
    "schema_gap": "schema_install",
    "decision_support_gap": "comparison_page",
    "content_gap": "content_brief",
    "trust_gap": "content_brief",
    "offsite_gap": "reddit_seed",
    "citation_gap": "citation_response",
}


async def push_diagnosis_to_actions(diagnosis_id: str) -> Dict[str, Any]:
    """Read a citation_diagnostics row and create geo_actions for each
    actionable sub-step. Idempotent: dedupes by (workspace_id, action_type,
    source_id) where source_id encodes diagnosis_id + sub-step index."""
    row = await fetch_one(
        "SELECT * FROM citation_diagnostics WHERE id = ?", (diagnosis_id,)
    )
    if not row:
        raise ValueError(f"unknown diagnosis {diagnosis_id}")

    workspace_id = row.get("workspace_id") or ""
    raw = from_json(row.get("raw_evidence") or "{}", {}) or {}
    structured = raw.get("structured") or {}
    actions_payload = from_json(row.get("actions") or "[]", []) or []

    created: List[Dict[str, str]] = []
    skipped: List[Dict[str, str]] = []

    # Primary recommended action: pick the action_type by inspecting the
    # gap that produced it (category=primary uses recommended_action text).
    primary_text = (row.get("recommended_action") or structured.get("recommended_action") or "").strip()
    if primary_text:
        # Reuse action_engine's step-to-type mapping.
        try:
            from .action_engine import _step_to_type as step_to_type
        except Exception:
            step_to_type = lambda s: "content_brief"  # type: ignore
        at_primary = step_to_type(primary_text)
        sid_primary = f"cdcmp:{diagnosis_id}:primary"
        aid = await _insert_geo_action(
            workspace_id, at_primary,
            title=primary_text[:120],
            description=primary_text,
            source_kind="citation_diagnostics_compare",
            source_id=sid_primary,
            target_url=row.get("our_url") or row.get("analyzed_url") or "",
            target_prompt_id=row.get("prompt_id") or "",
            priority=80.0,
            estimated_impact=(row.get("expected_impact") or "")[:200],
            payload={
                "diagnosis_id": diagnosis_id,
                "competitor_domain": row.get("competitor_domain") or "",
                "competitor_url": row.get("analyzed_url") or "",
                "our_url": row.get("our_url") or "",
                "structured": structured,
                "category": "primary",
                "difficulty": row.get("difficulty") or "",
            },
        )
        if aid:
            created.append({"action_id": aid, "type": at_primary, "category": "primary"})
        else:
            skipped.append({"category": "primary", "reason": "dedup_or_invalid"})

    # Sub-actions per gap field present in `structured`.
    for i, item in enumerate(actions_payload):
        if not isinstance(item, dict):
            continue
        cat = item.get("category") or ""
        if cat == "primary":
            continue
        step_text = (item.get("step") or "").strip()
        if not step_text:
            continue
        at = _GAP_TO_ACTION.get(cat, "content_brief")
        sid = f"cdcmp:{diagnosis_id}:{cat or i}"
        aid = await _insert_geo_action(
            workspace_id, at,
            title=step_text[:120],
            description=step_text,
            source_kind="citation_diagnostics_compare",
            source_id=sid,
            target_url=row.get("our_url") or row.get("analyzed_url") or "",
            target_prompt_id=row.get("prompt_id") or "",
            priority=65.0,
            estimated_impact=(item.get("impact") or row.get("expected_impact") or "")[:200],
            payload={
                "diagnosis_id": diagnosis_id,
                "competitor_domain": row.get("competitor_domain") or "",
                "competitor_url": row.get("analyzed_url") or "",
                "our_url": row.get("our_url") or "",
                "gap_field": cat,
                "structured": structured,
                "difficulty": row.get("difficulty") or "",
            },
        )
        if aid:
            created.append({"action_id": aid, "type": at, "category": cat or f"sub_{i}"})
        else:
            skipped.append({"category": cat or f"sub_{i}", "reason": "dedup_or_invalid"})

    return {
        "diagnosis_id": diagnosis_id,
        "workspace_id": workspace_id,
        "created": len(created),
        "skipped": len(skipped),
        "actions": created,
        "skipped_details": skipped,
    }
