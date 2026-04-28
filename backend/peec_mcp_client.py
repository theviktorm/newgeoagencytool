"""
Momentus AI — Peec.ai MCP Client (Tier 1 ingestion)

Live, server-side MCP client connecting to https://api.peec.ai/mcp.
Replaces CSV/REST ingestion with always-fresh data: mentions across
ChatGPT/Perplexity/Gemini, share of voice, competitor movement,
sentiment, and source attribution.

Design notes
─────────────
1. We DISCOVER tools at runtime (`session.list_tools()`) and match by
   keyword, because Peec's exact tool names aren't published. This makes
   the client resilient to renames and lets it evolve as Peec ships
   more capabilities. Override via `tool_overrides` if you want explicit
   names.

2. Every sync writes to three tables:
     - `peec_records`         (per-mention rows, sentiment-aware)
     - `sources`              (deduplicated URL inventory, sentiment rollup)
     - `mention_events`       (immutable observation log, used by insights)
     - `competitor_history`   (snapshot of competitor SoV)

3. Auth: bearer token in `Authorization` header. Peec's blog implies an
   OAuth flow at setup time — a long-lived token is the realistic
   server-side equivalent. Configure via `GEO_PEEC_MCP_TOKEN`.

4. If the MCP package isn't installed, every public function returns a
   structured error rather than crashing the import. This keeps the rest
   of Momentus AI working when MCP isn't set up yet.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager

from .config import settings
from .database import (
    execute, execute_many, fetch_all, fetch_one, gen_id, to_json, from_json,
)
from .models import normalize_model_source

logger = logging.getLogger("geo.peec_mcp")

# Lazy MCP imports — fail soft if package missing
try:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    ClientSession = None  # type: ignore
    streamablehttp_client = None  # type: ignore


# ═══════════════════════════════════════════════════════════════
# TOOL DISCOVERY — match by keyword to Peec's actual tool names
# ═══════════════════════════════════════════════════════════════

# Each capability lists keywords the tool name/description must contain.
# First match wins. Override via PeecMCPClient(tool_overrides={...}).
CAPABILITY_KEYWORDS = {
    "mentions":      [["mention"], ["citation"], ["visibility"]],
    "share_of_voice": [["share", "voice"], ["share_of_voice"], ["sov"]],
    "competitors":   [["competitor"]],
    "sentiment":     [["sentiment"]],
    "sources":       [["source"], ["url", "report"]],
    "prompts":       [["prompt"], ["query"]],
}


def _match_capability(tool_name: str, tool_desc: str, keywords: List[List[str]]) -> bool:
    """A tool matches a capability if every word in any keyword group is in the
    tool name or description (case-insensitive)."""
    haystack = f"{tool_name} {tool_desc}".lower()
    for group in keywords:
        if all(kw.lower() in haystack for kw in group):
            return True
    return False


# ═══════════════════════════════════════════════════════════════
# RESPONSE NORMALIZATION — MCP returns TextContent; parse to dict/list
# ═══════════════════════════════════════════════════════════════

def _extract_payload(call_result: Any) -> Any:
    """MCP `call_tool` returns CallToolResult with .content = list of
    TextContent / ImageContent / EmbeddedResource. We pull out the first
    text block and try to JSON-decode it. Fall back to raw string."""
    if call_result is None:
        return None
    content = getattr(call_result, "content", None)
    if not content:
        return None
    for block in content:
        text = getattr(block, "text", None)
        if not text:
            continue
        text = text.strip()
        # Try strict JSON
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        # Try to find a JSON object/array embedded in prose
        m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        return text
    return None


def _walk_records(payload: Any) -> List[Dict[str, Any]]:
    """Find a list of records in the payload. Peec MCP could shape responses
    several ways; we accept all common shapes."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("data", "items", "results", "records", "mentions",
                    "sources", "competitors", "rows"):
            v = payload.get(key)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
        # Single object
        return [payload]
    return []


def _norm_sentiment(raw: Any) -> Tuple[str, float]:
    """Normalize sentiment across formats: string label, numeric score, or both."""
    if raw is None or raw == "":
        return "", 0.0
    if isinstance(raw, (int, float)):
        score = float(raw)
        if score > 0.15:
            label = "positive"
        elif score < -0.15:
            label = "negative"
        else:
            label = "neutral"
        return label, max(-1.0, min(1.0, score))
    if isinstance(raw, dict):
        score = raw.get("score", raw.get("value", 0.0))
        label = raw.get("label", raw.get("sentiment", ""))
        try:
            score = float(score)
        except (TypeError, ValueError):
            score = 0.0
        return str(label).lower(), max(-1.0, min(1.0, score))
    if isinstance(raw, str):
        s = raw.lower().strip()
        if s in ("positive", "pos", "+", "good"):
            return "positive", 0.7
        if s in ("negative", "neg", "-", "bad"):
            return "negative", -0.7
        if s in ("neutral", "mixed", "0"):
            return "neutral", 0.0
        try:
            score = float(s)
            return _norm_sentiment(score)
        except ValueError:
            return s, 0.0
    return "", 0.0


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _pick(d: Dict[str, Any], *keys, default: Any = "") -> Any:
    """Return first present non-empty value among keys."""
    for k in keys:
        v = d.get(k)
        if v not in (None, "", []):
            return v
    return default


# ═══════════════════════════════════════════════════════════════
# CLIENT
# ═══════════════════════════════════════════════════════════════

class PeecMCPClient:
    """Server-side client for Peec.ai MCP. Discover tools, fetch data,
    write into our DB.

    Usage:
        client = PeecMCPClient()
        async with client.session() as session:
            await client.discover(session)
            data = await client.fetch_mentions(session, brand="acme")
    """

    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        timeout: Optional[int] = None,
        tool_overrides: Optional[Dict[str, str]] = None,
    ):
        self.url = url or settings.peec_mcp_url
        self.token = token or settings.peec_mcp_token
        self.timeout = timeout or settings.peec_mcp_timeout
        self.tool_overrides = tool_overrides or {}
        # Discovered map: capability -> tool name
        self.tool_map: Dict[str, str] = {}
        self.tools_raw: List[Dict[str, str]] = []

    # ── connection ──────────────────────────────────────────────

    @asynccontextmanager
    async def session(self):
        """Yield an initialized MCP ClientSession bound to Peec."""
        if not MCP_AVAILABLE:
            raise RuntimeError(
                "mcp package not installed. Run: pip install 'mcp>=1.2.0'"
            )
        if not self.token:
            raise RuntimeError(
                "GEO_PEEC_MCP_TOKEN not configured. Get a bearer token from your "
                "Peec account and set it in .env."
            )
        headers = {"Authorization": f"Bearer {self.token}"}
        async with streamablehttp_client(self.url, headers=headers) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session

    async def discover(self, session) -> Dict[str, str]:
        """List Peec MCP tools and resolve capability → tool-name map."""
        listing = await session.list_tools()
        tools = getattr(listing, "tools", []) or []
        self.tools_raw = [
            {"name": t.name, "description": getattr(t, "description", "") or ""}
            for t in tools
        ]
        for cap, kw_groups in CAPABILITY_KEYWORDS.items():
            # Explicit override wins
            if cap in self.tool_overrides:
                self.tool_map[cap] = self.tool_overrides[cap]
                continue
            for t in self.tools_raw:
                if _match_capability(t["name"], t["description"], kw_groups):
                    self.tool_map[cap] = t["name"]
                    break
        logger.info("Peec MCP tools resolved: %s", self.tool_map)
        return self.tool_map

    async def _call(self, session, capability: str, args: Dict[str, Any]) -> Any:
        tool = self.tool_map.get(capability)
        if not tool:
            logger.debug("No Peec MCP tool found for capability %s", capability)
            return None
        try:
            result = await session.call_tool(tool, args)
            return _extract_payload(result)
        except Exception as e:
            logger.warning("Peec MCP %s (%s) failed: %s", capability, tool, e)
            return None

    # ── public fetchers ─────────────────────────────────────────

    async def test_connection(self) -> Dict[str, Any]:
        """Quick health check. Returns shape suitable for /api/peec/mcp/status."""
        if not MCP_AVAILABLE:
            return {
                "connected": False, "configured": False,
                "error": "mcp package not installed",
                "tools": [], "tool_map": {},
            }
        if not self.token:
            return {
                "connected": False, "configured": False,
                "error": "GEO_PEEC_MCP_TOKEN not set",
                "tools": [], "tool_map": {},
            }
        try:
            async with self.session() as session:
                await self.discover(session)
                return {
                    "connected": True,
                    "configured": True,
                    "url": self.url,
                    "tools": self.tools_raw,
                    "tool_map": self.tool_map,
                }
        except Exception as e:
            return {
                "connected": False, "configured": True,
                "error": str(e),
                "tools": [], "tool_map": {},
            }

    async def fetch_mentions(
        self, session, brand: str = "", since: str = "", limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Pull recent brand mentions across AI engines."""
        args: Dict[str, Any] = {"limit": limit}
        if brand:
            args["brand"] = brand
        if since:
            args["since"] = since
        payload = await self._call(session, "mentions", args)
        return _walk_records(payload)

    async def fetch_sources(
        self, session, brand: str = "", limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Pull cited-source attribution (URL inventory)."""
        args: Dict[str, Any] = {"limit": limit}
        if brand:
            args["brand"] = brand
        payload = await self._call(session, "sources", args)
        return _walk_records(payload)

    async def fetch_competitors(
        self, session, brand: str = "",
    ) -> List[Dict[str, Any]]:
        """Pull competitor share-of-voice."""
        args: Dict[str, Any] = {}
        if brand:
            args["brand"] = brand
        payload = await self._call(session, "competitors", args)
        if not payload:
            payload = await self._call(session, "share_of_voice", args)
        return _walk_records(payload)


# ═══════════════════════════════════════════════════════════════
# SYNC ORCHESTRATION — write live MCP data into DB tables
# ═══════════════════════════════════════════════════════════════

async def sync_workspace_from_mcp(
    workspace_id: str,
    brand: str = "",
    since_hours: int = 168,  # default last 7 days
    limit: int = 500,
    client: Optional[PeecMCPClient] = None,
) -> Dict[str, Any]:
    """
    Top-level: connect to Peec MCP, pull everything for a workspace, write
    to peec_records / sources / mention_events / competitor_history.

    Returns a summary dict suitable for the API and the UI.
    """
    if client is None:
        client = PeecMCPClient()

    project_id = workspace_id
    batch_id = gen_id("mcp-")
    summary: Dict[str, Any] = {
        "success": False,
        "batch_id": batch_id,
        "workspace_id": workspace_id,
        "mentions": 0,
        "sources": 0,
        "competitors": 0,
        "events": 0,
        "tools_discovered": [],
        "error": "",
    }

    if not MCP_AVAILABLE:
        summary["error"] = "mcp package not installed"
        await _record_sync(workspace_id, "error", summary["error"], batch_id, 0, 0)
        return summary
    if not client.token:
        summary["error"] = "Peec MCP token not configured"
        await _record_sync(workspace_id, "error", summary["error"], batch_id, 0, 0)
        return summary

    # Fetch brand label from workspace if not supplied
    if not brand:
        ws = await fetch_one(
            "SELECT brand_name, name FROM workspaces WHERE id = ?", (workspace_id,)
        )
        if ws:
            brand = ws.get("brand_name") or ws.get("name") or ""

    since = (datetime.utcnow() - timedelta(hours=since_hours)).isoformat()

    # Ensure project row exists (workspace ↔ project mirror)
    await execute(
        "INSERT OR IGNORE INTO projects (id, name) VALUES (?, ?)",
        (project_id, brand or workspace_id),
    )

    try:
        async with client.session() as session:
            await client.discover(session)
            summary["tools_discovered"] = list(client.tool_map.keys())

            mentions = await client.fetch_mentions(
                session, brand=brand, since=since, limit=limit,
            )
            sources_data = await client.fetch_sources(session, brand=brand, limit=limit)
            competitors = await client.fetch_competitors(session, brand=brand)

        # ── 1. Mentions → peec_records + mention_events ──────────
        rec_rows: List[Tuple] = []
        evt_rows: List[Tuple] = []
        url_agg: Dict[str, Dict[str, Any]] = {}

        for m in mentions:
            url = _pick(m, "url", "source_url", "page_url", "link")
            if not url:
                continue
            title = _pick(m, "title", "page_title", "source_title")
            citation_count = _safe_int(_pick(m, "citation_count", "citations",
                                            "mentions", "count", default=1))
            citation_rate = _safe_float(_pick(m, "citation_rate", "rate", default=0.0))
            usage_count = _safe_int(_pick(m, "usage_count", "usage", default=0))
            position = _safe_float(_pick(m, "position", "rank", default=0.0))
            topic = _pick(m, "topic", "category", "subject", default="Uncategorized")
            prompt = _pick(m, "prompt", "query", "question", default="")
            model = normalize_model_source(str(_pick(m, "model", "model_source",
                                                     "engine", "ai_model", default="Other")))
            sent_label, sent_score = _norm_sentiment(_pick(m, "sentiment",
                                                            "sentiment_score",
                                                            default=None))

            rec_id = gen_id("pr-")
            evt_id = gen_id("me-")
            now = datetime.utcnow().isoformat()

            rec_rows.append((
                rec_id, project_id, url, title, usage_count, citation_count,
                citation_rate, _safe_int(_pick(m, "retrievals", default=0)),
                topic, to_json(_pick(m, "tags", default=[])),
                model, to_json(m), batch_id, now,
                sent_label, sent_score, position,
            ))
            evt_rows.append((
                evt_id, project_id, url, prompt, model,
                citation_count, citation_rate, position,
                sent_label, sent_score, to_json(m), now, batch_id,
            ))

            # Aggregate per-URL for sources
            agg = url_agg.setdefault(url, {
                "title": title, "topics": set(), "models": set(),
                "total_cites": 0, "max_rate": 0.0, "score_sum": 0.0,
                "score_n": 0, "prompts": [],
            })
            if title and not agg["title"]:
                agg["title"] = title
            agg["topics"].add(topic)
            agg["models"].add(model)
            agg["total_cites"] += citation_count
            agg["max_rate"] = max(agg["max_rate"], citation_rate)
            if sent_score:
                agg["score_sum"] += sent_score
                agg["score_n"] += 1
            if prompt and len(agg["prompts"]) < 5:
                agg["prompts"].append(prompt)

        # Also fold direct sources data
        for s in sources_data:
            url = _pick(s, "url", "source_url", "page_url")
            if not url:
                continue
            agg = url_agg.setdefault(url, {
                "title": "", "topics": set(), "models": set(),
                "total_cites": 0, "max_rate": 0.0, "score_sum": 0.0,
                "score_n": 0, "prompts": [],
            })
            if not agg["title"]:
                agg["title"] = _pick(s, "title", default="")
            cites = _safe_int(_pick(s, "citation_count", "citations", default=0))
            rate = _safe_float(_pick(s, "citation_rate", "rate", default=0.0))
            agg["total_cites"] = max(agg["total_cites"], cites)
            agg["max_rate"] = max(agg["max_rate"], rate)
            for t in _pick(s, "topics", "categories", default=[]) or []:
                if t:
                    agg["topics"].add(t)
            for m_src in _pick(s, "models", "model_sources", default=[]) or []:
                agg["models"].add(normalize_model_source(str(m_src)))

        if rec_rows:
            await execute_many(
                "INSERT INTO peec_records (id, project_id, url, title, usage_count, "
                "citation_count, citation_rate, retrievals, topic, tags, model_source, "
                "raw_data, import_batch_id, imported_at, sentiment, sentiment_score, "
                "position) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rec_rows,
            )
            summary["mentions"] = len(rec_rows)

        if evt_rows:
            await execute_many(
                "INSERT INTO mention_events (id, project_id, url, prompt, model_source, "
                "citation_count, citation_rate, position, sentiment, sentiment_score, raw, "
                "observed_at, sync_batch_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                evt_rows,
            )
            summary["events"] = len(evt_rows)

        # ── 2. Sources upsert ────────────────────────────────────
        existing_sources = await fetch_all(
            "SELECT id, url FROM sources WHERE project_id = ?", (project_id,)
        )
        existing_by_url = {s["url"]: s["id"] for s in existing_sources}
        now = datetime.utcnow().isoformat()
        src_count = 0
        for url, agg in url_agg.items():
            avg_score = (agg["score_sum"] / agg["score_n"]) if agg["score_n"] else 0.0
            topics_json = to_json(sorted(t for t in agg["topics"] if t))
            models_json = to_json(sorted(m for m in agg["models"] if m))
            prompts_json = to_json(agg["prompts"])
            if url in existing_by_url:
                await execute(
                    "UPDATE sources SET total_citation_count = total_citation_count + ?, "
                    "max_citation_rate = MAX(max_citation_rate, ?), topics = ?, "
                    "model_sources = ?, title = COALESCE(NULLIF(title, ''), ?), "
                    "avg_sentiment_score = ?, last_seen_at = ?, sample_prompts = ? "
                    "WHERE id = ?",
                    (agg["total_cites"], agg["max_rate"], topics_json, models_json,
                     agg["title"], avg_score, now, prompts_json,
                     existing_by_url[url]),
                )
            else:
                await execute(
                    "INSERT INTO sources (id, project_id, url, title, "
                    "total_citation_count, max_citation_rate, topics, model_sources, "
                    "avg_sentiment_score, last_seen_at, sample_prompts) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (gen_id("src-"), project_id, url, agg["title"],
                     agg["total_cites"], agg["max_rate"], topics_json, models_json,
                     avg_score, now, prompts_json),
                )
                src_count += 1
        summary["sources"] = src_count

        # ── 3. Competitor history ────────────────────────────────
        comp_rows: List[Tuple] = []
        for c in competitors:
            domain = _pick(c, "domain", "url", "site", "host")
            if not domain:
                continue
            # Strip protocol/path → bare domain
            domain = re.sub(r"^https?://", "", str(domain)).split("/")[0].lower()
            comp_rows.append((
                gen_id("ch-"), workspace_id, "",
                domain,
                _safe_int(_pick(c, "citation_count", "citations", default=0)),
                _safe_float(_pick(c, "share_of_voice", "share", "sov", default=0.0)),
                _safe_float(_pick(c, "avg_position", "position", default=0.0)),
                to_json(_pick(c, "top_topics", "topics", default=[])),
                now, batch_id,
            ))
            # Upsert into competitors master table
            await execute(
                "INSERT INTO competitors (id, workspace_id, domain, name, citation_count, "
                "avg_citation_rate, top_topics, last_checked) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(workspace_id, domain) DO UPDATE SET "
                "citation_count = excluded.citation_count, "
                "top_topics = excluded.top_topics, "
                "last_checked = excluded.last_checked",
                (gen_id("comp-"), workspace_id, domain,
                 _pick(c, "name", default=domain),
                 _safe_int(_pick(c, "citation_count", "citations", default=0)),
                 _safe_float(_pick(c, "share_of_voice", "share", default=0.0)),
                 to_json(_pick(c, "top_topics", "topics", default=[])),
                 now),
            )

        if comp_rows:
            await execute_many(
                "INSERT INTO competitor_history (id, workspace_id, competitor_id, domain, "
                "citation_count, share_of_voice, avg_position, top_topics, observed_at, "
                "sync_batch_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                comp_rows,
            )
            summary["competitors"] = len(comp_rows)

        # ── 4. Cluster regeneration (cheap, idempotent) ──────────
        await _regenerate_clusters_from_records(project_id)

        summary["success"] = True
        await _record_sync(
            workspace_id, "ok", "", batch_id,
            summary["mentions"], summary["competitors"],
        )

        # Tier 2: run insights scan after every successful sync.
        # Imported lazily to avoid circular import at module load.
        try:
            from .insights_engine import scan_workspace_insights
            insights = await scan_workspace_insights(workspace_id)
            summary["insights"] = insights
        except Exception as e:
            logger.warning("Insights scan after sync failed: %s", e)

        return summary

    except Exception as e:
        logger.exception("MCP sync failed for workspace %s", workspace_id)
        summary["error"] = str(e)
        await _record_sync(workspace_id, "error", str(e), batch_id, 0, 0)
        return summary


async def _regenerate_clusters_from_records(project_id: str):
    """Lightweight cluster rebuild — group by topic from peec_records."""
    rows = await fetch_all(
        "SELECT topic, COUNT(*) AS n, COUNT(DISTINCT url) AS urls, "
        "SUM(citation_count) AS cites, AVG(citation_rate) AS rate "
        "FROM peec_records WHERE project_id = ? GROUP BY topic",
        (project_id,),
    )
    existing = await fetch_all(
        "SELECT id, name FROM clusters WHERE project_id = ?", (project_id,)
    )
    name_map = {c["name"]: c["id"] for c in existing}
    for r in rows:
        topic = r["topic"] or "Uncategorized"
        if topic in name_map:
            await execute(
                "UPDATE clusters SET prompt_count = ?, url_count = ?, "
                "avg_citation_rate = ?, total_citations = ? WHERE id = ?",
                (r["n"], r["urls"], r["rate"] or 0.0, r["cites"] or 0,
                 name_map[topic]),
            )
        else:
            await execute(
                "INSERT INTO clusters (id, project_id, name, topics, prompt_count, "
                "url_count, avg_citation_rate, total_citations) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (gen_id("cl-"), project_id, topic, to_json([topic]),
                 r["n"], r["urls"], r["rate"] or 0.0, r["cites"] or 0),
            )


async def _record_sync(
    workspace_id: str, status: str, error: str, batch_id: str,
    mentions: int, competitors: int,
):
    """Persist last-sync state per workspace."""
    now = datetime.utcnow().isoformat()
    await execute(
        "INSERT INTO mcp_sync_state (workspace_id, last_sync_at, last_sync_status, "
        "last_sync_error, last_batch_id, mentions_imported, competitors_imported) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(workspace_id) DO UPDATE SET "
        "last_sync_at = excluded.last_sync_at, "
        "last_sync_status = excluded.last_sync_status, "
        "last_sync_error = excluded.last_sync_error, "
        "last_batch_id = excluded.last_batch_id, "
        "mentions_imported = excluded.mentions_imported, "
        "competitors_imported = excluded.competitors_imported",
        (workspace_id, now, status, error, batch_id, mentions, competitors),
    )


async def get_sync_state(workspace_id: str) -> Optional[Dict[str, Any]]:
    return await fetch_one(
        "SELECT * FROM mcp_sync_state WHERE workspace_id = ?", (workspace_id,)
    )


# ═══════════════════════════════════════════════════════════════
# BACKGROUND AUTO-SYNC (optional)
# ═══════════════════════════════════════════════════════════════

_auto_sync_task: Optional[asyncio.Task] = None


async def _auto_sync_loop():
    """Polls every workspace at the configured cadence."""
    cadence_min = settings.peec_mcp_auto_sync_minutes
    if cadence_min <= 0:
        return
    while True:
        try:
            workspaces = await fetch_all(
                "SELECT id FROM workspaces WHERE is_active = 1"
            )
            for ws in workspaces:
                try:
                    await sync_workspace_from_mcp(ws["id"])
                except Exception as e:
                    logger.warning("Auto-sync error for ws %s: %s", ws["id"], e)
        except Exception:
            logger.exception("Auto-sync loop error")
        await asyncio.sleep(cadence_min * 60)


async def start_auto_sync():
    """Kick off the background auto-sync if configured."""
    global _auto_sync_task
    if not MCP_AVAILABLE or not settings.has_peec_mcp:
        return
    if settings.peec_mcp_auto_sync_minutes <= 0:
        return
    if _auto_sync_task and not _auto_sync_task.done():
        return
    _auto_sync_task = asyncio.create_task(_auto_sync_loop())
    logger.info(
        "Peec MCP auto-sync started (every %s min)",
        settings.peec_mcp_auto_sync_minutes,
    )


async def stop_auto_sync():
    global _auto_sync_task
    if _auto_sync_task and not _auto_sync_task.done():
        _auto_sync_task.cancel()
        try:
            await _auto_sync_task
        except asyncio.CancelledError:
            pass
    _auto_sync_task = None
