"""
Momentus AI — Built-in MCP Server (Tier 3)

Exposes the Momentus pipeline as MCP tools so users can drive it from
Claude Desktop, Cursor, n8n, etc.

  Read tools:
    list_workspaces            – discover available client workspaces
    workspace_overview         – pipeline counts + cost for a workspace
    list_clusters              – clusters ranked by citations
    list_sources               – cited URLs ranked by citation count
    list_drafts                – drafts for a workspace, optionally by status
    list_recommendations       – open recommendations
    competitor_movement        – competitor share-of-voice deltas
    pipeline_activity          – 7-day activity feed
    get_workspace_metrics      – success metrics for a workspace

  Write tools:
    sync_peec                  – pull latest data from Peec MCP into workspace
    scan_insights              – run sentiment + competitor detectors
    analyze_cluster            – Claude gap-analysis on a cluster
    generate_brief             – produce a content brief
    generate_draft             – produce a draft from cluster + template
    review_draft               – approve / request revision / reject
    publish_draft              – export or push to CMS

Auth: bearer token. The same JWT issued by /api/auth/login. The token
must come from a user with at least `editor` role for write tools, or
any role for reads. Workspace scoping is enforced per call.

Transport: streamable-http, mounted onto the existing FastAPI app at
`/mcp`. Compatible with the standard MCP client config:

    {
      "mcpServers": {
        "momentus": {
          "url": "https://your-server/mcp",
          "headers": {"Authorization": "Bearer <token>"}
        }
      }
    }

If the `mcp` package isn't installed, we expose a stub `mcp_app = None`
and main.py skips the mount — the rest of the API keeps working.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .auth import has_permission, verify_token
from .config import settings
from .database import fetch_all, fetch_one, from_json, gen_id, execute, get_daily_cost, get_monthly_cost

logger = logging.getLogger("geo.mcp_server")

try:
    from mcp.server.fastmcp import FastMCP, Context
    MCP_SERVER_AVAILABLE = True
except ImportError:
    MCP_SERVER_AVAILABLE = False
    FastMCP = None  # type: ignore
    Context = None  # type: ignore


def _build_server():
    """Construct the FastMCP server. Returns the server instance or None
    if mcp isn't installed."""
    if not MCP_SERVER_AVAILABLE:
        return None

    mcp = FastMCP(
        "momentus-ai",
        instructions=(
            "Momentus AI is a multi-tenant content operations platform for "
            "Generative Engine Optimization. Use these tools to inspect "
            "workspaces, drive the analysis→brief→draft→publish pipeline, "
            "and act on recommendations. All workspace-scoped tools take a "
            "`workspace_id` (e.g. 'ws-abc123'). Use `list_workspaces` first "
            "if you don't know it."
        ),
    )

    # ── helpers ────────────────────────────────────────────────

    def _require_user(ctx: Optional[Any]) -> Dict[str, Any]:
        """Pull bearer token from request and verify it. Returns user dict.
        Raises ValueError if no/invalid token."""
        token = None
        if ctx is not None:
            try:
                req = ctx.request_context.request
                auth = req.headers.get("authorization", "") if req else ""
                if auth.lower().startswith("bearer "):
                    token = auth.split(" ", 1)[1]
            except Exception:
                pass
        if not token:
            raise ValueError("Authorization header missing. Pass a Momentus JWT.")
        payload = verify_token(token)
        if not payload:
            raise ValueError("Invalid or expired token.")
        return payload

    async def _user_can_access_workspace(user: Dict[str, Any], ws_id: str) -> bool:
        if user.get("role") == "superadmin":
            return True
        row = await fetch_one(
            "SELECT 1 FROM workspace_members WHERE user_id = ? AND workspace_id = ?",
            (user["sub"], ws_id),
        )
        return bool(row)

    async def _ws_or_403(user: Dict[str, Any], ws_id: str):
        if not await _user_can_access_workspace(user, ws_id):
            raise ValueError(f"Access denied to workspace {ws_id}")

    # ── READ TOOLS ─────────────────────────────────────────────

    @mcp.tool()
    async def list_workspaces(ctx: Context) -> List[Dict[str, Any]]:
        """List workspaces the caller has access to. Always start here."""
        user = _require_user(ctx)
        if user.get("role") == "superadmin":
            rows = await fetch_all(
                "SELECT id, name, slug, brand_name FROM workspaces "
                "WHERE is_active = 1 ORDER BY name"
            )
        else:
            rows = await fetch_all(
                """SELECT w.id, w.name, w.slug, w.brand_name, wm.role AS member_role
                   FROM workspaces w JOIN workspace_members wm ON w.id = wm.workspace_id
                   WHERE wm.user_id = ? AND w.is_active = 1 ORDER BY w.name""",
                (user["sub"],),
            )
        return rows

    @mcp.tool()
    async def workspace_overview(ctx: Context, workspace_id: str) -> Dict[str, Any]:
        """Pipeline counts (records, sources, clusters, drafts, exports),
        Claude cost for today and this month, and last MCP sync state."""
        user = _require_user(ctx)
        await _ws_or_403(user, workspace_id)
        pid = workspace_id

        async def cnt(sql: str, args=()) -> int:
            r = await fetch_one(sql, args)
            return r["cnt"] if r else 0

        records = await cnt("SELECT COUNT(*) cnt FROM peec_records WHERE project_id = ?", (pid,))
        sources = await cnt("SELECT COUNT(*) cnt FROM sources WHERE project_id = ?", (pid,))
        clusters = await cnt("SELECT COUNT(*) cnt FROM clusters WHERE project_id = ?", (pid,))
        drafts = await cnt(
            "SELECT COUNT(*) cnt FROM drafts d JOIN clusters c ON d.cluster_id = c.id "
            "WHERE c.project_id = ?", (pid,),
        )
        exports = await cnt(
            "SELECT COUNT(*) cnt FROM exports e JOIN drafts d ON e.draft_id = d.id "
            "JOIN clusters c ON d.cluster_id = c.id WHERE c.project_id = ?", (pid,),
        )
        sync = await fetch_one(
            "SELECT * FROM mcp_sync_state WHERE workspace_id = ?", (workspace_id,)
        )
        return {
            "workspace_id": workspace_id,
            "pipeline": {
                "records": records, "sources": sources, "clusters": clusters,
                "drafts": drafts, "exports": exports,
            },
            "cost": {
                "daily_usd": round(await get_daily_cost(pid), 4),
                "monthly_usd": round(await get_monthly_cost(pid), 4),
            },
            "last_sync": sync or None,
        }

    @mcp.tool()
    async def list_clusters(
        ctx: Context, workspace_id: str, sort_by: str = "citations", limit: int = 25,
    ) -> List[Dict[str, Any]]:
        """List clusters ranked by `citations` (default), `urls`, or `prompts`."""
        user = _require_user(ctx)
        await _ws_or_403(user, workspace_id)
        order = {
            "citations": "total_citations",
            "urls": "url_count",
            "prompts": "prompt_count",
        }.get(sort_by, "total_citations")
        rows = await fetch_all(
            f"SELECT id, name, prompt_count, url_count, total_citations, "
            f"avg_citation_rate FROM clusters WHERE project_id = ? "
            f"ORDER BY {order} DESC LIMIT ?",
            (workspace_id, limit),
        )
        return rows

    @mcp.tool()
    async def list_sources(
        ctx: Context, workspace_id: str, limit: int = 25, scrape_status: str = "",
    ) -> List[Dict[str, Any]]:
        """List source URLs ranked by total citations. Optionally filter by
        scrape_status (pending|scraped|failed)."""
        user = _require_user(ctx)
        await _ws_or_403(user, workspace_id)
        if scrape_status:
            rows = await fetch_all(
                "SELECT id, url, title, total_citation_count, max_citation_rate, "
                "scrape_status, avg_sentiment_score FROM sources "
                "WHERE project_id = ? AND scrape_status = ? "
                "ORDER BY total_citation_count DESC LIMIT ?",
                (workspace_id, scrape_status, limit),
            )
        else:
            rows = await fetch_all(
                "SELECT id, url, title, total_citation_count, max_citation_rate, "
                "scrape_status, avg_sentiment_score FROM sources "
                "WHERE project_id = ? ORDER BY total_citation_count DESC LIMIT ?",
                (workspace_id, limit),
            )
        return rows

    @mcp.tool()
    async def list_drafts(
        ctx: Context, workspace_id: str, status: str = "", limit: int = 25,
    ) -> List[Dict[str, Any]]:
        """List drafts. Optional status filter: pending|approved|rejected|revision."""
        user = _require_user(ctx)
        await _ws_or_403(user, workspace_id)
        if status:
            rows = await fetch_all(
                "SELECT d.id, d.title, d.status, d.template_type, d.word_count, "
                "d.created_at, c.name AS cluster_name "
                "FROM drafts d JOIN clusters c ON d.cluster_id = c.id "
                "WHERE c.project_id = ? AND d.status = ? "
                "ORDER BY d.created_at DESC LIMIT ?",
                (workspace_id, status, limit),
            )
        else:
            rows = await fetch_all(
                "SELECT d.id, d.title, d.status, d.template_type, d.word_count, "
                "d.created_at, c.name AS cluster_name "
                "FROM drafts d JOIN clusters c ON d.cluster_id = c.id "
                "WHERE c.project_id = ? ORDER BY d.created_at DESC LIMIT ?",
                (workspace_id, limit),
            )
        return rows

    @mcp.tool()
    async def list_recommendations(
        ctx: Context, workspace_id: str, status: str = "new",
    ) -> List[Dict[str, Any]]:
        """List open recommendations sorted by priority."""
        user = _require_user(ctx)
        await _ws_or_403(user, workspace_id)
        rows = await fetch_all(
            "SELECT id, rec_type, title, description, priority_score, cluster_id, status, created_at "
            "FROM recommendations WHERE workspace_id = ? AND status = ? "
            "ORDER BY priority_score DESC LIMIT 50",
            (workspace_id, status),
        )
        return rows

    @mcp.tool()
    async def competitor_movement(
        ctx: Context, workspace_id: str, days: int = 14,
    ) -> List[Dict[str, Any]]:
        """Most recent competitor share-of-voice snapshots, with deltas vs
        the prior snapshot."""
        user = _require_user(ctx)
        await _ws_or_403(user, workspace_id)
        snaps = await fetch_all(
            "SELECT domain, share_of_voice, citation_count, avg_position, observed_at "
            "FROM competitor_history WHERE workspace_id = ? "
            "ORDER BY domain, observed_at DESC",
            (workspace_id,),
        )
        by_domain: Dict[str, List[Dict[str, Any]]] = {}
        for s in snaps:
            by_domain.setdefault(s["domain"], []).append(s)
        out = []
        for d, rows in by_domain.items():
            cur = rows[0]
            prev = rows[1] if len(rows) > 1 else None
            out.append({
                "domain": d,
                "current_share": cur["share_of_voice"],
                "previous_share": prev["share_of_voice"] if prev else None,
                "delta": (cur["share_of_voice"] or 0) - (prev["share_of_voice"] or 0) if prev else 0,
                "citation_count": cur["citation_count"],
                "observed_at": cur["observed_at"],
            })
        out.sort(key=lambda r: abs(r["delta"]), reverse=True)
        return out

    @mcp.tool()
    async def pipeline_activity(
        ctx: Context, workspace_id: str, days: int = 7,
    ) -> Dict[str, Any]:
        """Recent pipeline events: imports, scrapes, analyses, drafts, exports."""
        user = _require_user(ctx)
        await _ws_or_403(user, workspace_id)
        from datetime import datetime, timedelta
        since = (datetime.utcnow() - timedelta(days=days)).isoformat()
        pid = workspace_id
        imports = await fetch_all(
            "SELECT import_batch_id, COUNT(*) as record_count, MIN(imported_at) as t "
            "FROM peec_records WHERE project_id = ? AND imported_at >= ? "
            "GROUP BY import_batch_id ORDER BY t DESC", (pid, since),
        )
        scrapes = await fetch_all(
            "SELECT id, url, scrape_status, scraped_at FROM sources "
            "WHERE project_id = ? AND scraped_at >= ? "
            "ORDER BY scraped_at DESC LIMIT 50", (pid, since),
        )
        drafts = await fetch_all(
            "SELECT d.id, d.title, d.status, d.created_at, c.name AS cluster_name "
            "FROM drafts d JOIN clusters c ON d.cluster_id = c.id "
            "WHERE c.project_id = ? AND d.created_at >= ? "
            "ORDER BY d.created_at DESC LIMIT 50", (pid, since),
        )
        return {"imports": imports, "scrapes": scrapes, "drafts": drafts}

    @mcp.tool()
    async def get_workspace_metrics(
        ctx: Context, workspace_id: str,
    ) -> List[Dict[str, Any]]:
        """Configured success metrics with current vs target values."""
        user = _require_user(ctx)
        await _ws_or_403(user, workspace_id)
        return await fetch_all(
            "SELECT * FROM success_metrics WHERE project_id = ?", (workspace_id,)
        )

    # ── WRITE TOOLS ────────────────────────────────────────────

    def _require_editor(user: Dict[str, Any]):
        if not has_permission(user.get("role", ""), "editor"):
            raise ValueError(
                f"Role '{user.get('role')}' insufficient. Editor or above required."
            )

    @mcp.tool()
    async def sync_peec(
        ctx: Context, workspace_id: str, since_hours: int = 168,
    ) -> Dict[str, Any]:
        """Pull live data from Peec MCP into this workspace. Replaces CSV
        import. Auto-runs the insights scan on completion."""
        user = _require_user(ctx)
        _require_editor(user)
        await _ws_or_403(user, workspace_id)
        from .peec_mcp_client import sync_workspace_from_mcp
        return await sync_workspace_from_mcp(
            workspace_id=workspace_id, since_hours=since_hours,
        )

    @mcp.tool()
    async def scan_insights(
        ctx: Context, workspace_id: str,
    ) -> Dict[str, Any]:
        """Run sentiment-drop + competitor-movement + emerging-topic
        detectors. Creates Recommendations and notifications."""
        user = _require_user(ctx)
        _require_editor(user)
        await _ws_or_403(user, workspace_id)
        from .insights_engine import scan_workspace_insights
        return await scan_workspace_insights(workspace_id)

    @mcp.tool()
    async def analyze_cluster(
        ctx: Context, cluster_id: str, prompt_type: str = "blog",
    ) -> Dict[str, Any]:
        """Run Claude gap analysis on a cluster's scraped sources."""
        user = _require_user(ctx)
        _require_editor(user)
        cluster = await fetch_one(
            "SELECT project_id FROM clusters WHERE id = ?", (cluster_id,)
        )
        if not cluster:
            raise ValueError(f"Cluster {cluster_id} not found")
        await _ws_or_403(user, cluster["project_id"])
        from .claude_engine import ClaudeEngine
        engine = ClaudeEngine()
        return await engine.analyze_sources(
            cluster_id=cluster_id, prompt_type=prompt_type,
        )

    @mcp.tool()
    async def generate_brief(
        ctx: Context, cluster_id: str, analysis_id: str = "",
    ) -> Dict[str, Any]:
        """Generate a content brief for a cluster (uses latest analysis)."""
        user = _require_user(ctx)
        _require_editor(user)
        cluster = await fetch_one(
            "SELECT project_id FROM clusters WHERE id = ?", (cluster_id,)
        )
        if not cluster:
            raise ValueError(f"Cluster {cluster_id} not found")
        await _ws_or_403(user, cluster["project_id"])
        from .claude_engine import ClaudeEngine
        engine = ClaudeEngine()
        return await engine.generate_brief(
            cluster_id=cluster_id, analysis_id=analysis_id,
        )

    @mcp.tool()
    async def generate_draft(
        ctx: Context, cluster_id: str, brief_id: str = "",
        template_type: str = "blog-post",
    ) -> Dict[str, Any]:
        """Generate a full draft from a cluster brief and template type."""
        user = _require_user(ctx)
        _require_editor(user)
        cluster = await fetch_one(
            "SELECT project_id FROM clusters WHERE id = ?", (cluster_id,)
        )
        if not cluster:
            raise ValueError(f"Cluster {cluster_id} not found")
        await _ws_or_403(user, cluster["project_id"])
        from .claude_engine import ClaudeEngine
        engine = ClaudeEngine()
        return await engine.generate_draft(
            cluster_id=cluster_id, brief_id=brief_id,
            template_type=template_type,
        )

    @mcp.tool()
    async def review_draft(
        ctx: Context, draft_id: str, decision: str, notes: str = "",
    ) -> Dict[str, Any]:
        """Approve, reject, or request revision. decision ∈
        {approved, rejected, revision}."""
        user = _require_user(ctx)
        if not has_permission(user.get("role", ""), "reviewer"):
            raise ValueError("Reviewer role or above required.")
        if decision not in ("approved", "rejected", "revision"):
            raise ValueError(f"Bad decision: {decision}")
        draft = await fetch_one(
            "SELECT d.id, c.project_id FROM drafts d "
            "JOIN clusters c ON d.cluster_id = c.id WHERE d.id = ?",
            (draft_id,),
        )
        if not draft:
            raise ValueError(f"Draft {draft_id} not found")
        await _ws_or_403(user, draft["project_id"])
        await execute(
            "UPDATE drafts SET status = ?, review_notes = ?, "
            "reviewed_by = ?, reviewed_at = datetime('now') WHERE id = ?",
            (decision, notes, user["sub"], draft_id),
        )
        return {"draft_id": draft_id, "status": decision}

    @mcp.tool()
    async def publish_draft(
        ctx: Context, draft_id: str, target: str = "export", format: str = "markdown",
    ) -> Dict[str, Any]:
        """Publish or export an approved draft.
        target ∈ {export, wordpress, webflow}, format ∈ {markdown, html, json}."""
        user = _require_user(ctx)
        _require_editor(user)
        draft = await fetch_one(
            "SELECT d.*, c.project_id FROM drafts d "
            "JOIN clusters c ON d.cluster_id = c.id WHERE d.id = ?",
            (draft_id,),
        )
        if not draft:
            raise ValueError(f"Draft {draft_id} not found")
        if draft["status"] != "approved":
            raise ValueError(f"Draft must be approved before publish (status={draft['status']})")
        await _ws_or_403(user, draft["project_id"])

        if target == "export":
            export_id = gen_id("exp-")
            await execute(
                "INSERT INTO exports (id, draft_id, format, content, exported_at) "
                "VALUES (?, ?, ?, ?, datetime('now'))",
                (export_id, draft_id, format, draft.get("content", "")),
            )
            return {"export_id": export_id, "format": format}
        elif target in ("wordpress", "webflow"):
            from .cms_publisher import publish_to_cms
            return await publish_to_cms(
                cms=target, title=draft.get("title", ""),
                content=draft.get("content", ""), draft_id=draft_id,
            )
        else:
            raise ValueError(f"Unknown target: {target}")

    return mcp


# Singleton
mcp_app = _build_server()


def get_streamable_http_app():
    """Return the ASGI app for streamable-http transport, or None.
    main.py mounts this at `/mcp`."""
    if mcp_app is None:
        return None
    try:
        return mcp_app.streamable_http_app()
    except Exception as e:
        logger.warning("Failed to build MCP streamable-http app: %s", e)
        return None
