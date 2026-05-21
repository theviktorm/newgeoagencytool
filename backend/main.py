"""
Momentus AI — FastAPI Application
All routes, middleware, error handling, security.
Serves as the single backend for the React frontend.
"""
from __future__ import annotations

import asyncio
import io
import json
import logging
import time
import traceback
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from pathlib import Path
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .database import (
    check_cost_limit, close_db, execute, fetch_all, fetch_one,
    from_json, gen_id, get_daily_cost, get_monthly_cost,
    init_db, to_json,
)
from .models import (
    AnalyzeRequest, ApiResponse, BatchRequest, ExportRequest,
    GenerateBriefRequest, GenerateDraftRequest, MeasureRequest,
    PeecConnectRequest, PeecImportCSVRequest, PromptTemplateRequest,
    PublishRequest, ReviewDraftRequest, ScrapeRequest, SuccessMetricRequest,
)
from .peec_connector import (
    PeecClient, get_cluster_sources, get_project_clusters,
    get_project_records, get_project_sources, ingest_from_api,
    ingest_records, measure_performance, parse_csv_content,
)
from . import peec_oauth
from .peec_mcp_client import (
    PeecMCPClient, sync_workspace_from_mcp, get_sync_state,
    start_auto_sync, stop_auto_sync, MCP_AVAILABLE,
)
from .insights_engine import scan_workspace_insights
from .scraper import scrape_urls
from .claude_engine import ClaudeEngine
from .cms_publisher import markdown_to_html, publish_to_cms
from .auth import init_auth
from .dashboard_api import auth_router, dashboard_router, workspace_router
from .automation import init_automation
from .ops_api import ops_router

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("geo.api")

# ═══════════════════════════════════════════════════════════════
# APP SETUP
# ═══════════════════════════════════════════════════════════════

app = FastAPI(
    title="Momentus AI API",
    description="Backend engine for Generative Engine Optimization",
    version="1.0.0",
)

# CORS — allow the React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ═══════════════════════════════════════════════════════════════
# RATE LIMITING (sliding window, bounded memory)
# ═══════════════════════════════════════════════════════════════
from collections import deque
_rate_limit_store: Dict[str, deque] = {}
_RATE_LIMIT_MAX_IPS = 10000  # prevent unbounded memory growth

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Sliding window rate limiter: N requests per minute per IP."""
    if settings.rate_limit_per_minute <= 0:
        return await call_next(request)

    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    window = 60.0

    if client_ip not in _rate_limit_store:
        # Evict oldest IP if store is too large
        if len(_rate_limit_store) >= _RATE_LIMIT_MAX_IPS:
            oldest_ip = next(iter(_rate_limit_store))
            del _rate_limit_store[oldest_ip]
        _rate_limit_store[client_ip] = deque()

    timestamps = _rate_limit_store[client_ip]

    # Efficiently remove old entries from the left
    while timestamps and now - timestamps[0] >= window:
        timestamps.popleft()

    if len(timestamps) >= settings.rate_limit_per_minute:
        return JSONResponse(
            status_code=429,
            content={"success": False, "error": "Rate limit exceeded. Try again in a minute."},
        )

    timestamps.append(now)
    return await call_next(request)

# ═══════════════════════════════════════════════════════════════
# GLOBAL ERROR HANDLER
# ═══════════════════════════════════════════════════════════════

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch-all error handler. Logs details, returns clean response."""
    logger.error("Unhandled error on %s %s: %s", request.method, request.url.path, exc)
    if settings.debug:
        logger.error(traceback.format_exc())

    if isinstance(exc, HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"success": False, "error": exc.detail},
        )

    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": str(exc) if settings.debug else "Internal server error",
        },
    )

# ═══════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    logger.info("Momentus AI starting — SQLite path: %s", settings.db_path)
    if not settings.db_path.startswith(("/data", "/var/data", "/mnt/data")):
        logger.warning(
            "DB path %s is NOT on a mounted volume. On Railway this means "
            "every redeploy wipes your data. Mount a volume and set "
            "GEO_DB_PATH=/data/momentus.db",
            settings.db_path,
        )
    # Essential, fast init only — wrapped so a schema/migration hiccup against
    # an existing production DB can NEVER prevent the container from coming up
    # and serving /api/health. A crash here makes Railway crash-loop → 502.
    try:
        logger.info("Initializing core database + auth...")
        await init_db()
        await init_auth()
        logger.info("Core init complete")
    except Exception as _e:
        logger.error("CORE INIT FAILED (continuing degraded): %s", _e, exc_info=True)
    # World-class layer + background services run AFTER we're serving so they
    # can never block readiness or crash boot. init is resolved via getattr so
    # a module without an init() (job_runner, scheduler) can't raise.
    from . import (
        brand_resolver as _br, entity_graph as _eg,
        alert_engine as _ae, job_runner as _jr, scheduler as _sch,
        action_engine as _act, backtest as _bt,
    )
    for name, mod in (
        ("brand_resolver", _br),
        ("entity_graph", _eg),
        ("alert_engine", _ae),
        ("action_engine", _act),
        ("backtest", _bt),
    ):
        fn = getattr(mod, "init", None)
        if fn is None:
            continue
        try:
            await fn()
            logger.info("Initialized %s schema", name)
        except Exception as _e:
            logger.warning("%s.init failed: %s", name, _e)
    try:
        await _jr.start_worker()
        logger.info("Job worker started")
    except Exception as _e:
        logger.warning("job_runner.start_worker failed: %s", _e)
    try:
        await _sch.start_scheduler()
        logger.info("Scheduler started")
    except Exception as _e:
        logger.warning("scheduler.start_scheduler failed: %s", _e)
    try:
        await init_automation()
    except Exception as _e:
        logger.warning("init_automation failed: %s", _e)
    if MCP_AVAILABLE and settings.has_peec_mcp and settings.peec_mcp_auto_sync_minutes > 0:
        try:
            await start_auto_sync()
            logger.info("Peec MCP auto-sync started")
        except Exception as _e:
            logger.warning("start_auto_sync failed: %s", _e)
    logger.info("Momentus AI ready on port %s", settings.port)

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down — stopping background tasks...")
    try:
        await stop_auto_sync()
    except Exception as _e:
        logger.warning("stop_auto_sync: %s", _e)
    try:
        from . import scheduler as _sch
        await _sch.stop_scheduler()
    except Exception as _e:
        logger.warning("stop_scheduler: %s", _e)
    try:
        from . import job_runner as _jr
        await _jr.stop_worker()
    except Exception as _e:
        logger.warning("stop_worker: %s", _e)
    logger.info("Closing database connection...")
    await close_db()

# ═══════════════════════════════════════════════════════════════
# MOUNT DASHBOARD & AUTH ROUTERS
# ═══════════════════════════════════════════════════════════════
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(workspace_router)
app.include_router(ops_router)

# ═══════════════════════════════════════════════════════════════
# MOUNT MOMENTUS MCP SERVER (Tier 3)
# Exposes Momentus tools to Claude Desktop / Cursor / n8n at /mcp
# ═══════════════════════════════════════════════════════════════
try:
    from .momentus_mcp_server import get_streamable_http_app
    _mcp_asgi = get_streamable_http_app()
    if _mcp_asgi is not None:
        app.mount("/mcp", _mcp_asgi)
        logger.info("Momentus MCP server mounted at /mcp")
    else:
        logger.info("Momentus MCP server not available (mcp package missing)")
except Exception as _e:
    logger.warning("Failed to mount Momentus MCP server: %s", _e)

# ═══════════════════════════════════════════════════════════════
# STATIC FRONTEND — serve index.html and dashboard.jsx
# ═══════════════════════════════════════════════════════════════

# Resolve frontend dir: sibling of the backend/ package
_FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

# No-cache headers prevent the frontend going stale across deploys. We
# hash-bust dashboard.jsx via a version query string in index.html, but
# also tell the browser + any CDN to never reuse a cached copy beyond
# revalidation. Pinning to no-cache lets each deploy ship instantly.
_NO_CACHE_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


@app.get("/")
async def root():
    index = _FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(index, media_type="text/html", headers=_NO_CACHE_HEADERS)
    return {"status": "ok", "service": "Momentus AI API", "version": "1.0.0"}

@app.get("/dashboard.jsx")
async def serve_dashboard_jsx():
    jsx = _FRONTEND_DIR / "dashboard.jsx"
    if jsx.exists():
        return FileResponse(jsx, media_type="application/javascript",
                            headers=_NO_CACHE_HEADERS)
    raise HTTPException(404, "dashboard.jsx not found")

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "version": "1.0.0",
        "has_peec_api": settings.has_peec_api,
        "has_claude_api": settings.has_claude_api,
        "has_wordpress": settings.has_wordpress,
        "has_webflow": settings.has_webflow,
    }

@app.get("/api/config")
async def get_config():
    """Return non-secret config for frontend."""
    return ApiResponse(data={
        "project_name": settings.project_name,
        "project_id": settings.project_id,
        "claude_model": settings.claude_default_model,
        "temperature": settings.claude_default_temperature,
        "max_tokens": settings.claude_max_tokens,
        "has_peec_api": settings.has_peec_api,
        "has_claude_api": settings.has_claude_api,
        "has_wordpress": settings.has_wordpress,
        "has_webflow": settings.has_webflow,
        "daily_cost_limit": settings.claude_daily_cost_limit,
        "monthly_cost_limit": settings.claude_monthly_cost_limit,
    }).dict()


# ═══════════════════════════════════════════════════════════════
# PEEC ROUTES
# ═══════════════════════════════════════════════════════════════

@app.post("/api/peec/connect")
async def peec_connect(req: PeecConnectRequest):
    """Check Peec API connection."""
    client = PeecClient(api_key=req.api_key, base_url=req.base_url)
    try:
        status = await client.check_connection()
        return ApiResponse(data=status.dict()).dict()
    finally:
        await client.close()

@app.post("/api/peec/import/csv")
async def peec_import_csv(
    file: UploadFile = File(...),
    project_id: str = Form("default"),
):
    """Import Peec data from CSV upload.

    Accepts `project_id` from multipart form data (preferred) so the frontend
    can send both the file and the workspace/project context in the same
    request body. Falls back to "default" when omitted.
    """
    if not file.filename or not (
        file.filename.endswith(".csv") or file.filename.endswith(".tsv")
    ):
        raise HTTPException(400, "File must be a .csv or .tsv")

    content = await file.read()
    if len(content) > 50 * 1024 * 1024:  # 50MB limit
        raise HTTPException(400, "CSV file too large (max 50MB)")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1")
    # Strip UTF-8 BOM if Excel/Google Sheets exported one — otherwise the
    # first header looks like "﻿status" and header matching fails.
    if text.startswith("﻿"):
        text = text.lstrip("﻿")

    # Peek at the header to route between Citations export and Prompts export.
    from .prompt_csv_importer import is_prompts_export, import_csv as import_prompts_csv
    import csv as _csv, io as _io
    try:
        peek_headers = next(_csv.reader(_io.StringIO(text)))
    except StopIteration:
        return ApiResponse(success=False, error="CSV has no rows").dict()

    is_prompts = is_prompts_export(peek_headers)
    logger.info("CSV import: headers=%s prompts_export=%s", peek_headers, is_prompts)

    if is_prompts:
        # Peec Prompts Export — feed the Prompt Battlefield engine.
        res = await import_prompts_csv(project_id, text)
        return ApiResponse(
            success=res.get("success", True),
            error=res.get("error") or None,
            data=res,
        ).dict()

    # Citations export (legacy URL importer)
    records, validation = parse_csv_content(text, project_id)
    if not validation.get("valid"):
        # Surface what we actually saw so the user/devs can diagnose.
        validation["headers_seen"] = peek_headers
        validation["detected_shape"] = "neither (no url column, no prompt column)"
        return ApiResponse(
            success=False,
            error=("CSV validation failed: no `url` column for a Citations "
                   "export AND no `prompt` column for a Prompts export. "
                   "Export the right report from Peec."),
            data=validation,
        ).dict()

    result = await ingest_records(records, project_id)
    result["validation"] = validation
    return ApiResponse(data=result).dict()


@app.post("/api/prompts/{ws_id}/import-csv")
async def prompts_import_csv(
    ws_id: str,
    file: UploadFile = File(...),
    target_brand: str = Form(""),
    classify: bool = Form(False),
):
    """Dedicated importer for Peec Prompts Export CSVs (one row per tracked
    prompt with visibility/sentiment/position/share_of_voice/mentions)."""
    if not file.filename or not (
        file.filename.endswith(".csv") or file.filename.endswith(".tsv")
    ):
        raise HTTPException(400, "File must be a .csv or .tsv")
    content = await file.read()
    if len(content) > 50 * 1024 * 1024:
        raise HTTPException(400, "CSV file too large (max 50MB)")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1")
    from .prompt_csv_importer import import_csv as import_prompts_csv
    res = await import_prompts_csv(ws_id, text, target_brand=target_brand, classify=classify)
    return ApiResponse(
        success=res.get("success", True), error=res.get("error") or None, data=res,
    ).dict()

@app.post("/api/peec/import/api")
async def peec_import_api(req: PeecConnectRequest):
    """Fetch and import data from Peec API."""
    result = await ingest_from_api(
        api_key=req.api_key,
        base_url=req.base_url,
        project_id=req.project_id or "default",
    )
    return ApiResponse(
        success=result.get("success", False),
        data=result,
        error=result.get("error"),
    ).dict()

@app.get("/api/peec/records/{project_id}")
async def get_records(project_id: str):
    """Get all Peec records for a project."""
    records = await get_project_records(project_id)
    return ApiResponse(data=records, meta={"count": len(records)}).dict()

# ─── Peec MCP (Tier 1: live ingestion) ─────────────────────────

@app.get("/api/peec/mcp/status")
async def peec_mcp_status(ws_id: str = ""):
    """Test the Peec MCP connection. If ws_id is provided, evaluates the
    per-workspace OAuth state too; otherwise reports global config only."""
    base = {
        "url": settings.peec_mcp_url,
        "configured": bool(settings.peec_mcp_url),
        "mcp_package_installed": MCP_AVAILABLE,
        "auto_sync_minutes": settings.peec_mcp_auto_sync_minutes,
        "public_base_url": settings.public_base_url,
    }
    if not ws_id:
        return ApiResponse(data=base).dict()
    client = PeecMCPClient(workspace_id=ws_id)
    status = await client.test_connection()
    oauth_status = await peec_oauth.get_status(ws_id, settings.peec_mcp_url)
    return ApiResponse(data={**base, **status, "oauth": oauth_status}).dict()


@app.post("/api/peec/mcp/auth/start/{ws_id}")
async def peec_mcp_auth_start(ws_id: str):
    """Begin OAuth flow for this workspace. Returns the authorize URL the
    browser should open. The user logs into Peec, Peec redirects back to
    /api/peec/mcp/auth/callback, tokens are persisted, popup closes itself."""
    redirect_uri = peec_oauth.public_redirect_uri(settings.public_base_url)
    try:
        flow = await peec_oauth.start_authorization(
            workspace_id=ws_id,
            server_url=settings.peec_mcp_url,
            redirect_uri=redirect_uri,
        )
    except Exception as e:
        return ApiResponse(success=False, error=str(e)).dict()
    return ApiResponse(data={
        "authorize_url": flow["authorize_url"],
        "state": flow["state"],
        "redirect_uri": redirect_uri,
    }).dict()


@app.get("/api/peec/mcp/auth/callback")
async def peec_mcp_auth_callback(
    code: str = "", state: str = "", error: str = "", error_description: str = "",
):
    """OAuth redirect target. Exchanges code → tokens, persists, then
    returns a tiny HTML page that messages the opener and closes itself."""
    if error or not code or not state:
        msg = error_description or error or "Missing code/state"
        return HTMLResponse(_oauth_callback_html(False, msg), status_code=400)
    try:
        await peec_oauth.handle_callback(state=state, code=code)
        return HTMLResponse(_oauth_callback_html(True, "Connected to Peec."))
    except Exception as e:
        return HTMLResponse(_oauth_callback_html(False, str(e)), status_code=400)


@app.post("/api/peec/mcp/auth/disconnect/{ws_id}")
async def peec_mcp_auth_disconnect(ws_id: str):
    await peec_oauth.disconnect(ws_id, settings.peec_mcp_url)
    return ApiResponse(data={"ok": True}).dict()


def _oauth_callback_html(ok: bool, msg: str) -> str:
    color = "#10b981" if ok else "#ef4444"
    title = "Peec connected" if ok else "Peec connection failed"
    safe_msg = (msg or "").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<!doctype html><html><head><meta charset="utf-8">
<title>{title}</title><style>body{{font-family:-apple-system,sans-serif;background:#0b0d11;color:#e6e6e6;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
.box{{max-width:420px;padding:32px;background:#13161c;border:1px solid #2a2f38;border-radius:10px;text-align:center}}
.dot{{width:42px;height:42px;border-radius:50%;background:{color};margin:0 auto 16px}}
h1{{font-size:18px;margin:0 0 8px}}p{{color:#9aa1ac;font-size:13px;margin:0}}
button{{margin-top:18px;padding:8px 16px;background:#1f2530;color:#e6e6e6;border:1px solid #2a2f38;border-radius:6px;cursor:pointer}}
</style></head><body><div class="box"><div class="dot"></div>
<h1>{title}</h1><p>{safe_msg}</p>
<button onclick="window.close()">Close</button></div>
<script>try{{window.opener&&window.opener.postMessage({{type:'peec-oauth',ok:{str(ok).lower()},msg:{safe_msg!r}}},'*');setTimeout(()=>window.close(),1500);}}catch(e){{}}</script>
</body></html>"""


@app.post("/api/peec/mcp/sync/{ws_id}")
async def peec_mcp_sync(ws_id: str, since_hours: int = 168, limit: int = 500):
    """Pull live data from Peec MCP into a workspace.

    Replaces CSV upload as the canonical ingestion path. Writes mentions →
    peec_records, deduplicated URLs → sources, and snapshots competitor
    share-of-voice into competitor_history.
    """
    result = await sync_workspace_from_mcp(
        workspace_id=ws_id, since_hours=since_hours, limit=limit,
    )
    return ApiResponse(
        success=result.get("success", False),
        data=result,
        error=result.get("error") or None,
    ).dict()


@app.get("/api/peec/mcp/sync/{ws_id}/state")
async def peec_mcp_sync_state(ws_id: str):
    """Last-sync metadata for a workspace."""
    state = await get_sync_state(ws_id)
    return ApiResponse(data=state or {}).dict()


# ─── Insights (Tier 2: sentiment + competitor alerts) ──────────

@app.post("/api/insights/scan/{ws_id}")
async def insights_scan(ws_id: str):
    """Run sentiment-drop and competitor-movement detectors. Generates
    Recommendations rows and notifications for the workspace owners."""
    result = await scan_workspace_insights(ws_id)
    return ApiResponse(data=result).dict()


# ═══════════════════════════════════════════════════════════════
# CONQUER ROUTES — 11-engine GEO platform
# ═══════════════════════════════════════════════════════════════
# Imports kept inside the route bodies only when the engine has heavy
# transitive deps; everything else imports at module top via lazy stubs
# so the OpenAPI spec stays clean.
from . import (
    prompt_engine, prompt_tracker, citation_intelligence, attack_map,
    revenue_priority, buyer_journey, reddit_engine, schema_engine,
    ai_overview, metadata_engine, authority_score, youtube_engine,
    # World-class layer
    scheduler, job_runner, brand_resolver, entity_graph,
    alert_engine, badge, comparative_report,
    # Action + trust + ops layer
    action_engine, backtest, integrations_status,
)
from fastapi.responses import Response


# ─── 1. Prompt Ownership Engine ────────────────────────────────

@app.get("/api/prompts/{ws_id}")
async def prompts_list(ws_id: str, stage: str = "", min_revenue: float = 0, limit: int = 500):
    rows = await prompt_engine.list_prompts(ws_id, stage=stage, min_revenue=min_revenue, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.post("/api/prompts/{ws_id}")
async def prompts_upsert(ws_id: str, payload: Dict[str, Any]):
    text = (payload or {}).get("text", "")
    target_brand = (payload or {}).get("target_brand", "")
    target_url = (payload or {}).get("target_url", "")
    cluster_id = (payload or {}).get("cluster_id", "")
    classify = bool((payload or {}).get("classify", True))
    row = await prompt_engine.upsert_prompt(
        ws_id, text, target_brand=target_brand, target_url=target_url,
        cluster_id=cluster_id, classify=classify,
    )
    return ApiResponse(data=row).dict()


@app.post("/api/prompts/{ws_id}/bulk")
async def prompts_bulk(ws_id: str, payload: Dict[str, Any]):
    texts = (payload or {}).get("texts") or []
    if not isinstance(texts, list):
        raise HTTPException(400, "texts must be a list")
    res = await prompt_engine.bulk_upsert_prompts(ws_id, texts,
        target_brand=(payload or {}).get("target_brand", ""), classify=False)
    return ApiResponse(data=res).dict()


@app.post("/api/prompts/{ws_id}/reclassify")
async def prompts_reclassify(ws_id: str, max_n: int = 200):
    res = await prompt_engine.reclassify_workspace(ws_id, max_n=max_n)
    return ApiResponse(data=res).dict()


@app.post("/api/prompts/{ws_id}/track/{prompt_id}")
async def prompt_track_one(ws_id: str, prompt_id: str, payload: Optional[Dict[str, Any]] = None):
    models = (payload or {}).get("models")
    res = await prompt_tracker.track_prompt(ws_id, prompt_id, models=models)
    return ApiResponse(data=res).dict()


@app.post("/api/prompts/{ws_id}/track")
async def prompt_track_workspace(ws_id: str, only_high_value: bool = True, max_prompts: int = 50, payload: Optional[Dict[str, Any]] = None):
    models = (payload or {}).get("models") if payload else None
    res = await prompt_tracker.track_workspace(
        ws_id, only_high_value=only_high_value, max_prompts=max_prompts, models=models,
    )
    return ApiResponse(data=res).dict()


@app.get("/api/prompts/{ws_id}/battlefield")
async def prompts_battlefield(ws_id: str):
    res = await prompt_engine.battlefield_summary(ws_id)
    return ApiResponse(data=res).dict()


# ─── 2. Citation Intelligence Layer ────────────────────────────

@app.post("/api/intel/diagnose/{ws_id}/{prompt_id}")
async def intel_diagnose(
    ws_id: str, prompt_id: str,
    competitor_domain: str = "", max_pages: int = 2,
    payload: Optional[Dict[str, Any]] = None,
):
    manual_urls = (payload or {}).get("manual_urls") if payload else None
    res = await citation_intelligence.diagnose_prompt(
        ws_id, prompt_id, competitor_domain=competitor_domain,
        max_pages=max_pages, manual_urls=manual_urls,
    )
    return ApiResponse(data=res).dict()


@app.post("/api/intel/diagnose/{ws_id}")
async def intel_diagnose_workspace(ws_id: str, top_n: int = 10):
    res = await citation_intelligence.diagnose_workspace(ws_id, top_n=top_n)
    return ApiResponse(data=res).dict()


@app.get("/api/intel/diagnostics/{ws_id}")
async def intel_list(ws_id: str, limit: int = 100):
    rows = await citation_intelligence.list_diagnostics(ws_id, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


# ─── 3. GEO Attack Map ─────────────────────────────────────────

@app.get("/api/attack-map/{ws_id}")
async def attack_map_list(ws_id: str):
    rows = await attack_map.list_attack_map(ws_id)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.post("/api/attack-map/{ws_id}/analyze")
async def attack_map_analyze(ws_id: str, payload: Dict[str, Any]):
    domain = (payload or {}).get("competitor_domain", "")
    if not domain:
        raise HTTPException(400, "competitor_domain required")
    res = await attack_map.analyze_competitor(ws_id, domain,
        sample_urls=int((payload or {}).get("sample_urls", 3)))
    return ApiResponse(data=res).dict()


@app.post("/api/attack-map/{ws_id}/analyze-all-known")
async def attack_map_analyze_all(ws_id: str, max_competitors: int = 12):
    res = await attack_map.analyze_all_known(ws_id, max_competitors=max_competitors)
    return ApiResponse(data=res).dict()


@app.get("/api/attack-map/{ws_id}/movements")
async def attack_map_movements(ws_id: str, days: int = 14):
    rows = await attack_map.list_movements(ws_id, days=days)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


# ─── 4. Revenue Priority Layer ─────────────────────────────────

@app.get("/api/revenue/{ws_id}/summary")
async def revenue_summary_route(ws_id: str):
    res = await revenue_priority.revenue_summary(ws_id)
    return ApiResponse(data=res).dict()


@app.get("/api/revenue/{ws_id}/priority")
async def revenue_priority_route(ws_id: str, top_n: int = 10):
    rows = await revenue_priority.revenue_priority_recs(ws_id, top_n=top_n)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.post("/api/revenue/{ws_id}/push-recs")
async def revenue_push_recs(ws_id: str, top_n: int = 5):
    res = await revenue_priority.push_recommendations(ws_id, top_n=top_n)
    return ApiResponse(data=res).dict()


# ─── 5. Buyer Journey Coverage Map ─────────────────────────────

@app.get("/api/journey/{ws_id}")
async def journey_get(ws_id: str, refresh: bool = True):
    res = await buyer_journey.coverage_map(ws_id, refresh=refresh)
    return ApiResponse(data=res).dict()


@app.post("/api/journey/{ws_id}/classify")
async def journey_classify(ws_id: str):
    res = await buyer_journey.classify_workspace(ws_id, force=True)
    return ApiResponse(data=res).dict()


@app.post("/api/journey/{ws_id}/deep-classify")
async def journey_deep_classify(ws_id: str, payload: Dict[str, Any]):
    urls = (payload or {}).get("urls") or []
    res = await buyer_journey.deep_classify_pages(ws_id, urls)
    return ApiResponse(data=res).dict()


# ─── 6. Reddit GEO Engine ──────────────────────────────────────

@app.post("/api/reddit/{ws_id}/harvest")
async def reddit_harvest(ws_id: str):
    res = await reddit_engine.harvest_workspace(ws_id)
    return ApiResponse(data=res).dict()


@app.post("/api/reddit/{ws_id}/harvest-subreddits")
async def reddit_harvest_subs(ws_id: str, payload: Dict[str, Any]):
    subs = (payload or {}).get("subreddits") or []
    if not subs:
        raise HTTPException(400, "subreddits list required")
    res = await reddit_engine.harvest_subreddits(ws_id, subs,
        max_threads_per_sub=int((payload or {}).get("max_threads_per_sub", 15)))
    return ApiResponse(data=res).dict()


@app.get("/api/reddit/{ws_id}")
async def reddit_list(ws_id: str, only_gaps: bool = False):
    rows = await reddit_engine.list_intel(ws_id, only_gaps=only_gaps)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.get("/api/reddit/{ws_id}/command-center")
async def reddit_cc(ws_id: str):
    res = await reddit_engine.command_center(ws_id)
    return ApiResponse(data=res).dict()


# ─── 7. Schema Opportunity Engine ──────────────────────────────

@app.post("/api/schema/{ws_id}/audit")
async def schema_audit_one(ws_id: str, payload: Dict[str, Any]):
    url = (payload or {}).get("url", "")
    if not url:
        raise HTTPException(400, "url required")
    res = await schema_engine.audit_url(ws_id, url,
        target_prompts=(payload or {}).get("target_prompts") or [],
        is_competitor=bool((payload or {}).get("is_competitor", False)))
    return ApiResponse(data=res).dict()


@app.post("/api/schema/{ws_id}/audit-workspace")
async def schema_audit_ws(ws_id: str, top_n: int = 20):
    res = await schema_engine.audit_workspace(ws_id, top_n=top_n)
    return ApiResponse(data=res).dict()


@app.get("/api/schema/{ws_id}")
async def schema_list(ws_id: str, limit: int = 200):
    rows = await schema_engine.list_audits(ws_id, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


# ─── 8. Google AI Overview Engine ──────────────────────────────

@app.get("/api/aio/{ws_id}")
async def aio_overview(ws_id: str):
    res = await ai_overview.overview(ws_id)
    return ApiResponse(data=res).dict()


@app.get("/api/aio/{ws_id}/losses")
async def aio_losses(ws_id: str, limit: int = 50):
    rows = await ai_overview.list_losses(ws_id, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.post("/api/aio/{ws_id}/detect-movements")
async def aio_movements(ws_id: str):
    res = await ai_overview.detect_movements(ws_id)
    return ApiResponse(data=res).dict()


@app.post("/api/aio/{ws_id}/track-all")
async def aio_track_all(ws_id: str, max_prompts: int = 30):
    """One-click: run the google_aio adapter on every high-value prompt."""
    res = await prompt_tracker.track_workspace(
        ws_id, only_high_value=True, max_prompts=max_prompts, models=["google_aio"],
    )
    return ApiResponse(data=res).dict()


# ─── 9. Metadata + Snippet Engine ──────────────────────────────

@app.post("/api/metadata/{ws_id}/draft/{draft_id}")
async def metadata_for_draft(ws_id: str, draft_id: str):
    res = await metadata_engine.generate_for_draft(draft_id)
    return ApiResponse(data=res).dict()


@app.post("/api/metadata/{ws_id}/url")
async def metadata_for_url(ws_id: str, payload: Dict[str, Any]):
    url = (payload or {}).get("url", "")
    if not url:
        raise HTTPException(400, "url required")
    if (payload or {}).get("audit"):
        res = await metadata_engine.audit_url(ws_id, url)
    else:
        res = await metadata_engine.generate_for_url(ws_id, url)
    return ApiResponse(data=res).dict()


@app.get("/api/metadata/{ws_id}")
async def metadata_list(ws_id: str, limit: int = 100):
    rows = await metadata_engine.list_packages(ws_id, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


# ─── 10. GEO Authority Score ───────────────────────────────────

@app.post("/api/authority/{ws_id}/compute")
async def authority_compute(ws_id: str):
    res = await authority_score.compute_for_workspace(ws_id)
    return ApiResponse(data=res).dict()


@app.post("/api/authority/{ws_id}/rebuild")
async def authority_rebuild(ws_id: str):
    res = await authority_score.rebuild_all(ws_id)
    return ApiResponse(data=res).dict()


@app.get("/api/authority/{ws_id}/latest")
async def authority_latest(ws_id: str):
    res = await authority_score.latest(ws_id)
    return ApiResponse(data=res).dict()


@app.get("/api/authority/{ws_id}/timeseries")
async def authority_ts(ws_id: str, subject_domain: str, months: int = 12):
    rows = await authority_score.timeseries(ws_id, subject_domain, months=months)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


# ─── 11. YouTube GEO Optimizer ─────────────────────────────────

@app.post("/api/youtube/{ws_id}/generate")
async def youtube_generate(ws_id: str, payload: Dict[str, Any]):
    p = payload or {}
    res = await youtube_engine.generate(
        ws_id,
        topic=p.get("topic", ""),
        expert_name=p.get("expert_name", ""),
        connected_service=p.get("connected_service", ""),
        target_prompt_ids=p.get("target_prompt_ids") or [],
        goal=p.get("goal", "trust"),
        transcript=p.get("transcript", ""),
        video_url=p.get("video_url", ""),
    )
    return ApiResponse(data=res).dict()


@app.post("/api/youtube/{ws_id}/audit")
async def youtube_audit_route(ws_id: str, payload: Dict[str, Any]):
    p = payload or {}
    if not p.get("video_url"):
        raise HTTPException(400, "video_url required")
    res = await youtube_engine.audit(
        ws_id, video_url=p.get("video_url", ""),
        title=p.get("title", ""), description=p.get("description", ""),
        transcript=p.get("transcript", ""),
        target_prompt_ids=p.get("target_prompt_ids") or [],
        expert_name=p.get("expert_name", ""),
        connected_service=p.get("connected_service", ""),
        goal=p.get("goal", "trust"),
    )
    return ApiResponse(data=res).dict()


@app.get("/api/youtube/{ws_id}")
async def youtube_list(ws_id: str, limit: int = 100):
    rows = await youtube_engine.list_assets(ws_id, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.get("/api/youtube/asset/{asset_id}/publish-package")
async def youtube_publish(asset_id: str):
    res = await youtube_engine.export_publish_package(asset_id)
    return ApiResponse(data=res).dict()


# ═══════════════════════════════════════════════════════════════
# WORLD-CLASS LAYER — scheduler, jobs, brands, graph, alerts,
# badge (public), comparative reports
# ═══════════════════════════════════════════════════════════════

# ─── Scheduler ───────────────────────────────────────────────
@app.get("/api/scheduler/{ws_id}/runs")
async def scheduler_runs(ws_id: str, limit: int = 50):
    rows = await scheduler.list_runs(ws_id, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.post("/api/scheduler/{ws_id}/run/{job_name}")
async def scheduler_run(ws_id: str, job_name: str):
    res = await scheduler.run_job_now(job_name, ws_id)
    return ApiResponse(data=res).dict()


# ─── Job queue ───────────────────────────────────────────────
@app.get("/api/jobs/{ws_id}")
async def jobs_list(ws_id: str, limit: int = 50):
    rows = await job_runner.list_jobs(ws_id, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.post("/api/jobs/{ws_id}/enqueue")
async def jobs_enqueue(ws_id: str, payload: Dict[str, Any]):
    job_type = (payload or {}).get("job_type", "")
    pl = (payload or {}).get("payload") or {}
    if not job_type:
        raise HTTPException(400, "job_type required")
    jid = await job_runner.enqueue(ws_id, job_type, pl)
    return ApiResponse(data={"id": jid}).dict()


@app.post("/api/jobs/{ws_id}/{job_id}/cancel")
async def jobs_cancel(ws_id: str, job_id: str):
    ok = await job_runner.cancel(job_id)
    return ApiResponse(data={"cancelled": ok}).dict()


# ─── Brand canonicalization ──────────────────────────────────
@app.get("/api/brands/{ws_id}")
async def brands_list(ws_id: str):
    rows = await brand_resolver.list_brands(ws_id)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.post("/api/brands/{ws_id}/canonicalize")
async def brands_canonicalize(ws_id: str, payload: Dict[str, Any]):
    name = (payload or {}).get("name", "")
    if not name:
        raise HTTPException(400, "name required")
    res = await brand_resolver.canonicalize(ws_id, name)
    return ApiResponse(data=res).dict()


@app.post("/api/brands/{ws_id}/merge")
async def brands_merge(ws_id: str, payload: Dict[str, Any]):
    keep = (payload or {}).get("keep_id", "")
    merge = (payload or {}).get("merge_id", "")
    if not keep or not merge:
        raise HTTPException(400, "keep_id and merge_id required")
    res = await brand_resolver.merge_brands(ws_id, keep, merge)
    return ApiResponse(data=res).dict()


@app.post("/api/brands/{ws_id}/set-canonical-for-us")
async def brands_set_us(ws_id: str, payload: Dict[str, Any]):
    name = (payload or {}).get("name", "")
    domain = (payload or {}).get("domain", "")
    if not name:
        raise HTTPException(400, "name required")
    res = await brand_resolver.set_canonical_for_us(ws_id, name, domain)
    return ApiResponse(data=res).dict()


# ─── Entity graph ────────────────────────────────────────────
@app.post("/api/graph/{ws_id}/rebuild")
async def graph_rebuild(ws_id: str):
    res = await entity_graph.rebuild_from_workspace(ws_id)
    return ApiResponse(data=res).dict()


@app.get("/api/graph/{ws_id}/summary")
async def graph_summary(ws_id: str):
    res = await entity_graph.graph_summary(ws_id)
    return ApiResponse(data=res).dict()


@app.get("/api/graph/{ws_id}/json")
async def graph_json(ws_id: str, top_n_nodes: int = 80):
    res = await entity_graph.get_graph_json(ws_id, top_n_nodes=top_n_nodes)
    return ApiResponse(data=res).dict()


@app.get("/api/graph/{ws_id}/neighbors/{node_id}")
async def graph_neighbors(ws_id: str, node_id: str, limit: int = 50):
    res = await entity_graph.neighbors(ws_id, node_id, limit=limit)
    return ApiResponse(data=res, meta={"count": len(res)}).dict()


@app.post("/api/graph/{ws_id}/explain")
async def graph_explain(ws_id: str, payload: Dict[str, Any]):
    pid = (payload or {}).get("prompt_id", "")
    brand = (payload or {}).get("competitor_brand", "")
    if not pid or not brand:
        raise HTTPException(400, "prompt_id and competitor_brand required")
    res = await entity_graph.explain_why_wins(ws_id, pid, brand)
    return ApiResponse(data=res).dict()


# ─── Alerts ──────────────────────────────────────────────────
@app.get("/api/alerts/{ws_id}/rules")
async def alerts_list_rules(ws_id: str):
    rows = await alert_engine.list_rules(ws_id)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.post("/api/alerts/{ws_id}/rules")
async def alerts_create_rule(ws_id: str, payload: Dict[str, Any]):
    p = payload or {}
    res = await alert_engine.create_rule(
        workspace_id=ws_id,
        name=p.get("name", ""),
        trigger_type=p.get("trigger_type", ""),
        params=p.get("params") or {},
        channels=p.get("channels") or ["inapp"],
        cooldown_minutes=int(p.get("cooldown_minutes", 60)),
        created_by=p.get("created_by", ""),
    )
    return ApiResponse(data=res).dict()


@app.put("/api/alerts/{ws_id}/rules/{rule_id}")
async def alerts_update_rule(ws_id: str, rule_id: str, payload: Dict[str, Any]):
    res = await alert_engine.update_rule(rule_id, **(payload or {}))
    return ApiResponse(data=res).dict()


@app.delete("/api/alerts/{ws_id}/rules/{rule_id}")
async def alerts_delete_rule(ws_id: str, rule_id: str):
    ok = await alert_engine.delete_rule(rule_id)
    return ApiResponse(data={"deleted": ok}).dict()


@app.get("/api/alerts/{ws_id}/events")
async def alerts_events(ws_id: str, limit: int = 100):
    rows = await alert_engine.list_events(ws_id, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.post("/api/alerts/{ws_id}/evaluate")
async def alerts_evaluate(ws_id: str):
    res = await alert_engine.evaluate_workspace(ws_id)
    return ApiResponse(data=res).dict()


# ─── Comparative report ──────────────────────────────────────
@app.post("/api/reports/comparative/{ws_id}")
async def report_comparative(ws_id: str, payload: Dict[str, Any]):
    comps = (payload or {}).get("competitor_domains") or []
    include = bool((payload or {}).get("include_prompts", True))
    html = await comparative_report.generate_html(ws_id, comps, include_prompts=include)
    return ApiResponse(data={"html": html}).dict()


@app.get("/api/reports/comparative/{ws_id}/html")
async def report_comparative_html(ws_id: str, competitors: str = ""):
    comps = [c.strip() for c in competitors.split(",") if c.strip()]
    html = await comparative_report.generate_html(ws_id, comps, include_prompts=True)
    return HTMLResponse(html, headers={"Cache-Control": "no-cache"})


# ─── Public Authority Score badge (NO AUTH) ──────────────────
@app.get("/badge/{slug}.svg")
async def public_badge(slug: str, style: str = "flat"):
    svg = await badge.render_badge_svg(slug, style=style)
    return Response(
        content=svg, media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=300"},
    )


@app.get("/badge/{slug}")
async def public_badge_page(slug: str):
    html = await badge.badge_html(slug)
    return HTMLResponse(html, headers={"Cache-Control": "public, max-age=300"})


# ─── Action Engine (diagnosis → executable Do-It queue) ────────

@app.post("/api/actions/{ws_id}/harvest")
async def actions_harvest(ws_id: str):
    """Scan all diagnostics and materialize pending actions."""
    res = await action_engine.harvest_actions(ws_id)
    return ApiResponse(data=res).dict()


@app.get("/api/actions/{ws_id}")
async def actions_list(ws_id: str, status: str = "", action_type: str = "", limit: int = 200):
    rows = await action_engine.list_actions(ws_id, status=status, action_type=action_type, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.get("/api/actions/{ws_id}/summary")
async def actions_summary(ws_id: str):
    res = await action_engine.queue_summary(ws_id)
    return ApiResponse(data=res).dict()


@app.post("/api/actions/{ws_id}/{action_id}/generate")
async def actions_generate(ws_id: str, action_id: str):
    """Produce the deliverable for one action via Claude."""
    res = await action_engine.generate_action(action_id)
    return ApiResponse(data=res, success=not res.get("error")).dict()


@app.put("/api/actions/{ws_id}/{action_id}/status")
async def actions_set_status(ws_id: str, action_id: str, status: str):
    res = await action_engine.update_status(action_id, status)
    return ApiResponse(data=res).dict()


# ─── GEO Sandbox / Backtest (prove ROI) ───────────────────────

@app.get("/api/backtest/{ws_id}/series")
async def backtest_series(ws_id: str, months: int = 6):
    rows = await backtest.historical_authority_series(ws_id, months=months)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


@app.post("/api/backtest/{ws_id}/project")
async def backtest_project(ws_id: str, action_count: int = 3, horizon_days: int = 90):
    res = await backtest.project_uplift(ws_id, action_count=action_count, horizon_days=horizon_days)
    return ApiResponse(data=res).dict()


@app.get("/api/backtest/{ws_id}/runs")
async def backtest_runs(ws_id: str, limit: int = 50):
    rows = await backtest.list_runs(ws_id, limit=limit)
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()


# ─── Integration status + deep health ─────────────────────────

@app.get("/api/integrations/status")
async def integrations_status_route():
    res = await integrations_status.integration_status()
    return ApiResponse(data=res).dict()


@app.get("/api/health/deep")
async def health_deep_route():
    res = await integrations_status.health_deep()
    return ApiResponse(data=res).dict()


@app.get("/api/peec/field-mapping")
async def get_field_mapping():
    """Return the Peec field mapping spec."""
    from .models import PEEC_FIELD_MAP, VALID_MODEL_SOURCES
    return ApiResponse(data={
        "field_map": PEEC_FIELD_MAP,
        "valid_model_sources": list(VALID_MODEL_SOURCES),
    }).dict()


# ═══════════════════════════════════════════════════════════════
# SOURCES ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/api/sources/{project_id}")
async def get_sources(project_id: str):
    """Get deduplicated source URLs ranked by citation count."""
    sources = await get_project_sources(project_id)
    return ApiResponse(data=sources, meta={"count": len(sources)}).dict()

@app.post("/api/sources/scrape")
async def scrape_sources(req: ScrapeRequest):
    """Scrape one or more URLs."""
    if not req.urls:
        raise HTTPException(400, "No URLs provided")
    if len(req.urls) > 50:
        raise HTTPException(400, "Maximum 50 URLs per request")

    result = await scrape_urls(req.urls, req.project_id)
    return ApiResponse(data=result).dict()

@app.get("/api/sources/scraped/{source_id}")
async def get_scraped_content(source_id: str):
    """Get scraped content for a source."""
    content = await fetch_one(
        "SELECT * FROM scraped_content WHERE source_id = ?", (source_id,)
    )
    if not content:
        raise HTTPException(404, "No scraped content for this source")

    content["headings"] = from_json(content.get("headings", "[]"), [])
    content["lists"] = from_json(content.get("lists", "[]"), [])
    content["faqs"] = from_json(content.get("faqs", "[]"), [])
    content["schema_markup"] = from_json(content.get("schema_markup", "{}"), {})

    return ApiResponse(data=content).dict()


# ═══════════════════════════════════════════════════════════════
# CLUSTERS ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/api/clusters/{project_id}")
async def get_clusters(project_id: str):
    """Get all topic clusters."""
    clusters = await get_project_clusters(project_id)
    return ApiResponse(data=clusters, meta={"count": len(clusters)}).dict()

@app.get("/api/clusters/{cluster_id}/sources")
async def get_cluster_source_list(cluster_id: str):
    """Get sources linked to a cluster with scraped content."""
    sources = await get_cluster_sources(cluster_id)
    return ApiResponse(data=sources, meta={"count": len(sources)}).dict()

@app.post("/api/clusters/merge")
async def merge_clusters(cluster_ids: List[str]):
    """Merge two or more clusters."""
    if len(cluster_ids) < 2:
        raise HTTPException(400, "Need at least 2 cluster IDs to merge")

    clusters = []
    for cid in cluster_ids:
        c = await fetch_one("SELECT * FROM clusters WHERE id = ?", (cid,))
        if c:
            clusters.append(c)

    if len(clusters) < 2:
        raise HTTPException(404, "One or more clusters not found")

    # Create merged cluster
    merged_name = " + ".join(c["name"] for c in clusters)
    all_tags = set()
    total_prompts = 0
    total_urls = 0
    total_citations = 0

    for c in clusters:
        all_tags.update(from_json(c.get("tags", "[]"), []))
        total_prompts += c.get("prompt_count", 0)
        total_urls += c.get("url_count", 0)
        total_citations += c.get("total_citations", 0)

    merged_id = gen_id("cl-")
    avg_rate = sum(c.get("avg_citation_rate", 0) for c in clusters) / len(clusters)

    await execute(
        "INSERT INTO clusters (id, project_id, name, topics, tags, prompt_count, "
        "url_count, avg_citation_rate, total_citations) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            merged_id, clusters[0]["project_id"], merged_name,
            to_json([c["name"] for c in clusters]), to_json(sorted(all_tags)),
            total_prompts, total_urls, avg_rate, total_citations,
        ),
    )

    # Move all records and sources to merged cluster
    for cid in cluster_ids:
        await execute(
            "UPDATE cluster_records SET cluster_id = ? WHERE cluster_id = ?",
            (merged_id, cid),
        )
        await execute(
            "UPDATE cluster_sources SET cluster_id = ? WHERE cluster_id = ?",
            (merged_id, cid),
        )
        await execute("DELETE FROM clusters WHERE id = ?", (cid,))

    return ApiResponse(data={"merged_id": merged_id, "name": merged_name}).dict()


# ═══════════════════════════════════════════════════════════════
# ANALYZE ROUTES
# ═══════════════════════════════════════════════════════════════

@app.post("/api/analyze")
async def run_analysis(req: AnalyzeRequest):
    """Run Claude source analysis on a cluster."""
    if not settings.has_claude_api:
        raise HTTPException(400, "Claude API key not configured. Set GEO_ANTHROPIC_API_KEY.")

    engine = ClaudeEngine()
    try:
        result = await engine.analyze_sources(
            cluster_id=req.cluster_id,
            prompt_type=req.prompt_type,
            model=req.model,
            temperature=req.temperature,
            max_tokens=req.max_tokens,
            use_cache=req.use_cache,
        )
        return ApiResponse(data=result).dict()
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))

@app.post("/api/analyze/brief")
async def generate_brief(req: GenerateBriefRequest):
    """Generate a content brief from analysis."""
    if not settings.has_claude_api:
        raise HTTPException(400, "Claude API key not configured.")

    engine = ClaudeEngine()
    try:
        result = await engine.generate_brief(
            cluster_id=req.cluster_id,
            analysis_id=req.analysis_id,
            model=req.model,
            temperature=req.temperature,
        )
        return ApiResponse(data=result).dict()
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))

@app.get("/api/analyze/{cluster_id}")
async def get_analyses(cluster_id: str):
    """Get all analyses for a cluster."""
    rows = await fetch_all(
        "SELECT * FROM analyses WHERE cluster_id = ? ORDER BY created_at DESC",
        (cluster_id,),
    )
    return ApiResponse(data=rows).dict()

@app.get("/api/briefs/{cluster_id}")
async def get_briefs(cluster_id: str):
    """Get all briefs for a cluster."""
    rows = await fetch_all(
        "SELECT * FROM briefs WHERE cluster_id = ? ORDER BY created_at DESC",
        (cluster_id,),
    )
    return ApiResponse(data=rows).dict()


# ═══════════════════════════════════════════════════════════════
# GENERATE ROUTES
# ═══════════════════════════════════════════════════════════════

@app.post("/api/generate")
async def generate_draft(req: GenerateDraftRequest):
    """Generate a content draft from a brief."""
    if not settings.has_claude_api:
        raise HTTPException(400, "Claude API key not configured.")

    engine = ClaudeEngine()
    try:
        result = await engine.generate_content(
            cluster_id=req.cluster_id,
            brief_id=req.brief_id,
            template_type=req.template_type,
            model=req.model,
            temperature=req.temperature,
            max_tokens=req.max_tokens,
        )
        return ApiResponse(data=result).dict()
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))

@app.get("/api/drafts/{project_id}")
async def get_drafts(project_id: str, status: Optional[str] = None):
    """Get all drafts, optionally filtered by status."""
    query = (
        "SELECT d.*, c.name as cluster_name FROM drafts d "
        "JOIN clusters c ON d.cluster_id = c.id "
        "WHERE c.project_id = ?"
    )
    params = [project_id]

    if status:
        query += " AND d.status = ?"
        params.append(status)

    query += " ORDER BY d.created_at DESC"
    rows = await fetch_all(query, tuple(params))

    for r in rows:
        r["source_snapshot"] = from_json(r.get("source_snapshot", "{}"), {})

    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()

@app.get("/api/drafts/detail/{draft_id}")
async def get_draft_detail(draft_id: str):
    """Get a single draft with full details including version history."""
    draft = await fetch_one(
        "SELECT d.*, c.name as cluster_name FROM drafts d "
        "JOIN clusters c ON d.cluster_id = c.id WHERE d.id = ?",
        (draft_id,),
    )
    if not draft:
        raise HTTPException(404, "Draft not found")

    draft["source_snapshot"] = from_json(draft.get("source_snapshot", "{}"), {})

    # Get version history
    versions = await fetch_all(
        "SELECT id, version, template_type, word_count, model, status, created_at "
        "FROM drafts WHERE cluster_id = ? AND template_type = ? ORDER BY version DESC",
        (draft["cluster_id"], draft["template_type"]),
    )

    draft["version_history"] = versions

    return ApiResponse(data=draft).dict()


# ═══════════════════════════════════════════════════════════════
# REVIEW ROUTES
# ═══════════════════════════════════════════════════════════════

@app.put("/api/review/{draft_id}")
async def review_draft(draft_id: str, req: ReviewDraftRequest):
    """Update a draft's review status."""
    draft = await fetch_one("SELECT id FROM drafts WHERE id = ?", (draft_id,))
    if not draft:
        raise HTTPException(404, "Draft not found")

    if req.status not in ("approved", "rejected", "revision", "pending"):
        raise HTTPException(400, "Invalid status. Use: approved, rejected, revision, pending")

    await execute(
        "UPDATE drafts SET status = ?, reviewer_notes = ?, reviewed_at = ? WHERE id = ?",
        (req.status, req.notes, datetime.utcnow().isoformat(), draft_id),
    )

    return ApiResponse(data={"draft_id": draft_id, "status": req.status}).dict()


# ═══════════════════════════════════════════════════════════════
# EXPORT ROUTES
# ═══════════════════════════════════════════════════════════════

@app.post("/api/export")
async def export_draft(req: ExportRequest):
    """Export a draft in the requested format."""
    draft = await fetch_one("SELECT * FROM drafts WHERE id = ?", (req.draft_id,))
    if not draft:
        raise HTTPException(404, "Draft not found")

    if req.format == "markdown":
        content = draft["content"]
    elif req.format == "html":
        content = markdown_to_html(draft["content"])
    elif req.format == "plaintext":
        # Strip markdown formatting
        import re
        content = draft["content"]
        content = re.sub(r'^#+\s+', '', content, flags=re.MULTILINE)
        content = re.sub(r'\*\*(.+?)\*\*', r'\1', content)
        content = re.sub(r'\*(.+?)\*', r'\1', content)
        content = re.sub(r'`(.+?)`', r'\1', content)
        content = re.sub(r'^- ', '* ', content, flags=re.MULTILINE)
    else:
        raise HTTPException(400, "Invalid format. Use: markdown, html, plaintext")

    # Record export
    export_id = gen_id("ex-")
    await execute(
        "INSERT INTO exports (id, draft_id, format, content, published_to) VALUES (?, ?, ?, ?, ?)",
        (export_id, req.draft_id, req.format, content, "manual"),
    )

    return ApiResponse(data={
        "export_id": export_id,
        "format": req.format,
        "content": content,
        "word_count": len(content.split()),
    }).dict()

@app.get("/api/exports/{project_id}")
async def get_exports(project_id: str):
    """Get all exports for a project."""
    rows = await fetch_all(
        "SELECT e.*, d.cluster_id, c.name as cluster_name "
        "FROM exports e "
        "JOIN drafts d ON e.draft_id = d.id "
        "JOIN clusters c ON d.cluster_id = c.id "
        "WHERE c.project_id = ? ORDER BY e.created_at DESC",
        (project_id,),
    )
    return ApiResponse(data=rows).dict()


# ═══════════════════════════════════════════════════════════════
# CMS PUBLISH ROUTES
# ═══════════════════════════════════════════════════════════════

@app.post("/api/publish")
async def publish(req: PublishRequest):
    """Publish a draft to WordPress or Webflow."""
    result = await publish_to_cms(
        draft_id=req.draft_id,
        cms=req.cms,
        title=req.title,
        slug=req.slug,
        status=req.status,
    )
    return ApiResponse(
        success=result.get("success", False),
        data=result,
        error=result.get("error"),
    ).dict()

@app.post("/api/publish/check/{cms}")
async def check_cms(cms: str):
    """Check CMS connection."""
    if cms == "wordpress":
        from .cms_publisher import WordPressPublisher
        pub = WordPressPublisher()
        result = await pub.check_connection()
    elif cms == "webflow":
        from .cms_publisher import WebflowPublisher
        pub = WebflowPublisher()
        result = await pub.check_connection()
    else:
        raise HTTPException(400, f"Unknown CMS: {cms}")

    return ApiResponse(data=result).dict()


# ═══════════════════════════════════════════════════════════════
# MEASUREMENT / FEEDBACK LOOP ROUTES
# ═══════════════════════════════════════════════════════════════

@app.post("/api/measure")
async def take_measurements(req: MeasureRequest):
    """Take Peec measurements for published URLs."""
    if not settings.has_peec_api:
        raise HTTPException(400, "Peec API not configured. Set GEO_PEEC_API_KEY.")

    result = await measure_performance(
        project_id=req.project_id,
        urls=req.urls,
        draft_id=req.draft_id,
    )
    return ApiResponse(
        success=result.get("success", False),
        data=result,
        error=result.get("error"),
    ).dict()

@app.get("/api/measurements/{project_id}")
async def get_measurements(project_id: str):
    """Get all measurements for a project."""
    rows = await fetch_all(
        "SELECT * FROM measurements WHERE project_id = ? ORDER BY measured_at DESC",
        (project_id,),
    )
    return ApiResponse(data=rows, meta={"count": len(rows)}).dict()

@app.get("/api/measurements/deltas/{project_id}")
async def get_measurement_deltas(project_id: str):
    """Get before/after deltas for all measured URLs."""
    urls = await fetch_all(
        "SELECT DISTINCT url FROM measurements WHERE project_id = ?",
        (project_id,),
    )

    deltas = []
    for url_row in urls:
        url = url_row["url"]
        first = await fetch_one(
            "SELECT * FROM measurements WHERE project_id = ? AND url = ? ORDER BY measured_at ASC LIMIT 1",
            (project_id, url),
        )
        latest = await fetch_one(
            "SELECT * FROM measurements WHERE project_id = ? AND url = ? ORDER BY measured_at DESC LIMIT 1",
            (project_id, url),
        )
        if first and latest and first["id"] != latest["id"]:
            for metric in ["citation_count", "citation_rate", "visibility", "position"]:
                before = first.get(metric, 0) or 0
                after = latest.get(metric, 0) or 0
                diff = after - before
                pct = (diff / before * 100) if before else 0
                deltas.append({
                    "url": url,
                    "metric": metric,
                    "before": before,
                    "after": after,
                    "delta": round(diff, 4),
                    "delta_pct": round(pct, 2),
                    "direction": "improved" if diff > 0 else ("declined" if diff < 0 else "stable"),
                })

    return ApiResponse(data=deltas).dict()


# ═══════════════════════════════════════════════════════════════
# SUCCESS METRICS ROUTES
# ═══════════════════════════════════════════════════════════════

@app.post("/api/metrics")
async def define_metric(req: SuccessMetricRequest):
    """Define a success metric with target."""
    metric_id = gen_id("sm-")
    await execute(
        "INSERT INTO success_metrics (id, project_id, metric_name, target_value, "
        "baseline_value, current_value, direction) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (metric_id, req.project_id, req.metric_name, req.target_value,
         req.baseline_value, req.baseline_value, req.direction),
    )
    return ApiResponse(data={"metric_id": metric_id}).dict()

@app.get("/api/metrics/{project_id}")
async def get_metrics(project_id: str):
    """Get all success metrics for a project."""
    rows = await fetch_all(
        "SELECT * FROM success_metrics WHERE project_id = ?",
        (project_id,),
    )
    return ApiResponse(data=rows).dict()


# ═══════════════════════════════════════════════════════════════
# TOKEN USAGE / COST ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/api/usage/{project_id}")
async def get_usage(project_id: str):
    """Get token usage and cost summary."""
    cost_check = await check_cost_limit(project_id)

    totals = await fetch_one(
        "SELECT COALESCE(SUM(tokens_input), 0) as total_input, "
        "COALESCE(SUM(tokens_output), 0) as total_output, "
        "COALESCE(SUM(cached_tokens), 0) as total_cached "
        "FROM token_usage WHERE project_id = ?",
        (project_id,),
    )

    return ApiResponse(data={
        **cost_check,
        "total_tokens_input": totals["total_input"] if totals else 0,
        "total_tokens_output": totals["total_output"] if totals else 0,
        "total_cached": totals["total_cached"] if totals else 0,
    }).dict()

@app.get("/api/usage/{project_id}/history")
async def get_usage_history(project_id: str, days: int = 30):
    """Get daily usage history."""
    rows = await fetch_all(
        "SELECT date(created_at) as date, "
        "SUM(tokens_input) as tokens_input, "
        "SUM(tokens_output) as tokens_output, "
        "SUM(cached_tokens) as cached_tokens, "
        "SUM(cost) as cost, "
        "COUNT(*) as calls "
        "FROM token_usage WHERE project_id = ? "
        "AND created_at >= datetime('now', ? || ' days') "
        "GROUP BY date(created_at) ORDER BY date DESC",
        (project_id, f"-{days}"),
    )
    return ApiResponse(data=rows).dict()


# ═══════════════════════════════════════════════════════════════
# BATCH PROCESSING ROUTES
# ═══════════════════════════════════════════════════════════════

@app.post("/api/batch/scrape")
async def batch_scrape(req: BatchRequest):
    """Batch scrape multiple URLs."""
    urls = [item.get("url", "") for item in req.items if item.get("url")]
    if not urls:
        raise HTTPException(400, "No valid URLs in batch items")

    # Create batch job record
    job_id = gen_id("bj-")
    await execute(
        "INSERT INTO batch_jobs (id, project_id, job_type, status, total_items, started_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (job_id, req.project_id, "scrape", "running", len(urls), datetime.utcnow().isoformat()),
    )

    # Run scraping
    result = await scrape_urls(urls, req.project_id)

    # Update job
    await execute(
        "UPDATE batch_jobs SET status = 'completed', completed_items = ?, "
        "failed_items = ?, results = ?, completed_at = ? WHERE id = ?",
        (result["scraped"], result["failed"], to_json(result),
         datetime.utcnow().isoformat(), job_id),
    )

    return ApiResponse(data={"job_id": job_id, **result}).dict()

@app.post("/api/batch/analyze")
async def batch_analyze(req: BatchRequest):
    """Batch analyze multiple clusters."""
    if not settings.has_claude_api:
        raise HTTPException(400, "Claude API key not configured.")

    engine = ClaudeEngine()
    cluster_ids = [item.get("cluster_id", "") for item in req.items if item.get("cluster_id")]

    job_id = gen_id("bj-")
    await execute(
        "INSERT INTO batch_jobs (id, project_id, job_type, status, total_items, started_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (job_id, req.project_id, "analyze", "running", len(cluster_ids), datetime.utcnow().isoformat()),
    )

    # Use Claude Batch API for large volumes, sequential for small
    results = []
    completed = 0
    failed = 0

    for cid in cluster_ids:
        try:
            r = await engine.analyze_sources(
                cluster_id=cid,
                prompt_type=req.items[0].get("prompt_type", "blog") if req.items else "blog",
                project_id=req.project_id,
            )
            results.append(r)
            completed += 1
        except Exception as e:
            results.append({"cluster_id": cid, "error": str(e)})
            failed += 1

    await execute(
        "UPDATE batch_jobs SET status = 'completed', completed_items = ?, "
        "failed_items = ?, results = ?, completed_at = ? WHERE id = ?",
        (completed, failed, to_json({"analyses": len(results)}),
         datetime.utcnow().isoformat(), job_id),
    )

    return ApiResponse(data={"job_id": job_id, "completed": completed, "failed": failed}).dict()

@app.get("/api/batch/{job_id}")
async def get_batch_status(job_id: str):
    """Get batch job status."""
    job = await fetch_one("SELECT * FROM batch_jobs WHERE id = ?", (job_id,))
    if not job:
        raise HTTPException(404, "Batch job not found")

    job["results"] = from_json(job.get("results", "{}"), {})
    progress = (job.get("completed_items", 0) / max(job.get("total_items", 1), 1)) * 100

    return ApiResponse(data={**job, "progress_pct": round(progress, 1)}).dict()


# ═══════════════════════════════════════════════════════════════
# PROMPT TEMPLATES ROUTES
# ═══════════════════════════════════════════════════════════════

@app.post("/api/prompts")
async def save_prompt_template(req: PromptTemplateRequest):
    """Save a custom prompt template (creates new version)."""
    existing = await fetch_one(
        "SELECT MAX(version) as max_v FROM prompt_templates "
        "WHERE project_id = ? AND category = ? AND sub_type = ?",
        (req.project_id, req.category, req.sub_type),
    )
    version = (existing["max_v"] or 0) + 1 if existing else 1

    # Deactivate previous versions
    await execute(
        "UPDATE prompt_templates SET is_active = 0 "
        "WHERE project_id = ? AND category = ? AND sub_type = ?",
        (req.project_id, req.category, req.sub_type),
    )

    prompt_id = gen_id("pt-")
    await execute(
        "INSERT INTO prompt_templates (id, project_id, name, category, sub_type, template, version) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (prompt_id, req.project_id, req.name, req.category, req.sub_type, req.template, version),
    )

    return ApiResponse(data={"prompt_id": prompt_id, "version": version}).dict()

@app.get("/api/prompts/{project_id}")
async def get_prompt_templates(project_id: str):
    """Get all active prompt templates."""
    rows = await fetch_all(
        "SELECT * FROM prompt_templates WHERE project_id = ? AND is_active = 1",
        (project_id,),
    )
    return ApiResponse(data=rows).dict()


# ═══════════════════════════════════════════════════════════════
# RUN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run():
    """Run the backend server."""
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="info",
        loop="asyncio",
        http="h11",
    )


if __name__ == "__main__":
    run()
