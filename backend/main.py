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
from fastapi.responses import FileResponse, JSONResponse
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
    logger.info("Initializing Momentus AI database...")
    await init_db()
    logger.info("Initializing auth tables...")
    await init_auth()
    logger.info("Initializing automation engine...")
    await init_automation()
    if MCP_AVAILABLE and settings.has_peec_mcp and settings.peec_mcp_auto_sync_minutes > 0:
        logger.info("Starting Peec MCP auto-sync...")
        await start_auto_sync()
    logger.info("Momentus AI ready on port %s", settings.port)

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down — stopping background tasks...")
    await stop_auto_sync()
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

@app.get("/")
async def root():
    index = _FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(index, media_type="text/html")
    return {"status": "ok", "service": "Momentus AI API", "version": "1.0.0"}

@app.get("/dashboard.jsx")
async def serve_dashboard_jsx():
    jsx = _FRONTEND_DIR / "dashboard.jsx"
    if jsx.exists():
        return FileResponse(jsx, media_type="application/javascript")
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

    records, validation = parse_csv_content(text, project_id)

    if not validation.get("valid"):
        return ApiResponse(
            success=False,
            error="CSV validation failed",
            data=validation,
        ).dict()

    result = await ingest_records(records, project_id)
    result["validation"] = validation

    return ApiResponse(data=result).dict()

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
async def peec_mcp_status():
    """Test the Peec MCP connection and report discovered tools."""
    client = PeecMCPClient()
    status = await client.test_connection()
    return ApiResponse(data=status).dict()


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
