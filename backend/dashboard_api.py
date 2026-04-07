"""
Momentus AI — Dashboard & Reporting API
Performance metrics, time series, client reporting, workspace management.
All routes mounted under /api/dashboard/* and /api/auth/*
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field

from .auth import (
    authenticate_user, create_token, create_user, create_workspace,
    add_workspace_member, get_user_workspaces, has_permission,
    init_auth, log_audit, verify_token,
)
from .database import (
    execute, fetch_all, fetch_one, from_json, gen_id, to_json,
    get_daily_cost, get_monthly_cost,
)

logger = logging.getLogger("geo.dashboard")

# ═══════════════════════════════════════════════════════════════
# REQUEST / RESPONSE MODELS
# ═══════════════════════════════════════════════════════════════

class LoginRequest(BaseModel):
    email: str
    password: str

class RegisterRequest(BaseModel):
    email: str
    password: str
    name: str = ""

class CreateWorkspaceRequest(BaseModel):
    name: str
    slug: str
    brand_name: str = ""
    domains: List[str] = Field(default_factory=list)
    target_countries: List[str] = Field(default_factory=list)
    target_languages: List[str] = Field(default_factory=list)
    target_models: List[str] = Field(default_factory=list)
    brand_voice: str = ""
    compliance_rules: str = ""

class UpdateWorkspaceRequest(BaseModel):
    name: Optional[str] = None
    brand_name: Optional[str] = None
    domains: Optional[List[str]] = None
    target_countries: Optional[List[str]] = None
    target_languages: Optional[List[str]] = None
    target_models: Optional[List[str]] = None
    brand_voice: Optional[str] = None
    compliance_rules: Optional[str] = None
    color_primary: Optional[str] = None
    color_accent: Optional[str] = None

class AddMemberRequest(BaseModel):
    email: str
    role: str = "editor"

class CreateCampaignRequest(BaseModel):
    name: str
    description: str = ""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    goals: Dict[str, Any] = Field(default_factory=dict)

class MetricTargetRequest(BaseModel):
    metric_name: str
    target_value: float
    baseline_value: float = 0.0
    direction: str = "increase"

class ReportRequest(BaseModel):
    period_start: str
    period_end: str
    report_type: str = "custom"

class ScheduleReportRequest(BaseModel):
    frequency: str = "weekly"
    recipients: List[str] = Field(default_factory=list)
    report_config: Dict[str, Any] = Field(default_factory=dict)

class InviteUserRequest(BaseModel):
    email: str
    name: str = ""
    role: str = "editor"
    password: str = ""  # if empty, generate temp password


# ═══════════════════════════════════════════════════════════════
# AUTH DEPENDENCY
# ═══════════════════════════════════════════════════════════════

async def get_current_user(authorization: str = Header(default="")) -> Dict[str, Any]:
    """Extract and verify the current user from Authorization header."""
    if not authorization:
        raise HTTPException(status_code=401, detail="No authorization token")
    token = authorization.replace("Bearer ", "")
    payload = verify_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    user = await fetch_one("SELECT id, email, name, role, is_active FROM users WHERE id = ?", (payload["sub"],))
    if not user or not user["is_active"]:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    return dict(user)


def require_role(min_role: str):
    """Factory for role-based access checks."""
    async def checker(user: Dict = Depends(get_current_user)):
        if not has_permission(user["role"], min_role):
            raise HTTPException(status_code=403, detail=f"Requires {min_role} role or higher")
        return user
    return checker


# ═══════════════════════════════════════════════════════════════
# ROUTERS
# ═══════════════════════════════════════════════════════════════

auth_router = APIRouter(prefix="/api/auth", tags=["auth"])
dashboard_router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])
workspace_router = APIRouter(prefix="/api/workspaces", tags=["workspaces"])


# ─── AUTH ROUTES ──────────────────────────────────────────────

@auth_router.post("/login")
async def login(req: LoginRequest, request: Request):
    """Authenticate and return JWT token."""
    user = await authenticate_user(req.email, req.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    token = create_token(user["id"], user["email"], user["role"])
    workspaces = await get_user_workspaces(user["id"])
    ip = request.client.host if request.client else ""
    await log_audit(user["id"], "login", ip_address=ip)
    return {
        "success": True,
        "data": {
            "token": token,
            "user": user,
            "workspaces": workspaces,
        }
    }


@auth_router.post("/register")
async def register(req: RegisterRequest):
    """Register a new user (admin-only in production, open for dev)."""
    try:
        user = await create_user(req.email, req.password, req.name, role="editor")
        token = create_token(user["id"], user["email"], user["role"])
        return {"success": True, "data": {"token": token, "user": user}}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@auth_router.get("/me")
async def get_me(user: Dict = Depends(get_current_user)):
    """Get current user profile."""
    workspaces = await get_user_workspaces(user["id"])
    return {"success": True, "data": {"user": user, "workspaces": workspaces}}


@auth_router.post("/invite")
async def invite_user(req: InviteUserRequest, user: Dict = Depends(require_role("admin"))):
    """Invite a new user to the platform."""
    import secrets
    password = req.password or secrets.token_urlsafe(12)
    try:
        new_user = await create_user(req.email, password, req.name, role=req.role)
        await log_audit(user["id"], "create", resource_type="user", resource_id=new_user["id"])
        return {"success": True, "data": {"user": new_user, "temp_password": password}}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ─── WORKSPACE ROUTES ────────────────────────────────────────

@workspace_router.get("/")
async def list_workspaces(user: Dict = Depends(get_current_user)):
    """List workspaces the user belongs to."""
    if user["role"] == "superadmin":
        workspaces = await fetch_all("SELECT * FROM workspaces WHERE is_active = 1 ORDER BY name")
    else:
        workspaces = await get_user_workspaces(user["id"])
    return {"success": True, "data": workspaces}


@workspace_router.post("/")
async def create_workspace_route(req: CreateWorkspaceRequest, user: Dict = Depends(require_role("admin"))):
    """Create a new client workspace."""
    ws = await create_workspace(req.name, req.slug, req.brand_name, created_by=user["id"])
    # Update additional fields
    await execute(
        """UPDATE workspaces SET
           domains = ?, target_countries = ?, target_languages = ?,
           target_models = ?, brand_voice = ?, compliance_rules = ?
           WHERE id = ?""",
        (to_json(req.domains), to_json(req.target_countries), to_json(req.target_languages),
         to_json(req.target_models), req.brand_voice, req.compliance_rules, ws["id"]),
    )
    # Also create a project for this workspace
    await execute(
        "INSERT OR IGNORE INTO projects (id, name, config) VALUES (?, ?, ?)",
        (ws["id"], ws["name"], to_json({"workspace_id": ws["id"]})),
    )
    await log_audit(user["id"], "create", ws["id"], "workspace", ws["id"])
    return {"success": True, "data": ws}


@workspace_router.get("/{ws_id}")
async def get_workspace(ws_id: str, user: Dict = Depends(get_current_user)):
    """Get workspace details."""
    ws = await fetch_one("SELECT * FROM workspaces WHERE id = ?", (ws_id,))
    if not ws:
        raise HTTPException(status_code=404, detail="Workspace not found")
    members = await fetch_all(
        """SELECT u.id, u.email, u.name, u.role as global_role, wm.role as workspace_role
           FROM users u JOIN workspace_members wm ON u.id = wm.user_id
           WHERE wm.workspace_id = ?""",
        (ws_id,),
    )
    campaigns = await fetch_all(
        "SELECT * FROM campaigns WHERE workspace_id = ? ORDER BY created_at DESC",
        (ws_id,),
    )
    return {"success": True, "data": {**dict(ws), "members": members, "campaigns": campaigns}}


@workspace_router.put("/{ws_id}")
async def update_workspace(ws_id: str, req: UpdateWorkspaceRequest, user: Dict = Depends(require_role("admin"))):
    """Update workspace settings."""
    updates = []
    params = []
    for field, value in req.dict(exclude_none=True).items():
        if isinstance(value, (list, dict)):
            updates.append(f"{field} = ?")
            params.append(to_json(value))
        else:
            updates.append(f"{field} = ?")
            params.append(value)
    if updates:
        updates.append("updated_at = datetime('now')")
        params.append(ws_id)
        await execute(f"UPDATE workspaces SET {', '.join(updates)} WHERE id = ?", tuple(params))
    await log_audit(user["id"], "update", ws_id, "workspace", ws_id)
    return {"success": True, "data": {"updated": True}}


@workspace_router.post("/{ws_id}/members")
async def add_member(ws_id: str, req: AddMemberRequest, user: Dict = Depends(require_role("admin"))):
    """Add a member to a workspace."""
    target = await fetch_one("SELECT id FROM users WHERE email = ?", (req.email,))
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    ok = await add_workspace_member(ws_id, target["id"], req.role)
    return {"success": ok}


@workspace_router.get("/{ws_id}/members")
async def list_members(ws_id: str, user: Dict = Depends(get_current_user)):
    """List workspace members."""
    members = await fetch_all(
        """SELECT u.id, u.email, u.name, u.role as global_role, wm.role as workspace_role, wm.joined_at
           FROM users u JOIN workspace_members wm ON u.id = wm.user_id
           WHERE wm.workspace_id = ?""",
        (ws_id,),
    )
    return {"success": True, "data": members}


# ─── CAMPAIGNS ────────────────────────────────────────────────

@workspace_router.post("/{ws_id}/campaigns")
async def create_campaign(ws_id: str, req: CreateCampaignRequest, user: Dict = Depends(require_role("editor"))):
    """Create a new campaign in a workspace."""
    cid = gen_id("cmp-")
    await execute(
        "INSERT INTO campaigns (id, workspace_id, name, description, start_date, end_date, goals, created_by) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (cid, ws_id, req.name, req.description, req.start_date, req.end_date, to_json(req.goals), user["id"]),
    )
    await log_audit(user["id"], "create", ws_id, "campaign", cid)
    return {"success": True, "data": {"id": cid, "name": req.name}}


@workspace_router.get("/{ws_id}/campaigns")
async def list_campaigns(ws_id: str, user: Dict = Depends(get_current_user)):
    """List campaigns in a workspace."""
    campaigns = await fetch_all(
        "SELECT * FROM campaigns WHERE workspace_id = ? ORDER BY created_at DESC",
        (ws_id,),
    )
    return {"success": True, "data": campaigns}


# ─── DASHBOARD METRICS ───────────────────────────────────────

@dashboard_router.get("/overview/{ws_id}")
async def dashboard_overview(ws_id: str, user: Dict = Depends(get_current_user)):
    """Get high-level dashboard metrics for a workspace."""
    # Use workspace_id as project_id (they're linked)
    project_id = ws_id

    # Pipeline counts
    records = await fetch_one(
        "SELECT COUNT(*) as cnt FROM peec_records WHERE project_id = ?", (project_id,)
    )
    sources = await fetch_one(
        "SELECT COUNT(*) as cnt FROM sources WHERE project_id = ?", (project_id,)
    )
    scraped = await fetch_one(
        "SELECT COUNT(*) as cnt FROM sources WHERE project_id = ? AND scrape_status = 'scraped'", (project_id,)
    )
    clusters = await fetch_one(
        "SELECT COUNT(*) as cnt FROM clusters WHERE project_id = ?", (project_id,)
    )
    drafts_total = await fetch_all(
        """SELECT status, COUNT(*) as cnt FROM drafts d
           JOIN clusters c ON d.cluster_id = c.id
           WHERE c.project_id = ?
           GROUP BY status""",
        (project_id,),
    )
    exports = await fetch_one(
        """SELECT COUNT(*) as cnt FROM exports e
           JOIN drafts d ON e.draft_id = d.id
           JOIN clusters c ON d.cluster_id = c.id
           WHERE c.project_id = ?""",
        (project_id,),
    )

    # Cost summary
    daily_cost = await get_daily_cost(project_id)
    monthly_cost = await get_monthly_cost(project_id)

    # Recent measurements
    measurements = await fetch_all(
        "SELECT * FROM measurements WHERE project_id = ? ORDER BY measured_at DESC LIMIT 20",
        (project_id,),
    )

    # Success metrics
    metrics = await fetch_all(
        "SELECT * FROM success_metrics WHERE project_id = ?", (project_id,)
    )

    draft_status = {row["status"]: row["cnt"] for row in drafts_total}

    return {
        "success": True,
        "data": {
            "pipeline": {
                "records": records["cnt"] if records else 0,
                "sources": sources["cnt"] if sources else 0,
                "scraped": scraped["cnt"] if scraped else 0,
                "clusters": clusters["cnt"] if clusters else 0,
                "drafts_pending": draft_status.get("pending", 0),
                "drafts_approved": draft_status.get("approved", 0),
                "drafts_rejected": draft_status.get("rejected", 0),
                "drafts_revision": draft_status.get("revision", 0),
                "exports": exports["cnt"] if exports else 0,
            },
            "cost": {
                "daily": round(daily_cost, 4),
                "monthly": round(monthly_cost, 4),
            },
            "measurements": measurements,
            "success_metrics": metrics,
        }
    }


@dashboard_router.get("/performance/{ws_id}")
async def performance_metrics(ws_id: str, days: int = 30, user: Dict = Depends(get_current_user)):
    """Get performance time series for a workspace."""
    project_id = ws_id
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()

    # Citation metrics over time
    citation_series = await fetch_all(
        """SELECT date(measured_at) as date,
                  AVG(citation_rate) as avg_citation_rate,
                  SUM(citation_count) as total_citations,
                  AVG(visibility) as avg_visibility,
                  AVG(position) as avg_position,
                  SUM(brand_mentions) as total_brand_mentions,
                  COUNT(*) as measurement_count
           FROM measurements
           WHERE project_id = ? AND measured_at >= ?
           GROUP BY date(measured_at)
           ORDER BY date""",
        (project_id, since),
    )

    # Per-model breakdown
    model_breakdown = await fetch_all(
        """SELECT model_source,
                  AVG(citation_rate) as avg_citation_rate,
                  SUM(citation_count) as total_citations,
                  AVG(visibility) as avg_visibility,
                  COUNT(*) as cnt
           FROM measurements
           WHERE project_id = ? AND measured_at >= ?
           GROUP BY model_source""",
        (project_id, since),
    )

    # Top performing URLs
    top_urls = await fetch_all(
        """SELECT url,
                  AVG(citation_rate) as avg_rate,
                  SUM(citation_count) as total_citations,
                  COUNT(*) as measurements
           FROM measurements
           WHERE project_id = ? AND measured_at >= ?
           GROUP BY url
           ORDER BY avg_rate DESC
           LIMIT 20""",
        (project_id, since),
    )

    # Cost time series
    cost_series = await fetch_all(
        """SELECT date(created_at) as date,
                  SUM(cost) as total_cost,
                  SUM(tokens_input) as total_input,
                  SUM(tokens_output) as total_output,
                  COUNT(*) as api_calls
           FROM token_usage
           WHERE project_id = ? AND created_at >= ?
           GROUP BY date(created_at)
           ORDER BY date""",
        (project_id, since),
    )

    return {
        "success": True,
        "data": {
            "citation_series": citation_series,
            "model_breakdown": model_breakdown,
            "top_urls": top_urls,
            "cost_series": cost_series,
            "period_days": days,
        }
    }


@dashboard_router.get("/deltas/{ws_id}")
async def performance_deltas(ws_id: str, user: Dict = Depends(get_current_user)):
    """Get before/after deltas for published content."""
    project_id = ws_id

    # Find URLs that have both pre-publish and post-publish measurements
    deltas = await fetch_all(
        """SELECT
             m_after.url,
             m_after.draft_id,
             m_before.citation_rate as before_rate,
             m_after.citation_rate as after_rate,
             m_after.citation_rate - m_before.citation_rate as delta_rate,
             m_before.citation_count as before_citations,
             m_after.citation_count as after_citations,
             m_after.citation_count - m_before.citation_count as delta_citations,
             m_before.visibility as before_visibility,
             m_after.visibility as after_visibility,
             m_after.visibility - m_before.visibility as delta_visibility,
             m_before.brand_mentions as before_mentions,
             m_after.brand_mentions as after_mentions,
             m_after.brand_mentions - m_before.brand_mentions as delta_mentions,
             m_before.measured_at as before_date,
             m_after.measured_at as after_date
           FROM measurements m_after
           JOIN measurements m_before ON m_after.url = m_before.url
              AND m_before.measured_at < m_after.measured_at
              AND m_before.draft_id IS NULL
              AND m_after.draft_id IS NOT NULL
           WHERE m_after.project_id = ?
           ORDER BY m_after.measured_at DESC
           LIMIT 50""",
        (project_id,),
    )

    # Aggregate deltas
    total_before_citations = sum(d["before_citations"] or 0 for d in deltas)
    total_after_citations = sum(d["after_citations"] or 0 for d in deltas)
    avg_before_rate = sum(d["before_rate"] or 0 for d in deltas) / max(len(deltas), 1)
    avg_after_rate = sum(d["after_rate"] or 0 for d in deltas) / max(len(deltas), 1)

    return {
        "success": True,
        "data": {
            "deltas": deltas,
            "summary": {
                "urls_tracked": len(deltas),
                "total_citation_lift": total_after_citations - total_before_citations,
                "avg_rate_before": round(avg_before_rate, 4),
                "avg_rate_after": round(avg_after_rate, 4),
                "avg_rate_lift": round(avg_after_rate - avg_before_rate, 4),
            }
        }
    }


@dashboard_router.get("/pipeline-activity/{ws_id}")
async def pipeline_activity(ws_id: str, days: int = 7, user: Dict = Depends(get_current_user)):
    """Get recent pipeline activity — what's been done, what's in progress."""
    project_id = ws_id
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()

    # Recent imports
    imports = await fetch_all(
        """SELECT import_batch_id, COUNT(*) as record_count, MIN(imported_at) as imported_at
           FROM peec_records
           WHERE project_id = ? AND imported_at >= ?
           GROUP BY import_batch_id
           ORDER BY imported_at DESC""",
        (project_id, since),
    )

    # Recent scrapes
    scrapes = await fetch_all(
        "SELECT id, url, title, scrape_status, scraped_at FROM sources "
        "WHERE project_id = ? AND scraped_at >= ? ORDER BY scraped_at DESC LIMIT 50",
        (project_id, since),
    )

    # Recent analyses
    analyses = await fetch_all(
        """SELECT a.*, c.name as cluster_name
           FROM analyses a JOIN clusters c ON a.cluster_id = c.id
           WHERE c.project_id = ? AND a.created_at >= ?
           ORDER BY a.created_at DESC LIMIT 20""",
        (project_id, since),
    )

    # Recent drafts
    drafts = await fetch_all(
        """SELECT d.*, c.name as cluster_name
           FROM drafts d JOIN clusters c ON d.cluster_id = c.id
           WHERE c.project_id = ? AND d.created_at >= ?
           ORDER BY d.created_at DESC LIMIT 20""",
        (project_id, since),
    )

    # Recent exports
    exports = await fetch_all(
        """SELECT e.*, d.cluster_id, c.name as cluster_name
           FROM exports e
           JOIN drafts d ON e.draft_id = d.id
           JOIN clusters c ON d.cluster_id = c.id
           WHERE c.project_id = ? AND e.created_at >= ?
           ORDER BY e.created_at DESC LIMIT 20""",
        (project_id, since),
    )

    # Batch jobs
    batches = await fetch_all(
        "SELECT * FROM batch_jobs WHERE project_id = ? ORDER BY created_at DESC LIMIT 10",
        (project_id,),
    )

    return {
        "success": True,
        "data": {
            "imports": imports,
            "scrapes": scrapes,
            "analyses": analyses,
            "drafts": drafts,
            "exports": exports,
            "batch_jobs": batches,
        }
    }


# ─── REPORTING ────────────────────────────────────────────────

@dashboard_router.post("/reports/{ws_id}")
async def generate_report(ws_id: str, req: ReportRequest, user: Dict = Depends(require_role("editor"))):
    """Generate a performance report snapshot."""
    project_id = ws_id

    # Gather all metrics for the period
    measurements = await fetch_all(
        "SELECT * FROM measurements WHERE project_id = ? AND measured_at BETWEEN ? AND ?",
        (project_id, req.period_start, req.period_end),
    )
    cost_data = await fetch_all(
        "SELECT * FROM token_usage WHERE project_id = ? AND created_at BETWEEN ? AND ?",
        (project_id, req.period_start, req.period_end),
    )
    drafts = await fetch_all(
        """SELECT d.* FROM drafts d
           JOIN clusters c ON d.cluster_id = c.id
           WHERE c.project_id = ? AND d.created_at BETWEEN ? AND ?""",
        (project_id, req.period_start, req.period_end),
    )

    metrics = {
        "measurement_count": len(measurements),
        "avg_citation_rate": sum(m["citation_rate"] or 0 for m in measurements) / max(len(measurements), 1),
        "total_citations": sum(m["citation_count"] or 0 for m in measurements),
        "total_cost": sum(c["cost"] or 0 for c in cost_data),
        "total_api_calls": len(cost_data),
        "drafts_created": len(drafts),
        "drafts_approved": len([d for d in drafts if d["status"] == "approved"]),
        "model_breakdown": {},
    }

    # Per-model stats
    model_stats = {}
    for m in measurements:
        src = m.get("model_source", "Other")
        if src not in model_stats:
            model_stats[src] = {"citations": 0, "measurements": 0, "rate_sum": 0}
        model_stats[src]["citations"] += m.get("citation_count", 0)
        model_stats[src]["measurements"] += 1
        model_stats[src]["rate_sum"] += m.get("citation_rate", 0)
    for src, stats in model_stats.items():
        stats["avg_rate"] = stats["rate_sum"] / max(stats["measurements"], 1)
        del stats["rate_sum"]
    metrics["model_breakdown"] = model_stats

    # Save snapshot
    report_id = gen_id("rpt-")
    await execute(
        "INSERT INTO report_snapshots (id, workspace_id, period_start, period_end, report_type, metrics, generated_by) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (report_id, ws_id, req.period_start, req.period_end, req.report_type, to_json(metrics), user["id"]),
    )

    await log_audit(user["id"], "create", ws_id, "report", report_id)
    return {"success": True, "data": {"id": report_id, "metrics": metrics}}


@dashboard_router.get("/reports/{ws_id}")
async def list_reports(ws_id: str, user: Dict = Depends(get_current_user)):
    """List saved report snapshots."""
    reports = await fetch_all(
        "SELECT * FROM report_snapshots WHERE workspace_id = ? ORDER BY created_at DESC LIMIT 50",
        (ws_id,),
    )
    for r in reports:
        r["metrics"] = from_json(r.get("metrics", "{}"))
    return {"success": True, "data": reports}


@dashboard_router.get("/reports/{ws_id}/{report_id}")
async def get_report(ws_id: str, report_id: str, user: Dict = Depends(get_current_user)):
    """Get a specific report snapshot."""
    report = await fetch_one(
        "SELECT * FROM report_snapshots WHERE id = ? AND workspace_id = ?",
        (report_id, ws_id),
    )
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    report["metrics"] = from_json(report.get("metrics", "{}"))
    return {"success": True, "data": report}


# ─── METRIC TARGETS ──────────────────────────────────────────

@dashboard_router.post("/metrics/{ws_id}")
async def set_metric_target(ws_id: str, req: MetricTargetRequest, user: Dict = Depends(require_role("editor"))):
    """Set or update a success metric target."""
    project_id = ws_id
    existing = await fetch_one(
        "SELECT id FROM success_metrics WHERE project_id = ? AND metric_name = ?",
        (project_id, req.metric_name),
    )
    if existing:
        await execute(
            "UPDATE success_metrics SET target_value = ?, baseline_value = ?, direction = ?, updated_at = datetime('now') WHERE id = ?",
            (req.target_value, req.baseline_value, req.direction, existing["id"]),
        )
        return {"success": True, "data": {"id": existing["id"], "updated": True}}
    else:
        mid = gen_id("sm-")
        await execute(
            "INSERT INTO success_metrics (id, project_id, metric_name, target_value, baseline_value, direction) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (mid, project_id, req.metric_name, req.target_value, req.baseline_value, req.direction),
        )
        return {"success": True, "data": {"id": mid, "created": True}}


@dashboard_router.get("/metrics/{ws_id}")
async def get_metric_targets(ws_id: str, user: Dict = Depends(get_current_user)):
    """Get all success metric targets for a workspace."""
    metrics = await fetch_all(
        "SELECT * FROM success_metrics WHERE project_id = ?", (ws_id,)
    )
    return {"success": True, "data": metrics}


# ─── AUDIT LOG ────────────────────────────────────────────────

@dashboard_router.get("/audit/{ws_id}")
async def get_audit_log(ws_id: str, limit: int = 50, user: Dict = Depends(require_role("admin"))):
    """Get audit log for a workspace."""
    logs = await fetch_all(
        """SELECT al.*, u.name as user_name, u.email as user_email
           FROM audit_log al
           LEFT JOIN users u ON al.user_id = u.id
           WHERE al.workspace_id = ?
           ORDER BY al.created_at DESC
           LIMIT ?""",
        (ws_id, limit),
    )
    return {"success": True, "data": logs}


# ─── USERS MANAGEMENT ────────────────────────────────────────

@dashboard_router.get("/users")
async def list_users(user: Dict = Depends(require_role("admin"))):
    """List all users (admin only)."""
    users = await fetch_all(
        "SELECT id, email, name, role, is_active, last_login, created_at FROM users ORDER BY name"
    )
    return {"success": True, "data": users}


@dashboard_router.put("/users/{user_id}/role")
async def update_user_role(user_id: str, role: str, user: Dict = Depends(require_role("superadmin"))):
    """Update a user's global role (superadmin only)."""
    await execute("UPDATE users SET role = ?, updated_at = datetime('now') WHERE id = ?", (role, user_id))
    await log_audit(user["id"], "update", resource_type="user", resource_id=user_id, details={"role": role})
    return {"success": True}


@dashboard_router.put("/users/{user_id}/active")
async def toggle_user_active(user_id: str, active: bool = True, user: Dict = Depends(require_role("admin"))):
    """Activate or deactivate a user."""
    await execute("UPDATE users SET is_active = ?, updated_at = datetime('now') WHERE id = ?", (1 if active else 0, user_id))
    return {"success": True}


# ─── CROSS-WORKSPACE OVERVIEW (superadmin) ────────────────────

@dashboard_router.get("/global-overview")
async def global_overview(user: Dict = Depends(require_role("superadmin"))):
    """Get cross-workspace metrics for superadmin."""
    workspaces = await fetch_all("SELECT * FROM workspaces WHERE is_active = 1")
    users = await fetch_one("SELECT COUNT(*) as cnt FROM users WHERE is_active = 1")
    total_records = await fetch_one("SELECT COUNT(*) as cnt FROM peec_records")
    total_drafts = await fetch_one("SELECT COUNT(*) as cnt FROM drafts")
    total_exports = await fetch_one("SELECT COUNT(*) as cnt FROM exports")

    # Cost across all projects
    total_cost = await fetch_one("SELECT COALESCE(SUM(cost), 0) as total FROM token_usage")

    return {
        "success": True,
        "data": {
            "workspaces": len(workspaces),
            "users": users["cnt"] if users else 0,
            "total_records": total_records["cnt"] if total_records else 0,
            "total_drafts": total_drafts["cnt"] if total_drafts else 0,
            "total_exports": total_exports["cnt"] if total_exports else 0,
            "total_cost": round(total_cost["total"], 2) if total_cost else 0,
            "workspace_list": workspaces,
        }
    }
