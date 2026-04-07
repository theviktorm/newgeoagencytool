"""
Momentus AI — Agency Operations API
Task board, job queue, automation rules, onboarding, billing,
competitors, recommendations, playbooks, notifications.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from .dashboard_api import get_current_user, require_role
from .database import execute, fetch_all, fetch_one, gen_id, to_json, from_json
from .automation import (
    create_job, get_job_stats, create_automation_rule,
    create_task, update_task_status, add_task_comment, get_task_board,
    get_notifications, mark_notification_read, mark_all_notifications_read,
    get_onboarding, update_onboarding,
    generate_billing_record, get_billing_history,
    track_competitor, get_competitors, update_competitor_stats,
    get_recommendations, create_recommendation,
    detect_refresh_candidates, get_team_performance,
)

logger = logging.getLogger("geo.ops")

ops_router = APIRouter(prefix="/api/ops", tags=["operations"])


# ═══════════════════════════════════════════════════════════════
# REQUEST MODELS
# ═══════════════════════════════════════════════════════════════

class CreateJobRequest(BaseModel):
    job_type: str
    params: Dict[str, Any] = Field(default_factory=dict)
    priority: int = 5
    campaign_id: str = ""
    scheduled_for: Optional[str] = None

class CreateTaskRequest(BaseModel):
    title: str
    task_type: str = "other"
    description: str = ""
    priority: str = "medium"
    assigned_to: str = ""
    campaign_id: str = ""
    cluster_id: str = ""
    draft_id: str = ""
    due_date: Optional[str] = None
    checklist: List[Dict[str, Any]] = Field(default_factory=list)

class UpdateTaskRequest(BaseModel):
    status: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    priority: Optional[str] = None
    assigned_to: Optional[str] = None
    due_date: Optional[str] = None

class TaskCommentRequest(BaseModel):
    text: str

class CreateRuleRequest(BaseModel):
    name: str
    trigger_event: str
    action_type: str
    trigger_config: Dict[str, Any] = Field(default_factory=dict)
    action_config: Dict[str, Any] = Field(default_factory=dict)

class OnboardingRequest(BaseModel):
    status: Optional[str] = None
    domain_info: Optional[str] = None
    target_topics: Optional[List[str]] = None
    competitors: Optional[List[str]] = None
    brand_voice_notes: Optional[str] = None
    cms_type: Optional[str] = None
    cms_access: Optional[int] = None
    approval_workflow: Optional[str] = None
    notes: Optional[str] = None
    completed_steps: Optional[List[str]] = None

class BillingRequest(BaseModel):
    period_start: str
    period_end: str

class CompetitorRequest(BaseModel):
    domain: str
    name: str = ""

class RecommendationRequest(BaseModel):
    rec_type: str
    title: str
    description: str = ""
    priority_score: float = 50.0
    cluster_id: str = ""


# ═══════════════════════════════════════════════════════════════
# JOB QUEUE ROUTES
# ═══════════════════════════════════════════════════════════════

@ops_router.post("/jobs/{ws_id}")
async def create_job_route(ws_id: str, req: CreateJobRequest, user: Dict = Depends(get_current_user)):
    job_id = await create_job(
        ws_id, req.job_type, req.params, req.priority,
        user["id"], req.campaign_id, req.scheduled_for,
    )
    return {"success": True, "data": {"id": job_id}}


@ops_router.get("/jobs/{ws_id}")
async def list_jobs(ws_id: str, status: str = "", limit: int = 50, user: Dict = Depends(get_current_user)):
    where = "workspace_id = ?"
    params = [ws_id]
    if status:
        where += " AND status = ?"
        params.append(status)
    params.append(limit)
    jobs = await fetch_all(
        f"SELECT * FROM job_queue WHERE {where} ORDER BY created_at DESC LIMIT ?",
        tuple(params),
    )
    for j in jobs:
        j["params"] = from_json(j.get("params", "{}"))
        j["result"] = from_json(j.get("result", "{}"))
    return {"success": True, "data": jobs}


@ops_router.get("/jobs/{ws_id}/stats")
async def job_stats(ws_id: str, user: Dict = Depends(get_current_user)):
    stats = await get_job_stats(ws_id)
    return {"success": True, "data": stats}


@ops_router.post("/jobs/{ws_id}/{job_id}/cancel")
async def cancel_job(ws_id: str, job_id: str, user: Dict = Depends(require_role("editor"))):
    await execute(
        "UPDATE job_queue SET status = 'cancelled', completed_at = datetime('now') WHERE id = ? AND workspace_id = ?",
        (job_id, ws_id),
    )
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# TASK BOARD ROUTES
# ═══════════════════════════════════════════════════════════════

@ops_router.get("/tasks/{ws_id}")
async def get_tasks(ws_id: str, campaign_id: str = "", assigned_to: str = "", task_type: str = "",
                    user: Dict = Depends(get_current_user)):
    filters = {}
    if campaign_id: filters["campaign_id"] = campaign_id
    if assigned_to: filters["assigned_to"] = assigned_to
    if task_type: filters["task_type"] = task_type
    board = await get_task_board(ws_id, filters or None)
    return {"success": True, "data": board}


@ops_router.post("/tasks/{ws_id}")
async def create_task_route(ws_id: str, req: CreateTaskRequest, user: Dict = Depends(get_current_user)):
    task_id = await create_task(
        ws_id, req.title, req.task_type, user["id"],
        req.description, req.priority, req.assigned_to,
        req.campaign_id, req.cluster_id, req.draft_id,
        req.due_date, req.checklist,
    )
    return {"success": True, "data": {"id": task_id}}


@ops_router.put("/tasks/{ws_id}/{task_id}")
async def update_task(ws_id: str, task_id: str, req: UpdateTaskRequest, user: Dict = Depends(get_current_user)):
    if req.status:
        await update_task_status(task_id, req.status, user["id"])
    updates = []
    params = []
    for field in ["title", "description", "priority", "assigned_to", "due_date"]:
        val = getattr(req, field, None)
        if val is not None:
            updates.append(f"{field} = ?")
            params.append(val)
    if updates:
        updates.append("updated_at = datetime('now')")
        params.append(task_id)
        await execute(f"UPDATE tasks SET {', '.join(updates)} WHERE id = ?", tuple(params))
    return {"success": True}


@ops_router.post("/tasks/{ws_id}/{task_id}/comment")
async def comment_on_task(ws_id: str, task_id: str, req: TaskCommentRequest, user: Dict = Depends(get_current_user)):
    await add_task_comment(task_id, user["id"], req.text)
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# AUTOMATION RULES
# ═══════════════════════════════════════════════════════════════

@ops_router.get("/automations/{ws_id}")
async def list_automations(ws_id: str, user: Dict = Depends(get_current_user)):
    rules = await fetch_all(
        "SELECT * FROM automation_rules WHERE workspace_id = ? ORDER BY created_at DESC",
        (ws_id,),
    )
    for r in rules:
        r["trigger_config"] = from_json(r.get("trigger_config", "{}"))
        r["action_config"] = from_json(r.get("action_config", "{}"))
    return {"success": True, "data": rules}


@ops_router.post("/automations/{ws_id}")
async def create_automation(ws_id: str, req: CreateRuleRequest, user: Dict = Depends(require_role("admin"))):
    rule_id = await create_automation_rule(
        ws_id, req.name, req.trigger_event,
        req.action_type, req.trigger_config, req.action_config, user["id"],
    )
    return {"success": True, "data": {"id": rule_id}}


@ops_router.put("/automations/{ws_id}/{rule_id}/toggle")
async def toggle_automation(ws_id: str, rule_id: str, user: Dict = Depends(require_role("admin"))):
    rule = await fetch_one("SELECT is_active FROM automation_rules WHERE id = ? AND workspace_id = ?", (rule_id, ws_id))
    if not rule:
        raise HTTPException(status_code=404)
    new_state = 0 if rule["is_active"] else 1
    await execute("UPDATE automation_rules SET is_active = ? WHERE id = ?", (new_state, rule_id))
    return {"success": True, "data": {"is_active": bool(new_state)}}


# ═══════════════════════════════════════════════════════════════
# NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════

@ops_router.get("/notifications")
async def list_notifications(unread: bool = False, user: Dict = Depends(get_current_user)):
    notifs = await get_notifications(user["id"], unread)
    unread_count = len([n for n in notifs if not n.get("is_read")])
    return {"success": True, "data": {"notifications": notifs, "unread_count": unread_count}}


@ops_router.post("/notifications/{notif_id}/read")
async def read_notification(notif_id: str, user: Dict = Depends(get_current_user)):
    await mark_notification_read(notif_id)
    return {"success": True}


@ops_router.post("/notifications/read-all")
async def read_all_notifications(user: Dict = Depends(get_current_user)):
    await mark_all_notifications_read(user["id"])
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# ONBOARDING
# ═══════════════════════════════════════════════════════════════

@ops_router.get("/onboarding/{ws_id}")
async def get_onboarding_route(ws_id: str, user: Dict = Depends(get_current_user)):
    ob = await get_onboarding(ws_id)
    return {"success": True, "data": ob}


@ops_router.put("/onboarding/{ws_id}")
async def update_onboarding_route(ws_id: str, req: OnboardingRequest, user: Dict = Depends(require_role("editor"))):
    data = req.dict(exclude_none=True)
    ob_id = await update_onboarding(ws_id, data)
    return {"success": True, "data": {"id": ob_id}}


# ═══════════════════════════════════════════════════════════════
# BILLING
# ═══════════════════════════════════════════════════════════════

@ops_router.post("/billing/{ws_id}")
async def generate_billing(ws_id: str, req: BillingRequest, user: Dict = Depends(require_role("admin"))):
    record = await generate_billing_record(ws_id, req.period_start, req.period_end)
    return {"success": True, "data": record}


@ops_router.get("/billing/{ws_id}")
async def list_billing(ws_id: str, user: Dict = Depends(get_current_user)):
    records = await get_billing_history(ws_id)
    return {"success": True, "data": records}


# ═══════════════════════════════════════════════════════════════
# COMPETITORS
# ═══════════════════════════════════════════════════════════════

@ops_router.get("/competitors/{ws_id}")
async def list_competitors(ws_id: str, user: Dict = Depends(get_current_user)):
    comps = await get_competitors(ws_id)
    return {"success": True, "data": comps}


@ops_router.post("/competitors/{ws_id}")
async def add_competitor(ws_id: str, req: CompetitorRequest, user: Dict = Depends(require_role("editor"))):
    comp_id = await track_competitor(ws_id, req.domain, req.name)
    return {"success": True, "data": {"id": comp_id}}


@ops_router.post("/competitors/{ws_id}/refresh")
async def refresh_competitors(ws_id: str, user: Dict = Depends(require_role("editor"))):
    await update_competitor_stats(ws_id)
    comps = await get_competitors(ws_id)
    return {"success": True, "data": comps}


# ═══════════════════════════════════════════════════════════════
# RECOMMENDATIONS
# ═══════════════════════════════════════════════════════════════

@ops_router.get("/recommendations/{ws_id}")
async def list_recommendations(ws_id: str, status: str = "new", user: Dict = Depends(get_current_user)):
    recs = await get_recommendations(ws_id, status)
    return {"success": True, "data": recs}


@ops_router.post("/recommendations/{ws_id}")
async def add_recommendation(ws_id: str, req: RecommendationRequest, user: Dict = Depends(require_role("editor"))):
    rec_id = await create_recommendation(
        ws_id, req.rec_type, req.title, req.description, req.priority_score, req.cluster_id,
    )
    return {"success": True, "data": {"id": rec_id}}


@ops_router.put("/recommendations/{ws_id}/{rec_id}")
async def update_recommendation_status(ws_id: str, rec_id: str, status: str = "accepted",
                                        user: Dict = Depends(get_current_user)):
    await execute("UPDATE recommendations SET status = ? WHERE id = ?", (status, rec_id))
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# CONTENT REFRESH
# ═══════════════════════════════════════════════════════════════

@ops_router.get("/refresh-candidates/{ws_id}")
async def refresh_candidates(ws_id: str, user: Dict = Depends(get_current_user)):
    candidates = await detect_refresh_candidates(ws_id)
    return {"success": True, "data": candidates}


# ═══════════════════════════════════════════════════════════════
# PLAYBOOKS / SOPs
# ═══════════════════════════════════════════════════════════════

@ops_router.get("/playbooks")
async def list_playbooks(ws_id: str = "", user: Dict = Depends(get_current_user)):
    if ws_id:
        playbooks = await fetch_all(
            "SELECT * FROM playbooks WHERE (workspace_id = ? OR workspace_id = '') AND is_active = 1 ORDER BY category, name",
            (ws_id,),
        )
    else:
        playbooks = await fetch_all(
            "SELECT * FROM playbooks WHERE is_active = 1 ORDER BY category, name"
        )
    for pb in playbooks:
        pb["steps"] = from_json(pb.get("steps", "[]"), [])
    return {"success": True, "data": playbooks}


@ops_router.get("/playbooks/{pb_id}")
async def get_playbook(pb_id: str, user: Dict = Depends(get_current_user)):
    pb = await fetch_one("SELECT * FROM playbooks WHERE id = ?", (pb_id,))
    if not pb:
        raise HTTPException(status_code=404)
    pb["steps"] = from_json(pb.get("steps", "[]"), [])
    return {"success": True, "data": pb}


# ═══════════════════════════════════════════════════════════════
# TEAM PERFORMANCE
# ═══════════════════════════════════════════════════════════════

@ops_router.get("/team-performance/{ws_id}")
async def team_performance(ws_id: str, days: int = 30, user: Dict = Depends(require_role("admin"))):
    perf = await get_team_performance(ws_id, days)
    return {"success": True, "data": perf}
