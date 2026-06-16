"""
MOMENTUS AI — WORKFLOW API ROUTES
Central project lifecycle endpoints.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException

from .auth import get_current_user, require_role
from .workflow_engine import (
    create_geo_project, get_geo_project, list_geo_projects, update_project_status,
    record_milestone, create_import_batch, get_import_batch, update_import_batch,
    list_import_batches, create_entity_onboarding, update_entity_onboarding,
    list_entity_onboarding, create_geo_audit, complete_geo_audit, get_geo_audit,
    list_geo_audits, create_publish_cycle, update_publish_cycle, get_publish_cycle,
    create_approval_gate, approve_gate, reject_gate, get_approval_gate,
    list_approval_gates, get_project_workflow_summary,
)
from .models import BaseRequest

workflow_router = APIRouter(prefix="/api/workflow", tags=["workflow"])

# ═══════════════════════════════════════════════════════════════
# GEO PROJECTS
# ═══════════════════════════════════════════════════════════════

class CreateProjectRequest(BaseRequest):
    name: str
    description: str = ""

@workflow_router.post("/{ws_id}/projects")
async def create_project(
    ws_id: str, req: CreateProjectRequest, user: Dict = Depends(require_role("editor"))
):
    """Create a new GEO project."""
    project_id = await create_geo_project(ws_id, req.name, req.description, user["id"])
    return {"success": True, "data": {"id": project_id}}


@workflow_router.get("/{ws_id}/projects")
async def list_projects(
    ws_id: str, status: str = "", user: Dict = Depends(get_current_user)
):
    """List GEO projects in a workspace."""
    projects = await list_geo_projects(ws_id, status)
    return {"success": True, "data": projects}


@workflow_router.get("/{ws_id}/projects/{project_id}")
async def get_project(
    ws_id: str, project_id: str, user: Dict = Depends(get_current_user)
):
    """Get a GEO project."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    return {"success": True, "data": proj}


class UpdateProjectRequest(BaseRequest):
    status: Optional[str] = None
    stage: Optional[str] = None

@workflow_router.put("/{ws_id}/projects/{project_id}")
async def update_project(
    ws_id: str, project_id: str, req: UpdateProjectRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Update a GEO project."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    if req.status:
        await update_project_status(project_id, req.status, req.stage or "")
    return {"success": True}


@workflow_router.get("/{ws_id}/projects/{project_id}/summary")
async def get_project_summary(
    ws_id: str, project_id: str, user: Dict = Depends(get_current_user)
):
    """Get complete workflow summary for a project."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    summary = await get_project_workflow_summary(project_id)
    return {"success": True, "data": summary}


# ═══════════════════════════════════════════════════════════════
# MILESTONES
# ═══════════════════════════════════════════════════════════════

class RecordMilestoneRequest(BaseRequest):
    milestone_type: str
    data: Dict[str, Any] = {}

@workflow_router.post("/{ws_id}/projects/{project_id}/milestones")
async def record_project_milestone(
    ws_id: str, project_id: str, req: RecordMilestoneRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Record a project milestone."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    milestone_id = await record_milestone(project_id, req.milestone_type, req.data)
    return {"success": True, "data": {"id": milestone_id}}


# ═══════════════════════════════════════════════════════════════
# IMPORT BATCHES
# ═══════════════════════════════════════════════════════════════

class CreateImportBatchRequest(BaseRequest):
    batch_type: str
    record_count: int = 0

@workflow_router.post("/{ws_id}/projects/{project_id}/import-batches")
async def create_batch(
    ws_id: str, project_id: str, req: CreateImportBatchRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Create an import batch."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    batch_id = await create_import_batch(project_id, req.batch_type, req.record_count)
    return {"success": True, "data": {"id": batch_id}}


@workflow_router.get("/{ws_id}/projects/{project_id}/import-batches")
async def list_batches(
    ws_id: str, project_id: str, user: Dict = Depends(get_current_user)
):
    """List import batches for a project."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    batches = await list_import_batches(project_id)
    return {"success": True, "data": batches}


@workflow_router.get("/{ws_id}/projects/{project_id}/import-batches/{batch_id}")
async def get_batch(
    ws_id: str, project_id: str, batch_id: str, user: Dict = Depends(get_current_user)
):
    """Get import batch details."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    batch = await get_import_batch(batch_id)
    if not batch or batch["project_id"] != project_id:
        raise HTTPException(status_code=404)
    return {"success": True, "data": batch}


class UpdateImportBatchRequest(BaseRequest):
    status: Optional[str] = None
    imported_count: Optional[int] = None
    error_count: Optional[int] = None
    validation_msg: Optional[str] = None
    mapping_data: Optional[Dict] = None
    before_snapshot: Optional[Dict] = None
    after_snapshot: Optional[Dict] = None

@workflow_router.put("/{ws_id}/projects/{project_id}/import-batches/{batch_id}")
async def update_batch(
    ws_id: str, project_id: str, batch_id: str, req: UpdateImportBatchRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Update import batch."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    batch = await get_import_batch(batch_id)
    if not batch or batch["project_id"] != project_id:
        raise HTTPException(status_code=404)
    data = req.dict(exclude_none=True)
    await update_import_batch(batch_id, data)
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# ENTITY ONBOARDING
# ═══════════════════════════════════════════════════════════════

class CreateEntityRequest(BaseRequest):
    entity_type: str
    name: str

@workflow_router.post("/{ws_id}/projects/{project_id}/entities")
async def create_entity(
    ws_id: str, project_id: str, req: CreateEntityRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Create entity onboarding record."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    entity_id = await create_entity_onboarding(project_id, req.entity_type, req.name)
    return {"success": True, "data": {"id": entity_id}}


@workflow_router.get("/{ws_id}/projects/{project_id}/entities")
async def list_entities(
    ws_id: str, project_id: str, user: Dict = Depends(get_current_user)
):
    """List entities for a project."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    entities = await list_entity_onboarding(project_id)
    return {"success": True, "data": entities}


class UpdateEntityRequest(BaseRequest):
    crawl_status: Optional[str] = None
    consistency_score: Optional[float] = None
    extracted_data: Optional[Dict] = None
    manual_overrides: Optional[Dict] = None

@workflow_router.put("/{ws_id}/projects/{project_id}/entities/{entity_id}")
async def update_entity(
    ws_id: str, project_id: str, entity_id: str, req: UpdateEntityRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Update entity onboarding."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    data = req.dict(exclude_none=True)
    await update_entity_onboarding(entity_id, data)
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# GEO AUDITS
# ═══════════════════════════════════════════════════════════════

class CreateAuditRequest(BaseRequest):
    audit_type: str

@workflow_router.post("/{ws_id}/projects/{project_id}/audits")
async def create_audit(
    ws_id: str, project_id: str, req: CreateAuditRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Create a GEO audit."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    audit_id = await create_geo_audit(project_id, req.audit_type)
    return {"success": True, "data": {"id": audit_id}}


@workflow_router.get("/{ws_id}/projects/{project_id}/audits")
async def list_audits(
    ws_id: str, project_id: str, user: Dict = Depends(get_current_user)
):
    """List audits for a project."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    audits = await list_geo_audits(project_id)
    return {"success": True, "data": audits}


@workflow_router.get("/{ws_id}/projects/{project_id}/audits/{audit_id}")
async def get_audit(
    ws_id: str, project_id: str, audit_id: str, user: Dict = Depends(get_current_user)
):
    """Get audit details."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    audit = await get_geo_audit(audit_id)
    if not audit or audit["project_id"] != project_id:
        raise HTTPException(status_code=404)
    return {"success": True, "data": audit}


class CompleteAuditRequest(BaseRequest):
    findings: List[Dict]
    action_ids: List[str] = []

@workflow_router.post("/{ws_id}/projects/{project_id}/audits/{audit_id}/complete")
async def complete_audit(
    ws_id: str, project_id: str, audit_id: str, req: CompleteAuditRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Mark audit complete with findings."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    audit = await get_geo_audit(audit_id)
    if not audit or audit["project_id"] != project_id:
        raise HTTPException(status_code=404)
    await complete_geo_audit(audit_id, req.findings, req.action_ids)
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# PUBLISH CYCLES
# ═══════════════════════════════════════════════════════════════

class CreatePublishCycleRequest(BaseRequest):
    action_ids: List[str]

@workflow_router.post("/{ws_id}/projects/{project_id}/publish-cycles")
async def create_cycle(
    ws_id: str, project_id: str, req: CreatePublishCycleRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Create a publish cycle."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    cycle_id = await create_publish_cycle(project_id, req.action_ids)
    return {"success": True, "data": {"id": cycle_id}}


class UpdatePublishCycleRequest(BaseRequest):
    publish_status: Optional[str] = None
    retrack_status: Optional[str] = None
    before_metrics: Optional[Dict] = None
    after_metrics: Optional[Dict] = None
    improvement: Optional[float] = None

@workflow_router.put("/{ws_id}/projects/{project_id}/publish-cycles/{cycle_id}")
async def update_cycle(
    ws_id: str, project_id: str, cycle_id: str, req: UpdatePublishCycleRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Update publish cycle."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    cycle = await get_publish_cycle(cycle_id)
    if not cycle or cycle["project_id"] != project_id:
        raise HTTPException(status_code=404)
    data = req.dict(exclude_none=True)
    await update_publish_cycle(cycle_id, data)
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# APPROVAL GATES
# ═══════════════════════════════════════════════════════════════

class CreateGateRequest(BaseRequest):
    gate_type: str

@workflow_router.post("/{ws_id}/projects/{project_id}/approval-gates")
async def create_gate(
    ws_id: str, project_id: str, req: CreateGateRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Create an approval gate."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    gate_id = await create_approval_gate(project_id, req.gate_type, user["id"])
    return {"success": True, "data": {"id": gate_id}}


@workflow_router.get("/{ws_id}/projects/{project_id}/approval-gates")
async def list_gates(
    ws_id: str, project_id: str, status: str = "", user: Dict = Depends(get_current_user)
):
    """List approval gates for a project."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    gates = await list_approval_gates(project_id, status)
    return {"success": True, "data": gates}


class ApproveGateRequest(BaseRequest):
    notes: str = ""

@workflow_router.post("/{ws_id}/projects/{project_id}/approval-gates/{gate_id}/approve")
async def approve_gate_route(
    ws_id: str, project_id: str, gate_id: str, req: ApproveGateRequest,
    user: Dict = Depends(require_role("admin"))
):
    """Approve an approval gate."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    gate = await get_approval_gate(gate_id)
    if not gate or gate["project_id"] != project_id:
        raise HTTPException(status_code=404)
    await approve_gate(gate_id, user["id"], req.notes)
    return {"success": True}


@workflow_router.post("/{ws_id}/projects/{project_id}/approval-gates/{gate_id}/reject")
async def reject_gate_route(
    ws_id: str, project_id: str, gate_id: str, req: ApproveGateRequest,
    user: Dict = Depends(require_role("admin"))
):
    """Reject an approval gate."""
    proj = await get_geo_project(project_id)
    if not proj or proj["workspace_id"] != ws_id:
        raise HTTPException(status_code=404)
    gate = await get_approval_gate(gate_id)
    if not gate or gate["project_id"] != project_id:
        raise HTTPException(status_code=404)
    await reject_gate(gate_id, user["id"], req.notes)
    return {"success": True}
