"""
MOMENTUS AI — APPROVAL GATES API
Human approval gates, confidence badges, estimated/missing data tracking.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

from typing import Dict

from fastapi import APIRouter, Depends, HTTPException

from .auth import get_current_user, require_role
from .models import BaseRequest
from .approval_gates import (
    create_approval_gate, approve_gate, reject_gate, request_changes_on_gate,
    get_approval_gate, list_pending_approvals, calculate_confidence_score,
    get_confidence_badge, store_confidence_score, mark_field_as_estimated,
    mark_field_as_missing, get_entity_data_quality, create_approval_workflow,
    get_approval_workflow_status, complete_approval_workflow,
)
from .workflow_engine import get_geo_project

approval_router = APIRouter(prefix="/api/approvals", tags=["approvals"])

# ═══════════════════════════════════════════════════════════════
# APPROVAL GATES
# ═══════════════════════════════════════════════════════════════

class CreateApprovalGateRequest(BaseRequest):
    project_id: str
    entity_id: str
    gate_type: str
    content: Dict

@approval_router.post("/gates")
async def create_gate(
    req: CreateApprovalGateRequest, user: Dict = Depends(require_role("editor"))
):
    """Create an approval gate."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    gate_id = await create_approval_gate(
        req.project_id, req.entity_id, req.gate_type, req.content
    )
    return {"success": True, "data": {"gate_id": gate_id}}


@approval_router.get("/gates/{gate_id}")
async def get_gate(gate_id: str, user: Dict = Depends(get_current_user)):
    """Get approval gate details."""
    gate = await get_approval_gate(gate_id)
    if not gate:
        raise HTTPException(status_code=404)
    return {"success": True, "data": gate}


class ApproveGateRequest(BaseRequest):
    approver_id: str
    notes: str = ""

@approval_router.post("/gates/{gate_id}/approve")
async def approve(
    gate_id: str, req: ApproveGateRequest,
    user: Dict = Depends(require_role("approver"))
):
    """Approve an approval gate."""
    gate = await get_approval_gate(gate_id)
    if not gate:
        raise HTTPException(status_code=404)
    
    await approve_gate(gate_id, req.approver_id, req.notes)
    return {"success": True, "data": {"status": "approved"}}


class RejectGateRequest(BaseRequest):
    approver_id: str
    reason: str

@approval_router.post("/gates/{gate_id}/reject")
async def reject(
    gate_id: str, req: RejectGateRequest,
    user: Dict = Depends(require_role("approver"))
):
    """Reject an approval gate."""
    gate = await get_approval_gate(gate_id)
    if not gate:
        raise HTTPException(status_code=404)
    
    await reject_gate(gate_id, req.approver_id, req.reason)
    return {"success": True, "data": {"status": "rejected"}}


class RequestChangesRequest(BaseRequest):
    reviewer_id: str
    changes_needed: str

@approval_router.post("/gates/{gate_id}/request-changes")
async def request_changes(
    gate_id: str, req: RequestChangesRequest,
    user: Dict = Depends(require_role("reviewer"))
):
    """Request changes on an approval gate."""
    gate = await get_approval_gate(gate_id)
    if not gate:
        raise HTTPException(status_code=404)
    
    await request_changes_on_gate(gate_id, req.reviewer_id, req.changes_needed)
    return {"success": True, "data": {"status": "changes_requested"}}


@approval_router.get("/projects/{project_id}/pending")
async def list_pending(
    project_id: str, user: Dict = Depends(get_current_user)
):
    """List pending approvals for a project."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    gates = await list_pending_approvals(project_id)
    return {"success": True, "data": gates}


# ═══════════════════════════════════════════════════════════════
# CONFIDENCE SCORING
# ═══════════════════════════════════════════════════════════════

class CalculateConfidenceRequest(BaseRequest):
    data_type: str
    data: Dict
    source: str = "auto"

@approval_router.post("/confidence/calculate")
async def calculate_confidence(
    req: CalculateConfidenceRequest, user: Dict = Depends(get_current_user)
):
    """Calculate confidence score for data."""
    score = await calculate_confidence_score(req.data_type, req.data, req.source)
    badge = await get_confidence_badge(score)
    return {"success": True, "data": {"score": score, "badge": badge}}


@approval_router.get("/confidence/badge/{score}")
async def get_badge(score: float, user: Dict = Depends(get_current_user)):
    """Get confidence badge for score."""
    badge = await get_confidence_badge(score)
    return {"success": True, "data": badge}


class StoreConfidenceRequest(BaseRequest):
    entity_id: str
    data_type: str
    score: float
    details: Dict = {}

@approval_router.post("/confidence/store")
async def store_confidence(
    req: StoreConfidenceRequest, user: Dict = Depends(require_role("editor"))
):
    """Store confidence score."""
    score_id = await store_confidence_score(
        req.entity_id, req.data_type, req.score, req.details
    )
    return {"success": True, "data": {"score_id": score_id}}


# ═══════════════════════════════════════════════════════════════
# ESTIMATED/MISSING DATA TRACKING
# ═══════════════════════════════════════════════════════════════

class MarkEstimatedRequest(BaseRequest):
    entity_id: str
    field_name: str
    estimated_value: str
    reason: str

@approval_router.post("/estimated-fields")
async def mark_estimated(
    req: MarkEstimatedRequest, user: Dict = Depends(require_role("editor"))
):
    """Mark field as estimated."""
    estimate_id = await mark_field_as_estimated(
        req.entity_id, req.field_name, req.estimated_value, req.reason
    )
    return {"success": True, "data": {"estimate_id": estimate_id}}


class MarkMissingRequest(BaseRequest):
    entity_id: str
    field_name: str
    impact: str = "medium"

@approval_router.post("/missing-fields")
async def mark_missing(
    req: MarkMissingRequest, user: Dict = Depends(require_role("editor"))
):
    """Mark field as missing."""
    missing_id = await mark_field_as_missing(
        req.entity_id, req.field_name, req.impact
    )
    return {"success": True, "data": {"missing_id": missing_id}}


@approval_router.get("/entities/{entity_id}/data-quality")
async def get_quality(
    entity_id: str, user: Dict = Depends(get_current_user)
):
    """Get entity data quality report."""
    quality = await get_entity_data_quality(entity_id)
    return {"success": True, "data": quality}


# ═══════════════════════════════════════════════════════════════
# APPROVAL WORKFLOWS
# ═══════════════════════════════════════════════════════════════

class CreateApprovalWorkflowRequest(BaseRequest):
    project_id: str
    workflow_type: str
    content: Dict

@approval_router.post("/workflows")
async def create_workflow(
    req: CreateApprovalWorkflowRequest, user: Dict = Depends(require_role("editor"))
):
    """Create an approval workflow."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    workflow_id = await create_approval_workflow(
        req.project_id, req.workflow_type, req.content
    )
    return {"success": True, "data": {"workflow_id": workflow_id}}


@approval_router.get("/workflows/{workflow_id}")
async def get_workflow(
    workflow_id: str, user: Dict = Depends(get_current_user)
):
    """Get approval workflow status."""
    workflow = await get_approval_workflow_status(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404)
    return {"success": True, "data": workflow}


class CompleteWorkflowRequest(BaseRequest):
    approver_id: str
    decision: str
    notes: str = ""

@approval_router.post("/workflows/{workflow_id}/complete")
async def complete_workflow(
    workflow_id: str, req: CompleteWorkflowRequest,
    user: Dict = Depends(require_role("approver"))
):
    """Complete an approval workflow."""
    workflow = await get_approval_workflow_status(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404)
    
    await complete_approval_workflow(workflow_id, req.approver_id, req.decision, req.notes)
    return {"success": True, "data": {"status": "completed"}}
