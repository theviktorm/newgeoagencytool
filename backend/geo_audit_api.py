"""
MOMENTUS AI — GEO AUDIT API
Run and manage unified technical audits.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

from typing import Dict

from fastapi import APIRouter, Depends, HTTPException

from .auth import get_current_user, require_role
from .geo_audit_engine import (
    run_full_audit, run_audit_by_type, get_audit_summary, AUDIT_TYPES,
)
from .workflow_engine import get_geo_project, get_geo_audit, list_geo_audits

geo_audit_router = APIRouter(prefix="/api/geo-audit", tags=["geo_audit"])

# ═══════════════════════════════════════════════════════════════
# AUDIT EXECUTION
# ═══════════════════════════════════════════════════════════════

@geo_audit_router.post("/projects/{project_id}/run-full")
async def run_full_audit_route(
    project_id: str, user: Dict = Depends(require_role("editor"))
):
    """Run full audit suite for a project."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404, detail="Project not found")
    
    audit_id = await run_full_audit(project_id)
    return {"success": True, "data": {"audit_id": audit_id}}


@geo_audit_router.post("/projects/{project_id}/run/{audit_type}")
async def run_audit_route(
    project_id: str, audit_type: str, user: Dict = Depends(require_role("editor"))
):
    """Run a specific audit type."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404, detail="Project not found")
    
    if audit_type not in AUDIT_TYPES:
        raise HTTPException(status_code=400, detail=f"Unknown audit type: {audit_type}")
    
    audit_id = await run_audit_by_type(project_id, audit_type)
    return {"success": True, "data": {"audit_id": audit_id}}


# ═══════════════════════════════════════════════════════════════
# AUDIT RETRIEVAL
# ═══════════════════════════════════════════════════════════════

@geo_audit_router.get("/projects/{project_id}/audits")
async def list_audits_route(
    project_id: str, user: Dict = Depends(get_current_user)
):
    """List all audits for a project."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    audits = await list_geo_audits(project_id)
    return {"success": True, "data": audits}


@geo_audit_router.get("/projects/{project_id}/audits/{audit_id}")
async def get_audit_route(
    project_id: str, audit_id: str, user: Dict = Depends(get_current_user)
):
    """Get audit details."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    audit = await get_geo_audit(audit_id)
    if not audit or audit["project_id"] != project_id:
        raise HTTPException(status_code=404)
    
    return {"success": True, "data": audit}


# ═══════════════════════════════════════════════════════════════
# AUDIT SUMMARY
# ═══════════════════════════════════════════════════════════════

@geo_audit_router.get("/projects/{project_id}/summary")
async def audit_summary_route(
    project_id: str, user: Dict = Depends(get_current_user)
):
    """Get audit summary for a project."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    summary = await get_audit_summary(project_id)
    return {"success": True, "data": summary}


# ═══════════════════════════════════════════════════════════════
# AUDIT TYPES INFO
# ═══════════════════════════════════════════════════════════════

@geo_audit_router.get("/types")
async def get_audit_types(user: Dict = Depends(get_current_user)):
    """Get available audit types."""
    return {
        "success": True,
        "data": [
            {"id": k, "name": k.upper(), "description": v}
            for k, v in AUDIT_TYPES.items()
        ]
    }
