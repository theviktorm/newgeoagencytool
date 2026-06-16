"""
MOMENTUS AI — PUBLISH CYCLE API
Publish guardrails, retracking, before/after reporting.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

from typing import Dict

from fastapi import APIRouter, Depends, HTTPException

from .auth import get_current_user, require_role
from .models import BaseRequest
from .publish_cycle_engine import (
    check_publish_readiness, publish_project, retrack_project,
    create_new_import_batch_from_retrack, generate_before_after_report,
    run_full_publish_cycle, list_publish_cycles, get_publish_cycle_details,
)
from .workflow_engine import get_geo_project

publish_cycle_router = APIRouter(prefix="/api/publish-cycle", tags=["publish_cycle"])

# ═══════════════════════════════════════════════════════════════
# READINESS CHECK
# ═══════════════════════════════════════════════════════════════

@publish_cycle_router.get("/projects/{project_id}/readiness")
async def check_readiness(
    project_id: str, user: Dict = Depends(get_current_user)
):
    """Check if project is ready to publish."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    readiness = await check_publish_readiness(project_id)
    return {"success": True, "data": readiness}


# ═══════════════════════════════════════════════════════════════
# PUBLISH EXECUTION
# ═══════════════════════════════════════════════════════════════

class PublishRequest(BaseRequest):
    publish_config: Dict = {}

@publish_cycle_router.post("/projects/{project_id}/publish")
async def publish_route(
    project_id: str, req: PublishRequest = PublishRequest(),
    user: Dict = Depends(require_role("editor"))
):
    """Publish project changes."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    try:
        publish_id = await publish_project(project_id, req.publish_config)
        return {"success": True, "data": {"publish_id": publish_id}}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# RETRACKING
# ═══════════════════════════════════════════════════════════════

@publish_cycle_router.post("/publish/{publish_id}/retrack")
async def retrack_route(
    publish_id: str, user: Dict = Depends(require_role("editor"))
):
    """Re-track project after publishing."""
    try:
        retrack_id = await retrack_project("", publish_id)
        return {"success": True, "data": {"retrack_id": retrack_id}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# NEW IMPORT BATCH FROM RETRACK
# ═══════════════════════════════════════════════════════════════

@publish_cycle_router.post("/retrack/{retrack_id}/create-import-batch")
async def create_import_from_retrack(
    retrack_id: str, user: Dict = Depends(require_role("editor"))
):
    """Create new import batch from retracked data."""
    try:
        batch_id = await create_new_import_batch_from_retrack("", retrack_id)
        return {"success": True, "data": {"batch_id": batch_id}}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# BEFORE/AFTER REPORTING
# ═══════════════════════════════════════════════════════════════

@publish_cycle_router.get("/publish/{publish_id}/batch/{batch_id}/report")
async def before_after_report(
    publish_id: str, batch_id: str, user: Dict = Depends(get_current_user)
):
    """Generate before/after comparison report."""
    try:
        report = await generate_before_after_report("", publish_id, batch_id)
        if "error" in report:
            raise HTTPException(status_code=404, detail=report["error"])
        return {"success": True, "data": report}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# FULL PUBLISH CYCLE
# ═══════════════════════════════════════════════════════════════

@publish_cycle_router.post("/projects/{project_id}/run-full-cycle")
async def run_full_cycle(
    project_id: str, user: Dict = Depends(require_role("editor"))
):
    """Run complete publish → retrack → import → report cycle."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    result = await run_full_publish_cycle(project_id)
    if not result["success"]:
        raise HTTPException(status_code=500, detail=result.get("error"))
    
    return {"success": True, "data": result}


# ═══════════════════════════════════════════════════════════════
# CYCLE HISTORY
# ═══════════════════════════════════════════════════════════════

@publish_cycle_router.get("/projects/{project_id}/cycles")
async def list_cycles(
    project_id: str, user: Dict = Depends(get_current_user)
):
    """List all publish cycles for a project."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    cycles = await list_publish_cycles(project_id)
    return {"success": True, "data": cycles}


@publish_cycle_router.get("/cycles/{cycle_id}")
async def get_cycle_details(
    cycle_id: str, user: Dict = Depends(get_current_user)
):
    """Get full publish cycle details."""
    cycle = await get_publish_cycle_details(cycle_id)
    if not cycle:
        raise HTTPException(status_code=404)
    
    return {"success": True, "data": cycle}
