"""
MOMENTUS AI — ENTITY ONBOARDING API
Crawl-driven profile extraction, consistency scoring, manual overrides.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException

from .auth import get_current_user, require_role
from .models import BaseRequest
from .entity_onboarding_engine import (
    start_entity_onboarding, apply_manual_overrides, validate_entity,
    finalize_entity, onboard_entities_batch, list_project_entities,
    get_entity_details, generate_entity_consistency_report,
)
from .workflow_engine import get_geo_project

entity_onboarding_router = APIRouter(prefix="/api/entity-onboarding", tags=["entity_onboarding"])

# ═══════════════════════════════════════════════════════════════
# ENTITY CRAWLING & ONBOARDING
# ═══════════════════════════════════════════════════════════════

class StartOnboardingRequest(BaseRequest):
    project_id: str
    entity_type: str
    name: str
    website: str = ""

@entity_onboarding_router.post("/start")
async def start_onboarding(
    req: StartOnboardingRequest, user: Dict = Depends(require_role("editor"))
):
    """Start entity onboarding with crawl."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404, detail="Project not found")
    
    entity_id = await start_entity_onboarding(
        req.project_id, req.entity_type, req.name, req.website
    )
    return {"success": True, "data": {"id": entity_id}}


@entity_onboarding_router.get("/{entity_id}")
async def get_entity(
    entity_id: str, user: Dict = Depends(get_current_user)
):
    """Get entity details."""
    entity = await get_entity_details(entity_id)
    if not entity:
        raise HTTPException(status_code=404)
    return {"success": True, "data": entity}


# ═══════════════════════════════════════════════════════════════
# MANUAL OVERRIDES
# ═══════════════════════════════════════════════════════════════

class ApplyOverridesRequest(BaseRequest):
    overrides: Dict[str, str]

@entity_onboarding_router.post("/{entity_id}/overrides")
async def apply_overrides(
    entity_id: str, req: ApplyOverridesRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Apply manual overrides to entity data."""
    entity = await get_entity_details(entity_id)
    if not entity:
        raise HTTPException(status_code=404)
    
    await apply_manual_overrides(entity_id, req.overrides)
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════════════

@entity_onboarding_router.post("/{entity_id}/validate")
async def validate_entity_route(
    entity_id: str, user: Dict = Depends(require_role("editor"))
):
    """Validate entity data."""
    entity = await get_entity_details(entity_id)
    if not entity:
        raise HTTPException(status_code=404)
    
    result = await validate_entity(entity_id)
    return {"success": True, "data": result}


@entity_onboarding_router.post("/{entity_id}/finalize")
async def finalize_entity_route(
    entity_id: str, user: Dict = Depends(require_role("editor"))
):
    """Mark entity as validated and ready."""
    entity = await get_entity_details(entity_id)
    if not entity:
        raise HTTPException(status_code=404)
    
    await finalize_entity(entity_id)
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# BATCH ONBOARDING
# ═══════════════════════════════════════════════════════════════

class BatchOnboardingRequest(BaseRequest):
    project_id: str
    entities: List[Dict]  # [{type, name, website}, ...]

@entity_onboarding_router.post("/batch")
async def batch_onboarding(
    req: BatchOnboardingRequest, user: Dict = Depends(require_role("editor"))
):
    """Onboard multiple entities in parallel."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404, detail="Project not found")
    
    result = await onboard_entities_batch(req.project_id, req.entities)
    return {"success": True, "data": result}


# ═══════════════════════════════════════════════════════════════
# PROJECT ENTITIES
# ═══════════════════════════════════════════════════════════════

@entity_onboarding_router.get("/project/{project_id}/entities")
async def list_entities(
    project_id: str, user: Dict = Depends(get_current_user)
):
    """List all entities for a project."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    entities = await list_project_entities(project_id)
    return {"success": True, "data": entities}


# ═══════════════════════════════════════════════════════════════
# CONSISTENCY REPORTING
# ═══════════════════════════════════════════════════════════════

@entity_onboarding_router.get("/project/{project_id}/consistency-report")
async def consistency_report(
    project_id: str, user: Dict = Depends(get_current_user)
):
    """Generate entity consistency report."""
    proj = await get_geo_project(project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    report = await generate_entity_consistency_report(project_id)
    return {"success": True, "data": report}
