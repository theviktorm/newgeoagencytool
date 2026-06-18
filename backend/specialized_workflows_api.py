"""
MOMENTUS AI — SPECIALIZED WORKFLOWS API
Local, GBP, Review, Authority, Off-Site, Reddit, YouTube, PR workflows.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

from typing import Dict

from fastapi import APIRouter, Depends, HTTPException

from .auth import get_current_user, require_role
from .models import BaseRequest
from .specialized_workflows import (
    create_local_seo_action, create_gbp_action, create_review_action,
    create_authority_action, create_offsite_action, create_reddit_action,
    create_youtube_action, create_pr_action, get_workflow_template,
    list_workflow_templates, create_action_from_template, WORKFLOW_TEMPLATES,
)
from .workflow_engine import get_geo_project

workflows_router = APIRouter(prefix="/api/workflows", tags=["workflows"])

# ═══════════════════════════════════════════════════════════════
# WORKFLOW TEMPLATES
# ═══════════════════════════════════════════════════════════════

@workflows_router.get("/templates")
async def list_all_templates(user: Dict = Depends(get_current_user)):
    """List all available workflow templates."""
    templates = {}
    for workflow_type in WORKFLOW_TEMPLATES.keys():
        templates[workflow_type] = await list_workflow_templates(workflow_type)
    return {"success": True, "data": templates}


@workflows_router.get("/templates/{workflow_type}")
async def list_templates(
    workflow_type: str, user: Dict = Depends(get_current_user)
):
    """List templates for a workflow type."""
    if workflow_type not in WORKFLOW_TEMPLATES:
        raise HTTPException(status_code=400, detail=f"Unknown workflow type: {workflow_type}")
    
    templates = await list_workflow_templates(workflow_type)
    return {"success": True, "data": templates}


# ═══════════════════════════════════════════════════════════════
# LOCAL SEO WORKFLOW
# ═══════════════════════════════════════════════════════════════

class CreateLocalSeoActionRequest(BaseRequest):
    project_id: str
    entity_id: str
    action_type: str
    details: Dict = {}

@workflows_router.post("/local-seo")
async def create_local_seo(
    req: CreateLocalSeoActionRequest, user: Dict = Depends(require_role("editor"))
):
    """Create a local SEO action."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    action_id = await create_local_seo_action(
        req.project_id, req.entity_id, req.action_type, req.details
    )
    return {"success": True, "data": {"action_id": action_id}}


# ═══════════════════════════════════════════════════════════════
# GOOGLE BUSINESS PROFILE WORKFLOW
# ═══════════════════════════════════════════════════════════════

class CreateGbpActionRequest(BaseRequest):
    project_id: str
    entity_id: str
    action_type: str
    details: Dict = {}

@workflows_router.post("/gbp")
async def create_gbp(
    req: CreateGbpActionRequest, user: Dict = Depends(require_role("editor"))
):
    """Create a Google Business Profile action."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    action_id = await create_gbp_action(
        req.project_id, req.entity_id, req.action_type, req.details
    )
    return {"success": True, "data": {"action_id": action_id}}


# ═══════════════════════════════════════════════════════════════
# REVIEW MANAGEMENT WORKFLOW
# ═══════════════════════════════════════════════════════════════

class CreateReviewActionRequest(BaseRequest):
    project_id: str
    entity_id: str
    action_type: str
    details: Dict = {}

@workflows_router.post("/review")
async def create_review(
    req: CreateReviewActionRequest, user: Dict = Depends(require_role("editor"))
):
    """Create a review management action."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    action_id = await create_review_action(
        req.project_id, req.entity_id, req.action_type, req.details
    )
    return {"success": True, "data": {"action_id": action_id}}


# ═══════════════════════════════════════════════════════════════
# AUTHORITY BUILDING WORKFLOW
# ═══════════════════════════════════════════════════════════════

class CreateAuthorityActionRequest(BaseRequest):
    project_id: str
    entity_id: str
    action_type: str
    details: Dict = {}

@workflows_router.post("/authority")
async def create_authority(
    req: CreateAuthorityActionRequest, user: Dict = Depends(require_role("editor"))
):
    """Create an authority-building action."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    action_id = await create_authority_action(
        req.project_id, req.entity_id, req.action_type, req.details
    )
    return {"success": True, "data": {"action_id": action_id}}


# ═══════════════════════════════════════════════════════════════
# OFF-SITE AUTHORITY WORKFLOW
# ═══════════════════════════════════════════════════════════════

class CreateOffsiteActionRequest(BaseRequest):
    project_id: str
    entity_id: str
    action_type: str
    details: Dict = {}

@workflows_router.post("/offsite")
async def create_offsite(
    req: CreateOffsiteActionRequest, user: Dict = Depends(require_role("editor"))
):
    """Create an off-site authority action."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    action_id = await create_offsite_action(
        req.project_id, req.entity_id, req.action_type, req.details
    )
    return {"success": True, "data": {"action_id": action_id}}


# ═══════════════════════════════════════════════════════════════
# REDDIT WORKFLOW
# ═══════════════════════════════════════════════════════════════

class CreateRedditActionRequest(BaseRequest):
    project_id: str
    entity_id: str
    action_type: str
    details: Dict = {}

@workflows_router.post("/reddit")
async def create_reddit(
    req: CreateRedditActionRequest, user: Dict = Depends(require_role("editor"))
):
    """Create a Reddit engagement action."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    action_id = await create_reddit_action(
        req.project_id, req.entity_id, req.action_type, req.details
    )
    return {"success": True, "data": {"action_id": action_id}}


# ═══════════════════════════════════════════════════════════════
# YOUTUBE WORKFLOW
# ═══════════════════════════════════════════════════════════════

class CreateYoutubeActionRequest(BaseRequest):
    project_id: str
    entity_id: str
    action_type: str
    details: Dict = {}

@workflows_router.post("/youtube")
async def create_youtube(
    req: CreateYoutubeActionRequest, user: Dict = Depends(require_role("editor"))
):
    """Create a YouTube engagement action."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    action_id = await create_youtube_action(
        req.project_id, req.entity_id, req.action_type, req.details
    )
    return {"success": True, "data": {"action_id": action_id}}


# ═══════════════════════════════════════════════════════════════
# PR / CONTENT WORKFLOW
# ═══════════════════════════════════════════════════════════════

class CreatePrActionRequest(BaseRequest):
    project_id: str
    entity_id: str
    action_type: str
    details: Dict = {}

@workflows_router.post("/pr")
async def create_pr(
    req: CreatePrActionRequest, user: Dict = Depends(require_role("editor"))
):
    """Create a PR/content action."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    action_id = await create_pr_action(
        req.project_id, req.entity_id, req.action_type, req.details
    )
    return {"success": True, "data": {"action_id": action_id}}


# ═══════════════════════════════════════════════════════════════
# TEMPLATE-BASED ACTION CREATION
# ═══════════════════════════════════════════════════════════════

class CreateFromTemplateRequest(BaseRequest):
    project_id: str
    entity_id: str
    workflow_type: str
    template_name: str

@workflows_router.post("/from-template")
async def create_from_template(
    req: CreateFromTemplateRequest, user: Dict = Depends(require_role("editor"))
):
    """Create an action from a template."""
    proj = await get_geo_project(req.project_id)
    if not proj:
        raise HTTPException(status_code=404)
    
    try:
        action_id = await create_action_from_template(
            req.project_id, req.entity_id, req.workflow_type, req.template_name
        )
        return {"success": True, "data": {"action_id": action_id}}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
