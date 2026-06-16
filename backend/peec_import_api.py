"""
MOMENTUS AI — PEEC IMPORT WIZARD API
CSV upload, field mapping, validation, import execution.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from .auth import get_current_user, require_role
from .models import BaseRequest
from .peec_import_wizard import (
    create_import_session, get_import_session, update_import_session,
    parse_csv_preview, auto_map_fields, validate_import_records,
    execute_import, list_imported_records,
)
from .workflow_engine import (
    create_import_batch, update_import_batch, get_import_batch,
    update_project_status, record_milestone,
)

peec_import_router = APIRouter(prefix="/api/peec-import", tags=["peec_import"])

# ═══════════════════════════════════════════════════════════════
# IMPORT SESSION MANAGEMENT
# ═══════════════════════════════════════════════════════════════

class CreateImportSessionRequest(BaseRequest):
    project_id: str
    session_type: str  # csv_upload | api_pull | manual_mapping

@peec_import_router.post("/sessions")
async def create_session(
    req: CreateImportSessionRequest, user: Dict = Depends(require_role("editor"))
):
    """Create a Peec import session."""
    session_id = await create_import_session(
        req.project_id, req.session_type, "", user["id"]
    )
    return {"success": True, "data": {"id": session_id}}


@peec_import_router.get("/sessions/{session_id}")
async def get_session(
    session_id: str, user: Dict = Depends(get_current_user)
):
    """Get import session details."""
    session = await get_import_session(session_id)
    if not session:
        raise HTTPException(status_code=404)
    return {"success": True, "data": session}


# ═══════════════════════════════════════════════════════════════
# CSV UPLOAD & PREVIEW
# ═══════════════════════════════════════════════════════════════

@peec_import_router.post("/sessions/{session_id}/upload-csv")
async def upload_csv(
    session_id: str, file: UploadFile = File(...),
    user: Dict = Depends(require_role("editor"))
):
    """Upload and preview CSV file."""
    session = await get_import_session(session_id)
    if not session:
        raise HTTPException(status_code=404)
    
    try:
        content = await file.read()
        csv_text = content.decode("utf-8")
        
        # Parse and preview
        preview = await parse_csv_preview(csv_text, limit=10)
        if "error" in preview:
            return {"success": False, "error": preview["error"]}
        
        # Update session
        await update_import_session(session_id, {
            "record_count": preview.get("row_count", 0),
            "file_size": len(content),
            "status": "mapping",
        })
        
        return {
            "success": True,
            "data": {
                "preview": preview,
                "session_id": session_id,
            }
        }
    except Exception as e:
        return {"success": False, "error": f"Upload failed: {str(e)}"}


# ═══════════════════════════════════════════════════════════════
# FIELD MAPPING
# ═══════════════════════════════════════════════════════════════

@peec_import_router.post("/sessions/{session_id}/auto-map")
async def auto_map(
    session_id: str, csv_headers: List[str],
    user: Dict = Depends(require_role("editor"))
):
    """Auto-detect field mappings."""
    session = await get_import_session(session_id)
    if not session:
        raise HTTPException(status_code=404)
    
    result = await auto_map_fields(session_id, csv_headers)
    return {"success": True, "data": result}


class ManualMappingRequest(BaseRequest):
    mappings: Dict[str, str]  # source_field -> target_field

@peec_import_router.post("/sessions/{session_id}/set-mappings")
async def set_mappings(
    session_id: str, req: ManualMappingRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Set manual field mappings."""
    session = await get_import_session(session_id)
    if not session:
        raise HTTPException(status_code=404)
    
    # Mappings already created by auto_map or manual creation
    await update_import_session(session_id, {"status": "validating"})
    return {"success": True}


# ═══════════════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════════════

class ValidateImportRequest(BaseRequest):
    rows: List[Dict]
    mappings: Dict[str, str]

@peec_import_router.post("/sessions/{session_id}/validate")
async def validate_import(
    session_id: str, req: ValidateImportRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Validate import records."""
    session = await get_import_session(session_id)
    if not session:
        raise HTTPException(status_code=404)
    
    pass_count, fail_count, issues = await validate_import_records(
        session_id, req.rows, req.mappings
    )
    
    return {
        "success": True,
        "data": {
            "pass_count": pass_count,
            "fail_count": fail_count,
            "issues": issues,
            "validation_complete": True,
        }
    }


# ═══════════════════════════════════════════════════════════════
# IMPORT EXECUTION
# ═══════════════════════════════════════════════════════════════

class ExecuteImportRequest(BaseRequest):
    rows: List[Dict]
    mappings: Dict[str, str]

@peec_import_router.post("/sessions/{session_id}/execute")
async def execute_import_route(
    session_id: str, req: ExecuteImportRequest,
    user: Dict = Depends(require_role("editor"))
):
    """Execute the import."""
    session = await get_import_session(session_id)
    if not session:
        raise HTTPException(status_code=404)
    
    # Create import batch
    batch_id = await create_import_batch(
        session["project_id"], "peec_csv", len(req.rows)
    )
    
    # Execute import
    result = await execute_import(session_id, batch_id, req.rows, req.mappings)
    
    # Update batch with results
    await update_import_batch(batch_id, {
        "status": "complete",
        "imported_count": result["imported"],
        "error_count": result["failed"],
        "before_snapshot": result["before_snapshot"],
        "after_snapshot": result["after_snapshot"],
        "mapping_data": req.mappings,
    })
    
    # Update session
    await update_import_session(session_id, {"status": "complete"})
    
    # Record milestone
    await record_milestone(
        session["project_id"],
        "peec_imported",
        {
            "batch_id": batch_id,
            "imported": result["imported"],
            "failed": result["failed"],
        }
    )
    
    # Update project stage
    await update_project_status(session["project_id"], "active", "entity_onboarding")
    
    return {
        "success": True,
        "data": {
            "batch_id": batch_id,
            "imported": result["imported"],
            "failed": result["failed"],
            "before_snapshot": result["before_snapshot"],
            "after_snapshot": result["after_snapshot"],
        }
    }


# ═══════════════════════════════════════════════════════════════
# IMPORT HISTORY & RECORDS
# ═══════════════════════════════════════════════════════════════

@peec_import_router.get("/sessions/{session_id}/records")
async def get_records(
    session_id: str, batch_id: str = "", user: Dict = Depends(get_current_user)
):
    """Get imported records from a session."""
    session = await get_import_session(session_id)
    if not session:
        raise HTTPException(status_code=404)
    
    records = await list_imported_records(session_id, batch_id)
    return {"success": True, "data": records}


@peec_import_router.get("/sessions/{session_id}/summary")
async def get_import_summary(
    session_id: str, user: Dict = Depends(get_current_user)
):
    """Get import session summary."""
    session = await get_import_session(session_id)
    if not session:
        raise HTTPException(status_code=404)
    
    # Count validation issues
    validation_pass = len([v for v in session.get("validations", []) if v["status"] == "pass"])
    validation_fail = len([v for v in session.get("validations", []) if v["status"] == "fail"])
    validation_warn = len([v for v in session.get("validations", []) if v["status"] == "warn"])
    
    return {
        "success": True,
        "data": {
            "session": session,
            "validation_summary": {
                "pass": validation_pass,
                "fail": validation_fail,
                "warn": validation_warn,
            },
            "mapping_count": len(session.get("mappings", [])),
        }
    }
