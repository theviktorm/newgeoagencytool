"""
MOMENTUS AI — APPROVAL GATES & CONFIDENCE SYSTEM
Human approval gates, confidence badges, and quality scoring.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional

from .database import execute, fetch_all, fetch_one, from_json, gen_id, to_json

logger = logging.getLogger("geo.approval")

# ═══════════════════════════════════════════════════════════════
# APPROVAL GATES
# ═══════════════════════════════════════════════════════════════

async def create_approval_gate(
    project_id: str, entity_id: str, gate_type: str, content: Dict
) -> str:
    """Create an approval gate for human review."""
    gate_id = gen_id("gate-")
    
    await execute(
        """INSERT INTO approval_gates
           (id, project_id, entity_id, gate_type, content, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
        (gate_id, project_id, entity_id, gate_type, to_json(content), "pending"),
    )
    
    logger.info(f"Created approval gate {gate_id} for {gate_type}")
    return gate_id


async def approve_gate(gate_id: str, approver_id: str, notes: str = "") -> bool:
    """Approve an approval gate."""
    await execute(
        """UPDATE approval_gates
           SET status = ?, approver_id = ?, approval_notes = ?, approved_at = datetime('now')
           WHERE id = ?""",
        ("approved", approver_id, notes, gate_id),
    )
    
    logger.info(f"Approved gate {gate_id}")
    return True


async def reject_gate(gate_id: str, approver_id: str, reason: str) -> bool:
    """Reject an approval gate."""
    await execute(
        """UPDATE approval_gates
           SET status = ?, approver_id = ?, rejection_reason = ?, rejected_at = datetime('now')
           WHERE id = ?""",
        ("rejected", approver_id, reason, gate_id),
    )
    
    logger.info(f"Rejected gate {gate_id}")
    return True


async def request_changes_on_gate(gate_id: str, reviewer_id: str, changes_needed: str) -> bool:
    """Request changes on an approval gate."""
    await execute(
        """UPDATE approval_gates
           SET status = ?, reviewer_id = ?, changes_needed = ?, changes_requested_at = datetime('now')
           WHERE id = ?""",
        ("changes_requested", reviewer_id, changes_needed, gate_id),
    )
    
    logger.info(f"Requested changes on gate {gate_id}")
    return True


async def get_approval_gate(gate_id: str) -> Optional[Dict]:
    """Get approval gate details."""
    gate = await fetch_one(
        "SELECT * FROM approval_gates WHERE id = ?", (gate_id,)
    )
    if gate:
        gate["content"] = from_json(gate.get("content", "{}"), {})
    return gate


async def list_pending_approvals(project_id: str) -> List[Dict]:
    """List pending approvals for a project."""
    gates = await fetch_all(
        """SELECT * FROM approval_gates
           WHERE project_id = ? AND status = 'pending'
           ORDER BY created_at DESC""",
        (project_id,),
    )
    
    for gate in gates:
        gate["content"] = from_json(gate.get("content", "{}"), {})
    
    return gates


# ═══════════════════════════════════════════════════════════════
# CONFIDENCE SCORING
# ═══════════════════════════════════════════════════════════════

async def calculate_confidence_score(
    data_type: str, data: Dict, source: str = "auto"
) -> float:
    """Calculate confidence score for data (0-100)."""
    score = 0.0
    
    if data_type == "entity_data":
        # Score based on data completeness and source quality
        required_fields = ["name", "phone", "email", "website", "address"]
        present_fields = len([f for f in required_fields if data.get(f)])
        completeness = (present_fields / len(required_fields)) * 40
        
        # Format quality
        format_score = 0
        if data.get("phone") and len(data["phone"]) >= 10:
            format_score += 10
        if data.get("email") and "@" in data["email"]:
            format_score += 10
        if data.get("website") and "http" in data["website"]:
            format_score += 10
        
        # Source quality
        source_quality = {
            "official": 30,
            "verified": 20,
            "crawled": 10,
            "user_input": 5,
        }
        source_score = source_quality.get(source, 5)
        
        score = completeness + format_score + source_score
    
    elif data_type == "audit_finding":
        # Score based on severity and actionability
        severity_scores = {
            "critical": 30,
            "warning": 20,
            "info": 10,
        }
        severity_score = severity_scores.get(data.get("severity"), 0)
        
        # Actionability
        actionability = 40 if data.get("recommendation") else 20
        
        # Source credibility
        credibility_scores = {
            "schema": 20,
            "local": 20,
            "gbp": 20,
            "review": 10,
        }
        credibility_score = credibility_scores.get(data.get("source"), 10)
        
        score = severity_score + actionability + credibility_score
    
    elif data_type == "import_record":
        # Score based on validation and matching
        validation_score = 30 if data.get("validation_passed") else 0
        
        # Match quality
        match_score = 0
        if data.get("match_confidence"):
            match_score = min(data["match_confidence"] * 40, 40)
        
        # Data quality
        quality_score = 0
        if data.get("required_fields_present"):
            quality_score = 30
        
        score = validation_score + match_score + quality_score
    
    return min(score, 100.0)


async def get_confidence_badge(score: float) -> Dict:
    """Get confidence badge details based on score."""
    if score >= 90:
        return {
            "level": "high",
            "label": "High Confidence",
            "color": "green",
            "icon": "check-circle",
        }
    elif score >= 70:
        return {
            "level": "medium",
            "label": "Medium Confidence",
            "color": "yellow",
            "icon": "alert-circle",
        }
    elif score >= 50:
        return {
            "level": "low",
            "label": "Low Confidence",
            "color": "orange",
            "icon": "alert-triangle",
        }
    else:
        return {
            "level": "very_low",
            "label": "Very Low Confidence",
            "color": "red",
            "icon": "x-circle",
        }


async def store_confidence_score(
    entity_id: str, data_type: str, score: float, details: Dict = None
) -> str:
    """Store confidence score for tracking."""
    score_id = gen_id("conf-")
    
    await execute(
        """INSERT INTO confidence_scores
           (id, entity_id, data_type, score, details, created_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))""",
        (score_id, entity_id, data_type, score, to_json(details or {})),
    )
    
    return score_id


# ═══════════════════════════════════════════════════════════════
# ESTIMATED/MISSING DATA TRACKING
# ═══════════════════════════════════════════════════════════════

async def mark_field_as_estimated(
    entity_id: str, field_name: str, estimated_value: str, reason: str
) -> str:
    """Mark a field as estimated/inferred."""
    estimate_id = gen_id("est-")
    
    await execute(
        """INSERT INTO estimated_fields
           (id, entity_id, field_name, estimated_value, reason, created_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))""",
        (estimate_id, entity_id, field_name, estimated_value, reason),
    )
    
    logger.info(f"Marked {field_name} as estimated for {entity_id}")
    return estimate_id


async def mark_field_as_missing(
    entity_id: str, field_name: str, impact: str = "medium"
) -> str:
    """Mark a field as missing/unavailable."""
    missing_id = gen_id("miss-")
    
    await execute(
        """INSERT INTO missing_fields
           (id, entity_id, field_name, impact_level, created_at)
           VALUES (?, ?, ?, ?, datetime('now'))""",
        (missing_id, entity_id, field_name, impact),
    )
    
    logger.info(f"Marked {field_name} as missing for {entity_id}")
    return missing_id


async def get_entity_data_quality(entity_id: str) -> Dict:
    """Get overall data quality report for entity."""
    # Get estimated fields
    estimated = await fetch_all(
        "SELECT * FROM estimated_fields WHERE entity_id = ?", (entity_id,)
    )
    
    # Get missing fields
    missing = await fetch_all(
        "SELECT * FROM missing_fields WHERE entity_id = ?", (entity_id,)
    )
    
    # Get confidence scores
    scores = await fetch_all(
        "SELECT * FROM confidence_scores WHERE entity_id = ? ORDER BY created_at DESC LIMIT 10",
        (entity_id,),
    )
    
    avg_confidence = sum(s["score"] for s in scores) / len(scores) if scores else 0
    
    return {
        "entity_id": entity_id,
        "estimated_field_count": len(estimated),
        "missing_field_count": len(missing),
        "average_confidence": round(avg_confidence, 1),
        "estimated_fields": estimated,
        "missing_fields": missing,
        "recent_scores": scores[:5],
    }


# ═══════════════════════════════════════════════════════════════
# APPROVAL WORKFLOW INTEGRATION
# ═══════════════════════════════════════════════════════════════

async def create_approval_workflow(
    project_id: str, workflow_type: str, content: Dict
) -> str:
    """Create an approval workflow."""
    workflow_id = gen_id("appr-")
    
    await execute(
        """INSERT INTO approval_workflows
           (id, project_id, workflow_type, content, status, created_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))""",
        (workflow_id, project_id, workflow_type, to_json(content), "pending"),
    )
    
    return workflow_id


async def get_approval_workflow_status(workflow_id: str) -> Optional[Dict]:
    """Get approval workflow status."""
    workflow = await fetch_one(
        "SELECT * FROM approval_workflows WHERE id = ?", (workflow_id,)
    )
    if workflow:
        workflow["content"] = from_json(workflow.get("content", "{}"), {})
    return workflow


async def complete_approval_workflow(
    workflow_id: str, approver_id: str, decision: str, notes: str = ""
) -> bool:
    """Complete an approval workflow."""
    status = "approved" if decision == "approve" else "rejected"
    
    await execute(
        """UPDATE approval_workflows
           SET status = ?, approver_id = ?, decision_notes = ?, completed_at = datetime('now')
           WHERE id = ?""",
        (status, approver_id, notes, workflow_id),
    )
    
    return True
