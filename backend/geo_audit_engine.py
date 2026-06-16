"""
MOMENTUS AI — UNIFIED TECHNICAL GEO AUDIT ENGINE
Schema, Local, GBP, Review, Authority, Entity Consistency audits.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .database import execute, fetch_all, fetch_one, from_json, gen_id, to_json
from .workflow_engine import create_geo_audit, complete_geo_audit

logger = logging.getLogger("geo.audit")

# ═══════════════════════════════════════════════════════════════
# AUDIT FINDING TYPES
# ═══════════════════════════════════════════════════════════════

AUDIT_TYPES = {
    "schema": "Business data schema completeness and correctness",
    "local": "Local SEO signals and optimization",
    "gbp": "Google Business Profile setup and optimization",
    "review": "Review management and sentiment",
    "authority": "Domain and entity authority signals",
    "entity_consistency": "Data consistency across sources",
}

SEVERITY_LEVELS = {
    "critical": 1,  # Blocks publish
    "warning": 2,   # Should fix before publish
    "info": 3,      # Nice to have
}

# ═══════════════════════════════════════════════════════════════
# SCHEMA AUDIT
# ═══════════════════════════════════════════════════════════════

async def audit_schema(project_id: str) -> List[Dict]:
    """Audit business data schema completeness."""
    findings = []
    
    # Get all entities
    entities = await fetch_all(
        "SELECT * FROM entity_onboarding WHERE project_id = ?", (project_id,)
    )
    
    required_fields = ["business_name", "phone", "address", "hours"]
    
    for entity in entities:
        extracted = from_json(entity.get("extracted_data", "{}"), {})
        overrides = from_json(entity.get("manual_overrides", "{}"), {})
        merged = {**extracted, **overrides}
        
        for field in required_fields:
            if not merged.get(field, "").strip():
                findings.append({
                    "type": "schema",
                    "entity_id": entity["id"],
                    "entity_name": entity["name"],
                    "field": field,
                    "severity": "critical",
                    "message": f"Required field '{field}' is missing",
                    "action_type": "fill_field",
                    "action_data": {"field": field},
                })
    
    # Check for data consistency
    if entities:
        avg_score = sum(e.get("consistency_score", 0) for e in entities) / len(entities)
        if avg_score < 50:
            findings.append({
                "type": "schema",
                "severity": "warning",
                "message": f"Average entity consistency score is low ({avg_score:.0f}%)",
                "action_type": "review_entities",
            })
    
    return findings


# ═══════════════════════════════════════════════════════════════
# LOCAL SEO AUDIT
# ═══════════════════════════════════════════════════════════════

async def audit_local_seo(project_id: str) -> List[Dict]:
    """Audit local SEO signals."""
    findings = []
    
    entities = await fetch_all(
        "SELECT * FROM entity_onboarding WHERE project_id = ?", (project_id,)
    )
    
    for entity in entities:
        extracted = from_json(entity.get("extracted_data", "{}"), {})
        
        # Check for NAP consistency
        if not extracted.get("phone"):
            findings.append({
                "type": "local",
                "entity_id": entity["id"],
                "entity_name": entity["name"],
                "severity": "warning",
                "message": "Phone number not found (NAP consistency issue)",
                "action_type": "add_phone",
            })
        
        # Check for local keywords in description
        desc = extracted.get("description", "").lower()
        if entity.get("name"):
            city = extracted.get("city", "").lower()
            if city and city not in desc:
                findings.append({
                    "type": "local",
                    "entity_id": entity["id"],
                    "entity_name": entity["name"],
                    "severity": "info",
                    "message": f"Location '{city}' not mentioned in description",
                    "action_type": "update_description",
                })
        
        # Check for business hours
        if not extracted.get("hours"):
            findings.append({
                "type": "local",
                "entity_id": entity["id"],
                "entity_name": entity["name"],
                "severity": "warning",
                "message": "Business hours not specified",
                "action_type": "add_hours",
            })
    
    return findings


# ═══════════════════════════════════════════════════════════════
# GOOGLE BUSINESS PROFILE AUDIT
# ═══════════════════════════════════════════════════════════════

async def audit_gbp(project_id: str) -> List[Dict]:
    """Audit Google Business Profile setup."""
    findings = []
    
    entities = await fetch_all(
        "SELECT * FROM entity_onboarding WHERE project_id = ?", (project_id,)
    )
    
    for entity in entities:
        extracted = from_json(entity.get("extracted_data", "{}"), {})
        social = extracted.get("social_profiles", {})
        
        # Check if GBP is claimed
        if not social.get("google"):
            findings.append({
                "type": "gbp",
                "entity_id": entity["id"],
                "entity_name": entity["name"],
                "severity": "critical",
                "message": "Google Business Profile not claimed or linked",
                "action_type": "claim_gbp",
            })
        
        # Check for photos
        # (Simplified - would need actual GBP API data)
        findings.append({
            "type": "gbp",
            "entity_id": entity["id"],
            "entity_name": entity["name"],
            "severity": "info",
            "message": "Add high-quality business photos to GBP",
            "action_type": "add_gbp_photos",
        })
    
    return findings


# ═══════════════════════════════════════════════════════════════
# REVIEW AUDIT
# ═══════════════════════════════════════════════════════════════

async def audit_reviews(project_id: str) -> List[Dict]:
    """Audit review management and sentiment."""
    findings = []
    
    entities = await fetch_all(
        "SELECT * FROM entity_onboarding WHERE project_id = ?", (project_id,)
    )
    
    for entity in entities:
        # Simplified review audit (would need actual review data)
        findings.append({
            "type": "review",
            "entity_id": entity["id"],
            "entity_name": entity["name"],
            "severity": "info",
            "message": "Monitor and respond to customer reviews",
            "action_type": "monitor_reviews",
        })
    
    return findings


# ═══════════════════════════════════════════════════════════════
# AUTHORITY AUDIT
# ═══════════════════════════════════════════════════════════════

async def audit_authority(project_id: str) -> List[Dict]:
    """Audit domain and entity authority signals."""
    findings = []
    
    entities = await fetch_all(
        "SELECT * FROM entity_onboarding WHERE project_id = ?", (project_id,)
    )
    
    for entity in entities:
        extracted = from_json(entity.get("extracted_data", "{}"), {})
        
        # Check for website
        if not extracted.get("website"):
            findings.append({
                "type": "authority",
                "entity_id": entity["id"],
                "entity_name": entity["name"],
                "severity": "warning",
                "message": "No website found (reduces authority)",
                "action_type": "add_website",
            })
        
        # Check for social profiles
        social = extracted.get("social_profiles", {})
        if not social or len(social) < 2:
            findings.append({
                "type": "authority",
                "entity_id": entity["id"],
                "entity_name": entity["name"],
                "severity": "info",
                "message": "Add more social media profiles to increase authority",
                "action_type": "add_social_profiles",
            })
    
    return findings


# ═══════════════════════════════════════════════════════════════
# ENTITY CONSISTENCY AUDIT
# ═══════════════════════════════════════════════════════════════

async def audit_entity_consistency(project_id: str) -> List[Dict]:
    """Audit data consistency across sources."""
    findings = []
    
    entities = await fetch_all(
        "SELECT * FROM entity_onboarding WHERE project_id = ?", (project_id,)
    )
    
    for entity in entities:
        score = entity.get("consistency_score", 0)
        
        if score < 50:
            findings.append({
                "type": "entity_consistency",
                "entity_id": entity["id"],
                "entity_name": entity["name"],
                "severity": "critical",
                "message": f"Low consistency score ({score:.0f}%) - data conflicts detected",
                "action_type": "review_consistency",
                "action_data": {"entity_id": entity["id"]},
            })
        elif score < 70:
            findings.append({
                "type": "entity_consistency",
                "entity_id": entity["id"],
                "entity_name": entity["name"],
                "severity": "warning",
                "message": f"Moderate consistency score ({score:.0f}%) - review for conflicts",
                "action_type": "review_consistency",
                "action_data": {"entity_id": entity["id"]},
            })
    
    return findings


# ═══════════════════════════════════════════════════════════════
# UNIFIED AUDIT EXECUTION
# ═══════════════════════════════════════════════════════════════

async def run_full_audit(project_id: str) -> str:
    """Run all audit types for a project."""
    audit_id = await create_geo_audit(project_id, "full_audit")
    
    try:
        # Run all audit types
        all_findings = []
        all_findings.extend(await audit_schema(project_id))
        all_findings.extend(await audit_local_seo(project_id))
        all_findings.extend(await audit_gbp(project_id))
        all_findings.extend(await audit_reviews(project_id))
        all_findings.extend(await audit_authority(project_id))
        all_findings.extend(await audit_entity_consistency(project_id))
        
        # Convert findings to actions
        action_ids = []
        for finding in all_findings:
            if finding.get("action_type"):
                action_id = gen_id("act-")
                action_ids.append(action_id)
                # Store action (would be in action_engine)
        
        # Complete audit
        await complete_geo_audit(audit_id, all_findings, action_ids)
        
        logger.info(f"Audit {audit_id} complete: {len(all_findings)} findings")
    except Exception as e:
        logger.error(f"Audit {audit_id} failed: {e}")
        await execute(
            "UPDATE geo_audits SET status = ? WHERE id = ?",
            ("failed", audit_id),
        )
    
    return audit_id


async def run_audit_by_type(project_id: str, audit_type: str) -> str:
    """Run a specific audit type."""
    audit_id = await create_geo_audit(project_id, audit_type)
    
    try:
        # Run specific audit
        if audit_type == "schema":
            findings = await audit_schema(project_id)
        elif audit_type == "local":
            findings = await audit_local_seo(project_id)
        elif audit_type == "gbp":
            findings = await audit_gbp(project_id)
        elif audit_type == "review":
            findings = await audit_reviews(project_id)
        elif audit_type == "authority":
            findings = await audit_authority(project_id)
        elif audit_type == "entity_consistency":
            findings = await audit_entity_consistency(project_id)
        else:
            findings = []
        
        # Convert findings to actions
        action_ids = []
        for finding in findings:
            if finding.get("action_type"):
                action_id = gen_id("act-")
                action_ids.append(action_id)
        
        # Complete audit
        await complete_geo_audit(audit_id, findings, action_ids)
        
        logger.info(f"Audit {audit_id} ({audit_type}) complete: {len(findings)} findings")
    except Exception as e:
        logger.error(f"Audit {audit_id} failed: {e}")
        await execute(
            "UPDATE geo_audits SET status = ? WHERE id = ?",
            ("failed", audit_id),
        )
    
    return audit_id


# ═══════════════════════════════════════════════════════════════
# AUDIT SUMMARY & REPORTING
# ═══════════════════════════════════════════════════════════════

async def get_audit_summary(project_id: str) -> Dict:
    """Get summary of all audits for a project."""
    audits = await fetch_all(
        "SELECT * FROM geo_audits WHERE project_id = ? ORDER BY created_at DESC LIMIT 10",
        (project_id,),
    )
    
    summary = {
        "total_audits": len(audits),
        "by_type": {},
        "recent_findings": [],
        "critical_count": 0,
        "warning_count": 0,
    }
    
    for audit in audits:
        audit_type = audit.get("audit_type", "unknown")
        summary["by_type"][audit_type] = summary["by_type"].get(audit_type, 0) + 1
        
        findings = from_json(audit.get("findings", "[]"), [])
        for finding in findings:
            if finding.get("severity") == "critical":
                summary["critical_count"] += 1
            elif finding.get("severity") == "warning":
                summary["warning_count"] += 1
            
            if len(summary["recent_findings"]) < 5:
                summary["recent_findings"].append({
                    "audit_type": audit_type,
                    "message": finding.get("message"),
                    "severity": finding.get("severity"),
                })
    
    return summary
