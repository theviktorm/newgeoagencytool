"""
MOMENTUS AI — PUBLISH-TO-RETRACK-TO-IMPORT CYCLE ENGINE
Publish guardrails, re-tracking, new import batch, before/after reporting.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .database import execute, fetch_all, fetch_one, from_json, gen_id, to_json
from .workflow_engine import (
    record_milestone, update_project_status, create_import_batch,
    update_import_batch, get_geo_project,
)

logger = logging.getLogger("geo.publish_cycle")

# ═══════════════════════════════════════════════════════════════
# PUBLISH GUARDRAILS
# ═══════════════════════════════════════════════════════════════

async def check_publish_readiness(project_id: str) -> Dict:
    """Check if project is ready to publish."""
    proj = await get_geo_project(project_id)
    if not proj:
        return {"ready": False, "errors": ["Project not found"]}
    
    errors = []
    warnings = []
    
    # Check entities
    entities = await fetch_all(
        "SELECT * FROM entity_onboarding WHERE project_id = ?", (project_id,)
    )
    
    if not entities:
        errors.append("No entities onboarded")
    else:
        # Check entity completeness
        for entity in entities:
            if entity.get("crawl_status") != "validated":
                errors.append(f"Entity '{entity['name']}' not validated")
            
            score = entity.get("consistency_score", 0)
            if score < 50:
                errors.append(f"Entity '{entity['name']}' has low consistency ({score:.0f}%)")
            elif score < 70:
                warnings.append(f"Entity '{entity['name']}' has moderate consistency ({score:.0f}%)")
    
    # Check audits
    audits = await fetch_all(
        "SELECT * FROM geo_audits WHERE project_id = ? ORDER BY created_at DESC LIMIT 1",
        (project_id,),
    )
    
    if not audits:
        warnings.append("No audits run yet")
    else:
        audit = audits[0]
        findings = from_json(audit.get("findings", "[]"), [])
        critical_count = len([f for f in findings if f.get("severity") == "critical"])
        
        if critical_count > 0:
            errors.append(f"{critical_count} critical audit findings must be resolved")
    
    # Check approvals
    if proj.get("approval_status") != "approved":
        errors.append("Project not approved for publishing")
    
    return {
        "ready": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "entity_count": len(entities),
    }


# ═══════════════════════════════════════════════════════════════
# PUBLISH EXECUTION
# ═══════════════════════════════════════════════════════════════

async def publish_project(project_id: str, publish_config: Dict = None) -> str:
    """Publish project changes."""
    config = publish_config or {}
    
    # Check readiness
    readiness = await check_publish_readiness(project_id)
    if not readiness["ready"]:
        raise ValueError(f"Project not ready: {', '.join(readiness['errors'])}")
    
    # Create publish record
    publish_id = gen_id("pub-")
    
    await execute(
        """INSERT INTO publish_cycles
           (id, project_id, status, publish_config, created_at)
           VALUES (?, ?, ?, ?, datetime('now'))""",
        (publish_id, project_id, "publishing", to_json(config)),
    )
    
    try:
        # Get entities to publish
        entities = await fetch_all(
            "SELECT * FROM entity_onboarding WHERE project_id = ?", (project_id,)
        )
        
        # Capture before snapshot
        before_snapshot = {
            "entity_count": len(entities),
            "entities": [
                {
                    "id": e["id"],
                    "name": e["name"],
                    "data": from_json(e.get("extracted_data", "{}"), {}),
                    "score": e.get("consistency_score", 0),
                }
                for e in entities
            ],
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        # Execute publish (simplified - would call actual publishing service)
        publish_results = {
            "entities_published": len(entities),
            "success_count": len(entities),
            "error_count": 0,
            "details": [
                {"entity_id": e["id"], "status": "published"}
                for e in entities
            ],
        }
        
        # Update publish record
        await execute(
            """UPDATE publish_cycles
               SET status = ?, before_snapshot = ?, publish_results = ?, updated_at = datetime('now')
               WHERE id = ?""",
            ("published", to_json(before_snapshot), to_json(publish_results), publish_id),
        )
        
        # Record milestone
        await record_milestone(project_id, "published", {
            "publish_id": publish_id,
            "entity_count": len(entities),
        })
        
        logger.info(f"Published project {project_id} via cycle {publish_id}")
    except Exception as e:
        logger.error(f"Publish failed for {project_id}: {e}")
        await execute(
            "UPDATE publish_cycles SET status = ? WHERE id = ?",
            ("failed", publish_id),
        )
        raise
    
    return publish_id


# ═══════════════════════════════════════════════════════════════
# RE-TRACKING
# ═══════════════════════════════════════════════════════════════

async def retrack_project(project_id: str, publish_id: str) -> str:
    """Re-track project after publishing."""
    retrack_id = gen_id("ret-")
    
    await execute(
        """INSERT INTO retrack_cycles
           (id, project_id, publish_id, status, created_at)
           VALUES (?, ?, ?, ?, datetime('now'))""",
        (retrack_id, project_id, publish_id, "tracking"),
    )
    
    try:
        # Get entities to retrack
        entities = await fetch_all(
            "SELECT * FROM entity_onboarding WHERE project_id = ?", (project_id,)
        )
        
        # Retrack each entity (simplified)
        retrack_results = {
            "entities_retracked": len(entities),
            "success_count": len(entities),
            "error_count": 0,
            "details": [
                {"entity_id": e["id"], "status": "retracked"}
                for e in entities
            ],
        }
        
        await execute(
            """UPDATE retrack_cycles
               SET status = ?, retrack_results = ?, updated_at = datetime('now')
               WHERE id = ?""",
            ("complete", to_json(retrack_results), retrack_id),
        )
        
        # Record milestone
        await record_milestone(project_id, "retracked", {
            "retrack_id": retrack_id,
            "entity_count": len(entities),
        })
        
        logger.info(f"Retracked project {project_id} via cycle {retrack_id}")
    except Exception as e:
        logger.error(f"Retrack failed for {project_id}: {e}")
        await execute(
            "UPDATE retrack_cycles SET status = ? WHERE id = ?",
            ("failed", retrack_id),
        )
        raise
    
    return retrack_id


# ═══════════════════════════════════════════════════════════════
# NEW IMPORT BATCH CREATION
# ═══════════════════════════════════════════════════════════════

async def create_new_import_batch_from_retrack(
    project_id: str, retrack_id: str
) -> str:
    """Create new import batch from retracked data."""
    # Get retrack results
    retrack = await fetch_one(
        "SELECT * FROM retrack_cycles WHERE id = ?", (retrack_id,)
    )
    if not retrack:
        raise ValueError(f"Retrack {retrack_id} not found")
    
    results = from_json(retrack.get("retrack_results", "{}"), {})
    
    # Create import batch
    batch_id = await create_import_batch(
        project_id, "retrack_import", results.get("entities_retracked", 0)
    )
    
    # Store retrack data as import source
    await execute(
        """UPDATE import_batches
           SET retrack_id = ?, source_data = ?, status = ?
           WHERE id = ?""",
        (retrack_id, to_json(results), "pending_validation", batch_id),
    )
    
    logger.info(f"Created import batch {batch_id} from retrack {retrack_id}")
    return batch_id


# ═══════════════════════════════════════════════════════════════
# BEFORE/AFTER COMPARISON
# ═══════════════════════════════════════════════════════════════

async def generate_before_after_report(
    project_id: str, publish_id: str, batch_id: str
) -> Dict:
    """Generate before/after comparison report."""
    # Get publish cycle
    publish = await fetch_one(
        "SELECT * FROM publish_cycles WHERE id = ?", (publish_id,)
    )
    if not publish:
        return {"error": "Publish cycle not found"}
    
    # Get import batch
    batch = await fetch_one(
        "SELECT * FROM import_batches WHERE id = ?", (batch_id,)
    )
    if not batch:
        return {"error": "Import batch not found"}
    
    before_snapshot = from_json(publish.get("before_snapshot", "{}"), {})
    after_snapshot = from_json(batch.get("after_snapshot", "{}"), {})
    
    # Compare entities
    entity_changes = []
    before_entities = {e["id"]: e for e in before_snapshot.get("entities", [])}
    after_entities = {e["id"]: e for e in after_snapshot.get("entities", [])}
    
    for entity_id, before_data in before_entities.items():
        after_data = after_entities.get(entity_id, {})
        
        # Calculate changes
        before_score = before_data.get("score", 0)
        after_score = after_data.get("score", 0)
        score_change = after_score - before_score
        
        before_fields = before_data.get("data", {})
        after_fields = after_data.get("data", {})
        
        field_changes = {}
        for field in set(list(before_fields.keys()) + list(after_fields.keys())):
            before_val = before_fields.get(field, "")
            after_val = after_fields.get(field, "")
            if before_val != after_val:
                field_changes[field] = {
                    "before": before_val,
                    "after": after_val,
                }
        
        entity_changes.append({
            "entity_id": entity_id,
            "entity_name": before_data.get("name", ""),
            "score_change": score_change,
            "before_score": before_score,
            "after_score": after_score,
            "field_changes": field_changes,
            "field_change_count": len(field_changes),
        })
    
    # Calculate summary
    total_score_change = sum(e["score_change"] for e in entity_changes)
    avg_score_change = total_score_change / len(entity_changes) if entity_changes else 0
    entities_improved = len([e for e in entity_changes if e["score_change"] > 0])
    entities_degraded = len([e for e in entity_changes if e["score_change"] < 0])
    
    return {
        "publish_id": publish_id,
        "batch_id": batch_id,
        "before_timestamp": before_snapshot.get("timestamp"),
        "after_timestamp": after_snapshot.get("timestamp"),
        "entity_count": len(entity_changes),
        "total_score_change": round(total_score_change, 1),
        "average_score_change": round(avg_score_change, 1),
        "entities_improved": entities_improved,
        "entities_degraded": entities_degraded,
        "entity_changes": entity_changes,
    }


# ═══════════════════════════════════════════════════════════════
# FULL PUBLISH CYCLE
# ═══════════════════════════════════════════════════════════════

async def run_full_publish_cycle(project_id: str) -> Dict:
    """Run complete publish → retrack → import → report cycle."""
    try:
        # Step 1: Publish
        publish_id = await publish_project(project_id)
        
        # Step 2: Retrack
        retrack_id = await retrack_project(project_id, publish_id)
        
        # Step 3: Create new import batch
        batch_id = await create_new_import_batch_from_retrack(project_id, retrack_id)
        
        # Step 4: Generate report
        report = await generate_before_after_report(project_id, publish_id, batch_id)
        
        # Update project status
        await update_project_status(project_id, "active", "review_results")
        
        # Record milestone
        await record_milestone(project_id, "cycle_complete", {
            "publish_id": publish_id,
            "retrack_id": retrack_id,
            "batch_id": batch_id,
        })
        
        return {
            "success": True,
            "publish_id": publish_id,
            "retrack_id": retrack_id,
            "batch_id": batch_id,
            "report": report,
        }
    except Exception as e:
        logger.error(f"Publish cycle failed for {project_id}: {e}")
        return {
            "success": False,
            "error": str(e),
        }


# ═══════════════════════════════════════════════════════════════
# CYCLE HISTORY
# ═══════════════════════════════════════════════════════════════

async def list_publish_cycles(project_id: str) -> List[Dict]:
    """List all publish cycles for a project."""
    cycles = await fetch_all(
        "SELECT * FROM publish_cycles WHERE project_id = ? ORDER BY created_at DESC",
        (project_id,),
    )
    
    for cycle in cycles:
        cycle["before_snapshot"] = from_json(cycle.get("before_snapshot", "{}"), {})
        cycle["publish_results"] = from_json(cycle.get("publish_results", "{}"), {})
    
    return cycles


async def get_publish_cycle_details(cycle_id: str) -> Optional[Dict]:
    """Get full publish cycle details."""
    cycle = await fetch_one(
        "SELECT * FROM publish_cycles WHERE id = ?", (cycle_id,)
    )
    if cycle:
        cycle["before_snapshot"] = from_json(cycle.get("before_snapshot", "{}"), {})
        cycle["publish_results"] = from_json(cycle.get("publish_results", "{}"), {})
    return cycle
