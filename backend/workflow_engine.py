"""
MOMENTUS AI — CENTRAL GEO PROJECT WORKFLOW ENGINE
Unified project lifecycle, status tracking, and orchestration.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .database import (
    execute, fetch_all, fetch_one, from_json, gen_id, to_json,
)

logger = logging.getLogger("geo.workflow")

# ═══════════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════════

WORKFLOW_SCHEMA = """
-- ─── GEO Projects (central workflow container) ───
CREATE TABLE IF NOT EXISTS geo_projects (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    name            TEXT NOT NULL,
    description     TEXT DEFAULT '',
    status          TEXT DEFAULT 'setup',  -- setup | onboarding | active | paused | archived
    stage           TEXT DEFAULT 'brand_setup',  -- brand_setup | import_peec | entity_onboarding | audit | actions | publish | retrack
    confidence      TEXT DEFAULT 'estimated',  -- estimated | validated | optimized
    created_by      TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(workspace_id, name)
);
CREATE INDEX IF NOT EXISTS idx_geo_projects_workspace ON geo_projects(workspace_id);
CREATE INDEX IF NOT EXISTS idx_geo_projects_status ON geo_projects(status);

-- ─── Project lifecycle milestones ───
CREATE TABLE IF NOT EXISTS project_milestones (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    milestone_type  TEXT NOT NULL,  -- brand_onboarded | peec_imported | audit_complete | actions_queued | published | retracked
    completed_at    TEXT,
    data            TEXT DEFAULT '{}',  -- JSON: contextual data
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(project_id) REFERENCES geo_projects(id)
);
CREATE INDEX IF NOT EXISTS idx_milestones_project ON project_milestones(project_id);

-- ─── Import batch history ───
CREATE TABLE IF NOT EXISTS import_batches (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    batch_type      TEXT NOT NULL,  -- peec_csv | peec_api | manual_mapping
    status          TEXT DEFAULT 'pending',  -- pending | validating | importing | complete | failed
    record_count    INTEGER DEFAULT 0,
    imported_count  INTEGER DEFAULT 0,
    error_count     INTEGER DEFAULT 0,
    validation_msg  TEXT DEFAULT '',
    mapping_data    TEXT DEFAULT '{}',  -- JSON: field mappings, transformations
    before_snapshot TEXT DEFAULT '{}',  -- JSON: state before import
    after_snapshot  TEXT DEFAULT '{}',  -- JSON: state after import
    started_at      TEXT,
    completed_at    TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(project_id) REFERENCES geo_projects(id)
);
CREATE INDEX IF NOT EXISTS idx_batches_project ON import_batches(project_id);
CREATE INDEX IF NOT EXISTS idx_batches_status ON import_batches(status);

-- ─── Brand/Entity onboarding ───
CREATE TABLE IF NOT EXISTS entity_onboarding (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    entity_type     TEXT NOT NULL,  -- brand | location | service_area
    name            TEXT NOT NULL,
    crawl_status    TEXT DEFAULT 'pending',  -- pending | crawling | extracted | validated | failed
    extracted_data  TEXT DEFAULT '{}',  -- JSON: profile, contact, hours, etc.
    manual_overrides TEXT DEFAULT '{}',  -- JSON: user corrections
    consistency_score REAL DEFAULT 0.0,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(project_id) REFERENCES geo_projects(id)
);
CREATE INDEX IF NOT EXISTS idx_entity_onboarding_project ON entity_onboarding(project_id);

-- ─── Unified Technical GEO Audit ───
CREATE TABLE IF NOT EXISTS geo_audits (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    audit_type      TEXT NOT NULL,  -- schema | local | gbp | review | authority | entity_consistency
    status          TEXT DEFAULT 'pending',  -- pending | running | complete | failed
    issue_count     INTEGER DEFAULT 0,
    critical_count  INTEGER DEFAULT 0,
    warning_count   INTEGER DEFAULT 0,
    findings        TEXT DEFAULT '[]',  -- JSON array of issues
    action_ids      TEXT DEFAULT '[]',  -- JSON array of linked action IDs
    run_at          TEXT,
    completed_at    TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(project_id) REFERENCES geo_projects(id)
);
CREATE INDEX IF NOT EXISTS idx_audits_project ON geo_audits(project_id);
CREATE INDEX IF NOT EXISTS idx_audits_status ON geo_audits(status);

-- ─── Publish-to-retrack workflow ───
CREATE TABLE IF NOT EXISTS publish_cycles (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    cycle_number    INTEGER DEFAULT 1,
    action_ids      TEXT NOT NULL,  -- JSON array
    publish_status  TEXT DEFAULT 'pending',  -- pending | publishing | published | failed
    retrack_status  TEXT DEFAULT 'pending',  -- pending | retracking | complete | failed
    before_metrics  TEXT DEFAULT '{}',  -- JSON: baseline
    after_metrics   TEXT DEFAULT '{}',  -- JSON: post-publish
    improvement     REAL DEFAULT 0.0,  -- percent change
    published_at    TEXT,
    retracked_at    TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(project_id) REFERENCES geo_projects(id)
);
CREATE INDEX IF NOT EXISTS idx_cycles_project ON publish_cycles(project_id);

-- ─── Human approval gates ───
CREATE TABLE IF NOT EXISTS approval_gates (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    gate_type       TEXT NOT NULL,  -- import_validation | audit_sign_off | publish_approval | retrack_confirmation
    status          TEXT DEFAULT 'pending',  -- pending | approved | rejected | needs_revision
    requested_by    TEXT DEFAULT '',
    approved_by     TEXT DEFAULT '',
    notes           TEXT DEFAULT '',
    requested_at    TEXT DEFAULT (datetime('now')),
    approved_at     TEXT,
    FOREIGN KEY(project_id) REFERENCES geo_projects(id)
);
CREATE INDEX IF NOT EXISTS idx_gates_project ON approval_gates(project_id);
CREATE INDEX IF NOT EXISTS idx_gates_status ON approval_gates(status);
"""

# ═══════════════════════════════════════════════════════════════
# INIT
# ═══════════════════════════════════════════════════════════════

async def init_workflow():
    """Initialize workflow tables."""
    await execute(WORKFLOW_SCHEMA)
    logger.info("Workflow tables initialized")


# ═══════════════════════════════════════════════════════════════
# GEO PROJECT LIFECYCLE
# ═══════════════════════════════════════════════════════════════

async def create_geo_project(
    workspace_id: str, name: str, description: str = "", created_by: str = ""
) -> str:
    """Create a new GEO project."""
    project_id = gen_id("proj-")
    await execute(
        """INSERT INTO geo_projects
           (id, workspace_id, name, description, status, stage, created_by)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (project_id, workspace_id, name, description, "setup", "brand_setup", created_by),
    )
    return project_id


async def get_geo_project(project_id: str) -> Optional[Dict]:
    """Get a GEO project by ID."""
    proj = await fetch_one("SELECT * FROM geo_projects WHERE id = ?", (project_id,))
    if proj:
        proj["milestones"] = await fetch_all(
            "SELECT * FROM project_milestones WHERE project_id = ? ORDER BY created_at",
            (project_id,),
        )
        for m in proj.get("milestones", []):
            m["data"] = from_json(m.get("data", "{}"), {})
    return proj


async def list_geo_projects(workspace_id: str, status: str = "") -> List[Dict]:
    """List GEO projects in a workspace."""
    where = "workspace_id = ?"
    params = [workspace_id]
    if status:
        where += " AND status = ?"
        params.append(status)
    projects = await fetch_all(
        f"SELECT * FROM geo_projects WHERE {where} ORDER BY updated_at DESC",
        tuple(params),
    )
    for p in projects:
        p["milestones"] = await fetch_all(
            "SELECT * FROM project_milestones WHERE project_id = ? ORDER BY created_at DESC LIMIT 5",
            (p["id"],),
        )
        for m in p.get("milestones", []):
            m["data"] = from_json(m.get("data", "{}"), {})
    return projects


async def update_project_status(project_id: str, status: str, stage: str = "") -> None:
    """Update project status and optionally stage."""
    updates = ["status = ?", "updated_at = datetime('now')"]
    params = [status]
    if stage:
        updates.insert(1, "stage = ?")
        params.insert(1, stage)
    params.append(project_id)
    await execute(
        f"UPDATE geo_projects SET {', '.join(updates)} WHERE id = ?",
        tuple(params),
    )


async def record_milestone(
    project_id: str, milestone_type: str, data: Dict = None
) -> str:
    """Record a project milestone."""
    milestone_id = gen_id("mil-")
    await execute(
        """INSERT INTO project_milestones
           (id, project_id, milestone_type, data, completed_at)
           VALUES (?, ?, ?, ?, datetime('now'))""",
        (milestone_id, project_id, milestone_type, to_json(data or {})),
    )
    return milestone_id


# ═══════════════════════════════════════════════════════════════
# IMPORT BATCH TRACKING
# ═══════════════════════════════════════════════════════════════

async def create_import_batch(
    project_id: str, batch_type: str, record_count: int = 0
) -> str:
    """Create an import batch."""
    batch_id = gen_id("batch-")
    await execute(
        """INSERT INTO import_batches
           (id, project_id, batch_type, record_count, status, started_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))""",
        (batch_id, project_id, batch_type, record_count, "pending"),
    )
    return batch_id


async def get_import_batch(batch_id: str) -> Optional[Dict]:
    """Get import batch details."""
    batch = await fetch_one("SELECT * FROM import_batches WHERE id = ?", (batch_id,))
    if batch:
        batch["mapping_data"] = from_json(batch.get("mapping_data", "{}"), {})
        batch["before_snapshot"] = from_json(batch.get("before_snapshot", "{}"), {})
        batch["after_snapshot"] = from_json(batch.get("after_snapshot", "{}"), {})
    return batch


async def update_import_batch(batch_id: str, data: Dict) -> None:
    """Update import batch status and data."""
    updates = []
    params = []
    for key in ["status", "imported_count", "error_count", "validation_msg"]:
        if key in data:
            updates.append(f"{key} = ?")
            params.append(data[key])
    for key in ["mapping_data", "before_snapshot", "after_snapshot"]:
        if key in data:
            updates.append(f"{key} = ?")
            params.append(to_json(data[key]))
    if data.get("status") == "complete":
        updates.append("completed_at = datetime('now')")
    if updates:
        updates.append("updated_at = datetime('now')")
        params.append(batch_id)
        await execute(
            f"UPDATE import_batches SET {', '.join(updates)} WHERE id = ?",
            tuple(params),
        )


async def list_import_batches(project_id: str) -> List[Dict]:
    """List import batches for a project."""
    batches = await fetch_all(
        "SELECT * FROM import_batches WHERE project_id = ? ORDER BY created_at DESC",
        (project_id,),
    )
    for b in batches:
        b["mapping_data"] = from_json(b.get("mapping_data", "{}"), {})
        b["before_snapshot"] = from_json(b.get("before_snapshot", "{}"), {})
        b["after_snapshot"] = from_json(b.get("after_snapshot", "{}"), {})
    return batches


# ═══════════════════════════════════════════════════════════════
# ENTITY ONBOARDING
# ═══════════════════════════════════════════════════════════════

async def create_entity_onboarding(
    project_id: str, entity_type: str, name: str
) -> str:
    """Create entity onboarding record."""
    entity_id = gen_id("ent-")
    await execute(
        """INSERT INTO entity_onboarding
           (id, project_id, entity_type, name, crawl_status)
           VALUES (?, ?, ?, ?, ?)""",
        (entity_id, project_id, entity_type, name, "pending"),
    )
    return entity_id


async def update_entity_onboarding(entity_id: str, data: Dict) -> None:
    """Update entity onboarding data."""
    updates = []
    params = []
    for key in ["crawl_status", "consistency_score"]:
        if key in data:
            updates.append(f"{key} = ?")
            params.append(data[key])
    for key in ["extracted_data", "manual_overrides"]:
        if key in data:
            updates.append(f"{key} = ?")
            params.append(to_json(data[key]))
    if updates:
        updates.append("updated_at = datetime('now')")
        params.append(entity_id)
        await execute(
            f"UPDATE entity_onboarding SET {', '.join(updates)} WHERE id = ?",
            tuple(params),
        )


async def list_entity_onboarding(project_id: str) -> List[Dict]:
    """List entities for a project."""
    entities = await fetch_all(
        "SELECT * FROM entity_onboarding WHERE project_id = ? ORDER BY created_at",
        (project_id,),
    )
    for e in entities:
        e["extracted_data"] = from_json(e.get("extracted_data", "{}"), {})
        e["manual_overrides"] = from_json(e.get("manual_overrides", "{}"), {})
    return entities


# ═══════════════════════════════════════════════════════════════
# GEO AUDIT
# ═══════════════════════════════════════════════════════════════

async def create_geo_audit(project_id: str, audit_type: str) -> str:
    """Create a GEO audit."""
    audit_id = gen_id("audit-")
    await execute(
        """INSERT INTO geo_audits
           (id, project_id, audit_type, status, run_at)
           VALUES (?, ?, ?, ?, datetime('now'))""",
        (audit_id, project_id, audit_type, "running"),
    )
    return audit_id


async def complete_geo_audit(
    audit_id: str, findings: List[Dict], action_ids: List[str] = None
) -> None:
    """Mark audit complete with findings."""
    critical = sum(1 for f in findings if f.get("severity") == "critical")
    warnings = sum(1 for f in findings if f.get("severity") == "warning")
    await execute(
        """UPDATE geo_audits
           SET status = ?, issue_count = ?, critical_count = ?, warning_count = ?,
               findings = ?, action_ids = ?, completed_at = datetime('now')
           WHERE id = ?""",
        (
            "complete",
            len(findings),
            critical,
            warnings,
            to_json(findings),
            to_json(action_ids or []),
            audit_id,
        ),
    )


async def get_geo_audit(audit_id: str) -> Optional[Dict]:
    """Get audit details."""
    audit = await fetch_one("SELECT * FROM geo_audits WHERE id = ?", (audit_id,))
    if audit:
        audit["findings"] = from_json(audit.get("findings", "[]"), [])
        audit["action_ids"] = from_json(audit.get("action_ids", "[]"), [])
    return audit


async def list_geo_audits(project_id: str) -> List[Dict]:
    """List audits for a project."""
    audits = await fetch_all(
        "SELECT * FROM geo_audits WHERE project_id = ? ORDER BY created_at DESC",
        (project_id,),
    )
    for a in audits:
        a["findings"] = from_json(a.get("findings", "[]"), [])
        a["action_ids"] = from_json(a.get("action_ids", "[]"), [])
    return audits


# ═══════════════════════════════════════════════════════════════
# PUBLISH-TO-RETRACK CYCLES
# ═══════════════════════════════════════════════════════════════

async def create_publish_cycle(project_id: str, action_ids: List[str]) -> str:
    """Create a publish cycle."""
    cycle_id = gen_id("cycle-")
    # Get next cycle number
    last = await fetch_one(
        "SELECT MAX(cycle_number) as n FROM publish_cycles WHERE project_id = ?",
        (project_id,),
    )
    next_num = (last.get("n") or 0) + 1
    await execute(
        """INSERT INTO publish_cycles
           (id, project_id, cycle_number, action_ids, publish_status)
           VALUES (?, ?, ?, ?, ?)""",
        (cycle_id, project_id, next_num, to_json(action_ids), "pending"),
    )
    return cycle_id


async def update_publish_cycle(cycle_id: str, data: Dict) -> None:
    """Update publish cycle status."""
    updates = []
    params = []
    for key in ["publish_status", "retrack_status", "improvement"]:
        if key in data:
            updates.append(f"{key} = ?")
            params.append(data[key])
    for key in ["before_metrics", "after_metrics"]:
        if key in data:
            updates.append(f"{key} = ?")
            params.append(to_json(data[key]))
    if data.get("publish_status") == "published":
        updates.append("published_at = datetime('now')")
    if data.get("retrack_status") == "complete":
        updates.append("retracked_at = datetime('now')")
    if updates:
        params.append(cycle_id)
        await execute(
            f"UPDATE publish_cycles SET {', '.join(updates)} WHERE id = ?",
            tuple(params),
        )


async def get_publish_cycle(cycle_id: str) -> Optional[Dict]:
    """Get publish cycle details."""
    cycle = await fetch_one("SELECT * FROM publish_cycles WHERE id = ?", (cycle_id,))
    if cycle:
        cycle["action_ids"] = from_json(cycle.get("action_ids", "[]"), [])
        cycle["before_metrics"] = from_json(cycle.get("before_metrics", "{}"), {})
        cycle["after_metrics"] = from_json(cycle.get("after_metrics", "{}"), {})
    return cycle


# ═══════════════════════════════════════════════════════════════
# APPROVAL GATES
# ═══════════════════════════════════════════════════════════════

async def create_approval_gate(
    project_id: str, gate_type: str, requested_by: str
) -> str:
    """Create an approval gate."""
    gate_id = gen_id("gate-")
    await execute(
        """INSERT INTO approval_gates
           (id, project_id, gate_type, status, requested_by)
           VALUES (?, ?, ?, ?, ?)""",
        (gate_id, project_id, gate_type, "pending", requested_by),
    )
    return gate_id


async def approve_gate(gate_id: str, approved_by: str, notes: str = "") -> None:
    """Approve a gate."""
    await execute(
        """UPDATE approval_gates
           SET status = ?, approved_by = ?, notes = ?, approved_at = datetime('now')
           WHERE id = ?""",
        ("approved", approved_by, notes, gate_id),
    )


async def reject_gate(gate_id: str, approved_by: str, notes: str = "") -> None:
    """Reject a gate."""
    await execute(
        """UPDATE approval_gates
           SET status = ?, approved_by = ?, notes = ?
           WHERE id = ?""",
        ("rejected", approved_by, notes, gate_id),
    )


async def get_approval_gate(gate_id: str) -> Optional[Dict]:
    """Get approval gate details."""
    return await fetch_one("SELECT * FROM approval_gates WHERE id = ?", (gate_id,))


async def list_approval_gates(project_id: str, status: str = "") -> List[Dict]:
    """List approval gates for a project."""
    where = "project_id = ?"
    params = [project_id]
    if status:
        where += " AND status = ?"
        params.append(status)
    return await fetch_all(
        f"SELECT * FROM approval_gates WHERE {where} ORDER BY requested_at DESC",
        tuple(params),
    )


# ═══════════════════════════════════════════════════════════════
# WORKFLOW SUMMARY
# ═══════════════════════════════════════════════════════════════

async def get_project_workflow_summary(project_id: str) -> Dict:
    """Get complete workflow summary for a project."""
    proj = await get_geo_project(project_id)
    if not proj:
        return {}

    batches = await list_import_batches(project_id)
    entities = await list_entity_onboarding(project_id)
    audits = await list_geo_audits(project_id)
    cycles = await fetch_all(
        "SELECT * FROM publish_cycles WHERE project_id = ? ORDER BY cycle_number DESC LIMIT 10",
        (project_id,),
    )
    for c in cycles:
        c["action_ids"] = from_json(c.get("action_ids", "[]"), [])
    gates = await list_approval_gates(project_id)

    return {
        "project": proj,
        "import_batches": batches,
        "entities": entities,
        "audits": audits,
        "publish_cycles": cycles,
        "approval_gates": gates,
        "stage_progress": {
            "brand_setup": len(entities) > 0,
            "import_peec": any(b["status"] == "complete" for b in batches),
            "entity_onboarding": all(e["crawl_status"] in ["extracted", "validated"] for e in entities),
            "audit": any(a["status"] == "complete" for a in audits),
            "actions": any(a["status"] == "complete" for a in audits),
            "publish": any(c["publish_status"] == "published" for c in cycles),
            "retrack": any(c["retrack_status"] == "complete" for c in cycles),
        },
    }
