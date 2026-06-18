"""
MOMENTUS AI — PEEC IMPORT WIZARD ENGINE
CSV/API import, field mapping, validation, before/after snapshots.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import csv
import io
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .database import execute, fetch_all, fetch_one, from_json, gen_id, to_json

logger = logging.getLogger("geo.peec_import")

# ═══════════════════════════════════════════════════════════════
# PEEC IMPORT SCHEMA
# ═══════════════════════════════════════════════════════════════

PEEC_IMPORT_SCHEMA = """
-- ─── Peec import sessions ───
CREATE TABLE IF NOT EXISTS peec_import_sessions (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    session_type    TEXT NOT NULL,  -- csv_upload | api_pull | manual_mapping
    status          TEXT DEFAULT 'setup',  -- setup | mapping | validating | importing | complete | failed
    file_name       TEXT DEFAULT '',
    file_size       INTEGER DEFAULT 0,
    record_count    INTEGER DEFAULT 0,
    created_by      TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(project_id) REFERENCES geo_projects(id)
);
CREATE INDEX IF NOT EXISTS idx_peec_sessions_project ON peec_import_sessions(project_id);

-- ─── Field mapping definitions ───
CREATE TABLE IF NOT EXISTS peec_field_mappings (
    id              TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL,
    source_field    TEXT NOT NULL,  -- CSV column or API field name
    target_field    TEXT NOT NULL,  -- Momentus schema field
    transform_rule  TEXT DEFAULT '',  -- JSON: transformation logic
    is_custom       BOOLEAN DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(session_id) REFERENCES peec_import_sessions(id)
);
CREATE INDEX IF NOT EXISTS idx_mappings_session ON peec_field_mappings(session_id);

-- ─── Import validation results ───
CREATE TABLE IF NOT EXISTS peec_validation_results (
    id              TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL,
    record_index    INTEGER DEFAULT 0,
    field_name      TEXT NOT NULL,
    validation_type TEXT NOT NULL,  -- required | format | uniqueness | range | custom
    status          TEXT DEFAULT 'pass',  -- pass | warn | fail
    message         TEXT DEFAULT '',
    suggested_value TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(session_id) REFERENCES peec_import_sessions(id)
);
CREATE INDEX IF NOT EXISTS idx_validation_session ON peec_validation_results(session_id);

-- ─── Imported records ───
CREATE TABLE IF NOT EXISTS peec_imported_records (
    id              TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL,
    batch_id        TEXT NOT NULL,
    record_index    INTEGER DEFAULT 0,
    source_data     TEXT NOT NULL,  -- JSON: original CSV/API row
    mapped_data     TEXT NOT NULL,  -- JSON: after field mapping
    validated_data  TEXT NOT NULL,  -- JSON: after validation
    import_status   TEXT DEFAULT 'pending',  -- pending | imported | failed | skipped
    error_msg       TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(session_id) REFERENCES peec_import_sessions(id),
    FOREIGN KEY(batch_id) REFERENCES import_batches(id)
);
CREATE INDEX IF NOT EXISTS idx_imported_session ON peec_imported_records(session_id);
CREATE INDEX IF NOT EXISTS idx_imported_batch ON peec_imported_records(batch_id);
"""

# ═══════════════════════════════════════════════════════════════
# INIT
# ═══════════════════════════════════════════════════════════════

async def init_peec_import():
    """Initialize Peec import tables."""
    await execute(PEEC_IMPORT_SCHEMA)
    logger.info("Peec import tables initialized")


# ═══════════════════════════════════════════════════════════════
# PEEC IMPORT SESSION
# ═══════════════════════════════════════════════════════════════

async def create_import_session(
    project_id: str, session_type: str, file_name: str = "", created_by: str = ""
) -> str:
    """Create a Peec import session."""
    session_id = gen_id("peec-")
    await execute(
        """INSERT INTO peec_import_sessions
           (id, project_id, session_type, file_name, status, created_by)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (session_id, project_id, session_type, file_name, "setup", created_by),
    )
    return session_id


async def get_import_session(session_id: str) -> Optional[Dict]:
    """Get import session details."""
    session = await fetch_one(
        "SELECT * FROM peec_import_sessions WHERE id = ?", (session_id,)
    )
    if session:
        session["mappings"] = await fetch_all(
            "SELECT * FROM peec_field_mappings WHERE session_id = ?", (session_id,)
        )
        session["validations"] = await fetch_all(
            "SELECT * FROM peec_validation_results WHERE session_id = ?", (session_id,)
        )
    return session


async def update_import_session(session_id: str, data: Dict) -> None:
    """Update import session status."""
    updates = []
    params = []
    for key in ["status", "record_count", "file_size"]:
        if key in data:
            updates.append(f"{key} = ?")
            params.append(data[key])
    if updates:
        updates.append("updated_at = datetime('now')")
        params.append(session_id)
        await execute(
            f"UPDATE peec_import_sessions SET {', '.join(updates)} WHERE id = ?",
            tuple(params),
        )


# ═══════════════════════════════════════════════════════════════
# CSV PARSING & PREVIEW
# ═══════════════════════════════════════════════════════════════

async def parse_csv_preview(csv_content: str, limit: int = 10) -> Dict:
    """Parse CSV and return preview with field detection."""
    try:
        lines = csv_content.strip().split('\n')
        reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(reader)[:limit]
        
        if not rows:
            return {"error": "No data rows found"}
        
        headers = list(rows[0].keys())
        return {
            "success": True,
            "headers": headers,
            "row_count": len(rows),
            "preview_rows": rows,
            "field_types": detect_field_types(rows, headers),
        }
    except Exception as e:
        return {"error": f"CSV parse failed: {str(e)}"}


def detect_field_types(rows: List[Dict], headers: List[str]) -> Dict[str, str]:
    """Detect field types from sample data."""
    types = {}
    for header in headers:
        samples = [r.get(header, "") for r in rows if r.get(header)]
        if not samples:
            types[header] = "string"
            continue
        
        # Check for common patterns
        if all(s.isdigit() for s in samples):
            types[header] = "integer"
        elif all(is_float(s) for s in samples):
            types[header] = "float"
        elif all(is_email(s) for s in samples):
            types[header] = "email"
        elif all(is_phone(s) for s in samples):
            types[header] = "phone"
        elif all(is_url(s) for s in samples):
            types[header] = "url"
        else:
            types[header] = "string"
    
    return types


def is_float(s: str) -> bool:
    try:
        float(s)
        return "." in s
    except:
        return False


def is_email(s: str) -> bool:
    return "@" in s and "." in s and len(s) > 5


def is_phone(s: str) -> bool:
    digits = "".join(c for c in s if c.isdigit())
    return len(digits) >= 10


def is_url(s: str) -> bool:
    return s.startswith(("http://", "https://", "www."))


# ═══════════════════════════════════════════════════════════════
# FIELD MAPPING
# ═══════════════════════════════════════════════════════════════

PEEC_FIELD_SCHEMA = {
    "business_name": {"type": "string", "required": True},
    "phone": {"type": "phone", "required": False},
    "email": {"type": "email", "required": False},
    "website": {"type": "url", "required": False},
    "address": {"type": "string", "required": False},
    "city": {"type": "string", "required": False},
    "state": {"type": "string", "required": False},
    "zip": {"type": "string", "required": False},
    "latitude": {"type": "float", "required": False},
    "longitude": {"type": "float", "required": False},
    "hours": {"type": "string", "required": False},
    "category": {"type": "string", "required": False},
    "description": {"type": "string", "required": False},
}


async def create_field_mapping(
    session_id: str, source_field: str, target_field: str, transform_rule: Dict = None
) -> str:
    """Create a field mapping."""
    mapping_id = gen_id("map-")
    await execute(
        """INSERT INTO peec_field_mappings
           (id, session_id, source_field, target_field, transform_rule)
           VALUES (?, ?, ?, ?, ?)""",
        (mapping_id, session_id, source_field, target_field, to_json(transform_rule or {})),
    )
    return mapping_id


async def auto_map_fields(session_id: str, csv_headers: List[str]) -> Dict:
    """Auto-detect and create field mappings."""
    mappings = {}
    unmapped = []
    
    for header in csv_headers:
        # Try exact match first
        if header.lower() in PEEC_FIELD_SCHEMA:
            target = header.lower()
        # Try fuzzy match
        elif any(kw in header.lower() for kw in ["name", "business", "company"]):
            target = "business_name"
        elif any(kw in header.lower() for kw in ["phone", "tel", "mobile"]):
            target = "phone"
        elif any(kw in header.lower() for kw in ["email", "mail"]):
            target = "email"
        elif any(kw in header.lower() for kw in ["website", "url", "site", "web"]):
            target = "website"
        elif any(kw in header.lower() for kw in ["address", "street", "location"]):
            target = "address"
        elif any(kw in header.lower() for kw in ["city", "town"]):
            target = "city"
        elif any(kw in header.lower() for kw in ["state", "province", "region"]):
            target = "state"
        elif any(kw in header.lower() for kw in ["zip", "postal", "postcode"]):
            target = "zip"
        elif any(kw in header.lower() for kw in ["lat", "latitude"]):
            target = "latitude"
        elif any(kw in header.lower() for kw in ["lon", "lng", "longitude"]):
            target = "longitude"
        elif any(kw in header.lower() for kw in ["hours", "hours_of_operation"]):
            target = "hours"
        elif any(kw in header.lower() for kw in ["category", "type", "industry"]):
            target = "category"
        elif any(kw in header.lower() for kw in ["description", "desc", "bio"]):
            target = "description"
        else:
            unmapped.append(header)
            continue
        
        mapping_id = await create_field_mapping(session_id, header, target)
        mappings[header] = target
    
    return {"mappings": mappings, "unmapped": unmapped}


# ═══════════════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════════════

async def validate_import_records(
    session_id: str, rows: List[Dict], mappings: Dict[str, str]
) -> Tuple[int, int, List[Dict]]:
    """Validate import records against schema."""
    pass_count = 0
    fail_count = 0
    validation_issues = []
    
    for idx, row in enumerate(rows):
        row_issues = []
        
        for source_field, target_field in mappings.items():
            value = row.get(source_field, "").strip()
            schema = PEEC_FIELD_SCHEMA.get(target_field, {})
            
            # Required field check
            if schema.get("required") and not value:
                row_issues.append({
                    "field": target_field,
                    "type": "required",
                    "status": "fail",
                    "message": f"{target_field} is required",
                })
                fail_count += 1
                continue
            
            if not value:
                continue
            
            # Format validation
            field_type = schema.get("type", "string")
            if field_type == "email" and not is_email(value):
                row_issues.append({
                    "field": target_field,
                    "type": "format",
                    "status": "warn",
                    "message": f"Invalid email format: {value}",
                })
            elif field_type == "phone" and not is_phone(value):
                row_issues.append({
                    "field": target_field,
                    "type": "format",
                    "status": "warn",
                    "message": f"Invalid phone format: {value}",
                })
            elif field_type == "url" and not is_url(value):
                row_issues.append({
                    "field": target_field,
                    "type": "format",
                    "status": "warn",
                    "message": f"Invalid URL format: {value}",
                })
            elif field_type in ["float", "integer"]:
                try:
                    float(value)
                except:
                    row_issues.append({
                        "field": target_field,
                        "type": "format",
                        "status": "fail",
                        "message": f"Expected numeric value: {value}",
                    })
                    fail_count += 1
        
        if not row_issues:
            pass_count += 1
        else:
            validation_issues.extend([{**issue, "record_index": idx} for issue in row_issues])
    
    # Store validation results
    for issue in validation_issues:
        await execute(
            """INSERT INTO peec_validation_results
               (id, session_id, record_index, field_name, validation_type, status, message)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                gen_id("val-"),
                session_id,
                issue["record_index"],
                issue["field"],
                issue["type"],
                issue["status"],
                issue["message"],
            ),
        )
    
    return pass_count, fail_count, validation_issues


# ═══════════════════════════════════════════════════════════════
# IMPORT EXECUTION & SNAPSHOTS
# ═══════════════════════════════════════════════════════════════

async def capture_before_snapshot(project_id: str) -> Dict:
    """Capture project state before import."""
    entities = await fetch_all(
        "SELECT COUNT(*) as count FROM entity_onboarding WHERE project_id = ?",
        (project_id,),
    )
    audits = await fetch_all(
        "SELECT COUNT(*) as count FROM geo_audits WHERE project_id = ?",
        (project_id,),
    )
    return {
        "timestamp": datetime.now().isoformat(),
        "entity_count": entities[0]["count"] if entities else 0,
        "audit_count": audits[0]["count"] if audits else 0,
    }


async def execute_import(
    session_id: str, batch_id: str, rows: List[Dict], mappings: Dict[str, str]
) -> Dict:
    """Execute the import and store records."""
    before_snap = await capture_before_snapshot(
        (await fetch_one("SELECT project_id FROM peec_import_sessions WHERE id = ?", (session_id,)))["project_id"]
    )
    
    imported = 0
    failed = 0
    
    for idx, row in enumerate(rows):
        record_id = gen_id("rec-")
        
        # Map fields
        mapped = {}
        for source_field, target_field in mappings.items():
            value = row.get(source_field, "").strip()
            if value:
                mapped[target_field] = value
        
        # Store record
        try:
            await execute(
                """INSERT INTO peec_imported_records
                   (id, session_id, batch_id, record_index, source_data, mapped_data, validated_data, import_status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record_id,
                    session_id,
                    batch_id,
                    idx,
                    to_json(row),
                    to_json(mapped),
                    to_json(mapped),  # Simplified validation
                    "imported",
                ),
            )
            imported += 1
        except Exception as e:
            failed += 1
            await execute(
                """INSERT INTO peec_imported_records
                   (id, session_id, batch_id, record_index, source_data, mapped_data, validated_data, import_status, error_msg)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    record_id,
                    session_id,
                    batch_id,
                    idx,
                    to_json(row),
                    to_json(mapped),
                    to_json({}),
                    "failed",
                    str(e),
                ),
            )
    
    after_snap = await capture_before_snapshot(
        (await fetch_one("SELECT project_id FROM peec_import_sessions WHERE id = ?", (session_id,)))["project_id"]
    )
    
    return {
        "imported": imported,
        "failed": failed,
        "before_snapshot": before_snap,
        "after_snapshot": after_snap,
    }


async def list_imported_records(session_id: str, batch_id: str = "") -> List[Dict]:
    """List imported records from a session."""
    where = "session_id = ?"
    params = [session_id]
    if batch_id:
        where += " AND batch_id = ?"
        params.append(batch_id)
    
    records = await fetch_all(
        f"SELECT * FROM peec_imported_records WHERE {where} ORDER BY record_index",
        tuple(params),
    )
    
    for r in records:
        r["source_data"] = from_json(r.get("source_data", "{}"), {})
        r["mapped_data"] = from_json(r.get("mapped_data", "{}"), {})
        r["validated_data"] = from_json(r.get("validated_data", "{}"), {})
    
    return records
