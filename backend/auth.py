"""
Momentus AI — Authentication & Authorization
JWT-based auth with role-based access control.
Roles: superadmin, admin, editor, reviewer, client (read-only).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from .config import settings
from .database import execute, fetch_all, fetch_one, gen_id

# ═══════════════════════════════════════════════════════════════
# SCHEMA ADDITIONS — run alongside main schema
# ═══════════════════════════════════════════════════════════════
AUTH_SCHEMA = """
-- ─── Users ───
CREATE TABLE IF NOT EXISTS users (
    id              TEXT PRIMARY KEY,
    email           TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL DEFAULT '',
    password_hash   TEXT NOT NULL,
    salt            TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'editor',  -- superadmin | admin | editor | reviewer | client
    avatar_url      TEXT DEFAULT '',
    is_active       INTEGER DEFAULT 1,
    last_login      TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

-- ─── Workspaces (multi-client) ───
CREATE TABLE IF NOT EXISTS workspaces (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL UNIQUE,
    brand_name      TEXT DEFAULT '',
    domains         TEXT DEFAULT '[]',        -- JSON array of website domains
    target_countries TEXT DEFAULT '[]',       -- JSON: ["US","UK","DE"]
    target_languages TEXT DEFAULT '[]',       -- JSON: ["en","de","es"]
    target_models   TEXT DEFAULT '[]',        -- JSON: ["ChatGPT","Perplexity","Gemini","Claude"]
    brand_voice     TEXT DEFAULT '',          -- brand voice rules (markdown)
    compliance_rules TEXT DEFAULT '',         -- forbidden claims / compliance rules
    logo_url        TEXT DEFAULT '',
    color_primary   TEXT DEFAULT '#2563EB',
    color_accent    TEXT DEFAULT '#10B981',
    settings        TEXT DEFAULT '{}',        -- JSON: additional workspace settings
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_workspaces_slug ON workspaces(slug);

-- ─── User ↔ Workspace membership ───
CREATE TABLE IF NOT EXISTS workspace_members (
    user_id         TEXT NOT NULL REFERENCES users(id),
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),
    role            TEXT NOT NULL DEFAULT 'editor',  -- admin | editor | reviewer | client
    joined_at       TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (user_id, workspace_id)
);

-- ─── Campaigns (within workspace) ───
CREATE TABLE IF NOT EXISTS campaigns (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),
    name            TEXT NOT NULL,
    description     TEXT DEFAULT '',
    status          TEXT DEFAULT 'active',    -- active | paused | completed | archived
    start_date      TEXT,
    end_date        TEXT,
    goals           TEXT DEFAULT '{}',        -- JSON: target metrics
    created_by      TEXT REFERENCES users(id),
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_campaigns_workspace ON campaigns(workspace_id);

-- ─── Sessions (JWT token tracking) ───
CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL REFERENCES users(id),
    token_hash      TEXT NOT NULL,
    expires_at      TEXT NOT NULL,
    ip_address      TEXT DEFAULT '',
    user_agent      TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token_hash);

-- ─── Audit log ───
CREATE TABLE IF NOT EXISTS audit_log (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    workspace_id    TEXT DEFAULT '',
    action          TEXT NOT NULL,           -- login | logout | create | update | delete | export | publish
    resource_type   TEXT DEFAULT '',         -- draft | source | cluster | workspace | user | campaign
    resource_id     TEXT DEFAULT '',
    details         TEXT DEFAULT '{}',       -- JSON
    ip_address      TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_workspace ON audit_log(workspace_id);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);

-- ─── Reporting snapshots ───
CREATE TABLE IF NOT EXISTS report_snapshots (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),
    campaign_id     TEXT REFERENCES campaigns(id),
    period_start    TEXT NOT NULL,
    period_end      TEXT NOT NULL,
    report_type     TEXT DEFAULT 'weekly',    -- daily | weekly | monthly | custom
    metrics         TEXT DEFAULT '{}',        -- JSON: aggregated metrics
    generated_by    TEXT REFERENCES users(id),
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_reports_workspace ON report_snapshots(workspace_id);

-- ─── Scheduled reports ───
CREATE TABLE IF NOT EXISTS scheduled_reports (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),
    frequency       TEXT DEFAULT 'weekly',    -- daily | weekly | monthly
    recipients      TEXT DEFAULT '[]',        -- JSON: email addresses
    report_config   TEXT DEFAULT '{}',        -- JSON: what to include
    last_sent       TEXT,
    next_send       TEXT,
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT DEFAULT (datetime('now'))
);
"""

# ═══════════════════════════════════════════════════════════════
# PASSWORD HASHING (PBKDF2-HMAC-SHA256)
# ═══════════════════════════════════════════════════════════════

def hash_password(password: str, salt: str = None) -> Tuple[str, str]:
    """Hash a password with PBKDF2. Returns (hash, salt)."""
    if not salt:
        salt = secrets.token_hex(32)
    pw_hash = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt.encode("utf-8"), 100_000
    )
    return pw_hash.hex(), salt


def verify_password(password: str, stored_hash: str, salt: str) -> bool:
    """Verify a password against stored hash."""
    computed_hash, _ = hash_password(password, salt)
    return hmac.compare_digest(computed_hash, stored_hash)


# ═══════════════════════════════════════════════════════════════
# JWT-LIKE TOKEN (HMAC-signed, no external dependency)
# ═══════════════════════════════════════════════════════════════

def _get_signing_key() -> str:
    """Get or generate signing key."""
    key = settings.api_secret_key
    if not key:
        key = "momentus-ai-dev-key-change-in-production"
    return key


def create_token(user_id: str, email: str, role: str, hours: int = 24) -> str:
    """Create a signed JWT-like token."""
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "exp": int(time.time()) + (hours * 3600),
        "iat": int(time.time()),
        "jti": secrets.token_hex(16),
    }
    payload_json = json.dumps(payload, separators=(",", ":"))
    import base64
    payload_b64 = base64.urlsafe_b64encode(payload_json.encode()).decode().rstrip("=")
    sig = hmac.new(
        _get_signing_key().encode(), payload_b64.encode(), hashlib.sha256
    ).hexdigest()
    return f"{payload_b64}.{sig}"


def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """Verify and decode a token. Returns payload or None."""
    try:
        parts = token.split(".")
        if len(parts) != 2:
            return None
        payload_b64, sig = parts
        expected_sig = hmac.new(
            _get_signing_key().encode(), payload_b64.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return None
        import base64
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
# USER MANAGEMENT
# ═══════════════════════════════════════════════════════════════

ROLE_HIERARCHY = {
    "superadmin": 100,
    "admin": 80,
    "editor": 60,
    "reviewer": 40,
    "client": 20,
}


def has_permission(user_role: str, required_role: str) -> bool:
    """Check if user role meets minimum required role."""
    return ROLE_HIERARCHY.get(user_role, 0) >= ROLE_HIERARCHY.get(required_role, 0)


async def create_user(
    email: str, password: str, name: str = "", role: str = "editor"
) -> Dict[str, Any]:
    """Create a new user account."""
    existing = await fetch_one("SELECT id FROM users WHERE email = ?", (email,))
    if existing:
        raise ValueError(f"User with email {email} already exists")

    user_id = gen_id("usr-")
    pw_hash, salt = hash_password(password)
    await execute(
        "INSERT INTO users (id, email, name, password_hash, salt, role) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, email, name, pw_hash, salt, role),
    )
    return {"id": user_id, "email": email, "name": name, "role": role}


async def authenticate_user(email: str, password: str) -> Optional[Dict[str, Any]]:
    """Authenticate user by email/password. Returns user dict or None."""
    user = await fetch_one(
        "SELECT id, email, name, password_hash, salt, role, is_active FROM users WHERE email = ?",
        (email,),
    )
    if not user or not user["is_active"]:
        return None
    if not verify_password(password, user["password_hash"], user["salt"]):
        return None

    await execute(
        "UPDATE users SET last_login = datetime('now') WHERE id = ?", (user["id"],)
    )
    return {
        "id": user["id"],
        "email": user["email"],
        "name": user["name"],
        "role": user["role"],
    }


async def get_user_workspaces(user_id: str) -> List[Dict[str, Any]]:
    """Get all workspaces a user belongs to."""
    return await fetch_all(
        """SELECT w.*, wm.role as member_role
           FROM workspaces w
           JOIN workspace_members wm ON w.id = wm.workspace_id
           WHERE wm.user_id = ? AND w.is_active = 1
           ORDER BY w.name""",
        (user_id,),
    )


async def create_workspace(
    name: str, slug: str, brand_name: str = "", created_by: str = ""
) -> Dict[str, Any]:
    """Create a new client workspace."""
    ws_id = gen_id("ws-")
    await execute(
        "INSERT INTO workspaces (id, name, slug, brand_name) VALUES (?, ?, ?, ?)",
        (ws_id, name, slug, brand_name),
    )
    if created_by:
        await execute(
            "INSERT INTO workspace_members (user_id, workspace_id, role) VALUES (?, ?, 'admin')",
            (created_by, ws_id),
        )
    return {"id": ws_id, "name": name, "slug": slug, "brand_name": brand_name}


async def add_workspace_member(
    workspace_id: str, user_id: str, role: str = "editor"
) -> bool:
    """Add a user to a workspace."""
    try:
        await execute(
            "INSERT OR REPLACE INTO workspace_members (user_id, workspace_id, role) VALUES (?, ?, ?)",
            (user_id, workspace_id, role),
        )
        return True
    except Exception:
        return False


async def log_audit(
    user_id: str, action: str, workspace_id: str = "",
    resource_type: str = "", resource_id: str = "",
    details: Dict = None, ip_address: str = ""
):
    """Write an audit log entry."""
    await execute(
        "INSERT INTO audit_log (id, user_id, workspace_id, action, resource_type, resource_id, details, ip_address) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (gen_id("aud-"), user_id, workspace_id, action, resource_type, resource_id,
         json.dumps(details or {}), ip_address),
    )


# ═══════════════════════════════════════════════════════════════
# INIT: create default superadmin on first run
# ═══════════════════════════════════════════════════════════════

async def init_auth():
    """Initialize auth tables and create default superadmin if none exists."""
    from .database import get_db
    db = await get_db()
    await db.executescript(AUTH_SCHEMA)
    await db.commit()

    admin = await fetch_one("SELECT id FROM users WHERE role = 'superadmin' LIMIT 1")
    if not admin:
        await create_user(
            email="admin@momentus.ai",
            password="admin123",
            name="Momentus Admin",
            role="superadmin",
        )
        # Create default workspace
        ws = await create_workspace(
            name="Default Workspace",
            slug="default",
            brand_name="Momentus AI",
        )
        admin = await fetch_one("SELECT id FROM users WHERE role = 'superadmin' LIMIT 1")
        if admin:
            await add_workspace_member(ws["id"], admin["id"], "admin")
