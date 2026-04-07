"""
Momentus AI — Database Layer
Full SQLite schema with async access via aiosqlite.
Every table the engine needs for traceable, versioned operation.
"""
from __future__ import annotations

import aiosqlite
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from .config import settings

DB_PATH = settings.db_path

# ═══════════════════════════════════════════════════════════════
# SCHEMA — executed on first run
# ═══════════════════════════════════════════════════════════════
SCHEMA = """
-- ─── Projects ───
CREATE TABLE IF NOT EXISTS projects (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    config          TEXT DEFAULT '{}',   -- JSON
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

-- ─── Peec raw records (immutable import log) ───
CREATE TABLE IF NOT EXISTS peec_records (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL REFERENCES projects(id),
    url             TEXT NOT NULL,
    title           TEXT DEFAULT '',
    usage_count     INTEGER DEFAULT 0,
    citation_count  INTEGER DEFAULT 0,
    citation_rate   REAL DEFAULT 0.0,
    retrievals      INTEGER DEFAULT 0,
    topic           TEXT DEFAULT 'Uncategorized',
    tags            TEXT DEFAULT '[]',     -- JSON array
    model_source    TEXT DEFAULT 'Other',
    raw_data        TEXT DEFAULT '{}',     -- full original row as JSON
    import_batch_id TEXT,
    imported_at     TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_peec_project ON peec_records(project_id);
CREATE INDEX IF NOT EXISTS idx_peec_topic ON peec_records(topic);
CREATE INDEX IF NOT EXISTS idx_peec_model ON peec_records(model_source);
CREATE INDEX IF NOT EXISTS idx_peec_url ON peec_records(url);

-- ─── Deduplicated source URLs ───
CREATE TABLE IF NOT EXISTS sources (
    id                  TEXT PRIMARY KEY,
    project_id          TEXT NOT NULL REFERENCES projects(id),
    url                 TEXT NOT NULL,
    title               TEXT DEFAULT '',
    total_citation_count INTEGER DEFAULT 0,
    max_citation_rate   REAL DEFAULT 0.0,
    topics              TEXT DEFAULT '[]',
    model_sources       TEXT DEFAULT '[]',
    scrape_status       TEXT DEFAULT 'pending',  -- pending | scraped | failed | skipped
    scraped_at          TEXT,
    created_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(project_id, url)
);
CREATE INDEX IF NOT EXISTS idx_sources_project ON sources(project_id);
CREATE INDEX IF NOT EXISTS idx_sources_status ON sources(scrape_status);

-- ─── Scraped content (structured extraction) ───
CREATE TABLE IF NOT EXISTS scraped_content (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    url             TEXT NOT NULL,
    title           TEXT DEFAULT '',
    headings        TEXT DEFAULT '[]',    -- JSON: [{level:int, text:str}]
    body_text       TEXT DEFAULT '',
    lists           TEXT DEFAULT '[]',    -- JSON: [{type:"ul"|"ol", items:[str]}]
    faqs            TEXT DEFAULT '[]',    -- JSON: [{question:str, answer:str}]
    meta_description TEXT DEFAULT '',
    schema_markup   TEXT DEFAULT '{}',
    word_count      INTEGER DEFAULT 0,
    raw_html_hash   TEXT DEFAULT '',      -- for change detection
    scraped_at      TEXT DEFAULT (datetime('now')),
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_scraped_source ON scraped_content(source_id);

-- ─── Topic clusters ───
CREATE TABLE IF NOT EXISTS clusters (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL REFERENCES projects(id),
    name            TEXT NOT NULL,
    topics          TEXT DEFAULT '[]',
    tags            TEXT DEFAULT '[]',
    prompt_count    INTEGER DEFAULT 0,
    url_count       INTEGER DEFAULT 0,
    avg_citation_rate REAL DEFAULT 0.0,
    total_citations INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_clusters_project ON clusters(project_id);

-- ─── Cluster ↔ Record mapping ───
CREATE TABLE IF NOT EXISTS cluster_records (
    cluster_id      TEXT NOT NULL REFERENCES clusters(id),
    peec_record_id  TEXT NOT NULL REFERENCES peec_records(id),
    PRIMARY KEY (cluster_id, peec_record_id)
);

-- ─── Cluster ↔ Source mapping ───
CREATE TABLE IF NOT EXISTS cluster_sources (
    cluster_id      TEXT NOT NULL REFERENCES clusters(id),
    source_id       TEXT NOT NULL REFERENCES sources(id),
    PRIMARY KEY (cluster_id, source_id)
);

-- ─── Claude source analyses ───
CREATE TABLE IF NOT EXISTS analyses (
    id              TEXT PRIMARY KEY,
    cluster_id      TEXT NOT NULL REFERENCES clusters(id),
    prompt_template TEXT DEFAULT '',
    prompt_type     TEXT DEFAULT 'blog',  -- blog | product | faq
    model           TEXT DEFAULT '',
    temperature     REAL DEFAULT 0.7,
    result          TEXT DEFAULT '',
    tokens_input    INTEGER DEFAULT 0,
    tokens_output   INTEGER DEFAULT 0,
    cost            REAL DEFAULT 0.0,
    cached_tokens   INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_analyses_cluster ON analyses(cluster_id);

-- ─── Content briefs ───
CREATE TABLE IF NOT EXISTS briefs (
    id              TEXT PRIMARY KEY,
    cluster_id      TEXT NOT NULL REFERENCES clusters(id),
    analysis_id     TEXT REFERENCES analyses(id),
    content         TEXT DEFAULT '',
    model           TEXT DEFAULT '',
    tokens_input    INTEGER DEFAULT 0,
    tokens_output   INTEGER DEFAULT 0,
    cost            REAL DEFAULT 0.0,
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_briefs_cluster ON briefs(cluster_id);

-- ─── Generated drafts with versioning ───
CREATE TABLE IF NOT EXISTS drafts (
    id              TEXT PRIMARY KEY,
    cluster_id      TEXT NOT NULL REFERENCES clusters(id),
    brief_id        TEXT REFERENCES briefs(id),
    version         INTEGER DEFAULT 1,
    template_type   TEXT DEFAULT 'blog',
    content         TEXT DEFAULT '',
    word_count      INTEGER DEFAULT 0,
    model           TEXT DEFAULT '',
    temperature     REAL DEFAULT 0.7,
    tokens_input    INTEGER DEFAULT 0,
    tokens_output   INTEGER DEFAULT 0,
    cost            REAL DEFAULT 0.0,
    status          TEXT DEFAULT 'pending',  -- pending | approved | rejected | revision
    reviewer_notes  TEXT DEFAULT '',
    reviewed_at     TEXT,
    parent_draft_id TEXT REFERENCES drafts(id),
    -- snapshot of what produced this draft
    source_snapshot TEXT DEFAULT '{}',   -- JSON: which sources, analysis, brief
    prompt_snapshot TEXT DEFAULT '',     -- exact prompt sent to Claude
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_drafts_cluster ON drafts(cluster_id);
CREATE INDEX IF NOT EXISTS idx_drafts_status ON drafts(status);

-- ─── Export / publication records ───
CREATE TABLE IF NOT EXISTS exports (
    id              TEXT PRIMARY KEY,
    draft_id        TEXT NOT NULL REFERENCES drafts(id),
    format          TEXT DEFAULT 'markdown',
    content         TEXT DEFAULT '',
    published_to    TEXT DEFAULT '',       -- wordpress | webflow | manual
    published_url   TEXT DEFAULT '',
    cms_post_id     TEXT DEFAULT '',
    published_at    TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_exports_draft ON exports(draft_id);

-- ─── Peec feedback measurements (re-measurement loop) ───
CREATE TABLE IF NOT EXISTS measurements (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL REFERENCES projects(id),
    url             TEXT NOT NULL,
    citation_count  INTEGER DEFAULT 0,
    citation_rate   REAL DEFAULT 0.0,
    visibility      REAL DEFAULT 0.0,
    position        REAL DEFAULT 0.0,
    sentiment       TEXT DEFAULT '',
    model_source    TEXT DEFAULT '',
    brand_mentions  INTEGER DEFAULT 0,
    draft_id        TEXT REFERENCES drafts(id),
    measured_at     TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_measurements_project ON measurements(project_id);
CREATE INDEX IF NOT EXISTS idx_measurements_url ON measurements(url);
CREATE INDEX IF NOT EXISTS idx_measurements_draft ON measurements(draft_id);

-- ─── Token usage tracking (cost control) ───
CREATE TABLE IF NOT EXISTS token_usage (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    model           TEXT DEFAULT '',
    tokens_input    INTEGER DEFAULT 0,
    tokens_output   INTEGER DEFAULT 0,
    cached_tokens   INTEGER DEFAULT 0,
    cost            REAL DEFAULT 0.0,
    operation       TEXT DEFAULT '',       -- analysis | brief | generation | batch
    reference_id    TEXT DEFAULT '',       -- the analysis/brief/draft id
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_usage_project ON token_usage(project_id);
CREATE INDEX IF NOT EXISTS idx_usage_date ON token_usage(created_at);

-- ─── Batch jobs ───
CREATE TABLE IF NOT EXISTS batch_jobs (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    job_type        TEXT DEFAULT '',       -- scrape | analyze | generate
    status          TEXT DEFAULT 'queued', -- queued | running | completed | failed | cancelled
    total_items     INTEGER DEFAULT 0,
    completed_items INTEGER DEFAULT 0,
    failed_items    INTEGER DEFAULT 0,
    results         TEXT DEFAULT '{}',     -- JSON summary
    error           TEXT DEFAULT '',
    anthropic_batch_id TEXT DEFAULT '',    -- Message Batches API id
    started_at      TEXT,
    completed_at    TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_batch_project ON batch_jobs(project_id);
CREATE INDEX IF NOT EXISTS idx_batch_status ON batch_jobs(status);

-- ─── Success metrics definitions ───
CREATE TABLE IF NOT EXISTS success_metrics (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL REFERENCES projects(id),
    metric_name     TEXT NOT NULL,         -- citation_count | citation_rate | visibility | position | brand_mentions
    target_value    REAL DEFAULT 0.0,
    baseline_value  REAL DEFAULT 0.0,
    current_value   REAL DEFAULT 0.0,
    direction       TEXT DEFAULT 'increase', -- increase | decrease
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

-- ─── Prompt template versions ───
CREATE TABLE IF NOT EXISTS prompt_templates (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,         -- source_analysis | brief_generation | content_generation
    sub_type        TEXT DEFAULT 'blog',   -- blog | product | faq | landing | kb
    template        TEXT NOT NULL,
    version         INTEGER DEFAULT 1,
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_prompts_project ON prompt_templates(project_id);
"""


# ═══════════════════════════════════════════════════════════════
# DATABASE ACCESS
# ═══════════════════════════════════════════════════════════════

async def get_db() -> aiosqlite.Connection:
    """Get a database connection with WAL mode and foreign keys."""
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def init_db():
    """Initialize database schema. Safe to call multiple times."""
    db = await get_db()
    try:
        await db.executescript(SCHEMA)
        # Ensure default project exists
        await db.execute(
            "INSERT OR IGNORE INTO projects (id, name) VALUES (?, ?)",
            (settings.project_id, settings.project_name),
        )
        await db.commit()
    finally:
        await db.close()


def gen_id(prefix: str = "") -> str:
    """Generate a prefixed UUID."""
    uid = uuid4().hex[:12]
    return f"{prefix}{uid}" if prefix else uid


# ═══════════════════════════════════════════════════════════════
# QUERY HELPERS
# ═══════════════════════════════════════════════════════════════

async def fetch_one(query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    db = await get_db()
    try:
        cursor = await db.execute(query, params)
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


async def fetch_all(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    db = await get_db()
    try:
        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def execute(query: str, params: tuple = ()) -> int:
    db = await get_db()
    try:
        cursor = await db.execute(query, params)
        await db.commit()
        return cursor.rowcount
    finally:
        await db.close()


async def execute_many(query: str, params_list: List[tuple]) -> int:
    db = await get_db()
    try:
        await db.executemany(query, params_list)
        await db.commit()
        return len(params_list)
    finally:
        await db.close()


async def insert_returning_id(query: str, params: tuple) -> str:
    """Insert and return the last row id (for TEXT PKs, pass id in params)."""
    db = await get_db()
    try:
        await db.execute(query, params)
        await db.commit()
        return params[0]  # first param is always our generated id
    finally:
        await db.close()


# ═══════════════════════════════════════════════════════════════
# JSON HELPERS
# ═══════════════════════════════════════════════════════════════

def to_json(obj: Any) -> str:
    """Safely serialize to JSON string for storage."""
    if isinstance(obj, str):
        return obj
    return json.dumps(obj, ensure_ascii=False, default=str)


def from_json(text: str, default: Any = None) -> Any:
    """Safely deserialize JSON string from storage."""
    if not text:
        return default if default is not None else {}
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return default if default is not None else {}


# ═══════════════════════════════════════════════════════════════
# COST TRACKING HELPERS
# ═══════════════════════════════════════════════════════════════

async def get_daily_cost(project_id: str) -> float:
    """Get total Claude API cost for today."""
    row = await fetch_one(
        "SELECT COALESCE(SUM(cost), 0) as total FROM token_usage "
        "WHERE project_id = ? AND date(created_at) = date('now')",
        (project_id,),
    )
    return row["total"] if row else 0.0


async def get_monthly_cost(project_id: str) -> float:
    """Get total Claude API cost for this month."""
    row = await fetch_one(
        "SELECT COALESCE(SUM(cost), 0) as total FROM token_usage "
        "WHERE project_id = ? AND strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')",
        (project_id,),
    )
    return row["total"] if row else 0.0


async def check_cost_limit(project_id: str) -> Dict[str, Any]:
    """Check if we're within daily and monthly cost limits."""
    daily = await get_daily_cost(project_id)
    monthly = await get_monthly_cost(project_id)
    return {
        "daily_cost": round(daily, 4),
        "monthly_cost": round(monthly, 4),
        "daily_limit": settings.claude_daily_cost_limit,
        "monthly_limit": settings.claude_monthly_cost_limit,
        "daily_ok": daily < settings.claude_daily_cost_limit,
        "monthly_ok": monthly < settings.claude_monthly_cost_limit,
        "allowed": daily < settings.claude_daily_cost_limit and monthly < settings.claude_monthly_cost_limit,
    }


async def record_token_usage(
    project_id: str,
    model: str,
    tokens_input: int,
    tokens_output: int,
    cached_tokens: int,
    cost: float,
    operation: str,
    reference_id: str = "",
):
    """Record a Claude API call's token usage and cost."""
    await execute(
        "INSERT INTO token_usage (id, project_id, model, tokens_input, tokens_output, "
        "cached_tokens, cost, operation, reference_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (gen_id("tu-"), project_id, model, tokens_input, tokens_output, cached_tokens, cost, operation, reference_id),
    )
