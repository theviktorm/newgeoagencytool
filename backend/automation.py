"""
Momentus AI — Automation & Orchestration Engine
Job queue, scheduled tasks, event-driven automations, notifications,
task management, SOP/playbook system, billing/usage tracking.
"""
from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from uuid import uuid4

from .config import settings
from .database import execute, fetch_all, fetch_one, gen_id, to_json, from_json

logger = logging.getLogger("geo.automation")

# ═══════════════════════════════════════════════════════════════
# SCHEMA — additional tables for orchestration
# ═══════════════════════════════════════════════════════════════
AUTOMATION_SCHEMA = """
-- ─── Job queue (unified) ───
CREATE TABLE IF NOT EXISTS job_queue (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    campaign_id     TEXT DEFAULT '',
    job_type        TEXT NOT NULL,          -- scrape | analyze | generate | publish | report | measure | import | refresh
    priority        INTEGER DEFAULT 5,      -- 1=urgent, 5=normal, 10=low
    status          TEXT DEFAULT 'queued',  -- queued | running | completed | failed | cancelled | retrying
    params          TEXT DEFAULT '{}',      -- JSON: job-specific parameters
    result          TEXT DEFAULT '{}',      -- JSON: output/result data
    error           TEXT DEFAULT '',
    retry_count     INTEGER DEFAULT 0,
    max_retries     INTEGER DEFAULT 3,
    created_by      TEXT DEFAULT '',        -- user id who triggered it
    assigned_to     TEXT DEFAULT '',        -- worker/agent id
    started_at      TEXT,
    completed_at    TEXT,
    scheduled_for   TEXT,                   -- NULL = run immediately, else future datetime
    parent_job_id   TEXT,                   -- for chained jobs
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_jobs_workspace ON job_queue(workspace_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON job_queue(status);
CREATE INDEX IF NOT EXISTS idx_jobs_type ON job_queue(job_type);
CREATE INDEX IF NOT EXISTS idx_jobs_priority ON job_queue(priority);
CREATE INDEX IF NOT EXISTS idx_jobs_scheduled ON job_queue(scheduled_for);

-- ─── Automation rules (event-driven) ───
CREATE TABLE IF NOT EXISTS automation_rules (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    name            TEXT NOT NULL,
    trigger_event   TEXT NOT NULL,          -- csv_uploaded | scrape_complete | analysis_complete | draft_approved | performance_drop | schedule
    trigger_config  TEXT DEFAULT '{}',      -- JSON: event matching config (e.g., schedule cron)
    action_type     TEXT NOT NULL,          -- parse_csv | run_scrape | run_analysis | run_generation | publish | measure | notify | create_job
    action_config   TEXT DEFAULT '{}',      -- JSON: action parameters
    is_active       INTEGER DEFAULT 1,
    last_triggered  TEXT,
    trigger_count   INTEGER DEFAULT 0,
    created_by      TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_rules_workspace ON automation_rules(workspace_id);
CREATE INDEX IF NOT EXISTS idx_rules_event ON automation_rules(trigger_event);

-- ─── Task board (agency operations) ───
CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    campaign_id     TEXT DEFAULT '',
    cluster_id      TEXT DEFAULT '',
    draft_id        TEXT DEFAULT '',
    title           TEXT NOT NULL,
    description     TEXT DEFAULT '',
    task_type       TEXT NOT NULL,          -- analysis | writing | review | publishing | measurement | onboarding | other
    status          TEXT DEFAULT 'todo',    -- todo | in_progress | blocked | review | done | cancelled
    priority        TEXT DEFAULT 'medium',  -- urgent | high | medium | low
    assigned_to     TEXT DEFAULT '',        -- user id
    created_by      TEXT NOT NULL,
    due_date        TEXT,
    completed_at    TEXT,
    tags            TEXT DEFAULT '[]',      -- JSON array
    comments        TEXT DEFAULT '[]',      -- JSON: [{user_id, text, created_at}]
    checklist       TEXT DEFAULT '[]',      -- JSON: [{text, done}]
    sort_order      INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_tasks_workspace ON tasks(workspace_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(assigned_to);
CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type);

-- ─── Notifications ───
CREATE TABLE IF NOT EXISTS notifications (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    workspace_id    TEXT DEFAULT '',
    type            TEXT NOT NULL,          -- info | warning | error | success | task | review | publish | alert
    title           TEXT NOT NULL,
    message         TEXT DEFAULT '',
    link            TEXT DEFAULT '',        -- internal link/route
    is_read         INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id);
CREATE INDEX IF NOT EXISTS idx_notif_read ON notifications(is_read);

-- ─── SOP / Playbook templates ───
CREATE TABLE IF NOT EXISTS playbooks (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT DEFAULT '',        -- empty = global
    name            TEXT NOT NULL,
    description     TEXT DEFAULT '',
    category        TEXT DEFAULT 'general', -- onboarding | analysis | generation | review | publishing | reporting
    steps           TEXT DEFAULT '[]',      -- JSON: [{order, title, description, checklist:[], assignee_role, est_minutes}]
    is_active       INTEGER DEFAULT 1,
    created_by      TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now'))
);

-- ─── Client onboarding state ───
CREATE TABLE IF NOT EXISTS onboarding (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL UNIQUE,
    status          TEXT DEFAULT 'pending', -- pending | in_progress | completed
    domain_info     TEXT DEFAULT '',
    target_topics   TEXT DEFAULT '[]',
    competitors     TEXT DEFAULT '[]',
    brand_voice_notes TEXT DEFAULT '',
    cms_type        TEXT DEFAULT '',        -- wordpress | webflow | shopify | headless | none
    cms_access      INTEGER DEFAULT 0,
    approval_workflow TEXT DEFAULT '',      -- who approves, how many rounds
    notes           TEXT DEFAULT '',
    completed_steps TEXT DEFAULT '[]',      -- JSON: checklist of completed items
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

-- ─── Billing / usage per client ───
CREATE TABLE IF NOT EXISTS billing_records (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    period_start    TEXT NOT NULL,
    period_end      TEXT NOT NULL,
    token_cost      REAL DEFAULT 0.0,      -- Claude API cost
    scrape_count    INTEGER DEFAULT 0,
    content_count   INTEGER DEFAULT 0,     -- drafts produced
    publish_count   INTEGER DEFAULT 0,
    total_cost      REAL DEFAULT 0.0,      -- rollup
    notes           TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_billing_workspace ON billing_records(workspace_id);

-- ─── Competitor tracking ───
CREATE TABLE IF NOT EXISTS competitors (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    domain          TEXT NOT NULL,
    name            TEXT DEFAULT '',
    citation_count  INTEGER DEFAULT 0,
    avg_citation_rate REAL DEFAULT 0.0,
    top_topics      TEXT DEFAULT '[]',     -- JSON
    last_checked    TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(workspace_id, domain)
);
CREATE INDEX IF NOT EXISTS idx_competitors_workspace ON competitors(workspace_id);

-- ─── Content refresh tracking ───
CREATE TABLE IF NOT EXISTS content_assets (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    url             TEXT NOT NULL,
    title           TEXT DEFAULT '',
    content_type    TEXT DEFAULT 'blog',
    publish_date    TEXT,
    last_measured   TEXT,
    current_citation_rate REAL DEFAULT 0.0,
    peak_citation_rate REAL DEFAULT 0.0,
    status          TEXT DEFAULT 'active',  -- active | needs_refresh | refreshing | archived
    refresh_priority REAL DEFAULT 0.0,
    draft_id        TEXT DEFAULT '',        -- linked draft
    cluster_id      TEXT DEFAULT '',
    notes           TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_assets_workspace ON content_assets(workspace_id);
CREATE INDEX IF NOT EXISTS idx_assets_status ON content_assets(status);

-- ─── Recommendation queue ───
CREATE TABLE IF NOT EXISTS recommendations (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    rec_type        TEXT NOT NULL,          -- new_topic | refresh | gap | opportunity | competitor_alert
    title           TEXT NOT NULL,
    description     TEXT DEFAULT '',
    priority_score  REAL DEFAULT 0.0,      -- 0-100, higher = more urgent
    cluster_id      TEXT DEFAULT '',
    source_data     TEXT DEFAULT '{}',      -- JSON: supporting data
    status          TEXT DEFAULT 'new',    -- new | accepted | dismissed | completed
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_rec_workspace ON recommendations(workspace_id);
CREATE INDEX IF NOT EXISTS idx_rec_status ON recommendations(status);
"""


# ═══════════════════════════════════════════════════════════════
# INIT
# ═══════════════════════════════════════════════════════════════

async def init_automation():
    """Initialize automation tables."""
    from .database import get_db
    db = await get_db()
    try:
        await db.executescript(AUTOMATION_SCHEMA)
        await db.commit()
        logger.info("Automation tables initialized")
    finally:
        await db.close()
    # Seed default playbooks
    existing = await fetch_one("SELECT id FROM playbooks LIMIT 1")
    if not existing:
        await seed_default_playbooks()


# ═══════════════════════════════════════════════════════════════
# JOB QUEUE ENGINE
# ═══════════════════════════════════════════════════════════════

async def create_job(
    workspace_id: str, job_type: str, params: Dict = None,
    priority: int = 5, created_by: str = "", campaign_id: str = "",
    scheduled_for: str = None, parent_job_id: str = None,
) -> str:
    """Create a new job in the queue."""
    job_id = gen_id("job-")
    await execute(
        """INSERT INTO job_queue
           (id, workspace_id, campaign_id, job_type, priority, params, created_by, scheduled_for, parent_job_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (job_id, workspace_id, campaign_id, job_type, priority,
         to_json(params or {}), created_by, scheduled_for, parent_job_id),
    )
    logger.info("Job created: %s type=%s ws=%s", job_id, job_type, workspace_id)

    # Fire automation rules for job creation
    await fire_event("job_created", workspace_id, {"job_id": job_id, "job_type": job_type})

    return job_id


async def get_next_job(job_types: List[str] = None) -> Optional[Dict]:
    """Get the next queued job (highest priority, oldest first)."""
    type_filter = ""
    params = []
    if job_types:
        placeholders = ",".join("?" for _ in job_types)
        type_filter = f"AND job_type IN ({placeholders})"
        params = list(job_types)

    # Also check scheduled_for
    params.append(datetime.utcnow().isoformat())
    job = await fetch_one(
        f"""SELECT * FROM job_queue
            WHERE status = 'queued'
            {type_filter}
            AND (scheduled_for IS NULL OR scheduled_for <= ?)
            ORDER BY priority ASC, created_at ASC
            LIMIT 1""",
        tuple(params),
    )
    return job


async def claim_job(job_id: str, worker_id: str = "main") -> bool:
    """Claim a job for processing."""
    rows = await execute(
        "UPDATE job_queue SET status = 'running', assigned_to = ?, started_at = datetime('now') "
        "WHERE id = ? AND status = 'queued'",
        (worker_id, job_id),
    )
    return rows > 0


async def complete_job(job_id: str, result: Dict = None):
    """Mark a job as completed."""
    await execute(
        "UPDATE job_queue SET status = 'completed', result = ?, completed_at = datetime('now') WHERE id = ?",
        (to_json(result or {}), job_id),
    )
    job = await fetch_one("SELECT * FROM job_queue WHERE id = ?", (job_id,))
    if job:
        await fire_event(f"{job['job_type']}_complete", job["workspace_id"],
                         {"job_id": job_id, "result": result or {}})


async def fail_job(job_id: str, error: str):
    """Mark a job as failed, retry if possible."""
    job = await fetch_one("SELECT * FROM job_queue WHERE id = ?", (job_id,))
    if not job:
        return

    if job["retry_count"] < job["max_retries"]:
        await execute(
            "UPDATE job_queue SET status = 'retrying', retry_count = retry_count + 1, error = ? WHERE id = ?",
            (error, job_id),
        )
        # Re-queue after delay
        await execute(
            "UPDATE job_queue SET status = 'queued', scheduled_for = datetime('now', '+1 minutes') WHERE id = ?",
            (job_id,),
        )
        logger.warning("Job %s failed, retrying (%d/%d): %s", job_id, job["retry_count"] + 1, job["max_retries"], error)
    else:
        await execute(
            "UPDATE job_queue SET status = 'failed', error = ?, completed_at = datetime('now') WHERE id = ?",
            (error, job_id),
        )
        logger.error("Job %s permanently failed: %s", job_id, error)
        # Create notification for failed job
        if job.get("created_by"):
            await create_notification(
                job["created_by"], job["workspace_id"], "error",
                f"Job failed: {job['job_type']}", error,
            )


async def get_job_stats(workspace_id: str) -> Dict:
    """Get job queue statistics."""
    stats = await fetch_all(
        "SELECT status, COUNT(*) as cnt FROM job_queue WHERE workspace_id = ? GROUP BY status",
        (workspace_id,),
    )
    by_type = await fetch_all(
        "SELECT job_type, COUNT(*) as cnt FROM job_queue WHERE workspace_id = ? AND status IN ('queued','running') GROUP BY job_type",
        (workspace_id,),
    )
    return {
        "status_counts": {r["status"]: r["cnt"] for r in stats},
        "active_by_type": {r["job_type"]: r["cnt"] for r in by_type},
    }


# ═══════════════════════════════════════════════════════════════
# EVENT-DRIVEN AUTOMATION
# ═══════════════════════════════════════════════════════════════

async def fire_event(event_name: str, workspace_id: str, event_data: Dict = None):
    """Fire an event and trigger matching automation rules."""
    rules = await fetch_all(
        "SELECT * FROM automation_rules WHERE workspace_id = ? AND trigger_event = ? AND is_active = 1",
        (workspace_id, event_name),
    )
    for rule in rules:
        try:
            action_config = from_json(rule.get("action_config", "{}"))
            action_type = rule["action_type"]

            # Execute the action
            if action_type == "create_job":
                job_type = action_config.get("job_type", "")
                if job_type:
                    await create_job(workspace_id, job_type, params={**action_config, **(event_data or {})})

            elif action_type == "notify":
                user_id = action_config.get("user_id", "")
                msg = action_config.get("message", f"Event: {event_name}")
                if user_id:
                    await create_notification(user_id, workspace_id, "info", event_name, msg)

            # Update trigger stats
            await execute(
                "UPDATE automation_rules SET last_triggered = datetime('now'), trigger_count = trigger_count + 1 WHERE id = ?",
                (rule["id"],),
            )
            logger.info("Automation rule %s triggered by %s", rule["name"], event_name)

        except Exception as e:
            logger.error("Automation rule %s failed: %s", rule["id"], e)


async def create_automation_rule(
    workspace_id: str, name: str, trigger_event: str,
    action_type: str, trigger_config: Dict = None,
    action_config: Dict = None, created_by: str = "",
) -> str:
    """Create a new automation rule."""
    rule_id = gen_id("rule-")
    await execute(
        "INSERT INTO automation_rules (id, workspace_id, name, trigger_event, trigger_config, action_type, action_config, created_by) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (rule_id, workspace_id, name, trigger_event,
         to_json(trigger_config or {}), action_type, to_json(action_config or {}), created_by),
    )
    return rule_id


# ═══════════════════════════════════════════════════════════════
# TASK MANAGEMENT
# ═══════════════════════════════════════════════════════════════

async def create_task(
    workspace_id: str, title: str, task_type: str,
    created_by: str, description: str = "", priority: str = "medium",
    assigned_to: str = "", campaign_id: str = "", cluster_id: str = "",
    draft_id: str = "", due_date: str = None, checklist: List = None,
) -> str:
    """Create a task on the board."""
    task_id = gen_id("task-")
    await execute(
        """INSERT INTO tasks
           (id, workspace_id, campaign_id, cluster_id, draft_id, title, description,
            task_type, priority, assigned_to, created_by, due_date, checklist)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (task_id, workspace_id, campaign_id, cluster_id, draft_id,
         title, description, task_type, priority, assigned_to, created_by,
         due_date, to_json(checklist or [])),
    )
    # Notify assignee
    if assigned_to:
        await create_notification(
            assigned_to, workspace_id, "task",
            f"New task: {title}", description,
        )
    return task_id


async def update_task_status(task_id: str, status: str, user_id: str = ""):
    """Update a task's status."""
    completed_at = "datetime('now')" if status == "done" else "NULL"
    await execute(
        f"UPDATE tasks SET status = ?, completed_at = {completed_at}, updated_at = datetime('now') WHERE id = ?",
        (status, task_id),
    )
    task = await fetch_one("SELECT * FROM tasks WHERE id = ?", (task_id,))
    if task and task.get("assigned_to") and task["assigned_to"] != user_id:
        await create_notification(
            task["assigned_to"], task["workspace_id"], "info",
            f"Task updated: {task['title']}", f"Status changed to {status}",
        )


async def add_task_comment(task_id: str, user_id: str, text: str):
    """Add a comment to a task."""
    task = await fetch_one("SELECT comments FROM tasks WHERE id = ?", (task_id,))
    if not task:
        return
    comments = from_json(task["comments"], [])
    comments.append({
        "user_id": user_id,
        "text": text,
        "created_at": datetime.utcnow().isoformat(),
    })
    await execute(
        "UPDATE tasks SET comments = ?, updated_at = datetime('now') WHERE id = ?",
        (to_json(comments), task_id),
    )


async def get_task_board(workspace_id: str, filters: Dict = None) -> Dict:
    """Get tasks organized by status columns (kanban-style)."""
    where_clauses = ["workspace_id = ?"]
    params = [workspace_id]

    if filters:
        if filters.get("campaign_id"):
            where_clauses.append("campaign_id = ?")
            params.append(filters["campaign_id"])
        if filters.get("assigned_to"):
            where_clauses.append("assigned_to = ?")
            params.append(filters["assigned_to"])
        if filters.get("task_type"):
            where_clauses.append("task_type = ?")
            params.append(filters["task_type"])

    where = " AND ".join(where_clauses)
    tasks = await fetch_all(
        f"SELECT * FROM tasks WHERE {where} ORDER BY sort_order, priority, created_at DESC",
        tuple(params),
    )

    board = {"todo": [], "in_progress": [], "blocked": [], "review": [], "done": []}
    for t in tasks:
        t["comments"] = from_json(t.get("comments", "[]"), [])
        t["checklist"] = from_json(t.get("checklist", "[]"), [])
        t["tags"] = from_json(t.get("tags", "[]"), [])
        col = t.get("status", "todo")
        if col in board:
            board[col].append(t)
        else:
            board["todo"].append(t)

    return board


# ═══════════════════════════════════════════════════════════════
# NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════

async def create_notification(
    user_id: str, workspace_id: str, ntype: str,
    title: str, message: str = "", link: str = "",
):
    """Create a notification for a user."""
    await execute(
        "INSERT INTO notifications (id, user_id, workspace_id, type, title, message, link) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (gen_id("ntf-"), user_id, workspace_id, ntype, title, message, link),
    )


async def get_notifications(user_id: str, unread_only: bool = False, limit: int = 50) -> List[Dict]:
    """Get notifications for a user."""
    filter_clause = "AND is_read = 0" if unread_only else ""
    return await fetch_all(
        f"SELECT * FROM notifications WHERE user_id = ? {filter_clause} ORDER BY created_at DESC LIMIT ?",
        (user_id, limit),
    )


async def mark_notification_read(notification_id: str):
    await execute("UPDATE notifications SET is_read = 1 WHERE id = ?", (notification_id,))


async def mark_all_notifications_read(user_id: str):
    await execute("UPDATE notifications SET is_read = 1 WHERE user_id = ? AND is_read = 0", (user_id,))


# ═══════════════════════════════════════════════════════════════
# CLIENT ONBOARDING
# ═══════════════════════════════════════════════════════════════

async def get_onboarding(workspace_id: str) -> Optional[Dict]:
    ob = await fetch_one("SELECT * FROM onboarding WHERE workspace_id = ?", (workspace_id,))
    if ob:
        ob["target_topics"] = from_json(ob.get("target_topics", "[]"), [])
        ob["competitors"] = from_json(ob.get("competitors", "[]"), [])
        ob["completed_steps"] = from_json(ob.get("completed_steps", "[]"), [])
    return ob


async def update_onboarding(workspace_id: str, data: Dict) -> str:
    """Create or update onboarding state."""
    existing = await fetch_one("SELECT id FROM onboarding WHERE workspace_id = ?", (workspace_id,))
    if existing:
        fields = []
        params = []
        for key in ["status", "domain_info", "brand_voice_notes", "cms_type", "cms_access", "approval_workflow", "notes"]:
            if key in data:
                fields.append(f"{key} = ?")
                params.append(data[key])
        for key in ["target_topics", "competitors", "completed_steps"]:
            if key in data:
                fields.append(f"{key} = ?")
                params.append(to_json(data[key]))
        if fields:
            fields.append("updated_at = datetime('now')")
            params.append(existing["id"])
            await execute(f"UPDATE onboarding SET {', '.join(fields)} WHERE id = ?", tuple(params))
        return existing["id"]
    else:
        ob_id = gen_id("ob-")
        await execute(
            """INSERT INTO onboarding
               (id, workspace_id, status, domain_info, target_topics, competitors,
                brand_voice_notes, cms_type, cms_access, approval_workflow, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ob_id, workspace_id, data.get("status", "pending"),
             data.get("domain_info", ""),
             to_json(data.get("target_topics", [])),
             to_json(data.get("competitors", [])),
             data.get("brand_voice_notes", ""),
             data.get("cms_type", ""),
             data.get("cms_access", 0),
             data.get("approval_workflow", ""),
             data.get("notes", "")),
        )
        return ob_id


# ═══════════════════════════════════════════════════════════════
# BILLING / USAGE TRACKING
# ═══════════════════════════════════════════════════════════════

async def generate_billing_record(workspace_id: str, period_start: str, period_end: str) -> Dict:
    """Generate a billing record for a workspace and period."""
    # Token costs
    cost_data = await fetch_one(
        "SELECT COALESCE(SUM(cost), 0) as total FROM token_usage WHERE project_id = ? AND created_at BETWEEN ? AND ?",
        (workspace_id, period_start, period_end),
    )
    # Scrape count
    scrape_data = await fetch_one(
        "SELECT COUNT(*) as cnt FROM sources WHERE project_id = ? AND scraped_at BETWEEN ? AND ?",
        (workspace_id, period_start, period_end),
    )
    # Content count
    draft_data = await fetch_one(
        """SELECT COUNT(*) as cnt FROM drafts d
           JOIN clusters c ON d.cluster_id = c.id
           WHERE c.project_id = ? AND d.created_at BETWEEN ? AND ?""",
        (workspace_id, period_start, period_end),
    )
    # Publish count
    pub_data = await fetch_one(
        """SELECT COUNT(*) as cnt FROM exports e
           JOIN drafts d ON e.draft_id = d.id
           JOIN clusters c ON d.cluster_id = c.id
           WHERE c.project_id = ? AND e.created_at BETWEEN ? AND ?""",
        (workspace_id, period_start, period_end),
    )

    record = {
        "token_cost": round(cost_data["total"], 4) if cost_data else 0,
        "scrape_count": scrape_data["cnt"] if scrape_data else 0,
        "content_count": draft_data["cnt"] if draft_data else 0,
        "publish_count": pub_data["cnt"] if pub_data else 0,
    }
    record["total_cost"] = record["token_cost"]  # Add markup/pricing logic here

    bill_id = gen_id("bill-")
    await execute(
        "INSERT INTO billing_records (id, workspace_id, period_start, period_end, token_cost, scrape_count, content_count, publish_count, total_cost) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (bill_id, workspace_id, period_start, period_end,
         record["token_cost"], record["scrape_count"], record["content_count"],
         record["publish_count"], record["total_cost"]),
    )
    record["id"] = bill_id
    return record


async def get_billing_history(workspace_id: str) -> List[Dict]:
    return await fetch_all(
        "SELECT * FROM billing_records WHERE workspace_id = ? ORDER BY period_end DESC",
        (workspace_id,),
    )


# ═══════════════════════════════════════════════════════════════
# COMPETITOR TRACKING
# ═══════════════════════════════════════════════════════════════

async def track_competitor(workspace_id: str, domain: str, name: str = "") -> str:
    """Add a competitor to track."""
    existing = await fetch_one(
        "SELECT id FROM competitors WHERE workspace_id = ? AND domain = ?",
        (workspace_id, domain),
    )
    if existing:
        return existing["id"]
    comp_id = gen_id("comp-")
    await execute(
        "INSERT INTO competitors (id, workspace_id, domain, name) VALUES (?, ?, ?, ?)",
        (comp_id, workspace_id, domain, name),
    )
    return comp_id


async def update_competitor_stats(workspace_id: str):
    """Update competitor stats from measurements data."""
    competitors = await fetch_all(
        "SELECT * FROM competitors WHERE workspace_id = ?", (workspace_id,),
    )
    for comp in competitors:
        stats = await fetch_one(
            """SELECT COUNT(*) as citations, AVG(citation_rate) as avg_rate
               FROM measurements
               WHERE project_id = ? AND url LIKE ?""",
            (workspace_id, f"%{comp['domain']}%"),
        )
        if stats:
            await execute(
                "UPDATE competitors SET citation_count = ?, avg_citation_rate = ?, last_checked = datetime('now') WHERE id = ?",
                (stats["citations"] or 0, round(stats["avg_rate"] or 0, 4), comp["id"]),
            )


async def get_competitors(workspace_id: str) -> List[Dict]:
    comps = await fetch_all(
        "SELECT * FROM competitors WHERE workspace_id = ? ORDER BY citation_count DESC",
        (workspace_id,),
    )
    for c in comps:
        c["top_topics"] = from_json(c.get("top_topics", "[]"), [])
    return comps


# ═══════════════════════════════════════════════════════════════
# CONTENT REFRESH ENGINE
# ═══════════════════════════════════════════════════════════════

async def detect_refresh_candidates(workspace_id: str) -> List[Dict]:
    """Find published content that may need refreshing."""
    assets = await fetch_all(
        """SELECT * FROM content_assets
           WHERE workspace_id = ? AND status = 'active'
           AND current_citation_rate < peak_citation_rate * 0.7
           ORDER BY (peak_citation_rate - current_citation_rate) DESC
           LIMIT 20""",
        (workspace_id,),
    )
    return assets


async def create_recommendation(
    workspace_id: str, rec_type: str, title: str,
    description: str = "", priority_score: float = 50.0,
    cluster_id: str = "", source_data: Dict = None,
) -> str:
    """Create a recommendation."""
    rec_id = gen_id("rec-")
    await execute(
        "INSERT INTO recommendations (id, workspace_id, rec_type, title, description, priority_score, cluster_id, source_data) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (rec_id, workspace_id, rec_type, title, description, priority_score, cluster_id, to_json(source_data or {})),
    )
    return rec_id


async def get_recommendations(workspace_id: str, status: str = "new") -> List[Dict]:
    recs = await fetch_all(
        "SELECT * FROM recommendations WHERE workspace_id = ? AND status = ? ORDER BY priority_score DESC",
        (workspace_id, status),
    )
    for r in recs:
        r["source_data"] = from_json(r.get("source_data", "{}"))
    return recs


# ═══════════════════════════════════════════════════════════════
# TEAM PERFORMANCE
# ═══════════════════════════════════════════════════════════════

async def get_team_performance(workspace_id: str, days: int = 30) -> List[Dict]:
    """Get per-user performance metrics."""
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    return await fetch_all(
        """SELECT
             u.id, u.name, u.email,
             (SELECT COUNT(*) FROM tasks t WHERE t.assigned_to = u.id AND t.workspace_id = ? AND t.status = 'done' AND t.completed_at >= ?) as tasks_completed,
             (SELECT COUNT(*) FROM tasks t WHERE t.assigned_to = u.id AND t.workspace_id = ? AND t.status IN ('todo','in_progress')) as tasks_open,
             (SELECT COUNT(*) FROM drafts d JOIN clusters c ON d.cluster_id = c.id WHERE c.project_id = ? AND d.created_at >= ?) as drafts_generated,
             (SELECT COUNT(*) FROM drafts d JOIN clusters c ON d.cluster_id = c.id WHERE c.project_id = ? AND d.status = 'approved' AND d.reviewed_at >= ?) as drafts_reviewed
           FROM users u
           JOIN workspace_members wm ON u.id = wm.user_id
           WHERE wm.workspace_id = ?
           ORDER BY tasks_completed DESC""",
        (workspace_id, since, workspace_id, workspace_id, since, workspace_id, since, workspace_id),
    )


# ═══════════════════════════════════════════════════════════════
# DEFAULT PLAYBOOKS
# ═══════════════════════════════════════════════════════════════

async def seed_default_playbooks():
    """Seed default SOP/playbook templates."""
    playbooks = [
        {
            "name": "Client Onboarding",
            "category": "onboarding",
            "description": "Standard checklist for onboarding a new GEO client.",
            "steps": [
                {"order": 1, "title": "Collect domain info", "description": "Get primary domain, subdomains, and any CDN/staging URLs", "checklist": ["Primary domain", "Subdomains", "Staging URLs"], "assignee_role": "admin", "est_minutes": 15},
                {"order": 2, "title": "Collect target topics", "description": "Define 10-20 initial topic clusters the client wants to rank for in AI engines", "checklist": ["Topic list", "Priority ranking", "Competitor topics"], "assignee_role": "editor", "est_minutes": 30},
                {"order": 3, "title": "Collect brand voice", "description": "Document tone, style, banned phrases, required positioning", "checklist": ["Tone guide", "Banned phrases", "Required claims", "Sample content"], "assignee_role": "editor", "est_minutes": 20},
                {"order": 4, "title": "Set up Peec integration", "description": "Connect Peec API or set up CSV export workflow", "checklist": ["API key or CSV access", "Initial import", "Verify field mapping"], "assignee_role": "admin", "est_minutes": 15},
                {"order": 5, "title": "Configure workspace", "description": "Set up workspace with brand settings, models, countries, compliance rules", "checklist": ["Brand settings", "Target models", "Countries/languages", "Compliance rules"], "assignee_role": "admin", "est_minutes": 10},
                {"order": 6, "title": "Set up CMS access", "description": "Get CMS credentials and test publish path", "checklist": ["CMS type identified", "API credentials", "Test publish"], "assignee_role": "admin", "est_minutes": 20},
                {"order": 7, "title": "Define approval workflow", "description": "Who reviews, who approves, how many rounds", "checklist": ["Reviewer assigned", "Approval chain defined", "Notification settings"], "assignee_role": "admin", "est_minutes": 10},
                {"order": 8, "title": "Run first analysis cycle", "description": "Import data, scrape top sources, run analysis, generate first draft", "checklist": ["Data imported", "Sources scraped", "Analysis complete", "First draft generated"], "assignee_role": "editor", "est_minutes": 60},
            ],
        },
        {
            "name": "Content Production Cycle",
            "category": "generation",
            "description": "Standard workflow for producing a GEO-optimized content asset.",
            "steps": [
                {"order": 1, "title": "Import latest Peec data", "description": "Upload latest CSV or sync via API", "checklist": ["Data uploaded", "Records validated"], "assignee_role": "editor", "est_minutes": 5},
                {"order": 2, "title": "Review source rankings", "description": "Check top-cited sources and identify patterns", "checklist": ["Top 20 sources reviewed", "Patterns documented"], "assignee_role": "editor", "est_minutes": 15},
                {"order": 3, "title": "Run cluster analysis", "description": "Analyze the target cluster with Claude", "checklist": ["Analysis complete", "Key patterns identified", "Brief generated"], "assignee_role": "editor", "est_minutes": 10},
                {"order": 4, "title": "Generate content draft", "description": "Generate draft using brief and template", "checklist": ["Draft generated", "Word count verified", "Brand voice checked"], "assignee_role": "editor", "est_minutes": 5},
                {"order": 5, "title": "Editorial review", "description": "Review for accuracy, tone, compliance", "checklist": ["Factual accuracy", "Brand voice compliance", "No forbidden claims", "Internal links added"], "assignee_role": "reviewer", "est_minutes": 20},
                {"order": 6, "title": "Publish", "description": "Push to CMS and verify live page", "checklist": ["Published to CMS", "Live URL verified", "Schema markup present"], "assignee_role": "editor", "est_minutes": 10},
                {"order": 7, "title": "Schedule re-measurement", "description": "Set up Peec re-measurement in 7-14 days", "checklist": ["Measurement scheduled", "Baseline recorded"], "assignee_role": "editor", "est_minutes": 5},
            ],
        },
        {
            "name": "Weekly Performance Review",
            "category": "reporting",
            "description": "Weekly check on citation metrics and content performance.",
            "steps": [
                {"order": 1, "title": "Sync latest Peec data", "description": "Import latest measurements", "checklist": ["Data synced", "No import errors"], "assignee_role": "editor", "est_minutes": 5},
                {"order": 2, "title": "Review performance deltas", "description": "Check before/after for all published content", "checklist": ["Deltas reviewed", "Wins documented", "Drops flagged"], "assignee_role": "editor", "est_minutes": 15},
                {"order": 3, "title": "Update competitor tracking", "description": "Check competitor citation changes", "checklist": ["Competitor data updated", "New gaps identified"], "assignee_role": "editor", "est_minutes": 10},
                {"order": 4, "title": "Generate client report", "description": "Create report snapshot for the period", "checklist": ["Report generated", "Key metrics highlighted"], "assignee_role": "editor", "est_minutes": 10},
                {"order": 5, "title": "Plan next actions", "description": "Based on data, decide what to produce/refresh next", "checklist": ["Next topics selected", "Refresh candidates identified", "Tasks created"], "assignee_role": "admin", "est_minutes": 15},
            ],
        },
    ]

    for pb in playbooks:
        await execute(
            "INSERT INTO playbooks (id, name, description, category, steps) VALUES (?, ?, ?, ?, ?)",
            (gen_id("pb-"), pb["name"], pb["description"], pb["category"], to_json(pb["steps"])),
        )
    logger.info("Default playbooks seeded")
