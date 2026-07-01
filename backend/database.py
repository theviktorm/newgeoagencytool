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
from . import db_driver  # optional Postgres driver; SQLite path never touches it

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
    prompt          TEXT NOT NULL,
    query           TEXT,
    question        TEXT,
    ai_engine       TEXT NOT NULL,
    platform        TEXT,
    model           TEXT,
    date            TEXT NOT NULL,
    run_id          TEXT,
    brand_mentioned BOOLEAN DEFAULT 0,
    brand_rank      INTEGER,
    brand_position  TEXT,
    visibility_score REAL,
    sentiment       TEXT,
    share_of_voice  REAL,
    answer_text     TEXT,
    answer_summary  TEXT,
    cited_urls      TEXT DEFAULT '[]',
    citation_urls   TEXT DEFAULT '[]',
    sources         TEXT DEFAULT '[]',
    source_type     TEXT,
    url_cited       TEXT,
    domain_cited    TEXT,
    reddit_citation TEXT,
    competitor_names TEXT DEFAULT '[]',
    competitor_domains TEXT DEFAULT '[]',
    competitor_ranks TEXT DEFAULT '[]',
    competitor_positions TEXT DEFAULT '[]',
    competitor_visibility REAL,
    topic           TEXT,
    cluster         TEXT,
    tag             TEXT DEFAULT '[]',
    buyer_intent    TEXT,
    location        TEXT,
    country         TEXT,
    language        TEXT,
    device          TEXT,
    aio_presence    BOOLEAN,
    prompt_score    REAL,
    model_response_id TEXT,
    raw_data        TEXT DEFAULT '{}',
    import_batch_id TEXT NOT NULL REFERENCES import_batches(id),
    imported_at     TEXT NOT NULL,
    confidence      TEXT,
    data_source     TEXT DEFAULT 'Peec.ai import'
);
CREATE INDEX IF NOT EXISTS idx_peec_project ON peec_records(project_id);
CREATE INDEX IF NOT EXISTS idx_peec_prompt ON peec_records(prompt);
CREATE INDEX IF NOT EXISTS idx_peec_ai_engine ON peec_records(ai_engine);
CREATE INDEX IF NOT EXISTS idx_peec_date ON peec_records(date);
CREATE INDEX IF NOT EXISTS idx_peec_import_batch ON peec_records(import_batch_id);
CREATE INDEX IF NOT EXISTS idx_peec_brand_mentioned ON peec_records(brand_mentioned);
CREATE INDEX IF NOT EXISTS idx_peec_topic ON peec_records(topic);
CREATE INDEX IF NOT EXISTS idx_peec_cluster ON peec_records(cluster);
CREATE INDEX IF NOT EXISTS idx_peec_buyer_intent ON peec_records(buyer_intent);
CREATE INDEX IF NOT EXISTS idx_peec_location ON peec_records(location);
CREATE INDEX IF NOT EXISTS idx_peec_country ON peec_records(country);
CREATE INDEX IF NOT EXISTS idx_peec_language ON peec_records(language);
CREATE INDEX IF NOT EXISTS idx_peec_device ON peec_records(device);
CREATE INDEX IF NOT EXISTS idx_peec_aio_presence ON peec_records(aio_presence);

-- ─── Import Batches ───
CREATE TABLE IF NOT EXISTS import_batches (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    file_name       TEXT NOT NULL,
    source          TEXT DEFAULT 'Peec.ai',
    imported_by     TEXT NOT NULL,
    imported_at     TEXT NOT NULL,
    rows_total      INTEGER DEFAULT 0,
    rows_success    INTEGER DEFAULT 0,
    rows_failed     INTEGER DEFAULT 0,
    warnings        TEXT,
    mapping_config  TEXT DEFAULT '{}',
    status          TEXT DEFAULT 'completed'
);
CREATE INDEX IF NOT EXISTS idx_import_batches_workspace ON import_batches(workspace_id);
CREATE INDEX IF NOT EXISTS idx_import_batches_status ON import_batches(status);

-- ─── Deduplicated source URLs ───
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
# DATABASE ACCESS — Connection Pool
# Reuses a single persistent connection instead of open/close per query.
# ═══════════════════════════════════════════════════════════════

import asyncio

_db_pool: Optional[aiosqlite.Connection] = None
_db_lock = asyncio.Lock()


async def get_db() -> aiosqlite.Connection:
    """Get the shared database connection (lazy-initialized, reused)."""
    # ── Postgres path (OFF by default; only when DATABASE_URL is postgres://) ──
    if db_driver._is_postgres():
        return db_driver._PGConnectionProxy()
    # ── SQLite path (UNCHANGED) ──
    global _db_pool
    if _db_pool is None or not _db_pool._running:
        async with _db_lock:
            if _db_pool is None or not _db_pool._running:
                _db_pool = await aiosqlite.connect(DB_PATH)
                _db_pool.row_factory = aiosqlite.Row
                await _db_pool.execute("PRAGMA journal_mode=WAL")
                await _db_pool.execute("PRAGMA foreign_keys=ON")
                await _db_pool.execute("PRAGMA busy_timeout=5000")
                await _db_pool.execute("PRAGMA cache_size=-20000")  # 20MB cache
                await _db_pool.execute("PRAGMA synchronous=NORMAL")
    return _db_pool


async def close_db():
    """Close the shared connection (call on shutdown)."""
    # ── Postgres path: close the asyncpg pool ──
    if db_driver._is_postgres():
        await db_driver.close_pool()
        return
    # ── SQLite path (UNCHANGED) ──
    global _db_pool
    if _db_pool and _db_pool._running:
        await _db_pool.close()
        _db_pool = None


MCP_SCHEMA = """
-- ─── MCP-driven mention events (live stream from Peec MCP) ───
-- One row per (url, model, prompt) observation. Lets us track sentiment
-- and citation deltas over time without re-importing CSVs.
CREATE TABLE IF NOT EXISTS mention_events (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    url             TEXT NOT NULL,
    prompt          TEXT DEFAULT '',
    model_source    TEXT DEFAULT 'Other',
    citation_count  INTEGER DEFAULT 0,
    citation_rate   REAL DEFAULT 0.0,
    position        REAL DEFAULT 0.0,
    sentiment       TEXT DEFAULT '',          -- positive | neutral | negative | ''
    sentiment_score REAL DEFAULT 0.0,         -- -1.0 .. 1.0
    raw             TEXT DEFAULT '{}',        -- JSON: full MCP payload
    observed_at     TEXT DEFAULT (datetime('now')),
    sync_batch_id   TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_mevents_project ON mention_events(project_id);
CREATE INDEX IF NOT EXISTS idx_mevents_url ON mention_events(url);
CREATE INDEX IF NOT EXISTS idx_mevents_observed ON mention_events(observed_at);
CREATE INDEX IF NOT EXISTS idx_mevents_sentiment ON mention_events(sentiment);

-- ─── Competitor share-of-voice history (one row per snapshot) ───
CREATE TABLE IF NOT EXISTS competitor_history (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    competitor_id       TEXT DEFAULT '',
    domain              TEXT NOT NULL,
    citation_count      INTEGER DEFAULT 0,
    share_of_voice      REAL DEFAULT 0.0,     -- 0..1
    avg_position        REAL DEFAULT 0.0,
    top_topics          TEXT DEFAULT '[]',
    observed_at         TEXT DEFAULT (datetime('now')),
    sync_batch_id       TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_chist_workspace ON competitor_history(workspace_id);
CREATE INDEX IF NOT EXISTS idx_chist_observed ON competitor_history(observed_at);

-- ─── Per-workspace MCP sync state ───
CREATE TABLE IF NOT EXISTS mcp_sync_state (
    workspace_id        TEXT PRIMARY KEY,
    last_sync_at        TEXT,
    last_sync_status    TEXT DEFAULT '',      -- ok | error | partial
    last_sync_error     TEXT DEFAULT '',
    last_batch_id       TEXT DEFAULT '',
    mentions_imported   INTEGER DEFAULT 0,
    competitors_imported INTEGER DEFAULT 0,
    auto_sync_enabled   INTEGER DEFAULT 0
);

-- ─── Per-workspace OAuth client + tokens for Peec MCP ───
-- Peec MCP uses OAuth 2.1 with Dynamic Client Registration + PKCE.
-- One row per (workspace, MCP server URL). access_token is a short-lived
-- bearer; refresh_token rolls it forward.
CREATE TABLE IF NOT EXISTS peec_oauth_clients (
    workspace_id        TEXT NOT NULL,
    server_url          TEXT NOT NULL,
    issuer              TEXT DEFAULT '',
    authorization_endpoint TEXT DEFAULT '',
    token_endpoint      TEXT DEFAULT '',
    registration_endpoint TEXT DEFAULT '',
    client_id           TEXT DEFAULT '',
    client_secret       TEXT DEFAULT '',
    redirect_uri        TEXT DEFAULT '',
    scope               TEXT DEFAULT '',
    access_token        TEXT DEFAULT '',
    refresh_token       TEXT DEFAULT '',
    expires_at          TEXT,                  -- ISO8601 UTC
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, server_url)
);

-- ─── In-flight authorize requests (state ↔ code_verifier) ───
-- Short-lived; cleaned up after callback or after 10 min.
CREATE TABLE IF NOT EXISTS peec_oauth_states (
    state               TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    server_url          TEXT NOT NULL,
    code_verifier       TEXT NOT NULL,
    created_at          TEXT DEFAULT (datetime('now'))
);
"""


# ═══════════════════════════════════════════════════════════════
# CONQUER_SCHEMA — 11 engines from the GEO conquest spec
# ═══════════════════════════════════════════════════════════════

CONQUER_SCHEMA = """
-- ───────────────────────────────────────────────────────────────
-- 1. PROMPT OWNERSHIP ENGINE
--    The actual battlefield: questions AI answers, not URLs.
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS prompts (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    text                TEXT NOT NULL,
    language            TEXT DEFAULT 'en',
    -- prompt_type: informational | comparative | decision | purchase
    prompt_type         TEXT DEFAULT 'informational',
    -- buyer_stage: awareness | problem | solution | comparison | trust | objection | decision
    buyer_stage         TEXT DEFAULT 'awareness',
    revenue_score       REAL DEFAULT 0.0,        -- 0..100, business value
    estimated_value_eur REAL DEFAULT 0.0,        -- per-conversion EUR estimate (optional)
    target_brand        TEXT DEFAULT '',         -- the brand we want to own this
    target_url          TEXT DEFAULT '',         -- the page that should win
    cluster_id          TEXT DEFAULT '',
    status              TEXT DEFAULT 'tracked',  -- tracked | ignored | won | lost
    classifier_meta     TEXT DEFAULT '{}',       -- JSON: LLM rationale, tags, etc.
    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_prompts_workspace ON prompts(workspace_id);
CREATE INDEX IF NOT EXISTS idx_prompts_stage ON prompts(buyer_stage);
CREATE INDEX IF NOT EXISTS idx_prompts_revenue ON prompts(revenue_score DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_prompts_uniq ON prompts(workspace_id, text);

-- One row per (prompt, model, observed_at): the AI SERP tracker.
CREATE TABLE IF NOT EXISTS prompt_observations (
    id                  TEXT PRIMARY KEY,
    prompt_id           TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    model               TEXT NOT NULL,           -- chatgpt | gemini | perplexity | claude | google_aio
    observed_at         TEXT DEFAULT (datetime('now')),
    -- Brands seen in the answer, in order. JSON list of {name, position, snippet}
    brands_appeared     TEXT DEFAULT '[]',
    -- Sources/URLs the AI cited. JSON list of {url, domain, title, position}
    sources_cited       TEXT DEFAULT '[]',
    our_brand_present   INTEGER DEFAULT 0,       -- 1 = our target brand surfaced
    our_brand_position  INTEGER DEFAULT 0,       -- 1 = first mention
    ai_overview_present INTEGER DEFAULT 0,       -- 1 = Google AI Overview visible
    answer_text         TEXT DEFAULT '',         -- raw AI answer (truncated)
    sentiment_score     REAL DEFAULT 0.0,
    sync_batch_id       TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_pobs_prompt ON prompt_observations(prompt_id);
CREATE INDEX IF NOT EXISTS idx_pobs_workspace ON prompt_observations(workspace_id);
CREATE INDEX IF NOT EXISTS idx_pobs_observed ON prompt_observations(observed_at);
CREATE INDEX IF NOT EXISTS idx_pobs_model ON prompt_observations(model);

-- Per-prompt rolling ownership snapshot. Refreshed on every observation.
CREATE TABLE IF NOT EXISTS prompt_ownership (
    workspace_id        TEXT NOT NULL,
    prompt_id           TEXT NOT NULL,
    -- Our ownership: 0..100 weighted by mentions × position × model coverage
    our_score           REAL DEFAULT 0.0,
    -- Top competitor: domain + score they hold
    leader_domain       TEXT DEFAULT '',
    leader_score        REAL DEFAULT 0.0,
    -- Full breakdown: JSON {"medicover.hu": 78, "waberer.hu": 45, ...}
    competitor_scores   TEXT DEFAULT '{}',
    models_covered      INTEGER DEFAULT 0,
    last_observed_at    TEXT,
    PRIMARY KEY (workspace_id, prompt_id)
);

-- Campaign goals: "Q2: own top 10 orthopedic prompts in Budapest"
CREATE TABLE IF NOT EXISTS campaign_goals (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    name                TEXT NOT NULL,
    description         TEXT DEFAULT '',
    target_prompts      TEXT DEFAULT '[]',       -- JSON list of prompt_ids
    target_ownership    REAL DEFAULT 70.0,       -- target our_score average
    starts_at           TEXT,
    ends_at             TEXT,
    status              TEXT DEFAULT 'active',   -- active | won | abandoned
    progress_score      REAL DEFAULT 0.0,
    created_at          TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_camp_workspace ON campaign_goals(workspace_id);

-- ───────────────────────────────────────────────────────────────
-- 2. CITATION INTELLIGENCE LAYER
--    Why are we losing? Not just who is winning.
-- ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS citation_diagnostics (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    prompt_id           TEXT DEFAULT '',
    cluster_id          TEXT DEFAULT '',
    competitor_domain   TEXT NOT NULL,
    analyzed_url        TEXT DEFAULT '',
    -- Sub-scores (0..100): each is "how strong is competitor on THIS axis"
    content_score       REAL DEFAULT 0.0,        -- topical depth, FAQ, comparison, decision support
    schema_score        REAL DEFAULT 0.0,        -- schema breadth + depth
    authority_score     REAL DEFAULT 0.0,        -- PR, podcasts, mentions
    reddit_score        REAL DEFAULT 0.0,
    youtube_score       REAL DEFAULT 0.0,
    entity_score        REAL DEFAULT 0.0,        -- entity consistency across platforms
    -- Free-text diagnosis from Claude: the WHY
    diagnosis           TEXT DEFAULT '',
    -- Concrete remediation steps. JSON list of {step, priority, impact}
    actions             TEXT DEFAULT '[]',
    raw_evidence        TEXT DEFAULT '{}',       -- JSON: scraped content, schema, etc.
    analyzed_at         TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_cdiag_workspace ON citation_diagnostics(workspace_id);
CREATE INDEX IF NOT EXISTS idx_cdiag_prompt ON citation_diagnostics(prompt_id);
CREATE INDEX IF NOT EXISTS idx_cdiag_competitor ON citation_diagnostics(competitor_domain);

-- ───────────────────────────────────────────────────────────────
-- 3. GEO ATTACK MAP
--    Per-competitor strength × weakness matrix for tactical strikes.
-- ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS competitor_capabilities (
    workspace_id        TEXT NOT NULL,
    competitor_domain   TEXT NOT NULL,
    -- 9 dimensions from the conquer spec; scored 0..100.
    schema_score        REAL DEFAULT 0.0,
    reddit_score        REAL DEFAULT 0.0,
    youtube_score       REAL DEFAULT 0.0,
    faq_depth_score     REAL DEFAULT 0.0,
    decision_support_score REAL DEFAULT 0.0,
    review_score        REAL DEFAULT 0.0,
    entity_consistency_score REAL DEFAULT 0.0,
    pr_score            REAL DEFAULT 0.0,
    local_authority_score REAL DEFAULT 0.0,
    overall_strength    REAL DEFAULT 0.0,
    -- Cached recommendation: where to attack first
    weakest_axis        TEXT DEFAULT '',
    weakest_axis_score  REAL DEFAULT 0.0,
    summary             TEXT DEFAULT '',
    analyzed_at         TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, competitor_domain)
);
CREATE INDEX IF NOT EXISTS idx_capabilities_workspace ON competitor_capabilities(workspace_id);

-- Time-series of capability scores so we can fire alerts when a competitor
-- suddenly gains in a dimension ("competitor gained Reddit authority").
CREATE TABLE IF NOT EXISTS capability_history (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    competitor_domain   TEXT NOT NULL,
    axis                TEXT NOT NULL,           -- schema | reddit | youtube | ...
    score               REAL DEFAULT 0.0,
    delta_vs_prev       REAL DEFAULT 0.0,
    observed_at         TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_caphist_workspace ON capability_history(workspace_id);
CREATE INDEX IF NOT EXISTS idx_caphist_observed ON capability_history(observed_at);

-- ───────────────────────────────────────────────────────────────
-- 5. REDDIT GEO ENGINE
--    Treat Reddit citations as authority infrastructure, not URLs.
-- ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS reddit_intel (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    subreddit           TEXT NOT NULL,
    thread_url          TEXT NOT NULL,
    thread_title        TEXT DEFAULT '',
    prompt_id           TEXT DEFAULT '',         -- which prompt surfaced this
    discussion_type     TEXT DEFAULT '',         -- recommendation | review | question | comparison
    brands_mentioned    TEXT DEFAULT '[]',
    our_brand_mentioned INTEGER DEFAULT 0,
    sentiment_score     REAL DEFAULT 0.0,
    sentiment_label     TEXT DEFAULT '',
    is_opportunity_gap  INTEGER DEFAULT 0,       -- 1 = our brand absent
    suggested_action    TEXT DEFAULT '',         -- what to seed/comment/reply
    observed_at         TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_reddit_workspace ON reddit_intel(workspace_id);
CREATE INDEX IF NOT EXISTS idx_reddit_subreddit ON reddit_intel(subreddit);
CREATE INDEX IF NOT EXISTS idx_reddit_gap ON reddit_intel(is_opportunity_gap);

-- ───────────────────────────────────────────────────────────────
-- 6. SCHEMA OPPORTUNITY ENGINE
-- ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_audits (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    page_url            TEXT NOT NULL,
    is_competitor       INTEGER DEFAULT 0,       -- 1 = competitor, 0 = our page
    -- JSON list of detected schema types
    schema_types        TEXT DEFAULT '[]',
    schema_depth_score  REAL DEFAULT 0.0,        -- 0..100
    -- JSON list of {type, missing_fields, severity, why_it_hurts}
    missing_critical    TEXT DEFAULT '[]',
    target_prompts      TEXT DEFAULT '[]',
    diagnosis           TEXT DEFAULT '',
    recommendations     TEXT DEFAULT '[]',
    raw_jsonld          TEXT DEFAULT '[]',
    analyzed_at         TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_schaud_workspace ON schema_audits(workspace_id);
CREATE INDEX IF NOT EXISTS idx_schaud_page ON schema_audits(page_url);

-- ───────────────────────────────────────────────────────────────
-- 7. BUYER JOURNEY COVERAGE MAP
-- ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS journey_coverage (
    workspace_id        TEXT NOT NULL,
    stage               TEXT NOT NULL,           -- one of 7 stages
    page_count          INTEGER DEFAULT 0,
    prompt_count        INTEGER DEFAULT 0,
    citation_count      INTEGER DEFAULT 0,
    coverage_score      REAL DEFAULT 0.0,        -- 0..100 vs benchmark
    gap_severity        TEXT DEFAULT 'none',     -- none | low | medium | critical
    recommendation      TEXT DEFAULT '',
    analyzed_at         TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, stage)
);

-- Per-page stage classification (lets us answer "do we have a comparison
-- page for prompt X?"). Filled by buyer_journey.classify_workspace.
CREATE TABLE IF NOT EXISTS page_stages (
    workspace_id        TEXT NOT NULL,
    url                 TEXT NOT NULL,
    stage               TEXT NOT NULL,
    confidence          REAL DEFAULT 0.0,
    classified_at       TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (workspace_id, url)
);

-- ───────────────────────────────────────────────────────────────
-- 8. GOOGLE AI OVERVIEW ENGINE
--    Subset of prompt_observations where model='google_aio', plus a
--    purpose-built tracker so alerts fire fast.
-- ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS aio_tracking (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    prompt_id           TEXT NOT NULL,
    observed_at         TEXT DEFAULT (datetime('now')),
    aio_present         INTEGER DEFAULT 0,
    our_brand_in_aio    INTEGER DEFAULT 0,
    visible_brands      TEXT DEFAULT '[]',
    source_urls         TEXT DEFAULT '[]',
    publisher_domains   TEXT DEFAULT '[]',
    snapshot_html       TEXT DEFAULT '',
    delta_vs_prev       TEXT DEFAULT ''          -- e.g. "competitor entered", "AIO appeared"
);
CREATE INDEX IF NOT EXISTS idx_aio_workspace ON aio_tracking(workspace_id);
CREATE INDEX IF NOT EXISTS idx_aio_observed ON aio_tracking(observed_at);

-- ───────────────────────────────────────────────────────────────
-- 9. METADATA + SNIPPET OPTIMIZATION ENGINE
-- ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metadata_packages (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    page_url            TEXT DEFAULT '',
    draft_id            TEXT DEFAULT '',         -- linked to drafts.id when generated for a draft
    seo_title           TEXT DEFAULT '',
    meta_description    TEXT DEFAULT '',
    og_title            TEXT DEFAULT '',
    og_description      TEXT DEFAULT '',
    canonical_url       TEXT DEFAULT '',
    snippet_target      TEXT DEFAULT '',         -- the question this page should win
    faq_extractions     TEXT DEFAULT '[]',       -- JSON: list of {q, a}
    internal_links      TEXT DEFAULT '[]',       -- JSON: suggested authority pages to link
    aio_compatibility_score REAL DEFAULT 0.0,    -- 0..100
    audit_findings      TEXT DEFAULT '[]',       -- if generated as audit, what's wrong
    generated_at        TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_meta_workspace ON metadata_packages(workspace_id);
CREATE INDEX IF NOT EXISTS idx_meta_draft ON metadata_packages(draft_id);

-- ───────────────────────────────────────────────────────────────
-- 10. GEO AUTHORITY SCORE
-- ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS authority_scores (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    subject_domain      TEXT NOT NULL,           -- our domain or a competitor's
    is_us               INTEGER DEFAULT 0,
    observed_at         TEXT DEFAULT (datetime('now')),
    -- Total + 7 sub-scores (0..100)
    total_score         REAL DEFAULT 0.0,
    citation_score      REAL DEFAULT 0.0,
    prompt_ownership_score REAL DEFAULT 0.0,
    schema_score        REAL DEFAULT 0.0,
    offsite_score       REAL DEFAULT 0.0,
    reddit_score        REAL DEFAULT 0.0,
    entity_score        REAL DEFAULT 0.0,
    local_score         REAL DEFAULT 0.0,
    -- breakdown JSON: {"why_total": "...", "biggest_lever": "..."}
    rationale           TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_authsc_workspace ON authority_scores(workspace_id);
CREATE INDEX IF NOT EXISTS idx_authsc_observed ON authority_scores(observed_at);
CREATE INDEX IF NOT EXISTS idx_authsc_subject ON authority_scores(subject_domain);

-- ───────────────────────────────────────────────────────────────
-- 12. TRACK-ALL LOGGING (Explainability Phase 2)
--     One row per track_workspace run so the agency can prove the
--     engine actually ran, what it checked, and what changed.
-- ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tracking_runs (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    started_at          TEXT DEFAULT (datetime('now')),
    finished_at         TEXT,
    prompts_checked     INTEGER DEFAULT 0,
    models_checked      TEXT DEFAULT '[]',       -- JSON list of model ids
    new_losses          INTEGER DEFAULT 0,        -- delta vs previous run
    new_wins            INTEGER DEFAULT 0,
    citation_changes    INTEGER DEFAULT 0,
    aio_changes         INTEGER DEFAULT 0,
    errors              INTEGER DEFAULT 0,
    duration_ms         INTEGER DEFAULT 0,
    data_source         TEXT DEFAULT 'live',      -- live | peec_replay | mixed
    confidence          TEXT DEFAULT 'estimated', -- estimated | verified | imported
    notes               TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_trkruns_workspace ON tracking_runs(workspace_id);
CREATE INDEX IF NOT EXISTS idx_trkruns_started ON tracking_runs(started_at);

-- ───────────────────────────────────────────────────────────────
-- 11. YOUTUBE GEO OPTIMIZER
-- ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS youtube_assets (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    video_url           TEXT DEFAULT '',
    topic               TEXT DEFAULT '',
    expert_name         TEXT DEFAULT '',
    connected_service   TEXT DEFAULT '',
    target_prompts      TEXT DEFAULT '[]',       -- JSON list of prompt_ids
    goal                TEXT DEFAULT '',         -- trust | lead-gen | FAQ | comparison
    -- Generated package fields
    optimized_title     TEXT DEFAULT '',
    description         TEXT DEFAULT '',
    chapters            TEXT DEFAULT '[]',       -- JSON list of {ts, title}
    faq_extractions     TEXT DEFAULT '[]',
    embed_strategy      TEXT DEFAULT '',
    schema_recommendation TEXT DEFAULT '',
    audit_findings      TEXT DEFAULT '[]',
    generated_at        TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_yt_workspace ON youtube_assets(workspace_id);
"""


async def _migrate(db: aiosqlite.Connection):
    """Idempotent ALTER TABLE migrations. SQLite throws on duplicate columns;
    catch and continue. Cheap on every boot."""
    migrations = [
        # Sentiment on Peec records (Tier 2)
        "ALTER TABLE peec_records ADD COLUMN sentiment TEXT DEFAULT ''",
        "ALTER TABLE peec_records ADD COLUMN sentiment_score REAL DEFAULT 0.0",
        "ALTER TABLE peec_records ADD COLUMN position REAL DEFAULT 0.0",
        # Sentiment rollup on sources
        "ALTER TABLE sources ADD COLUMN avg_sentiment_score REAL DEFAULT 0.0",
        "ALTER TABLE sources ADD COLUMN last_seen_at TEXT",
        # Source attribution: which prompt(s) cited this URL
        "ALTER TABLE sources ADD COLUMN sample_prompts TEXT DEFAULT '[]'",
        # Explainability Phase 1: provenance + nuanced ownership on prompts
        "ALTER TABLE prompts ADD COLUMN source TEXT DEFAULT 'unknown'",
        "ALTER TABLE prompts ADD COLUMN confidence TEXT DEFAULT 'estimated'",
        "ALTER TABLE prompts ADD COLUMN ownership_level INTEGER DEFAULT 0",
        "ALTER TABLE prompts ADD COLUMN ownership_status TEXT DEFAULT ''",
        "ALTER TABLE prompts ADD COLUMN last_tracked_at TEXT",
        # Explainability Phase 3: comparative citation intelligence
        "ALTER TABLE citation_diagnostics ADD COLUMN our_url TEXT DEFAULT ''",
        "ALTER TABLE citation_diagnostics ADD COLUMN recommended_action TEXT DEFAULT ''",
        "ALTER TABLE citation_diagnostics ADD COLUMN expected_impact TEXT DEFAULT ''",
        "ALTER TABLE citation_diagnostics ADD COLUMN difficulty TEXT DEFAULT ''",
        "ALTER TABLE citation_diagnostics ADD COLUMN confidence TEXT DEFAULT 'estimated'",
        "ALTER TABLE citation_diagnostics ADD COLUMN source_label TEXT DEFAULT 'claude_analyzed'",
        # Phase 3: graph node label provenance (auto vs manual)
        "ALTER TABLE graph_nodes ADD COLUMN label_source TEXT DEFAULT 'auto'",
        # Phase 3: tracked competitor flag
        "ALTER TABLE competitor_capabilities ADD COLUMN tracked INTEGER DEFAULT 0",
    ]
    for sql in migrations:
        try:
            await db.execute(sql)
        except Exception:
            # column already exists or table doesn't yet — ignore
            pass


async def init_db():
    """Initialize database schema. Safe to call multiple times.

    Each block runs independently so one failing statement (e.g. an index on
    a column an older production table lacks) can't abort the rest or crash boot.
    """
    import logging as _logging
    _log = _logging.getLogger("geo.database")
    # ── Postgres path: apply pg_schema.sql via the pool, then return. We do
    #    NOT run the SQLite executescript DDL on Postgres. ──
    if db_driver._is_postgres():
        import os as _os
        pool = await db_driver.get_pool()
        _schema_path = _os.path.join(_os.path.dirname(__file__), "pg_schema.sql")
        with open(_schema_path, "r", encoding="utf-8") as _fh:
            _pg_sql = _fh.read()
        async with pool.acquire() as _conn:
            try:
                await _conn.execute(_pg_sql)
            except Exception as _e:
                _log.error("init_db: pg_schema.sql failed: %s", _e)
        try:
            await db_driver.execute(
                "INSERT OR IGNORE INTO projects (id, name) VALUES (?, ?)",
                (settings.project_id, settings.project_name),
            )
        except Exception as _e:
            _log.error("init_db: seed project (pg) failed: %s", _e)
        return
    # ── SQLite path (UNCHANGED) ──
    db = await get_db()
    for _name, _script in (
        ("SCHEMA", SCHEMA), ("MCP_SCHEMA", MCP_SCHEMA), ("CONQUER_SCHEMA", CONQUER_SCHEMA),
    ):
        try:
            await db.executescript(_script)
        except Exception as _e:
            _log.error("init_db: %s block failed: %s", _name, _e)
    try:
        await _migrate(db)
    except Exception as _e:
        _log.error("init_db: _migrate failed: %s", _e)
    try:
        await db.execute(
            "INSERT OR IGNORE INTO projects (id, name) VALUES (?, ?)",
            (settings.project_id, settings.project_name),
        )
    except Exception as _e:
        _log.error("init_db: seed project failed: %s", _e)
    await db.commit()


def gen_id(prefix: str = "") -> str:
    """Generate a prefixed UUID."""
    uid = uuid4().hex[:12]
    return f"{prefix}{uid}" if prefix else uid


# ═══════════════════════════════════════════════════════════════
# QUERY HELPERS — all reuse the shared connection
# ═══════════════════════════════════════════════════════════════

async def fetch_one(query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
    if db_driver._is_postgres():
        return await db_driver.fetch_one(query, params)
    db = await get_db()
    cursor = await db.execute(query, params)
    row = await cursor.fetchone()
    return dict(row) if row else None


async def fetch_all(query: str, params: tuple = ()) -> List[Dict[str, Any]]:
    if db_driver._is_postgres():
        return await db_driver.fetch_all(query, params)
    db = await get_db()
    cursor = await db.execute(query, params)
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def execute(query: str, params: tuple = ()) -> int:
    if db_driver._is_postgres():
        return await db_driver.execute(query, params)
    db = await get_db()
    cursor = await db.execute(query, params)
    await db.commit()
    return cursor.rowcount


async def execute_many(query: str, params_list: List[tuple]) -> int:
    if db_driver._is_postgres():
        return await db_driver.execute_many(query, params_list)
    db = await get_db()
    await db.executemany(query, params_list)
    await db.commit()
    return len(params_list)


async def insert_returning_id(query: str, params: tuple) -> str:
    """Insert and return the last row id (for TEXT PKs, pass id in params)."""
    db = await get_db()
    await db.execute(query, params)
    await db.commit()
    return params[0]


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
