-- ════════════════════════════════════════════════════════════════════════
-- Momentus AI — Postgres schema (OPTIONAL, additive)
-- ════════════════════════════════════════════════════════════════════════
-- This file mirrors the SQLite schema strings defined across backend/*.py:
--   * database.py   : SCHEMA, MCP_SCHEMA, CONQUER_SCHEMA
--   * auth.py       : AUTH_SCHEMA
--   * automation.py : AUTOMATION_SCHEMA (job_queue, automation_rules, tasks,
--                     notifications, playbooks, onboarding, billing_records,
--                     competitors, content_assets, recommendations)
--   * brand_resolver.py, entity_graph.py, alert_engine.py, action_engine.py,
--     backtest.py, scheduler.py : self-registering module schemas
--
-- Conversions applied vs. SQLite DDL:
--   * datetime('now')  → now()
--   * TEXT PRIMARY KEY kept as-is (app generates UUID-ish TEXT ids itself;
--     there are NO INTEGER PRIMARY KEY / AUTOINCREMENT tables in this app).
--   * INTEGER / REAL / TEXT preserved. Booleans stay INTEGER (0/1) to keep
--     the app's existing 0/1 reads/writes byte-compatible (correctness >
--     optimization). JSON columns stay TEXT for the same reason.
--
-- Idempotent: every statement uses IF NOT EXISTS so init_db() can re-run.
-- ════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════
-- database.py :: SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS projects (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    config          TEXT DEFAULT '{}',
    created_at      TEXT DEFAULT (now()::text),
    updated_at      TEXT DEFAULT (now()::text)
);

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
    tags            TEXT DEFAULT '[]',
    model_source    TEXT DEFAULT 'Other',
    raw_data        TEXT DEFAULT '{}',
    import_batch_id TEXT,
    imported_at     TEXT DEFAULT (now()::text),
    -- columns added by _migrate() in database.py:
    sentiment       TEXT DEFAULT '',
    sentiment_score REAL DEFAULT 0.0,
    position        REAL DEFAULT 0.0
);
CREATE INDEX IF NOT EXISTS idx_peec_project ON peec_records(project_id);
CREATE INDEX IF NOT EXISTS idx_peec_topic ON peec_records(topic);
CREATE INDEX IF NOT EXISTS idx_peec_model ON peec_records(model_source);
CREATE INDEX IF NOT EXISTS idx_peec_url ON peec_records(url);

CREATE TABLE IF NOT EXISTS sources (
    id                  TEXT PRIMARY KEY,
    project_id          TEXT NOT NULL REFERENCES projects(id),
    url                 TEXT NOT NULL,
    title               TEXT DEFAULT '',
    total_citation_count INTEGER DEFAULT 0,
    max_citation_rate   REAL DEFAULT 0.0,
    topics              TEXT DEFAULT '[]',
    model_sources       TEXT DEFAULT '[]',
    scrape_status       TEXT DEFAULT 'pending',
    scraped_at          TEXT,
    created_at          TEXT DEFAULT (now()::text),
    -- columns added by _migrate() in database.py:
    avg_sentiment_score REAL DEFAULT 0.0,
    last_seen_at        TEXT,
    sample_prompts      TEXT DEFAULT '[]',
    UNIQUE(project_id, url)
);
CREATE INDEX IF NOT EXISTS idx_sources_project ON sources(project_id);
CREATE INDEX IF NOT EXISTS idx_sources_status ON sources(scrape_status);

CREATE TABLE IF NOT EXISTS scraped_content (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    url             TEXT NOT NULL,
    title           TEXT DEFAULT '',
    headings        TEXT DEFAULT '[]',
    body_text       TEXT DEFAULT '',
    lists           TEXT DEFAULT '[]',
    faqs            TEXT DEFAULT '[]',
    meta_description TEXT DEFAULT '',
    schema_markup   TEXT DEFAULT '{}',
    word_count      INTEGER DEFAULT 0,
    raw_html_hash   TEXT DEFAULT '',
    scraped_at      TEXT DEFAULT (now()::text),
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_scraped_source ON scraped_content(source_id);

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
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_clusters_project ON clusters(project_id);

CREATE TABLE IF NOT EXISTS cluster_records (
    cluster_id      TEXT NOT NULL REFERENCES clusters(id),
    peec_record_id  TEXT NOT NULL REFERENCES peec_records(id),
    PRIMARY KEY (cluster_id, peec_record_id)
);

CREATE TABLE IF NOT EXISTS cluster_sources (
    cluster_id      TEXT NOT NULL REFERENCES clusters(id),
    source_id       TEXT NOT NULL REFERENCES sources(id),
    PRIMARY KEY (cluster_id, source_id)
);

CREATE TABLE IF NOT EXISTS analyses (
    id              TEXT PRIMARY KEY,
    cluster_id      TEXT NOT NULL REFERENCES clusters(id),
    prompt_template TEXT DEFAULT '',
    prompt_type     TEXT DEFAULT 'blog',
    model           TEXT DEFAULT '',
    temperature     REAL DEFAULT 0.7,
    result          TEXT DEFAULT '',
    tokens_input    INTEGER DEFAULT 0,
    tokens_output   INTEGER DEFAULT 0,
    cost            REAL DEFAULT 0.0,
    cached_tokens   INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_analyses_cluster ON analyses(cluster_id);

CREATE TABLE IF NOT EXISTS briefs (
    id              TEXT PRIMARY KEY,
    cluster_id      TEXT NOT NULL REFERENCES clusters(id),
    analysis_id     TEXT REFERENCES analyses(id),
    content         TEXT DEFAULT '',
    model           TEXT DEFAULT '',
    tokens_input    INTEGER DEFAULT 0,
    tokens_output   INTEGER DEFAULT 0,
    cost            REAL DEFAULT 0.0,
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_briefs_cluster ON briefs(cluster_id);

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
    status          TEXT DEFAULT 'pending',
    reviewer_notes  TEXT DEFAULT '',
    reviewed_at     TEXT,
    parent_draft_id TEXT REFERENCES drafts(id),
    source_snapshot TEXT DEFAULT '{}',
    prompt_snapshot TEXT DEFAULT '',
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_drafts_cluster ON drafts(cluster_id);
CREATE INDEX IF NOT EXISTS idx_drafts_status ON drafts(status);

CREATE TABLE IF NOT EXISTS exports (
    id              TEXT PRIMARY KEY,
    draft_id        TEXT NOT NULL REFERENCES drafts(id),
    format          TEXT DEFAULT 'markdown',
    content         TEXT DEFAULT '',
    published_to    TEXT DEFAULT '',
    published_url   TEXT DEFAULT '',
    cms_post_id     TEXT DEFAULT '',
    published_at    TEXT,
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_exports_draft ON exports(draft_id);

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
    measured_at     TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_measurements_project ON measurements(project_id);
CREATE INDEX IF NOT EXISTS idx_measurements_url ON measurements(url);
CREATE INDEX IF NOT EXISTS idx_measurements_draft ON measurements(draft_id);

CREATE TABLE IF NOT EXISTS token_usage (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    model           TEXT DEFAULT '',
    tokens_input    INTEGER DEFAULT 0,
    tokens_output   INTEGER DEFAULT 0,
    cached_tokens   INTEGER DEFAULT 0,
    cost            REAL DEFAULT 0.0,
    operation       TEXT DEFAULT '',
    reference_id    TEXT DEFAULT '',
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_usage_project ON token_usage(project_id);
CREATE INDEX IF NOT EXISTS idx_usage_date ON token_usage(created_at);

CREATE TABLE IF NOT EXISTS batch_jobs (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    job_type        TEXT DEFAULT '',
    status          TEXT DEFAULT 'queued',
    total_items     INTEGER DEFAULT 0,
    completed_items INTEGER DEFAULT 0,
    failed_items    INTEGER DEFAULT 0,
    results         TEXT DEFAULT '{}',
    error           TEXT DEFAULT '',
    anthropic_batch_id TEXT DEFAULT '',
    started_at      TEXT,
    completed_at    TEXT,
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_batch_project ON batch_jobs(project_id);
CREATE INDEX IF NOT EXISTS idx_batch_status ON batch_jobs(status);

CREATE TABLE IF NOT EXISTS success_metrics (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL REFERENCES projects(id),
    metric_name     TEXT NOT NULL,
    target_value    REAL DEFAULT 0.0,
    baseline_value  REAL DEFAULT 0.0,
    current_value   REAL DEFAULT 0.0,
    direction       TEXT DEFAULT 'increase',
    created_at      TEXT DEFAULT (now()::text),
    updated_at      TEXT DEFAULT (now()::text)
);

CREATE TABLE IF NOT EXISTS prompt_templates (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,
    sub_type        TEXT DEFAULT 'blog',
    template        TEXT NOT NULL,
    version         INTEGER DEFAULT 1,
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_prompts_project ON prompt_templates(project_id);


-- ════════════════════════════════════════════════════════════════════════
-- database.py :: MCP_SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS mention_events (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    url             TEXT NOT NULL,
    prompt          TEXT DEFAULT '',
    model_source    TEXT DEFAULT 'Other',
    citation_count  INTEGER DEFAULT 0,
    citation_rate   REAL DEFAULT 0.0,
    position        REAL DEFAULT 0.0,
    sentiment       TEXT DEFAULT '',
    sentiment_score REAL DEFAULT 0.0,
    raw             TEXT DEFAULT '{}',
    observed_at     TEXT DEFAULT (now()::text),
    sync_batch_id   TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_mevents_project ON mention_events(project_id);
CREATE INDEX IF NOT EXISTS idx_mevents_url ON mention_events(url);
CREATE INDEX IF NOT EXISTS idx_mevents_observed ON mention_events(observed_at);
CREATE INDEX IF NOT EXISTS idx_mevents_sentiment ON mention_events(sentiment);

CREATE TABLE IF NOT EXISTS competitor_history (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    competitor_id       TEXT DEFAULT '',
    domain              TEXT NOT NULL,
    citation_count      INTEGER DEFAULT 0,
    share_of_voice      REAL DEFAULT 0.0,
    avg_position        REAL DEFAULT 0.0,
    top_topics          TEXT DEFAULT '[]',
    observed_at         TEXT DEFAULT (now()::text),
    sync_batch_id       TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_chist_workspace ON competitor_history(workspace_id);
CREATE INDEX IF NOT EXISTS idx_chist_observed ON competitor_history(observed_at);

CREATE TABLE IF NOT EXISTS mcp_sync_state (
    workspace_id        TEXT PRIMARY KEY,
    last_sync_at        TEXT,
    last_sync_status    TEXT DEFAULT '',
    last_sync_error     TEXT DEFAULT '',
    last_batch_id       TEXT DEFAULT '',
    mentions_imported   INTEGER DEFAULT 0,
    competitors_imported INTEGER DEFAULT 0,
    auto_sync_enabled   INTEGER DEFAULT 0
);

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
    expires_at          TEXT,
    created_at          TEXT DEFAULT (now()::text),
    updated_at          TEXT DEFAULT (now()::text),
    PRIMARY KEY (workspace_id, server_url)
);

CREATE TABLE IF NOT EXISTS peec_oauth_states (
    state               TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    server_url          TEXT NOT NULL,
    code_verifier       TEXT NOT NULL,
    created_at          TEXT DEFAULT (now()::text)
);


-- ════════════════════════════════════════════════════════════════════════
-- database.py :: CONQUER_SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS prompts (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    text                TEXT NOT NULL,
    language            TEXT DEFAULT 'en',
    prompt_type         TEXT DEFAULT 'informational',
    buyer_stage         TEXT DEFAULT 'awareness',
    revenue_score       REAL DEFAULT 0.0,
    estimated_value_eur REAL DEFAULT 0.0,
    target_brand        TEXT DEFAULT '',
    target_url          TEXT DEFAULT '',
    cluster_id          TEXT DEFAULT '',
    status              TEXT DEFAULT 'tracked',
    classifier_meta     TEXT DEFAULT '{}',
    created_at          TEXT DEFAULT (now()::text),
    updated_at          TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_prompts_workspace ON prompts(workspace_id);
CREATE INDEX IF NOT EXISTS idx_prompts_stage ON prompts(buyer_stage);
CREATE INDEX IF NOT EXISTS idx_prompts_revenue ON prompts(revenue_score DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_prompts_uniq ON prompts(workspace_id, text);

CREATE TABLE IF NOT EXISTS prompt_observations (
    id                  TEXT PRIMARY KEY,
    prompt_id           TEXT NOT NULL,
    workspace_id        TEXT NOT NULL,
    model               TEXT NOT NULL,
    observed_at         TEXT DEFAULT (now()::text),
    brands_appeared     TEXT DEFAULT '[]',
    sources_cited       TEXT DEFAULT '[]',
    our_brand_present   INTEGER DEFAULT 0,
    our_brand_position  INTEGER DEFAULT 0,
    ai_overview_present INTEGER DEFAULT 0,
    answer_text         TEXT DEFAULT '',
    sentiment_score     REAL DEFAULT 0.0,
    sync_batch_id       TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_pobs_prompt ON prompt_observations(prompt_id);
CREATE INDEX IF NOT EXISTS idx_pobs_workspace ON prompt_observations(workspace_id);
CREATE INDEX IF NOT EXISTS idx_pobs_observed ON prompt_observations(observed_at);
CREATE INDEX IF NOT EXISTS idx_pobs_model ON prompt_observations(model);

CREATE TABLE IF NOT EXISTS prompt_ownership (
    workspace_id        TEXT NOT NULL,
    prompt_id           TEXT NOT NULL,
    our_score           REAL DEFAULT 0.0,
    leader_domain       TEXT DEFAULT '',
    leader_score        REAL DEFAULT 0.0,
    competitor_scores   TEXT DEFAULT '{}',
    models_covered      INTEGER DEFAULT 0,
    last_observed_at    TEXT,
    PRIMARY KEY (workspace_id, prompt_id)
);

CREATE TABLE IF NOT EXISTS campaign_goals (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    name                TEXT NOT NULL,
    description         TEXT DEFAULT '',
    target_prompts      TEXT DEFAULT '[]',
    target_ownership    REAL DEFAULT 70.0,
    starts_at           TEXT,
    ends_at             TEXT,
    status              TEXT DEFAULT 'active',
    progress_score      REAL DEFAULT 0.0,
    created_at          TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_camp_workspace ON campaign_goals(workspace_id);

CREATE TABLE IF NOT EXISTS citation_diagnostics (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    prompt_id           TEXT DEFAULT '',
    cluster_id          TEXT DEFAULT '',
    competitor_domain   TEXT NOT NULL,
    analyzed_url        TEXT DEFAULT '',
    content_score       REAL DEFAULT 0.0,
    schema_score        REAL DEFAULT 0.0,
    authority_score     REAL DEFAULT 0.0,
    reddit_score        REAL DEFAULT 0.0,
    youtube_score       REAL DEFAULT 0.0,
    entity_score        REAL DEFAULT 0.0,
    diagnosis           TEXT DEFAULT '',
    actions             TEXT DEFAULT '[]',
    raw_evidence        TEXT DEFAULT '{}',
    analyzed_at         TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_cdiag_workspace ON citation_diagnostics(workspace_id);
CREATE INDEX IF NOT EXISTS idx_cdiag_prompt ON citation_diagnostics(prompt_id);
CREATE INDEX IF NOT EXISTS idx_cdiag_competitor ON citation_diagnostics(competitor_domain);

CREATE TABLE IF NOT EXISTS competitor_capabilities (
    workspace_id        TEXT NOT NULL,
    competitor_domain   TEXT NOT NULL,
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
    weakest_axis        TEXT DEFAULT '',
    weakest_axis_score  REAL DEFAULT 0.0,
    summary             TEXT DEFAULT '',
    analyzed_at         TEXT DEFAULT (now()::text),
    PRIMARY KEY (workspace_id, competitor_domain)
);
CREATE INDEX IF NOT EXISTS idx_capabilities_workspace ON competitor_capabilities(workspace_id);

CREATE TABLE IF NOT EXISTS capability_history (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    competitor_domain   TEXT NOT NULL,
    axis                TEXT NOT NULL,
    score               REAL DEFAULT 0.0,
    delta_vs_prev       REAL DEFAULT 0.0,
    observed_at         TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_caphist_workspace ON capability_history(workspace_id);
CREATE INDEX IF NOT EXISTS idx_caphist_observed ON capability_history(observed_at);

CREATE TABLE IF NOT EXISTS reddit_intel (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    subreddit           TEXT NOT NULL,
    thread_url          TEXT NOT NULL,
    thread_title        TEXT DEFAULT '',
    prompt_id           TEXT DEFAULT '',
    discussion_type     TEXT DEFAULT '',
    brands_mentioned    TEXT DEFAULT '[]',
    our_brand_mentioned INTEGER DEFAULT 0,
    sentiment_score     REAL DEFAULT 0.0,
    sentiment_label     TEXT DEFAULT '',
    is_opportunity_gap  INTEGER DEFAULT 0,
    suggested_action    TEXT DEFAULT '',
    observed_at         TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_reddit_workspace ON reddit_intel(workspace_id);
CREATE INDEX IF NOT EXISTS idx_reddit_subreddit ON reddit_intel(subreddit);
CREATE INDEX IF NOT EXISTS idx_reddit_gap ON reddit_intel(is_opportunity_gap);

CREATE TABLE IF NOT EXISTS schema_audits (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    page_url            TEXT NOT NULL,
    is_competitor       INTEGER DEFAULT 0,
    schema_types        TEXT DEFAULT '[]',
    schema_depth_score  REAL DEFAULT 0.0,
    missing_critical    TEXT DEFAULT '[]',
    target_prompts      TEXT DEFAULT '[]',
    diagnosis           TEXT DEFAULT '',
    recommendations     TEXT DEFAULT '[]',
    raw_jsonld          TEXT DEFAULT '[]',
    analyzed_at         TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_schaud_workspace ON schema_audits(workspace_id);
CREATE INDEX IF NOT EXISTS idx_schaud_page ON schema_audits(page_url);

CREATE TABLE IF NOT EXISTS journey_coverage (
    workspace_id        TEXT NOT NULL,
    stage               TEXT NOT NULL,
    page_count          INTEGER DEFAULT 0,
    prompt_count        INTEGER DEFAULT 0,
    citation_count      INTEGER DEFAULT 0,
    coverage_score      REAL DEFAULT 0.0,
    gap_severity        TEXT DEFAULT 'none',
    recommendation      TEXT DEFAULT '',
    analyzed_at         TEXT DEFAULT (now()::text),
    PRIMARY KEY (workspace_id, stage)
);

CREATE TABLE IF NOT EXISTS page_stages (
    workspace_id        TEXT NOT NULL,
    url                 TEXT NOT NULL,
    stage               TEXT NOT NULL,
    confidence          REAL DEFAULT 0.0,
    classified_at       TEXT DEFAULT (now()::text),
    PRIMARY KEY (workspace_id, url)
);

CREATE TABLE IF NOT EXISTS aio_tracking (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    prompt_id           TEXT NOT NULL,
    observed_at         TEXT DEFAULT (now()::text),
    aio_present         INTEGER DEFAULT 0,
    our_brand_in_aio    INTEGER DEFAULT 0,
    visible_brands      TEXT DEFAULT '[]',
    source_urls         TEXT DEFAULT '[]',
    publisher_domains   TEXT DEFAULT '[]',
    snapshot_html       TEXT DEFAULT '',
    delta_vs_prev       TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_aio_workspace ON aio_tracking(workspace_id);
CREATE INDEX IF NOT EXISTS idx_aio_observed ON aio_tracking(observed_at);

CREATE TABLE IF NOT EXISTS metadata_packages (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    page_url            TEXT DEFAULT '',
    draft_id            TEXT DEFAULT '',
    seo_title           TEXT DEFAULT '',
    meta_description    TEXT DEFAULT '',
    og_title            TEXT DEFAULT '',
    og_description      TEXT DEFAULT '',
    canonical_url       TEXT DEFAULT '',
    snippet_target      TEXT DEFAULT '',
    faq_extractions     TEXT DEFAULT '[]',
    internal_links      TEXT DEFAULT '[]',
    aio_compatibility_score REAL DEFAULT 0.0,
    audit_findings      TEXT DEFAULT '[]',
    generated_at        TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_meta_workspace ON metadata_packages(workspace_id);
CREATE INDEX IF NOT EXISTS idx_meta_draft ON metadata_packages(draft_id);

CREATE TABLE IF NOT EXISTS authority_scores (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    subject_domain      TEXT NOT NULL,
    is_us               INTEGER DEFAULT 0,
    observed_at         TEXT DEFAULT (now()::text),
    total_score         REAL DEFAULT 0.0,
    citation_score      REAL DEFAULT 0.0,
    prompt_ownership_score REAL DEFAULT 0.0,
    schema_score        REAL DEFAULT 0.0,
    offsite_score       REAL DEFAULT 0.0,
    reddit_score        REAL DEFAULT 0.0,
    entity_score        REAL DEFAULT 0.0,
    local_score         REAL DEFAULT 0.0,
    rationale           TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_authsc_workspace ON authority_scores(workspace_id);
CREATE INDEX IF NOT EXISTS idx_authsc_observed ON authority_scores(observed_at);
CREATE INDEX IF NOT EXISTS idx_authsc_subject ON authority_scores(subject_domain);

CREATE TABLE IF NOT EXISTS youtube_assets (
    id                  TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    video_url           TEXT DEFAULT '',
    topic               TEXT DEFAULT '',
    expert_name         TEXT DEFAULT '',
    connected_service   TEXT DEFAULT '',
    target_prompts      TEXT DEFAULT '[]',
    goal                TEXT DEFAULT '',
    optimized_title     TEXT DEFAULT '',
    description         TEXT DEFAULT '',
    chapters            TEXT DEFAULT '[]',
    faq_extractions     TEXT DEFAULT '[]',
    embed_strategy      TEXT DEFAULT '',
    schema_recommendation TEXT DEFAULT '',
    audit_findings      TEXT DEFAULT '[]',
    generated_at        TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_yt_workspace ON youtube_assets(workspace_id);


-- ════════════════════════════════════════════════════════════════════════
-- auth.py :: AUTH_SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS users (
    id              TEXT PRIMARY KEY,
    email           TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL DEFAULT '',
    password_hash   TEXT NOT NULL,
    salt            TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'editor',
    avatar_url      TEXT DEFAULT '',
    is_active       INTEGER DEFAULT 1,
    last_login      TEXT,
    created_at      TEXT DEFAULT (now()::text),
    updated_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

CREATE TABLE IF NOT EXISTS workspaces (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL UNIQUE,
    brand_name      TEXT DEFAULT '',
    domains         TEXT DEFAULT '[]',
    target_countries TEXT DEFAULT '[]',
    target_languages TEXT DEFAULT '[]',
    target_models   TEXT DEFAULT '[]',
    brand_voice     TEXT DEFAULT '',
    compliance_rules TEXT DEFAULT '',
    logo_url        TEXT DEFAULT '',
    color_primary   TEXT DEFAULT '#2563EB',
    color_accent    TEXT DEFAULT '#10B981',
    settings        TEXT DEFAULT '{}',
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT DEFAULT (now()::text),
    updated_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_workspaces_slug ON workspaces(slug);

CREATE TABLE IF NOT EXISTS workspace_members (
    user_id         TEXT NOT NULL REFERENCES users(id),
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),
    role            TEXT NOT NULL DEFAULT 'editor',
    joined_at       TEXT DEFAULT (now()::text),
    PRIMARY KEY (user_id, workspace_id)
);

CREATE TABLE IF NOT EXISTS campaigns (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),
    name            TEXT NOT NULL,
    description     TEXT DEFAULT '',
    status          TEXT DEFAULT 'active',
    start_date      TEXT,
    end_date        TEXT,
    goals           TEXT DEFAULT '{}',
    created_by      TEXT REFERENCES users(id),
    created_at      TEXT DEFAULT (now()::text),
    updated_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_campaigns_workspace ON campaigns(workspace_id);

CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL REFERENCES users(id),
    token_hash      TEXT NOT NULL,
    expires_at      TEXT NOT NULL,
    ip_address      TEXT DEFAULT '',
    user_agent      TEXT DEFAULT '',
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token_hash);

CREATE TABLE IF NOT EXISTS audit_log (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    workspace_id    TEXT DEFAULT '',
    action          TEXT NOT NULL,
    resource_type   TEXT DEFAULT '',
    resource_id     TEXT DEFAULT '',
    details         TEXT DEFAULT '{}',
    ip_address      TEXT DEFAULT '',
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_workspace ON audit_log(workspace_id);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);

CREATE TABLE IF NOT EXISTS report_snapshots (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),
    campaign_id     TEXT REFERENCES campaigns(id),
    period_start    TEXT NOT NULL,
    period_end      TEXT NOT NULL,
    report_type     TEXT DEFAULT 'weekly',
    metrics         TEXT DEFAULT '{}',
    generated_by    TEXT REFERENCES users(id),
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_reports_workspace ON report_snapshots(workspace_id);

CREATE TABLE IF NOT EXISTS scheduled_reports (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL REFERENCES workspaces(id),
    frequency       TEXT DEFAULT 'weekly',
    recipients      TEXT DEFAULT '[]',
    report_config   TEXT DEFAULT '{}',
    last_sent       TEXT,
    next_send       TEXT,
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT DEFAULT (now()::text)
);


-- ════════════════════════════════════════════════════════════════════════
-- automation.py :: AUTOMATION_SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS job_queue (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    campaign_id     TEXT DEFAULT '',
    job_type        TEXT NOT NULL,
    priority        INTEGER DEFAULT 5,
    status          TEXT DEFAULT 'queued',
    params          TEXT DEFAULT '{}',
    result          TEXT DEFAULT '{}',
    error           TEXT DEFAULT '',
    retry_count     INTEGER DEFAULT 0,
    max_retries     INTEGER DEFAULT 3,
    created_by      TEXT DEFAULT '',
    assigned_to     TEXT DEFAULT '',
    started_at      TEXT,
    completed_at    TEXT,
    scheduled_for   TEXT,
    parent_job_id   TEXT,
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_jobs_workspace ON job_queue(workspace_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON job_queue(status);
CREATE INDEX IF NOT EXISTS idx_jobs_type ON job_queue(job_type);
CREATE INDEX IF NOT EXISTS idx_jobs_priority ON job_queue(priority);
CREATE INDEX IF NOT EXISTS idx_jobs_scheduled ON job_queue(scheduled_for);

CREATE TABLE IF NOT EXISTS automation_rules (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    name            TEXT NOT NULL,
    trigger_event   TEXT NOT NULL,
    trigger_config  TEXT DEFAULT '{}',
    action_type     TEXT NOT NULL,
    action_config   TEXT DEFAULT '{}',
    is_active       INTEGER DEFAULT 1,
    last_triggered  TEXT,
    trigger_count   INTEGER DEFAULT 0,
    created_by      TEXT DEFAULT '',
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_rules_workspace ON automation_rules(workspace_id);
CREATE INDEX IF NOT EXISTS idx_rules_event ON automation_rules(trigger_event);

CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    campaign_id     TEXT DEFAULT '',
    cluster_id      TEXT DEFAULT '',
    draft_id        TEXT DEFAULT '',
    title           TEXT NOT NULL,
    description     TEXT DEFAULT '',
    task_type       TEXT NOT NULL,
    status          TEXT DEFAULT 'todo',
    priority        TEXT DEFAULT 'medium',
    assigned_to     TEXT DEFAULT '',
    created_by      TEXT NOT NULL,
    due_date        TEXT,
    completed_at    TEXT,
    tags            TEXT DEFAULT '[]',
    comments        TEXT DEFAULT '[]',
    checklist       TEXT DEFAULT '[]',
    sort_order      INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (now()::text),
    updated_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_tasks_workspace ON tasks(workspace_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(assigned_to);
CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(task_type);

CREATE TABLE IF NOT EXISTS notifications (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    workspace_id    TEXT DEFAULT '',
    type            TEXT NOT NULL,
    title           TEXT NOT NULL,
    message         TEXT DEFAULT '',
    link            TEXT DEFAULT '',
    is_read         INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id);
CREATE INDEX IF NOT EXISTS idx_notif_read ON notifications(is_read);

CREATE TABLE IF NOT EXISTS playbooks (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT DEFAULT '',
    name            TEXT NOT NULL,
    description     TEXT DEFAULT '',
    category        TEXT DEFAULT 'general',
    steps           TEXT DEFAULT '[]',
    is_active       INTEGER DEFAULT 1,
    created_by      TEXT DEFAULT '',
    created_at      TEXT DEFAULT (now()::text)
);

CREATE TABLE IF NOT EXISTS onboarding (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL UNIQUE,
    status          TEXT DEFAULT 'pending',
    domain_info     TEXT DEFAULT '',
    target_topics   TEXT DEFAULT '[]',
    competitors     TEXT DEFAULT '[]',
    brand_voice_notes TEXT DEFAULT '',
    cms_type        TEXT DEFAULT '',
    cms_access      INTEGER DEFAULT 0,
    approval_workflow TEXT DEFAULT '',
    notes           TEXT DEFAULT '',
    completed_steps TEXT DEFAULT '[]',
    created_at      TEXT DEFAULT (now()::text),
    updated_at      TEXT DEFAULT (now()::text)
);

CREATE TABLE IF NOT EXISTS billing_records (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    period_start    TEXT NOT NULL,
    period_end      TEXT NOT NULL,
    token_cost      REAL DEFAULT 0.0,
    scrape_count    INTEGER DEFAULT 0,
    content_count   INTEGER DEFAULT 0,
    publish_count   INTEGER DEFAULT 0,
    total_cost      REAL DEFAULT 0.0,
    notes           TEXT DEFAULT '',
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_billing_workspace ON billing_records(workspace_id);

CREATE TABLE IF NOT EXISTS competitors (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    domain          TEXT NOT NULL,
    name            TEXT DEFAULT '',
    citation_count  INTEGER DEFAULT 0,
    avg_citation_rate REAL DEFAULT 0.0,
    top_topics      TEXT DEFAULT '[]',
    last_checked    TEXT,
    created_at      TEXT DEFAULT (now()::text),
    UNIQUE(workspace_id, domain)
);
CREATE INDEX IF NOT EXISTS idx_competitors_workspace ON competitors(workspace_id);

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
    status          TEXT DEFAULT 'active',
    refresh_priority REAL DEFAULT 0.0,
    draft_id        TEXT DEFAULT '',
    cluster_id      TEXT DEFAULT '',
    notes           TEXT DEFAULT '',
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_assets_workspace ON content_assets(workspace_id);
CREATE INDEX IF NOT EXISTS idx_assets_status ON content_assets(status);

CREATE TABLE IF NOT EXISTS recommendations (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    rec_type        TEXT NOT NULL,
    title           TEXT NOT NULL,
    description     TEXT DEFAULT '',
    priority_score  REAL DEFAULT 0.0,
    cluster_id      TEXT DEFAULT '',
    source_data     TEXT DEFAULT '{}',
    status          TEXT DEFAULT 'new',
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_rec_workspace ON recommendations(workspace_id);
CREATE INDEX IF NOT EXISTS idx_rec_status ON recommendations(status);


-- ════════════════════════════════════════════════════════════════════════
-- brand_resolver.py :: _SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS brands (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    canonical_name  TEXT NOT NULL,
    display_name    TEXT DEFAULT '',
    domain          TEXT DEFAULT '',
    is_us           INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_brands_workspace ON brands(workspace_id);
CREATE INDEX IF NOT EXISTS idx_brands_us ON brands(workspace_id, is_us);

CREATE TABLE IF NOT EXISTS brand_aliases (
    canonical_id     TEXT NOT NULL,
    workspace_id     TEXT NOT NULL,
    alias            TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    source           TEXT DEFAULT 'auto',
    confidence       REAL DEFAULT 1.0,
    created_at       TEXT DEFAULT (now()::text),
    PRIMARY KEY (workspace_id, normalized_alias)
);
CREATE INDEX IF NOT EXISTS idx_aliases_canonical ON brand_aliases(canonical_id);


-- ════════════════════════════════════════════════════════════════════════
-- entity_graph.py :: SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS graph_nodes (
    id            TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL,
    node_type     TEXT NOT NULL,
    label         TEXT NOT NULL,
    meta          TEXT DEFAULT '{}',
    created_at    TEXT DEFAULT (now()::text),
    updated_at    TEXT DEFAULT (now()::text)
);
CREATE INDEX IF NOT EXISTS idx_gn_ws ON graph_nodes(workspace_id);
CREATE INDEX IF NOT EXISTS idx_gn_type ON graph_nodes(node_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gn_uniq ON graph_nodes(workspace_id, node_type, label);

CREATE TABLE IF NOT EXISTS graph_edges (
    id            TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL,
    from_id       TEXT NOT NULL,
    to_id         TEXT NOT NULL,
    edge_type     TEXT NOT NULL,
    weight        REAL DEFAULT 1.0,
    last_seen_at  TEXT DEFAULT (now()::text),
    evidence      TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_ge_ws ON graph_edges(workspace_id);
CREATE INDEX IF NOT EXISTS idx_ge_from ON graph_edges(workspace_id, from_id);
CREATE INDEX IF NOT EXISTS idx_ge_to ON graph_edges(workspace_id, to_id);
CREATE INDEX IF NOT EXISTS idx_ge_type ON graph_edges(edge_type);


-- ════════════════════════════════════════════════════════════════════════
-- alert_engine.py :: ALERT_SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS alert_rules (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL,
    name           TEXT NOT NULL,
    trigger_type   TEXT NOT NULL,
    params         TEXT DEFAULT '{}',
    channels       TEXT DEFAULT '[]',
    cooldown_minutes INTEGER DEFAULT 60,
    enabled        INTEGER DEFAULT 1,
    created_by     TEXT DEFAULT '',
    created_at     TEXT DEFAULT (now()::text),
    last_fired_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_ar_ws ON alert_rules(workspace_id);

CREATE TABLE IF NOT EXISTS alert_events (
    id             TEXT PRIMARY KEY,
    rule_id        TEXT NOT NULL,
    workspace_id   TEXT NOT NULL,
    fired_at       TEXT DEFAULT (now()::text),
    severity       TEXT DEFAULT 'info',
    title          TEXT NOT NULL,
    message        TEXT DEFAULT '',
    payload        TEXT DEFAULT '{}',
    delivered_to   TEXT DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_ae_ws ON alert_events(workspace_id);
CREATE INDEX IF NOT EXISTS idx_ae_rule ON alert_events(rule_id);


-- ════════════════════════════════════════════════════════════════════════
-- action_engine.py :: _SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS geo_actions (
    id                TEXT PRIMARY KEY,
    workspace_id      TEXT,
    action_type       TEXT,
    status            TEXT DEFAULT 'pending',
    title             TEXT,
    description       TEXT,
    source_kind       TEXT,
    source_id         TEXT,
    target_url        TEXT DEFAULT '',
    target_prompt_id  TEXT DEFAULT '',
    priority          REAL DEFAULT 50,
    estimated_impact  TEXT DEFAULT '',
    payload           TEXT DEFAULT '{}',
    generated_output  TEXT DEFAULT '',
    created_at        TEXT DEFAULT (now()::text),
    updated_at        TEXT DEFAULT (now()::text),
    completed_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_geoact_workspace ON geo_actions(workspace_id);
CREATE INDEX IF NOT EXISTS idx_geoact_status ON geo_actions(status);


-- ════════════════════════════════════════════════════════════════════════
-- backtest.py :: _SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS backtest_runs (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    scenario        TEXT DEFAULT '',
    created_at      TEXT DEFAULT (now()::text),
    baseline_score  REAL DEFAULT 0.0,
    projected_score REAL DEFAULT 0.0,
    lift            REAL DEFAULT 0.0,
    horizon_days    INTEGER DEFAULT 90,
    assumptions     TEXT DEFAULT '{}',
    result          TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_backtest_workspace ON backtest_runs(workspace_id);


-- ════════════════════════════════════════════════════════════════════════
-- scheduler.py :: _STATE_SCHEMA
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS scheduler_state (
    job_name        TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,
    last_run_at     TEXT,
    last_status     TEXT DEFAULT '',
    last_error      TEXT DEFAULT '',
    next_run_at     TEXT,
    PRIMARY KEY (job_name, workspace_id)
);
CREATE INDEX IF NOT EXISTS idx_sched_next ON scheduler_state(next_run_at);
