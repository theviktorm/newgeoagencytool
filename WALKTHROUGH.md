# Momentus AI — Operator Walkthrough & Tutorial

A practical guide for running the Generative Engine Optimization (GEO) platform end-to-end. Written for superadmins and content ops leads who want to understand every moving part and turn it into a daily workflow.

---

## 0. What this thing actually is

Momentus AI is a **multi-tenant content operations platform for Generative Engine Optimization** — optimizing what LLMs (ChatGPT, Perplexity, Gemini, Claude, Copilot) cite when users ask questions in your clients' categories.

The core loop:

```
Peec.ai citation data  →  scrape the cited pages  →  cluster by intent
     ↓
 Claude analyzes each cluster (gaps, opportunities, brief)
     ↓
 Claude generates draft content aligned to the brief
     ↓
 Human review (editor / reviewer / client)
     ↓
 Export or publish to WordPress / Webflow
     ↓
 Re-measure citations  →  report deltas  →  repeat
```

Everything is scoped to a **workspace** (one per client). Workspace ID doubles as project ID, so all pipeline tables key on the same value.

---

## 1. First-run setup

### 1.1 Install and start the backend

```bash
cd /Users/viktormozsa/Desktop/geo-engine
python -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
cp .env.example .env   # fill in GEO_ANTHROPIC_API_KEY at minimum
python -m backend.main
```

The server comes up at `http://localhost:8100` and serves both the API and the single-page React app.

### 1.2 Log in as superadmin

Navigate to `http://localhost:8100` and use the bootstrap credentials:

```
admin@momentus.ai
admin123
```

Change this password immediately via **Administration → Team → Edit → Role/Password** once real users exist. The default account is created by `init_auth()` on first DB boot, along with a "Default Workspace" that's mirrored into the `projects` table.

### 1.3 Environment variables you'll actually need

| Variable | Purpose | Required? |
|---|---|---|
| `GEO_ANTHROPIC_API_KEY` | Claude API for analysis + generation | **Yes** for anything beyond CSV import |
| `GEO_API_SECRET_KEY` | JWT signing key — set a real random string in prod | **Yes** in prod |
| `GEO_PEEC_API_KEY` | Direct Peec pull instead of CSV upload | Optional |
| `GEO_WORDPRESS_URL` / `GEO_WORDPRESS_USERNAME` / `GEO_WORDPRESS_APP_PASSWORD` | WordPress publishing | Optional |
| `GEO_WEBFLOW_API_TOKEN` / `GEO_WEBFLOW_COLLECTION_ID` | Webflow publishing | Optional |
| `GEO_CLAUDE_DAILY_COST_LIMIT` / `GEO_CLAUDE_MONTHLY_COST_LIMIT` | Budget caps enforced per call | Recommended |

---

## 2. The mental model — one workspace = one client

Before touching any page, understand this mapping. It's the single most important thing to know:

- **Workspace** (clients table) = client brand with voice, compliance, target models, target countries, target languages
- **Project** (projects table) = mirror of the workspace ID so all pipeline tables (peec_records, sources, clusters, drafts, analyses, exports, metrics) can join on `project_id`
- **Campaign** = an initiative inside a workspace (e.g. "Q2 AI-search launch push")
- **Superadmin** sees every workspace; **admin/editor/reviewer/client** are scoped to workspaces they've been added to via `workspace_members`

When you switch workspaces in the sidebar dropdown, the entire right pane re-fetches scoped to that `wsId`.

### Role matrix

| Role | Typical user | Can do |
|---|---|---|
| superadmin | You | Everything, cross-workspace, user management |
| admin | Account lead | Manage workspace, invite users, configure settings |
| editor | Content strategist | Import, scrape, analyze, generate, edit drafts |
| reviewer | Senior editor / QA | Approve / reject / request revision |
| client | Customer stakeholder | Read-only view of their own workspace |

Permission checks live in `backend/auth.py` via `ROLE_HIERARCHY` and are enforced by `require_role("editor")` etc on each endpoint.

---

## 3. The pipeline — page by page

The sidebar has 5 sections. Here's what each page does, what it's reading from, and when you'd actually use it.

### 3.1 Analytics — "how are we doing"

| Page | What it shows | Backing endpoint |
|---|---|---|
| **Overview** | Live pipeline counts (records/sources/clusters/drafts/exports), daily+monthly Claude cost, success metrics | `GET /api/dashboard/overview/{ws_id}` |
| **Performance** | Time-series of citations, rate, model breakdown | `GET /api/dashboard/performance/{ws_id}` |
| **Content Deltas** | Before/after comparisons — did published drafts move the needle? | `GET /api/dashboard/deltas/{ws_id}` |
| **Model Breakdown** | Per-model (ChatGPT vs Perplexity vs Gemini vs Claude) citation share | Client-side slice of sources data |

**Use Overview as your daily standup dashboard.** The cost panel also tells you whether a batch job has been running away.

### 3.2 Pipeline — the content factory

This is where 90% of the daily work happens. Each page is one station on the assembly line.

#### Data Import
- Paste CSV/TSV text **or** upload a file
- Accepts Peec exports in multiple column formats — the backend's `PEEC_FIELD_MAP` (models.py:336) auto-detects aliases like `url`/`source_url`/`page_url`, `citations`/`total_citations`/`mentions`, etc.
- On success the UI clears the textarea and auto-switches to the **Sources** tab so you see what you just imported
- POST `/api/peec/import/csv` with the file **and** `project_id` in the same multipart form body. The `project_id` is the active workspace id — this was the bug fixed in commit `cfed14b`

**Tip:** if you import and nothing shows, check the workspace selector dropdown at the top of the sidebar — a stale default workspace is the #1 reason records "disappear."

#### Sources
- Every URL cited by an LLM gets one row here, deduplicated
- Shows `total_citation_count`, `max_citation_rate`, which models cited it, and scrape status
- Filter by topic, sort by citations, and pick which ones to scrape
- This is your "inventory of cited pages" — these are the pages you need to understand and eventually replace or supplement

#### Scraper
- Select one or many sources → run scrape
- Stores full page HTML + extracted text on the source row (`scrape_status = 'scraped'`)
- Background job via `POST /api/batch/scrape`; watch progress in **Job Queue**

#### Analysis
- Clusters are computed from sources during import (grouped by URL pattern + topic)
- Each cluster lists its `url_count`, `prompt_count`, `total_citations`
- Clicking a cluster and hitting **Analyze** runs Claude against the scraped text of every source in the cluster and produces a gap analysis
- Hit **Generate Brief** to produce a structured content brief (headings, talking points, required facts, tone)
- Backing: `POST /api/analyze`, `POST /api/analyze/brief`, `GET /api/briefs/{cluster_id}`

#### Content Studio
- Picks a cluster + brief + template → generates a full draft via Claude
- Templates live in the Prompts page; default types: `blog-post`, `faq`, `comparison`, `how-to`
- Drafts flow into the review queue with `status = 'pending'`
- Backing: `POST /api/generate`, `GET /api/drafts/{project_id}`

#### Publishing
- Review drafts (approve / request revision / reject)
- Export to markdown/HTML/JSON (`POST /api/export`)
- Publish straight to WordPress or Webflow (`POST /api/publish`) if credentials are configured
- Connection check: `POST /api/publish/check/{cms}`

### 3.3 Operations — coordinating the team

| Page | Purpose |
|---|---|
| **Pipeline Activity** | Unified feed of imports/scrapes/analyses/drafts/exports for the last 7 days — great for standups |
| **Task Board** | Kanban for team work items tied to specific drafts/clusters — uses `ops_router /tasks/{ws_id}` |
| **Campaigns** | Group drafts + goals into a campaign (e.g. "Q2 launch"). Tracks start/end date, target metrics, owner |
| **Reports** | Generate weekly/monthly/custom PDF-able snapshots for clients. Can be scheduled (`scheduled_reports`) |
| **Playbooks** | Pre-built recipes for common situations ("lost position in comparison queries" etc.) — seeds the Recommendations engine |

### 3.4 Intelligence — where to focus next

| Page | Purpose |
|---|---|
| **Competitors** | Track competitor domains, see which ones are gaining citation share |
| **Recommendations** | Auto-generated next-best-actions from the playbook engine (refresh X page, add FAQ to Y cluster) |
| **Onboarding** | First-time client checklist — brand voice set? compliance rules? competitors listed? |

### 3.5 Administration — the superadmin toolkit

| Page | Purpose |
|---|---|
| **Workspaces** | Create new client workspaces, set brand voice, compliance rules, target models/countries/languages. Each new workspace is also mirrored into `projects` |
| **Team** | Add users, set roles, invite by email, deactivate accounts |
| **Billing** | Monthly cost per workspace (currently Claude API metering) |
| **Job Queue** | All running batch jobs (scrape/analyze), status, retry |
| **Automations** | Trigger-based rules: "when a new record arrives, auto-scrape its source" etc. (`ops_router /automations/{ws_id}`) |
| **Prompts** | System prompts / templates powering analysis and generation |
| **Monitoring** | Usage history, Claude token/cost trends, rate limit signals |
| **Audit Log** | Every user action: login, create, update, delete, export, publish |
| **Settings** | Workspace-level settings editor (brand voice, compliance, logo/colors) |

---

## 4. The daily workflow — a typical day as a content ops lead

Here's how I'd drive a single client workspace end-to-end in one working day.

### Morning — ingest and orient (15 min)

1. **Log in**, pick the client's workspace from the sidebar dropdown
2. **Overview** — check today's pipeline counts and yesterday's Claude cost
3. **Pipeline Activity** — skim the 7-day feed to see what's in flight
4. If a fresh Peec export arrived, **Data Import** → paste/upload → confirm auto-switch to Sources

### Mid-morning — triage and scrape (30 min)

5. **Sources** — sort by `total_citation_count` desc. Anything with 3+ citations that isn't scraped yet → select → **Scrape** (kicks off `POST /api/batch/scrape`)
6. Open **Job Queue** to confirm the scrape job is running, not stuck
7. Come back in 5 min; scraped pages show `scrape_status = 'scraped'` in green

### Afternoon — analyze clusters (45 min)

8. **Analysis** — clusters auto-populated from the import
9. Pick the 3-5 highest-value clusters (most citations × most URLs)
10. For each: **Analyze** (Claude runs the gap analysis) → **Generate Brief**
11. Review each brief. If the brief is wrong, tweak the cluster composition or re-run

### Late afternoon — generate drafts (30 min)

12. **Content Studio** — pick cluster → pick template → **Generate**
13. Drafts land in the queue with `status = 'pending'`
14. Light edit on each draft to match brand voice. Save.

### End of day — route for review

15. **Task Board** — create a review task per draft, assign to reviewer
16. Reviewer later: **Publishing** → approve / request revision
17. Once approved: **Publishing → Export** (for manual handoff) or **Publish** (direct to WP/Webflow)

### Weekly

- **Reports** → generate a weekly snapshot for the client
- **Content Deltas** → check which published pieces shifted citation rates
- **Competitors** → note any new competitor gains
- **Recommendations** → work the auto-generated punch list

---

## 5. Command-line cheat sheet (for power users)

When the UI is slow or you want to script something, talk directly to the API. Every endpoint needs `Authorization: Bearer <token>` except `/auth/login`.

```bash
# Log in, capture token
TOKEN=$(curl -s -X POST http://localhost:8100/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"admin@momentus.ai","password":"admin123"}' \
  | python -c 'import json,sys;print(json.load(sys.stdin)["data"]["token"])')

# List workspaces
curl -s http://localhost:8100/api/workspaces/ \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool

WS=ws-xxxxxxxx   # pick from above

# Dashboard overview
curl -s "http://localhost:8100/api/dashboard/overview/$WS" \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool

# CSV import (the key bit: project_id in the FORM body)
curl -s -X POST http://localhost:8100/api/peec/import/csv \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@peec_export.csv" \
  -F "project_id=$WS"

# Pull records for this workspace
curl -s "http://localhost:8100/api/peec/records/$WS" \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool

# Kick a batch scrape
curl -s -X POST http://localhost:8100/api/batch/scrape \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"project_id\":\"$WS\",\"source_ids\":[\"src-abc\",\"src-def\"]}"

# Analyze a cluster
curl -s -X POST http://localhost:8100/api/analyze \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"cluster_id":"clu-xyz"}'

# Generate a draft
curl -s -X POST http://localhost:8100/api/generate \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"cluster_id":"clu-xyz","template_type":"blog-post"}'
```

All responses are wrapped in `{success, data, error?, meta}` — always read `r.data`, never the bare list. This was the bug pattern fixed across ~15 frontend call sites in commit `cfed14b`.

---

## 6. Troubleshooting — the things that have already bitten us

| Symptom | Cause | Fix |
|---|---|---|
| "I uploaded a CSV and nothing happened" | Before `cfed14b`: `project_id` was routed to query string, fell back to `"default"` | Fixed — ensure you're on latest main |
| Sources page shows 0 citations | Frontend read wrong column name | Fixed — now reads `total_citation_count` |
| Cluster sidebar shows "0 records · mixed" | Frontend read `record_count`/`intent` which don't exist | Fixed — now reads `url_count`/`prompt_count`/`total_citations` |
| Records table all "—" | Frontend bound to `prompt`/`model` not `title`/`model_source` | Fixed |
| 404 on `/api/dashboard/activity/...` | Wrong path — real route is `/api/dashboard/pipeline-activity/{ws_id}` | N/A — frontend already uses the right path |
| Claude API error "daily cost limit exceeded" | `GEO_CLAUDE_DAILY_COST_LIMIT` hit | Raise the limit in `.env` or wait until tomorrow |
| Login rejected as superadmin | Password was changed and forgotten | SQLite: `sqlite3 geo_engine.db "UPDATE users SET password_hash='...', salt='...' WHERE email='admin@momentus.ai'"` — or reseed by deleting the row; `init_auth()` will recreate |
| "Workspace not found" after creating one | Workspace wasn't mirrored into `projects` | Fixed in `cfed14b` — `init_auth` and `create_workspace` now do the mirror |

---

## 7. Architecture in 30 seconds

```
┌───────────────────────────────────────────────────────────────┐
│  dashboard.jsx  (React 18 standalone + Babel, no build step) │
│  Served by FastAPI at /                                       │
└─────────────────────┬─────────────────────────────────────────┘
                      │ fetch() with Bearer JWT
┌─────────────────────▼─────────────────────────────────────────┐
│  FastAPI  (backend/main.py)                                  │
│  ┌─────────────┬────────────────┬─────────────────────────┐  │
│  │ auth_router │ workspace_r.   │ dashboard_router        │  │
│  │ /api/auth   │ /api/workspaces│ /api/dashboard          │  │
│  ├─────────────┼────────────────┼─────────────────────────┤  │
│  │ ops_router  │ pipeline eps   │ peec_connector          │  │
│  │ /api/ops    │ /api/*         │ + claude_engine         │  │
│  └─────────────┴────────────────┴─────────────────────────┘  │
└─────────────────────┬─────────────────────────────────────────┘
                      │ aiosqlite (pooled)
              ┌───────▼──────────┐       ┌────────────────────┐
              │  geo_engine.db   │       │  Anthropic API     │
              │  (SQLite)        │       │  (Claude Sonnet)   │
              └──────────────────┘       └────────────────────┘
```

Key tables: `users`, `workspaces`, `workspace_members`, `projects`, `peec_records`, `sources`, `clusters`, `analyses`, `briefs`, `drafts`, `exports`, `measurements`, `campaigns`, `tasks`, `automations`, `batch_jobs`, `audit_log`.

---

## 8. What to build next (roadmap)

- Automations: wire the `automations` table to a background worker so "auto-scrape on import" actually fires
- Scheduled reports: the schema exists (`scheduled_reports`) but no cron worker yet
- Per-draft version history (currently overwrites on save)
- Client portal: a restricted view for `client` role that hides the pipeline and shows only published drafts + deltas
- Bulk re-analyze on schema/brand-voice change
- S3 / object storage for scraped HTML (currently in SQLite BLOB — fine for 1k pages, not 100k)

---

**One-line summary:** Momentus AI turns a Peec citation export into briefed, drafted, reviewed, and published content — one pipeline per client workspace — with Claude doing the analysis and generation at every step. Your job as operator is to curate which clusters are worth the Claude spend and to be the last-mile editorial brain before publish.
