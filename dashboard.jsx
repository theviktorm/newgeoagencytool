/* ═══════════════════════════════════════════════════════════════
   MOMENTUS AI — PERFORMANCE DASHBOARD & ADMIN PANEL
   Multi-client workspace, auth, metrics, reporting
   ═══════════════════════════════════════════════════════════════ */

const { useState, useEffect, useReducer, useCallback, useRef, useMemo, createContext, useContext } = React;

const API = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
  ? 'http://localhost:8100'
  : window.location.origin;

// ═══════════════════════════════════════════════════════════════
// CONTEXT & AUTH STATE
// ═══════════════════════════════════════════════════════════════

const AuthContext = createContext(null);

function useAuth() {
  return useContext(AuthContext);
}

const initialState = {
  user: null,
  token: null,
  workspaces: [],
  activeWorkspace: null,
  view: 'warroom',
  overview: null,
  performance: null,
  deltas: null,
  activity: null,
  reports: [],
  metrics: [],
  users: [],
  auditLog: [],
  campaigns: [],
  loading: {},
  error: null,
  modal: null,
};

function reducer(state, action) {
  switch (action.type) {
    case 'LOGIN':
      return { ...state, user: action.user, token: action.token, workspaces: action.workspaces, activeWorkspace: action.workspaces[0] || null };
    case 'LOGOUT':
      return { ...initialState };
    case 'SET_VIEW':
      return { ...state, view: action.view };
    case 'SET_WORKSPACE':
      return { ...state, activeWorkspace: action.workspace, overview: null, performance: null, deltas: null, activity: null, reports: [], metrics: [], campaigns: [] };
    case 'SET_WORKSPACES':
      return { ...state, workspaces: action.workspaces };
    case 'SET_OVERVIEW':
      return { ...state, overview: action.data };
    case 'SET_PERFORMANCE':
      return { ...state, performance: action.data };
    case 'SET_DELTAS':
      return { ...state, deltas: action.data };
    case 'SET_ACTIVITY':
      return { ...state, activity: action.data };
    case 'SET_REPORTS':
      return { ...state, reports: action.data };
    case 'SET_METRICS':
      return { ...state, metrics: action.data };
    case 'SET_USERS':
      return { ...state, users: action.data };
    case 'SET_AUDIT':
      return { ...state, auditLog: action.data };
    case 'SET_CAMPAIGNS':
      return { ...state, campaigns: action.data };
    case 'SET_LOADING':
      return { ...state, loading: { ...state.loading, [action.key]: action.value } };
    case 'SET_ERROR':
      return { ...state, error: action.error };
    case 'SET_MODAL':
      return { ...state, modal: action.modal };
    default:
      return state;
  }
}

// ═══════════════════════════════════════════════════════════════
// API HELPERS
// ═══════════════════════════════════════════════════════════════

// Global auth error callback — set by App to trigger logout on 401
let _onAuthError = null;
function setAuthErrorHandler(fn) { _onAuthError = fn; }

async function api(path, opts = {}, token = null) {
  const headers = {};
  if (!(opts.body instanceof FormData)) {
    headers['Content-Type'] = 'application/json';
  }
  if (token) headers['Authorization'] = `Bearer ${token}`;

  // Add 30s timeout to all requests
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);

  try {
    const res = await fetch(`${API}${path}`, {
      ...opts,
      headers: { ...headers, ...(opts.headers || {}) },
      signal: controller.signal,
    });
    clearTimeout(timeout);

    // Handle 401 — token expired or invalid
    if (res.status === 401) {
      if (_onAuthError) _onAuthError();
      throw new Error('Session expired. Please sign in again.');
    }

    const data = await res.json();
    if (!res.ok) {
      const msg = data.detail || data.error || `HTTP ${res.status}`;
      throw new Error(typeof msg === 'string' ? msg : JSON.stringify(msg));
    }
    return data;
  } catch (err) {
    clearTimeout(timeout);
    if (err.name === 'AbortError') {
      throw new Error('Request timed out. The server may be busy.');
    }
    if (err.message?.includes('Failed to fetch')) {
      throw new Error('Backend not reachable. Check that the API server is running.');
    }
    throw err;
  }
}

// ═══════════════════════════════════════════════════════════════
// LOGIN SCREEN
// ═══════════════════════════════════════════════════════════════

function LoginScreen({ onLogin }) {
  const [email, setEmail] = useState('admin@momentus.ai');
  const [password, setPassword] = useState('admin123');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [mode, setMode] = useState('login'); // login | register

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const endpoint = mode === 'login' ? '/api/auth/login' : '/api/auth/register';
      const body = mode === 'login'
        ? { email, password }
        : { email, password, name: email.split('@')[0] };
      const res = await api(endpoint, { method: 'POST', body: JSON.stringify(body) });
      if (res.success) {
        onLogin(res.data);
      } else {
        setError(res.error || 'Authentication failed');
      }
    } catch (err) {
      setError(err.message);
    }
    setLoading(false);
  };

  return (
    <div className="login-screen">
      <div className="login-card">
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
          <div style={{
            width: 40, height: 40, borderRadius: 10,
            background: 'linear-gradient(135deg, #3B82F6, #8B5CF6)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontFamily: 'var(--font-mono)', fontWeight: 700, fontSize: 16, color: 'white'
          }}>M</div>
          <div>
            <h1 style={{ fontSize: 20, marginBottom: 0 }}>Momentus AI</h1>
            <div className="subtitle" style={{ marginBottom: 0 }}>Performance Dashboard</div>
          </div>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="form-group">
            <label className="form-label">Email</label>
            <input className="form-input" type="email" value={email}
              onChange={e => setEmail(e.target.value)} placeholder="you@company.com" />
          </div>
          <div className="form-group">
            <label className="form-label">Password</label>
            <input className="form-input" type="password" value={password}
              onChange={e => setPassword(e.target.value)} placeholder="Enter password" />
          </div>

          {error && (
            <div style={{ background: 'var(--rose-dim)', color: 'var(--rose)', padding: '8px 12px',
              borderRadius: 6, fontSize: 12, marginBottom: 14 }}>
              {error}
            </div>
          )}

          <button className="btn btn-primary" type="submit"
            style={{ width: '100%', justifyContent: 'center', padding: '10px', fontSize: 13 }}
            disabled={loading}>
            {loading ? 'Connecting...' : mode === 'login' ? 'Sign In' : 'Create Account'}
          </button>

          <div style={{ textAlign: 'center', marginTop: 14, fontSize: 12, color: 'var(--text-tertiary)' }}>
            {mode === 'login' ? (
              <span>No account? <a style={{ color: 'var(--blue)', cursor: 'pointer' }}
                onClick={() => setMode('register')}>Register</a></span>
            ) : (
              <span>Have an account? <a style={{ color: 'var(--blue)', cursor: 'pointer' }}
                onClick={() => setMode('login')}>Sign in</a></span>
            )}
          </div>
        </form>

        <div style={{ marginTop: 20, padding: '10px 12px', background: 'var(--bg-raised)',
          borderRadius: 6, fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
          Default: admin@momentus.ai / admin123
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// SIDEBAR
// ═══════════════════════════════════════════════════════════════

const NAV_ITEMS = [
  // \u2500\u2500 Command Center: daily outcomes \u2500\u2500
  { section: 'Command Center', items: [
    { id: 'warroom', label: 'Live War Room', icon: '\u25ce' },
    { id: 'battlefield', label: 'Prompt Battlefield', icon: '\u2691' },
    { id: 'revenue', label: 'Revenue Priority', icon: '\u20AC' },
    { id: 'authority', label: 'Authority Score', icon: '\u2605' },
  ]},
  { section: 'Intelligence', items: [
    { id: 'citation_intel', label: 'Citation Intel', icon: '\u25c8' },
    { id: 'attack_map', label: 'Attack Map', icon: '\u2694' },
    { id: 'graph', label: 'Authority Graph', icon: '\u2042' },
    { id: 'journey', label: 'Buyer Journey', icon: '\u27ff' },
    { id: 'aio', label: 'AI Overview', icon: '\u25C9' },
    { id: 'competitors', label: 'Competitors', icon: '\u2691' },
    { id: 'competitor_profiles', label: 'Competitor Profiles', icon: '\u2609' },
  ]},
  { section: 'Execute', items: [
    { id: 'actions', label: 'Action Engine', icon: '\u2699' },
    { id: 'content', label: 'Content Studio', icon: '\u270E' },
    { id: 'schema_engine', label: 'Schema Engine', icon: '\u232c' },
    { id: 'metadata_studio', label: 'Metadata Studio', icon: '\u25a7' },
    { id: 'reddit', label: 'Reddit Command', icon: '\u2634' },
    { id: 'youtube', label: 'YouTube GEO', icon: '\u25b6' },
    { id: 'publishing', label: 'Publishing', icon: '\u21EA' },
  ]},
  { section: 'Data', items: [
    { id: 'import', label: 'Data Import', icon: '\u21E9' },
    { id: 'sources', label: 'Sources', icon: '\u25C9' },
    { id: 'analysis', label: 'Cluster Analysis', icon: '\u2B21' },
    { id: 'brands', label: 'Brand Manager', icon: '\u24B7' },
  ]},
  { section: 'Operations', items: [
    { id: 'alerts', label: 'Alerts', icon: '\u26A0' },
    { id: 'backtest', label: 'GEO Sandbox', icon: '\u232c' },
    { id: 'report', label: 'Comparative Report', icon: '\u2637' },
    { id: 'jobs', label: 'Job Queue', icon: '\u21BB' },
  ]},
  { section: 'Admin', items: [
    { id: 'workspaces', label: 'Workspaces', icon: '\u2302' },
    { id: 'users', label: 'Team', icon: '\u263A' },
    { id: 'metrics', label: 'Metric Dictionary', icon: '?' },
    { id: 'integrations', label: 'Integrations', icon: '\u2630' },
    { id: 'settings', label: 'Settings', icon: '\u2699' },
    { id: 'audit', label: 'Audit Log', icon: '\u2318' },
  ]},
];

function Sidebar({ state, dispatch }) {
  return (
    <div className="sidebar">
      <div className="sidebar-logo">
        <div className="logo-mark">M</div>
        <div>
          <div className="logo-text">Momentus AI</div>
          <div className="logo-sub">Dashboard</div>
        </div>
      </div>

      {/* Workspace selector */}
      <div style={{ padding: '12px 16px' }}>
        <select className="ws-selector"
          value={state.activeWorkspace?.id || ''}
          onChange={e => {
            const ws = state.workspaces.find(w => w.id === e.target.value);
            if (ws) dispatch({ type: 'SET_WORKSPACE', workspace: ws });
          }}>
          {state.workspaces.map(ws => (
            <option key={ws.id} value={ws.id}>{ws.name}</option>
          ))}
        </select>
      </div>

      {NAV_ITEMS.map(section => (
        <div className="sidebar-section" key={section.section}>
          <div className="sidebar-section-label">{section.section}</div>
          {section.items.map(item => (
            <div key={item.id}
              className={`sidebar-item ${state.view === item.id ? 'active' : ''}`}
              onClick={() => dispatch({ type: 'SET_VIEW', view: item.id })}>
              <span className="item-icon">{item.icon}</span>
              <span>{item.label}</span>
            </div>
          ))}
        </div>
      ))}

      <div className="sidebar-bottom">
        <div className="sidebar-item" onClick={() => {
          localStorage.removeItem('geo_token');
          dispatch({ type: 'LOGOUT' });
        }}>
          <span className="item-icon">{'\u2190'}</span>
          <span>Sign Out</span>
        </div>
        <div style={{ fontSize: 10, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)',
          padding: '8px 8px 0', textAlign: 'center' }}>
          Momentus AI v1.0
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// HEADER
// ═══════════════════════════════════════════════════════════════

function Header({ state }) {
  const viewLabels = {
    overview: 'Overview',
    performance: 'Performance Metrics',
    deltas: 'Content Deltas',
    models: 'Model Breakdown',
    import: 'Data Import',
    sources: 'Source Discovery',
    scraper: 'Scraper',
    analysis: 'Analysis Engine',
    content: 'Content Studio',
    publishing: 'Publishing & Delivery',
    pipeline: 'Pipeline Activity',
    tasks: 'Task Board',
    campaigns: 'Campaigns',
    reports: 'Reports',
    playbooks: 'Playbooks & SOPs',
    competitors: 'Competitor Analysis',
    recommendations: 'Recommendations',
    onboarding: 'Client Onboarding',
    workspaces: 'Workspaces',
    users: 'Team Management',
    billing: 'Billing & Usage',
    jobs: 'Job Queue',
    automations: 'Automation Rules',
    prompts: 'Prompt Templates',
    monitoring: 'System Monitoring',
    audit: 'Audit Log',
    settings: 'Workspace Settings',
  };

  const initials = (state.user?.name || state.user?.email || '??')
    .split(/[\s@]/).map(s => s[0]?.toUpperCase()).join('').slice(0, 2);

  return (
    <div className="main-header">
      <span className="page-title">{viewLabels[state.view] || state.view}</span>
      {state.activeWorkspace && (
        <span style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
          {state.activeWorkspace.name}
        </span>
      )}
      <div className="header-meta">
        <span className="status-dot" />
        <span>API Connected</span>
        <div className="user-pill">
          <div className="user-avatar">{initials}</div>
          <span>{state.user?.name || state.user?.email}</span>
          <span className="badge blue" style={{ marginLeft: 4 }}>{state.user?.role}</span>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// PAGINATION HELPER
// ═══════════════════════════════════════════════════════════════

function usePagination(items, perPage = 25) {
  const [page, setPage] = useState(0);
  const totalPages = Math.max(1, Math.ceil((items?.length || 0) / perPage));
  const safeP = Math.min(page, totalPages - 1);
  const paged = (items || []).slice(safeP * perPage, (safeP + 1) * perPage);
  return { page: safeP, setPage, totalPages, paged, total: items?.length || 0 };
}

function PaginationBar({ page, totalPages, setPage, total, label = 'items' }) {
  if (totalPages <= 1) return null;
  return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', borderTop: '1px solid var(--border-subtle)', fontSize: 11, color: 'var(--text-tertiary)' }}>
      <span>{total} {label}</span>
      <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
        <button className="btn btn-sm" disabled={page <= 0} onClick={() => setPage(page - 1)}>Prev</button>
        <span style={{ fontFamily: 'var(--font-mono)', padding: '0 8px' }}>{page + 1} / {totalPages}</span>
        <button className="btn btn-sm" disabled={page >= totalPages - 1} onClick={() => setPage(page + 1)}>Next</button>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// CHART COMPONENTS
// ═══════════════════════════════════════════════════════════════

function BarChart({ data, color = 'blue', height = 160, labelKey = 'date', valueKey = 'value' }) {
  if (!data?.length) return <div className="empty-state"><div className="empty-icon">{'\u2637'}</div><h4>No data yet</h4></div>;
  const maxVal = Math.max(...data.map(d => d[valueKey] || 0), 1);
  return (
    <div className="chart-area" style={{ height }}>
      {data.map((d, i) => {
        const pct = ((d[valueKey] || 0) / maxVal) * 100;
        const label = d[labelKey]?.slice(-5) || '';
        return (
          <div key={i} className={`chart-bar ${color}`}
            style={{ height: `${Math.max(pct, 2)}%` }}
            data-label={label}
            title={`${label}: ${(d[valueKey] || 0).toLocaleString()}`} />
        );
      })}
    </div>
  );
}

function MiniSparkline({ values, color = 'var(--blue)', width = 120, height = 32 }) {
  if (!values?.length) return null;
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const range = max - min || 1;
  const points = values.map((v, i) => {
    const x = (i / Math.max(values.length - 1, 1)) * width;
    const y = height - ((v - min) / range) * (height - 4) - 2;
    return `${x},${y}`;
  }).join(' ');
  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      <polyline points={points} fill="none" stroke={color} strokeWidth="1.5" strokeLinejoin="round" />
    </svg>
  );
}

function DonutChart({ segments, size = 140 }) {
  if (!segments?.length || segments.every(s => !s.value)) {
    return <div style={{ width: size, height: size, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--text-muted)', fontSize: 11 }}>No data</div>;
  }
  const total = segments.reduce((s, seg) => s + (seg.value || 0), 0) || 1;
  let cumulative = 0;
  const r = size / 2;
  const innerR = r * 0.6;
  return (
    <div style={{ position: 'relative', width: size, height: size }}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        {segments.map((seg, i) => {
          const startAngle = (cumulative / total) * 2 * Math.PI - Math.PI / 2;
          cumulative += seg.value;
          const endAngle = (cumulative / total) * 2 * Math.PI - Math.PI / 2;
          const largeArc = seg.value / total > 0.5 ? 1 : 0;
          const path = [
            `M ${r + innerR * Math.cos(startAngle)} ${r + innerR * Math.sin(startAngle)}`,
            `L ${r + r * Math.cos(startAngle)} ${r + r * Math.sin(startAngle)}`,
            `A ${r} ${r} 0 ${largeArc} 1 ${r + r * Math.cos(endAngle)} ${r + r * Math.sin(endAngle)}`,
            `L ${r + innerR * Math.cos(endAngle)} ${r + innerR * Math.sin(endAngle)}`,
            `A ${innerR} ${innerR} 0 ${largeArc} 0 ${r + innerR * Math.cos(startAngle)} ${r + innerR * Math.sin(startAngle)}`,
            'Z'
          ].join(' ');
          return <path key={i} d={path} fill={seg.color} opacity="0.85" />;
        })}
      </svg>
      <div style={{
        position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%, -50%)',
        textAlign: 'center',
      }}>
        <div style={{ fontFamily: 'var(--font-mono)', fontSize: 18, fontWeight: 600 }}>{total}</div>
        <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>Total</div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// EXPLAINABILITY — Phase 1 reusable components
// ═══════════════════════════════════════════════════════════════

// Module-level cache so metric definitions are fetched once per session.
const _metricDictCache = {};

function _titleCase(s) {
  return String(s || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase());
}

// 1. ConfidenceBadge — small pill mapping a confidence/source level to a color.
const CONFIDENCE_COLORS = {
  verified: 'var(--emerald)',
  manually_confirmed: 'var(--emerald)',
  imported: 'var(--blue)',
  claude_analyzed: 'var(--purple)',
  scraped: 'var(--cyan)',
  estimated: 'var(--amber)',
  seeded: 'var(--amber)',
  stale: 'var(--text-muted)',
  needs_review: 'var(--text-muted)',
  error: 'var(--rose)',
};

function ConfidenceBadge({ level }) {
  const key = String(level || 'estimated').toLowerCase();
  const color = CONFIDENCE_COLORS[key] || 'var(--text-muted)';
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', padding: '1px 7px',
      borderRadius: 9, fontSize: 9, fontWeight: 600, fontFamily: 'var(--font-mono)',
      letterSpacing: '0.02em', color, background: `${color}22`, whiteSpace: 'nowrap',
    }}>{_titleCase(key)}</span>
  );
}

// 2. OwnershipLevelBadge — 0–5 level + label, colored by status.
const OWNERSHIP_LEVELS = {
  not_visible: 0, mentioned: 1, listed: 2, top3: 3, co_owned: 4,
  recommended: 4, owned: 5, lost: 0, volatile: 2, emerging: 1,
};
const OWNERSHIP_COLORS = {
  owned: 'var(--emerald)', recommended: 'var(--emerald)', co_owned: 'var(--cyan)',
  top3: 'var(--blue)', listed: 'var(--amber)', mentioned: 'var(--amber)',
  not_visible: 'var(--text-muted)', lost: 'var(--rose)', volatile: 'var(--purple)',
  emerging: 'var(--amber)',
};

function OwnershipLevelBadge({ level, status }) {
  const key = String(status || level || '').toLowerCase();
  if (!key) {
    return <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>—</span>;
  }
  const color = OWNERSHIP_COLORS[key] || 'var(--text-muted)';
  const num = level != null && !isNaN(Number(level)) ? Number(level) : (OWNERSHIP_LEVELS[key] ?? '—');
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 4, padding: '1px 7px',
      borderRadius: 9, fontSize: 10, fontWeight: 600, fontFamily: 'var(--font-mono)',
      color, background: `${color}22`, whiteSpace: 'nowrap',
    }}>
      <b style={{ fontSize: 11 }}>L{num}</b>
      <span>{_titleCase(key)}</span>
    </span>
  );
}

// 3. MetricTooltip — "?" icon that opens a modal explaining a metric.
function MetricTooltip({ metricKey }) {
  const { token } = useContext(AuthContext);
  const [open, setOpen] = useState(false);
  const [data, setData] = useState(_metricDictCache[metricKey] || null);
  const [error, setError] = useState('');
  const loading = open && !data && !error;

  useEffect(() => {
    if (!open || data || error) return;
    if (_metricDictCache[metricKey]) { setData(_metricDictCache[metricKey]); return; }
    let cancelled = false;
    api(`/api/metrics/dictionary/${metricKey}`, {}, token)
      .then(r => {
        const d = r.data || r;
        _metricDictCache[metricKey] = d;
        if (!cancelled) setData(d);
      })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load'); });
    return () => { cancelled = true; };
  }, [open, metricKey, token]);

  useEffect(() => {
    if (!open) return;
    const onKey = e => { if (e.key === 'Escape') setOpen(false); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open]);

  return (
    <React.Fragment>
      <sup
        onClick={e => { e.stopPropagation(); setOpen(true); }}
        title="What is this?"
        style={{
          cursor: 'help', marginLeft: 3, fontSize: 9, fontWeight: 700,
          color: 'var(--text-muted)', border: '1px solid var(--border-default)',
          borderRadius: '50%', width: 13, height: 13, display: 'inline-flex',
          alignItems: 'center', justifyContent: 'center', lineHeight: 1, userSelect: 'none',
        }}
      >?</sup>
      {open && (
        <div className="modal-overlay" onClick={() => setOpen(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h3>{data ? data.name : _titleCase(metricKey)}</h3>
              <button className="modal-close" onClick={() => setOpen(false)}>{'×'}</button>
            </div>
            <div className="modal-body">
              {loading && <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading…</div>}
              {error && <div style={{ color: 'var(--rose)', fontSize: 12 }}>{error}</div>}
              {data && (
                <div style={{ display: 'grid', gap: 12, fontSize: 12 }}>
                  <div style={{ color: 'var(--text-secondary)', lineHeight: 1.55 }}>{data.definition}</div>
                  {data.formula && (
                    <div>
                      <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 3 }}>Formula</div>
                      <code style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--cyan)', background: 'var(--bg-raised)', padding: '6px 8px', borderRadius: 4, display: 'block', whiteSpace: 'pre-wrap' }}>{data.formula}</code>
                    </div>
                  )}
                  {(data.data_sources || []).length > 0 && (
                    <div>
                      <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Data sources</div>
                      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                        {data.data_sources.map(s => <span key={s} className="badge">{s}</span>)}
                      </div>
                    </div>
                  )}
                  <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'center' }}>
                    {data.refresh_frequency && (
                      <div><span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Refresh: </span><span style={{ color: 'var(--text-secondary)' }}>{data.refresh_frequency}</span></div>
                    )}
                    {data.confidence && (
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                        <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Confidence</span>
                        <ConfidenceBadge level={data.confidence} />
                      </div>
                    )}
                  </div>
                  {data.low_score_meaning && (
                    <div>
                      <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 3 }}>What a low score means</div>
                      <div style={{ color: 'var(--text-secondary)', lineHeight: 1.55 }}>{data.low_score_meaning}</div>
                    </div>
                  )}
                  {data.recommended_action && (
                    <div style={{ borderLeft: '2px solid var(--emerald)', paddingLeft: 10 }}>
                      <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 3 }}>Recommended action</div>
                      <div style={{ color: 'var(--text-primary)', lineHeight: 1.55 }}>{data.recommended_action}</div>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </React.Fragment>
  );
}

// 4. RevenueLogicModal — explains how a revenue estimate was derived for a prompt.
function RevenueLogicModal({ wsId, promptId, onClose }) {
  const { token } = useContext(AuthContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const loading = !data && !error;

  useEffect(() => {
    if (!wsId || !promptId) return;
    let cancelled = false;
    api(`/api/revenue/${wsId}/breakdown/${promptId}`, {}, token)
      .then(r => { if (!cancelled) setData(r.data || r); })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load'); });
    return () => { cancelled = true; };
  }, [wsId, promptId, token]);

  useEffect(() => {
    const onKey = e => { if (e.key === 'Escape') onClose && onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const eur = v => `€${Math.round(Number(v) || 0).toLocaleString()}`;
  const row = (label, value, opts = {}) => (
    <tr style={opts.bold ? { borderTop: '1px solid var(--border-default)' } : null}>
      <td style={{ padding: '6px 8px', color: opts.bold ? 'var(--text-primary)' : 'var(--text-secondary)', fontWeight: opts.bold ? 700 : 400 }}>{label}</td>
      <td style={{ padding: '6px 8px', textAlign: 'right', fontFamily: 'var(--font-mono)', fontWeight: opts.bold ? 700 : 500, color: opts.color || 'var(--text-primary)' }}>{value}</td>
    </tr>
  );

  return (
    <div className="modal-overlay" onClick={() => onClose && onClose()}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Revenue logic</h3>
          <button className="modal-close" onClick={() => onClose && onClose()}>{'×'}</button>
        </div>
        <div className="modal-body">
          <div style={{ fontSize: 10, color: 'var(--amber)', marginBottom: 10, fontStyle: 'italic' }}>
            Estimated — not real conversion data.
          </div>
          {loading && <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading…</div>}
          {error && <div style={{ color: 'var(--rose)', fontSize: 12 }}>{error}</div>}
          {data && (
            <React.Fragment>
              <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
                <tbody>
                  {row('Base value', eur(data.base_value))}
                  {row(`Buyer stage — ${data.buyer_stage || '—'}`, `×${(Number(data.stage_weight) || 0).toFixed(2)}`)}
                  {row('Ownership gap', `×${(Number(data.ownership_gap_factor) || 0).toFixed(2)}`)}
                  {row('Competitor strength', data.competitor_strength != null ? String(data.competitor_strength) : '—')}
                  {row('Estimated customer value', eur(data.estimated_customer_value))}
                  {row('→ Final revenue estimate', eur(data.final_revenue_estimate), { bold: true, color: 'var(--amber)' })}
                  {row('Priority score', data.priority_score != null ? Math.round(data.priority_score) : '—')}
                </tbody>
              </table>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 12 }}>
                <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Confidence</span>
                <ConfidenceBadge level={data.confidence} />
                {data.data_source && <span className="badge">{data.data_source}</span>}
              </div>
              {data.formula_text && (
                <code style={{ display: 'block', marginTop: 12, fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-secondary)', background: 'var(--bg-raised)', padding: '8px 10px', borderRadius: 4, whiteSpace: 'pre-wrap', lineHeight: 1.5 }}>{data.formula_text}</code>
              )}
            </React.Fragment>
          )}
        </div>
      </div>
    </div>
  );
}

// 5. ScoreBreakdownCard — full authority-score decomposition with levers.
function ScoreBreakdownCard({ wsId }) {
  const { token } = useContext(AuthContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const loading = !data && !error;

  useEffect(() => {
    if (!wsId) return;
    let cancelled = false;
    setData(null); setError('');
    api(`/api/authority/${wsId}/breakdown`, {}, token)
      .then(r => { if (!cancelled) setData(r.data || r); })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load'); });
    return () => { cancelled = true; };
  }, [wsId, token]);

  const total = data ? Math.round(((data.found || 0) / (data.total || 1)) * 100) : null;
  const components = (data && data.components) || [];

  return (
    <div className="card">
      <div className="card-header">Score breakdown — how your Authority Score is built</div>
      {loading && <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading breakdown…</div>}
      {error && <div style={{ color: 'var(--rose)', fontSize: 12 }}>{error}</div>}
      {data && components.length === 0 && !loading && (
        <div className="empty-state">★<br/>No breakdown yet. Compute the Authority Score first.</div>
      )}
      {data && components.length > 0 && (
        <React.Fragment>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 10, marginBottom: 14 }}>
            <span style={{ fontSize: 36, fontWeight: 700, color: total >= 70 ? 'var(--emerald)' : total >= 40 ? 'var(--amber)' : 'var(--rose)' }}>
              {data.found != null && data.total != null ? `${data.found}/${data.total}` : '—'}
            </span>
            <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>components present</span>
          </div>
          <div style={{ display: 'grid', gap: 10 }}>
            {components.map(c => {
              const score = Math.round(Number(c.score) || 0);
              const weight = Math.round((Number(c.weight) || 0) * (Number(c.weight) <= 1 ? 100 : 1));
              const barColor = score >= 60 ? 'var(--emerald)' : score >= 30 ? 'var(--amber)' : 'var(--rose)';
              return (
                <div key={c.key} style={{ padding: 10, background: 'var(--bg-raised)', borderRadius: 6 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                    <span style={{ fontWeight: 600, fontSize: 12 }}>
                      {c.label}<MetricTooltip metricKey={c.key} />
                    </span>
                    <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <span style={{ fontFamily: 'var(--font-mono)', fontSize: 12, color: barColor }}>{score}/100</span>
                      <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>· {weight}% wt</span>
                      <ConfidenceBadge level={c.confidence} />
                    </span>
                  </div>
                  <div style={{ height: 5, background: 'var(--border-subtle)', borderRadius: 3, overflow: 'hidden', marginBottom: 6 }}>
                    <div style={{ height: '100%', width: `${score}%`, background: barColor, borderRadius: 3 }} />
                  </div>
                  {c.recommended_fix && (
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>→ {c.recommended_fix}{c.estimated_impact ? ` (${c.estimated_impact})` : ''}</div>
                  )}
                </div>
              );
            })}
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 14 }}>
            {data.biggest_lever && <span className="badge purple">Biggest lever: {data.biggest_lever}</span>}
            {data.fastest_win && <span className="badge emerald">Fastest win: {data.fastest_win}</span>}
            {data.hardest_gap && <span className="badge rose">Hardest gap: {data.hardest_gap}</span>}
          </div>
        </React.Fragment>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// PHASE 2 SHARED COMPONENTS
// ═══════════════════════════════════════════════════════════════

// 6. CitationBreakdownCard — full citation diagnostic.
function CitationBreakdownCard({ wsId }) {
  const { token } = useContext(AuthContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const loading = !data && !error;

  useEffect(() => {
    if (!wsId) return;
    let cancelled = false;
    setData(null); setError('');
    api(`/api/citation/${wsId}/breakdown`, {}, token)
      .then(r => { if (!cancelled) setData(r.data || r); })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load'); });
    return () => { cancelled = true; };
  }, [wsId, token]);

  const stat = (label, obj, fmt) => {
    const v = obj?.value;
    const conf = obj?.confidence || 'needs_review';
    const shown = (v == null) ? '—' : (fmt ? fmt(v) : Math.round(Number(v)));
    return (
      <div className="metric-card" style={{ padding: 10, textAlign: 'center' }}>
        <div className="metric-label" style={{ fontSize: 9 }}>{label}</div>
        <div className="metric-value" style={{ fontSize: 20 }}>{shown}</div>
        <div style={{ marginTop: 2 }}><ConfidenceBadge level={conf} /></div>
      </div>
    );
  };

  const pct = v => `${Math.round(Number(v) * (Number(v) <= 1 ? 100 : 1))}%`;

  const platforms = [
    { key: 'chatgpt', label: 'ChatGPT' },
    { key: 'gemini', label: 'Gemini' },
    { key: 'claude', label: 'Claude' },
    { key: 'perplexity', label: 'Perplexity' },
    { key: 'google_aio', label: 'Google AIO' },
  ];

  const SOURCE_TYPES = [
    { key: 'own_website', label: 'Own website', color: 'var(--emerald)' },
    { key: 'competitor_website', label: 'Competitor', color: 'var(--rose)' },
    { key: 'review_site', label: 'Review site', color: 'var(--cyan)' },
    { key: 'reddit', label: 'Reddit', color: 'var(--amber)' },
    { key: 'youtube', label: 'YouTube', color: 'var(--cyan)' },
    { key: 'local_directory', label: 'Local directory', color: 'var(--cyan)' },
    { key: 'media', label: 'Media', color: 'var(--amber)' },
    { key: 'medical_directory', label: 'Medical directory', color: 'var(--cyan)' },
    { key: 'gbp', label: 'Google Business', color: 'var(--cyan)' },
    { key: 'product_feed', label: 'Product feed', color: 'var(--amber)' },
    { key: 'unknown', label: 'Unknown', color: 'var(--text-muted)' },
  ];

  const sourceTotals = data?.source_type_breakdown || {};
  const maxSource = Math.max(1, ...SOURCE_TYPES.map(s => sourceTotals[s.key] || 0));
  const gapList = (data?.citation_gap || []).filter(g => (g.competitor_count || 0) > 0 && (g.our_count || 0) === 0);

  return (
    <div className="card">
      <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
        <span>Citation breakdown — where AI cites you<MetricTooltip metricKey="citation_score" /></span>
        {data && <ConfidenceBadge level={data.confidence || 'estimated'} />}
      </div>
      {loading && <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading breakdown…</div>}
      {error && <div style={{ color: 'var(--rose)', fontSize: 12 }}>{error}</div>}
      {data && (
        <React.Fragment>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 6, marginBottom: 12 }}>
            {stat('OWNED SHARE', data.owned_citation_share, pct)}
            {stat('3P SHARE', data.third_party_share, pct)}
            {stat('QUALITY', data.citation_quality)}
            {stat('POSITION', data.citation_position)}
            {stat('SENTIMENT', data.citation_sentiment)}
            {stat('FRESHNESS', data.citation_freshness)}
            <div className="metric-card" style={{ padding: 10, textAlign: 'center' }}>
              <div className="metric-label" style={{ fontSize: 9 }}>OVERALL</div>
              <div className="metric-value" style={{ fontSize: 20 }}>
                {data.citation_quality?.value != null ? Math.round(Number(data.citation_quality.value)) : '—'}
              </div>
              <div style={{ marginTop: 2 }}><ConfidenceBadge level={data.confidence || 'estimated'} /></div>
            </div>
          </div>

          <div style={{ marginBottom: 12 }}>
            <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Platform coverage</div>
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
              {platforms.map(p => {
                const c = (data.platform_coverage || {})[p.key] || 0;
                const col = c > 0 ? 'var(--emerald)' : 'var(--text-muted)';
                return (
                  <span key={p.key} style={{
                    display: 'inline-flex', alignItems: 'center', gap: 6, padding: '4px 10px',
                    borderRadius: 14, fontSize: 11, fontWeight: 600,
                    color: col, background: `${col}22`, border: `1px solid ${col}44`,
                  }}>{p.label} <b style={{ fontFamily: 'var(--font-mono)' }}>{c}</b></span>
                );
              })}
            </div>
          </div>

          <div style={{ marginBottom: 12 }}>
            <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Source-type breakdown</div>
            <div style={{ display: 'grid', gap: 4 }}>
              {SOURCE_TYPES.map(s => {
                const v = sourceTotals[s.key] || 0;
                const w = Math.round((v / maxSource) * 100);
                return (
                  <div key={s.key} style={{ display: 'grid', gridTemplateColumns: '140px 1fr 40px', gap: 8, alignItems: 'center', fontSize: 11 }}>
                    <span style={{ color: 'var(--text-secondary)' }}>{s.label}</span>
                    <div style={{ height: 8, background: 'var(--border-subtle)', borderRadius: 4, overflow: 'hidden' }}>
                      <div style={{ height: '100%', width: `${w}%`, background: s.color, borderRadius: 4 }} />
                    </div>
                    <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: s.color, textAlign: 'right' }}>{v}</span>
                  </div>
                );
              })}
            </div>
          </div>

          <div>
            <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Citation gap — competitors cited, we are missing</div>
            {gapList.length === 0 ? (
              <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>No major source-type gaps detected.</div>
            ) : (
              <div style={{ display: 'grid', gap: 4 }}>
                {gapList.map(g => {
                  const label = (SOURCE_TYPES.find(s => s.key === g.source_type) || {}).label || g.source_type;
                  return (
                    <div key={g.source_type} style={{
                      display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                      padding: '6px 10px', borderRadius: 4,
                      background: 'rgba(244,63,94,0.08)', border: '1px solid rgba(244,63,94,0.25)',
                      fontSize: 11,
                    }}>
                      <span><b style={{ color: 'var(--rose)' }}>{label}</b> — competitors are cited here but we are missing</span>
                      <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>{g.competitor_count} vs {g.our_count}</span>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </React.Fragment>
      )}
    </div>
  );
}

// 7. DominatorListCard — competitors winning prompts, expandable.
function DominatorListCard({ wsId, limit = 10, expandable = true, title = 'Top dominators' }) {
  const { token } = useContext(AuthContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const [expanded, setExpanded] = useState({});
  const loading = !data && !error;

  useEffect(() => {
    if (!wsId) return;
    let cancelled = false;
    setData(null); setError('');
    api(`/api/dominators/${wsId}?limit=${limit}`, {}, token)
      .then(r => { if (!cancelled) setData(r.data || r || []); })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load'); });
    return () => { cancelled = true; };
  }, [wsId, token, limit]);

  const eur = v => `€${Math.round(Number(v) || 0).toLocaleString('en-US')}`;
  const FACTOR_KEYS = [
    { key: 'prompt_wins', label: 'Wins' },
    { key: 'revenue_weight', label: 'Revenue' },
    { key: 'position_weight', label: 'Position' },
    { key: 'platform_coverage', label: 'Platforms' },
    { key: 'consistency', label: 'Consistency' },
  ];

  return (
    <div className="card">
      <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
        <span>{title}<MetricTooltip metricKey="dominator" /></span>
        {data && <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{data.length} competitors</span>}
      </div>
      {loading && <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading…</div>}
      {error && <div style={{ color: 'var(--rose)', fontSize: 12 }}>{error}</div>}
      {data && data.length === 0 && (
        <div className="empty-state">⚔<br/>No dominators detected yet.</div>
      )}
      {data && data.length > 0 && (
        <div style={{ display: 'grid', gap: 6 }}>
          {data.map(d => {
            const score = Math.round(Number(d.dominator_score) || 0);
            const isOpen = expandable && expanded[d.domain];
            const factors = d.factors || {};
            return (
              <div key={d.domain} style={{ border: '1px solid var(--border-subtle)', borderRadius: 6, padding: 10, background: 'var(--bg-raised)' }}>
                <div
                  onClick={() => expandable && setExpanded(s => ({ ...s, [d.domain]: !s[d.domain] }))}
                  style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', cursor: expandable ? 'pointer' : 'default', gap: 10 }}
                >
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontWeight: 600, fontSize: 13, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {expandable && <span style={{ color: 'var(--text-muted)', marginRight: 4 }}>{isOpen ? '▾' : '▸'}</span>}
                      {d.domain}
                    </div>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>
                      Prompts won: <b style={{ color: 'var(--text-primary)' }}>{d.prompts_won || 0}</b>{' '}
                      · High-value: <b style={{ color: 'var(--text-primary)' }}>{d.high_value_prompts_won || 0}</b>{' '}
                      · Captured: <b style={{ color: 'var(--amber)' }}>{eur(d.est_revenue_captured)}</b>
                    </div>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <span style={{ fontSize: 22, fontWeight: 700, color: score >= 70 ? 'var(--rose)' : score >= 40 ? 'var(--amber)' : 'var(--text-muted)' }}>{score}</span>
                    <ConfidenceBadge level={d.confidence || 'estimated'} />
                  </div>
                </div>
                {isOpen && (
                  <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid var(--border-subtle)', display: 'grid', gap: 8 }}>
                    <div>
                      <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Why dominant</div>
                      <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.5 }}>{d.why_dominant || '—'}</div>
                    </div>
                    <div>
                      <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Factors</div>
                      <div style={{ display: 'grid', gap: 4 }}>
                        {FACTOR_KEYS.map(f => {
                          const v = Math.min(1, Math.max(0, Number(factors[f.key]) || 0));
                          const w = Math.round(v * 100);
                          const col = v >= 0.6 ? 'var(--rose)' : v >= 0.3 ? 'var(--amber)' : 'var(--text-muted)';
                          return (
                            <div key={f.key} style={{ display: 'grid', gridTemplateColumns: '110px 1fr 40px', gap: 6, alignItems: 'center', fontSize: 11 }}>
                              <span style={{ color: 'var(--text-secondary)' }}>{f.label}</span>
                              <div style={{ height: 6, background: 'var(--border-subtle)', borderRadius: 3, overflow: 'hidden' }}>
                                <div style={{ height: '100%', width: `${w}%`, background: col, borderRadius: 3 }} />
                              </div>
                              <span style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: col, textAlign: 'right' }}>{w}</span>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                    {(d.main_stages || []).length > 0 && (
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Main stages</div>
                        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                          {d.main_stages.map(s => <span key={s} className="badge purple">{s}</span>)}
                        </div>
                      </div>
                    )}
                    {(d.top_topics || []).length > 0 && (
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Top topics</div>
                        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                          {d.top_topics.map(t => <span key={t} className="badge">{t}</span>)}
                        </div>
                      </div>
                    )}
                    {(d.platforms || []).length > 0 && (
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Platforms</div>
                        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                          {d.platforms.map(p => <span key={p} className="badge emerald">{p}</span>)}
                        </div>
                      </div>
                    )}
                    {d.weakest_factor && (
                      <div>
                        <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginRight: 6 }}>Weakest:</span>
                        <span className="badge rose">{d.weakest_factor}</span>
                      </div>
                    )}
                    {d.recommended_attack && (
                      <div style={{ borderLeft: '2px solid var(--emerald)', paddingLeft: 10 }}>
                        <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 3 }}>Recommended attack</div>
                        <div style={{ fontSize: 12, color: 'var(--text-primary)', lineHeight: 1.5 }}>{d.recommended_attack}</div>
                      </div>
                    )}
                    {d.avg_position != null && (
                      <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Avg position: <b style={{ color: 'var(--text-primary)' }}>{Number(d.avg_position).toFixed(1)}</b></div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// 8. EmergingPanelCard — emerging prompts grouped by subtype.
const EMERGING_SUBTYPE_LABEL = {
  new_prompt: 'New prompts',
  rising_intent: 'Rising intent',
  no_clear_winner: 'No clear winner',
  aio_emerging: 'AIO emerging',
  competitor_emerging: 'Competitor emerging',
  revenue_emerging: 'Revenue emerging',
};

function EmergingPanelCard({ wsId }) {
  const { token } = useContext(AuthContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const loading = !data && !error;

  useEffect(() => {
    if (!wsId) return;
    let cancelled = false;
    setData(null); setError('');
    api(`/api/emerging/${wsId}`, {}, token)
      .then(r => { if (!cancelled) setData(r.data || r || []); })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load'); });
    return () => { cancelled = true; };
  }, [wsId, token]);

  const groups = useMemo(() => {
    const g = {};
    (data || []).forEach(item => {
      const k = item.subtype || 'new_prompt';
      if (!g[k]) g[k] = [];
      g[k].push(item);
    });
    return g;
  }, [data]);

  return (
    <div className="card">
      <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
        <span>Emerging prompts — where momentum is shifting</span>
        {data && <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{data.length} items</span>}
      </div>
      {loading && <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading…</div>}
      {error && <div style={{ color: 'var(--rose)', fontSize: 12 }}>{error}</div>}
      {data && data.length === 0 && (
        <div className="empty-state" style={{ fontSize: 12 }}>◌<br/>No emerging signals yet.</div>
      )}
      {data && data.length > 0 && (
        <div style={{ display: 'grid', gap: 10 }}>
          {Object.keys(groups).map(sub => (
            <div key={sub}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                <span className="badge purple">{EMERGING_SUBTYPE_LABEL[sub] || sub}</span>
                <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{groups[sub].length} prompts</span>
              </div>
              <div style={{ display: 'grid', gap: 4 }}>
                {groups[sub].map(it => (
                  <div key={it.prompt_id} style={{
                    padding: '6px 10px', border: '1px solid var(--border-subtle)',
                    borderRadius: 4, background: 'var(--bg-raised)', fontSize: 11,
                    display: 'grid', gridTemplateColumns: '1fr auto', gap: 8, alignItems: 'center',
                  }}>
                    <div style={{ minWidth: 0 }}>
                      <div style={{ fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{it.text}</div>
                      {it.why && <div style={{ color: 'var(--text-muted)', marginTop: 2, lineHeight: 1.4 }}>{it.why}</div>}
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}>
                      {it.revenue_score != null && (
                        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--amber)' }}>
                          rev {Math.round(Number(it.revenue_score))}
                        </span>
                      )}
                      <ConfidenceBadge level={it.confidence || 'estimated'} />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// 9. TrackingLogPanel — recent tracking runs (live | peec_replay | mixed).
function TrackingLogPanel({ wsId, onRefresh }) {
  const { token } = useContext(AuthContext);
  const [data, setData] = useState(null);
  const [error, setError] = useState('');
  const [reloadIdx, setReloadIdx] = useState(0);
  const loading = !data && !error;

  useEffect(() => {
    if (!wsId) return;
    let cancelled = false;
    setData(null); setError('');
    api(`/api/tracking/${wsId}/runs`, {}, token)
      .then(r => { if (!cancelled) setData(r.data || r || []); })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load'); });
    return () => { cancelled = true; };
  }, [wsId, token, reloadIdx]);

  const sourceChip = src => {
    const v = String(src || 'live').toLowerCase();
    const map = {
      live: { color: 'var(--emerald)', title: 'Live model API calls' },
      peec_replay: { color: 'var(--amber)', title: 'No live model API keys — using cached Peec data' },
      mixed: { color: 'var(--cyan)', title: 'Mixed live + replay' },
    };
    const cfg = map[v] || { color: 'var(--text-muted)', title: v };
    return (
      <span title={cfg.title} style={{
        display: 'inline-block', padding: '1px 7px', borderRadius: 9,
        fontSize: 9, fontWeight: 600, fontFamily: 'var(--font-mono)',
        color: cfg.color, background: `${cfg.color}22`, whiteSpace: 'nowrap',
      }}>{v}</span>
    );
  };

  const fmtTime = ts => {
    if (!ts) return '—';
    try { return new Date(ts).toLocaleString(); } catch { return String(ts); }
  };
  const fmtDur = ms => {
    if (ms == null) return '—';
    const s = Math.round(Number(ms) / 1000);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    return `${m}m ${s % 60}s`;
  };
  const handleRefresh = () => {
    if (onRefresh) onRefresh();
    setReloadIdx(i => i + 1);
  };

  return (
    <div className="card">
      <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
        <span>Tracking runs</span>
        <button className="btn btn-sm" onClick={handleRefresh}>Refresh</button>
      </div>
      {loading && <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading…</div>}
      {error && <div style={{ color: 'var(--rose)', fontSize: 12 }}>{error}</div>}
      {data && data.length === 0 && (
        <div className="empty-state" style={{ fontSize: 12 }}>
          ⌁<br/>No tracking runs yet. Click 'Track All High-Value' to start.
          <div style={{ marginTop: 8 }}>
            <button className="btn btn-sm" onClick={handleRefresh}>Refresh</button>
          </div>
        </div>
      )}
      {data && data.length > 0 && (
        <div style={{ overflowX: 'auto' }}>
          <table className="data-table" style={{ fontSize: 11 }}>
            <thead>
              <tr>
                <th>Started</th><th>Finished</th><th>Prompts</th><th>Models</th>
                <th>New Wins</th><th>New Losses</th><th>Citation Δ</th><th>AIO Δ</th>
                <th>Errors</th><th>Duration</th><th>Source</th><th>Conf</th><th>Notes</th>
              </tr>
            </thead>
            <tbody>
              {data.map(r => {
                const losses = Number(r.new_losses) || 0;
                const wins = Number(r.new_wins) || 0;
                const models = Array.isArray(r.models_checked) ? r.models_checked : [];
                const notes = r.notes || '';
                return (
                  <tr key={r.id}>
                    <td style={{ whiteSpace: 'nowrap' }}>{fmtTime(r.started_at)}</td>
                    <td style={{ whiteSpace: 'nowrap' }}>{fmtTime(r.finished_at)}</td>
                    <td style={{ textAlign: 'right' }}>{r.prompts_checked ?? '—'}</td>
                    <td><span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{models.length ? models.join(', ') : '—'}</span></td>
                    <td style={{ textAlign: 'right', color: wins > 0 ? 'var(--emerald)' : 'var(--text-muted)', fontWeight: wins > 0 ? 600 : 400 }}>{wins}</td>
                    <td style={{ textAlign: 'right', color: losses > 0 ? 'var(--rose)' : 'var(--text-muted)', fontWeight: losses > 0 ? 600 : 400 }}>{losses}</td>
                    <td style={{ textAlign: 'right' }}>{r.citation_changes ?? 0}</td>
                    <td style={{ textAlign: 'right' }}>{r.aio_changes ?? 0}</td>
                    <td style={{ textAlign: 'right', color: (r.errors || 0) > 0 ? 'var(--rose)' : 'var(--text-muted)' }}>{r.errors ?? 0}</td>
                    <td style={{ whiteSpace: 'nowrap' }}>{fmtDur(r.duration_ms)}</td>
                    <td>{sourceChip(r.data_source)}</td>
                    <td><ConfidenceBadge level={r.confidence || 'estimated'} /></td>
                    <td title={notes} style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-muted)' }}>{notes || '—'}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// COMPARATIVE DIAGNOSIS MODAL (Phase 3)
// ═══════════════════════════════════════════════════════════════

function _relTime(ts) {
  if (!ts) return '—';
  try {
    const d = new Date(ts);
    const diff = (Date.now() - d.getTime()) / 1000;
    if (diff < 60) return `${Math.round(diff)}s ago`;
    if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
    if (diff < 86400 * 30) return `${Math.round(diff / 86400)}d ago`;
    return d.toLocaleDateString();
  } catch { return String(ts); }
}

function ComparativeDiagnosisModal({ wsId, promptId, preloaded, onClose, onPushedToActions, competitorDomain, manualCompetitorUrl, ourUrl }) {
  const { token } = useContext(AuthContext);
  const [data, setData] = useState(preloaded || null);
  const [error, setError] = useState('');
  const [pushing, setPushing] = useState(false);
  const [pushMsg, setPushMsg] = useState('');
  const loading = !data && !error;

  useEffect(() => {
    if (preloaded || !wsId || !promptId) return;
    let cancelled = false;
    const body = {};
    if (competitorDomain) body.competitor_domain = competitorDomain;
    if (manualCompetitorUrl) body.manual_competitor_url = manualCompetitorUrl;
    if (ourUrl) body.our_url = ourUrl;
    api(`/api/intel/${wsId}/compare/${promptId}`, {
      method: 'POST',
      body: Object.keys(body).length ? JSON.stringify(body) : JSON.stringify({}),
    }, token)
      .then(r => { if (!cancelled) setData(r.data || r); })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load'); });
    return () => { cancelled = true; };
  }, [wsId, promptId, preloaded, competitorDomain, manualCompetitorUrl, ourUrl, token]);

  useEffect(() => {
    const onKey = e => { if (e.key === 'Escape') onClose && onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const pushToActions = async () => {
    if (!data?.diagnosis_id) return;
    setPushing(true); setPushMsg('');
    try {
      const r = await api(`/api/intel/diagnosis/${data.diagnosis_id}/push-actions`, { method: 'POST' }, token);
      const d = r.data || r;
      const n = d.created ?? (d.action_ids || []).length ?? 0;
      setPushMsg(`${n} actions queued ✓`);
      if (onPushedToActions) onPushedToActions(d);
    } catch (e) {
      setPushMsg('Push failed: ' + (e.message || 'unknown'));
    }
    setPushing(false);
  };

  const stageColor = { decision: 'rose', trust: 'amber', comparison: 'purple', objection: 'blue', solution: 'emerald', problem: 'blue', awareness: 'gray' };
  const diffColors = { easy: 'var(--emerald)', medium: 'var(--amber)', hard: 'var(--rose)' };

  const GAP_FIELDS = [
    { key: 'content_gap', label: 'Content' },
    { key: 'schema_gap', label: 'Schema' },
    { key: 'trust_gap', label: 'Trust' },
    { key: 'offsite_gap', label: 'Offsite' },
    { key: 'citation_gap', label: 'Citation' },
    { key: 'decision_support_gap', label: 'Decision support' },
  ];

  const linkify = url => {
    if (!url) return null;
    let href = String(url);
    if (!/^https?:\/\//i.test(href)) href = 'https://' + href;
    return (
      <a href={href} target="_blank" rel="noopener noreferrer"
         style={{ color: 'var(--blue)', fontFamily: 'var(--font-mono)', fontSize: 11, wordBreak: 'break-all' }}>
        {String(url).slice(0, 90)}
      </a>
    );
  };

  const gaps = data?.gaps || {};
  const whatLack = gaps.what_they_have_we_lack;
  const lackList = Array.isArray(whatLack)
    ? whatLack
    : (typeof whatLack === 'string' && whatLack.trim()
        ? whatLack.split(/[\r\n;•·]+|(?:^|\s)-\s+/).map(s => s.trim()).filter(Boolean)
        : []);

  const diff = String(data?.implementation_difficulty || '').toLowerCase();
  const diffColor = diffColors[diff] || 'var(--text-muted)';

  return (
    <div className="modal-overlay" onClick={() => onClose && onClose()}>
      <div className="modal" onClick={e => e.stopPropagation()} style={{ maxWidth: 920, width: '92vw' }}>
        <div className="modal-header">
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <h3 style={{ marginBottom: 0 }}>Comparative Citation Diagnosis</h3>
            {data && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                <span style={{ fontSize: 12, color: 'var(--text-secondary)', maxWidth: 600 }}>{data.prompt_text || ''}</span>
                {data.buyer_stage && <span className={`badge ${stageColor[data.buyer_stage] || 'gray'}`}>{data.buyer_stage}</span>}
                <ConfidenceBadge level={data.confidence || 'estimated'} />
              </div>
            )}
          </div>
          <button className="modal-close" onClick={() => onClose && onClose()}>{'×'}</button>
        </div>
        <div className="modal-body">
          {loading && <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading comparative diagnosis…</div>}
          {error && <div style={{ color: 'var(--rose)', fontSize: 12 }}>{error}</div>}
          {!loading && !error && !data && (
            <div className="empty-state">◎<br/>No diagnosis available.</div>
          )}
          {data && (
            <div style={{ display: 'grid', gap: 14 }}>
              {/* Two-column: competitor vs us */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
                <div className="card" style={{ padding: 10 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Competitor (winning)</div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6, flexWrap: 'wrap' }}>
                    <span className="badge rose">{data.winning_competitor || '—'}</span>
                  </div>
                  {linkify(data.competitor_url) || <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>—</span>}
                </div>
                <div className="card" style={{ padding: 10 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Us (best matching)</div>
                  {data.our_url
                    ? linkify(data.our_url)
                    : <span className="badge rose">Missing page</span>}
                </div>
              </div>

              {/* Gaps grid */}
              <div className="card" style={{ padding: 10 }}>
                <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Gaps</div>
                <div style={{ display: 'grid', gap: 4 }}>
                  {GAP_FIELDS.map(f => {
                    const v = gaps[f.key];
                    const has = typeof v === 'string' ? v.trim().length > 0 : !!v;
                    const color = has ? 'var(--rose)' : 'var(--text-muted)';
                    return (
                      <div key={f.key} style={{ display: 'grid', gridTemplateColumns: '140px 10px 1fr', gap: 8, alignItems: 'baseline', padding: '4px 0', borderBottom: '1px solid var(--border-subtle)' }}>
                        <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-secondary)' }}>{f.label}</span>
                        <span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: '50%', background: color }}></span>
                        <span style={{ fontSize: 12, color: has ? 'var(--text-primary)' : 'var(--text-muted)', lineHeight: 1.45 }}>
                          {has ? (typeof v === 'string' ? v : JSON.stringify(v)) : 'No material gap detected'}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Why they win */}
              {gaps.why_they_win && (
                <div style={{ borderLeft: '2px solid var(--purple)', paddingLeft: 10 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 3 }}>Why they win</div>
                  <div style={{ fontSize: 12, color: 'var(--text-secondary)', fontStyle: 'italic', lineHeight: 1.55 }}>{gaps.why_they_win}</div>
                </div>
              )}

              {/* What they have we lack */}
              {lackList.length > 0 && (
                <div>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>What they have we lack</div>
                  <ul style={{ fontSize: 12, paddingLeft: 18, color: 'var(--text-secondary)', lineHeight: 1.5, margin: 0 }}>
                    {lackList.map((it, i) => <li key={i} style={{ marginBottom: 3 }}>{it}</li>)}
                  </ul>
                </div>
              )}

              {/* Footer panel */}
              <div className="card" style={{ padding: 12, background: 'var(--bg-raised)' }}>
                {data.recommended_action && (
                  <div style={{ marginBottom: 8 }}>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 3 }}>Recommended action</div>
                    <div style={{ fontSize: 13, fontWeight: 700, lineHeight: 1.5 }}>{data.recommended_action}</div>
                  </div>
                )}
                <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'center', fontSize: 11 }}>
                  {data.expected_impact && (
                    <div>
                      <span style={{ color: 'var(--text-muted)', textTransform: 'uppercase', fontSize: 10 }}>Expected impact: </span>
                      <span style={{ color: 'var(--emerald)' }}>{data.expected_impact}</span>
                    </div>
                  )}
                  {data.implementation_difficulty && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                      <span style={{ color: 'var(--text-muted)', textTransform: 'uppercase', fontSize: 10 }}>Difficulty</span>
                      <span style={{
                        display: 'inline-block', padding: '1px 7px', borderRadius: 9,
                        fontSize: 10, fontWeight: 600, fontFamily: 'var(--font-mono)',
                        color: diffColor, background: `${diffColor}22`,
                      }}>{diff}</span>
                    </div>
                  )}
                  {data.source_label && <span className="badge">{data.source_label}</span>}
                  {data.last_analyzed && (
                    <span style={{ color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                      {_relTime(data.last_analyzed)}
                    </span>
                  )}
                </div>
              </div>

              {/* Push button */}
              <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', gap: 10 }}>
                {pushMsg && (
                  <span style={{ fontSize: 11, color: pushMsg.includes('failed') ? 'var(--rose)' : 'var(--emerald)' }}>{pushMsg}</span>
                )}
                <button className="btn btn-primary" onClick={pushToActions} disabled={pushing || !data.diagnosis_id}>
                  {pushing ? 'Pushing…' : 'Push to Action Engine'}
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// OVERVIEW PAGE
// ═══════════════════════════════════════════════════════════════

function OverviewPage({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;

  useEffect(() => {
    if (!wsId) return;
    dispatch({ type: 'SET_LOADING', key: 'overview', value: true });
    api(`/api/dashboard/overview/${wsId}`, {}, state.token)
      .then(res => {
        if (res.success) dispatch({ type: 'SET_OVERVIEW', data: res.data });
      })
      .catch(err => console.warn('API:', err.message))
      .finally(() => dispatch({ type: 'SET_LOADING', key: 'overview', value: false }));
  }, [wsId]);

  const ov = state.overview;
  const p = ov?.pipeline || {};
  const cost = ov?.cost || {};

  return (
    <div className="fade-in">
      {/* Pipeline Metrics */}
      <div className="metrics-grid">
        <MetricCard label="Records Ingested" value={p.records || 0} color="blue" />
        <MetricCard label="Sources Found" value={p.sources || 0} color="cyan" />
        <MetricCard label="Pages Scraped" value={p.scraped || 0} color="purple" />
        <MetricCard label="Topic Clusters" value={p.clusters || 0} color="amber" />
        <MetricCard label="Drafts Approved" value={p.drafts_approved || 0} color="emerald"
          subtitle={`${p.drafts_pending || 0} pending`} />
        <MetricCard label="Published" value={p.exports || 0} color="blue" />
        <MetricCard label="Daily Cost" value={`$${(cost.daily || 0).toFixed(2)}`} color="amber" />
        <MetricCard label="Monthly Cost" value={`$${(cost.monthly || 0).toFixed(2)}`} color="rose" />
      </div>

      <div className="grid-2-1 mb-lg">
        {/* Pipeline funnel */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Pipeline Funnel</span>
          </div>
          <div className="card-body">
            <PipelineFunnel pipeline={p} />
          </div>
        </div>

        {/* Draft status donut */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Draft Status</span>
          </div>
          <div className="card-body" style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 16 }}>
            <DonutChart segments={[
              { value: p.drafts_approved || 0, color: 'var(--emerald)' },
              { value: p.drafts_pending || 0, color: 'var(--amber)' },
              { value: p.drafts_revision || 0, color: 'var(--blue)' },
              { value: p.drafts_rejected || 0, color: 'var(--rose)' },
            ]} size={130} />
            <div style={{ display: 'flex', gap: 14, fontSize: 11 }}>
              <span><span style={{ color: 'var(--emerald)' }}>{'\u25CF'}</span> Approved</span>
              <span><span style={{ color: 'var(--amber)' }}>{'\u25CF'}</span> Pending</span>
              <span><span style={{ color: 'var(--blue)' }}>{'\u25CF'}</span> Revision</span>
              <span><span style={{ color: 'var(--rose)' }}>{'\u25CF'}</span> Rejected</span>
            </div>
          </div>
        </div>
      </div>

      {/* Success Metrics */}
      {ov?.success_metrics?.length > 0 && (
        <div className="card mb-lg">
          <div className="card-header">
            <span className="card-title">Success Metrics</span>
          </div>
          <div className="card-body">
            {ov.success_metrics.map((m, i) => (
              <MetricProgress key={i} metric={m} />
            ))}
          </div>
        </div>
      )}

      {/* Recent Measurements */}
      {ov?.measurements?.length > 0 && (
        <div className="card">
          <div className="card-header">
            <span className="card-title">Recent Measurements</span>
            <span className="badge blue">{ov.measurements.length}</span>
          </div>
          <div className="card-body" style={{ padding: 0 }}>
            <table className="data-table">
              <thead><tr>
                <th>URL</th><th>Citations</th><th>Rate</th><th>Visibility</th><th>Model</th><th>Measured</th>
              </tr></thead>
              <tbody>
                {ov.measurements.slice(0, 10).map((m, i) => (
                  <tr key={i}>
                    <td style={{ maxWidth: 280, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--blue)' }}>
                      {m.url}
                    </td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{m.citation_count}</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{(m.citation_rate * 100).toFixed(1)}%</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{(m.visibility || 0).toFixed(2)}</td>
                    <td><span className="badge purple">{m.model_source || 'N/A'}</span></td>
                    <td style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', fontSize: 11 }}>
                      {m.measured_at?.slice(0, 16)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {!ov && !state.loading.overview && (
        <div className="empty-state">
          <div className="empty-icon">{'\u25A3'}</div>
          <h4>No workspace data</h4>
          <p style={{ fontSize: 12 }}>Import Peec data via the operator app to see metrics here.</p>
        </div>
      )}
    </div>
  );
}

function MetricCard({ label, value, color = 'blue', subtitle, delta, deltaDir }) {
  return (
    <div className={`metric-card ${color}`}>
      <div className="metric-label">{label}</div>
      <div className="metric-value">{typeof value === 'number' ? value.toLocaleString() : value}</div>
      {subtitle && <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>{subtitle}</div>}
      {delta !== undefined && (
        <div className={`metric-delta ${deltaDir || 'flat'}`}>
          {deltaDir === 'up' ? '\u2191' : deltaDir === 'down' ? '\u2193' : '\u2014'} {delta}
        </div>
      )}
    </div>
  );
}

function PipelineFunnel({ pipeline }) {
  const stages = [
    { label: 'Ingested', value: pipeline.records || 0, color: 'var(--blue)' },
    { label: 'Sources', value: pipeline.sources || 0, color: 'var(--cyan)' },
    { label: 'Scraped', value: pipeline.scraped || 0, color: 'var(--purple)' },
    { label: 'Clusters', value: pipeline.clusters || 0, color: 'var(--amber)' },
    { label: 'Approved', value: pipeline.drafts_approved || 0, color: 'var(--emerald)' },
    { label: 'Published', value: pipeline.exports || 0, color: 'var(--blue)' },
  ];
  const maxVal = Math.max(...stages.map(s => s.value), 1);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {stages.map((stage, i) => (
        <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ width: 70, fontSize: 11, color: 'var(--text-secondary)', textAlign: 'right' }}>{stage.label}</span>
          <div style={{ flex: 1, height: 20, background: 'var(--bg-overlay)', borderRadius: 4, overflow: 'hidden' }}>
            <div style={{
              height: '100%', width: `${(stage.value / maxVal) * 100}%`,
              background: stage.color, borderRadius: 4,
              minWidth: stage.value > 0 ? 4 : 0,
              transition: 'width 0.5s ease',
            }} />
          </div>
          <span style={{ width: 50, fontFamily: 'var(--font-mono)', fontSize: 12, textAlign: 'right' }}>
            {stage.value}
          </span>
        </div>
      ))}
    </div>
  );
}

function MetricProgress({ metric }) {
  const current = metric.current_value || 0;
  const target = metric.target_value || 1;
  const baseline = metric.baseline_value || 0;
  const pct = Math.min(((current - baseline) / (target - baseline)) * 100, 100);
  const isGood = metric.direction === 'increase' ? current >= baseline : current <= baseline;

  return (
    <div style={{ marginBottom: 14 }}>
      <div className="flex-between" style={{ marginBottom: 4 }}>
        <span style={{ fontSize: 12, fontWeight: 500 }}>{metric.metric_name}</span>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 12 }}>
          {current.toFixed(2)} / {target.toFixed(2)}
        </span>
      </div>
      <div className="progress-bar">
        <div className={`progress-fill ${isGood ? 'emerald' : 'amber'}`} style={{ width: `${Math.max(pct, 0)}%` }} />
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// PERFORMANCE PAGE
// ═══════════════════════════════════════════════════════════════

function PerformancePage({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;
  const [days, setDays] = useState(30);

  useEffect(() => {
    if (!wsId) return;
    dispatch({ type: 'SET_LOADING', key: 'performance', value: true });
    api(`/api/dashboard/performance/${wsId}?days=${days}`, {}, state.token)
      .then(res => {
        if (res.success) dispatch({ type: 'SET_PERFORMANCE', data: res.data });
      })
      .catch(err => console.warn('API:', err.message))
      .finally(() => dispatch({ type: 'SET_LOADING', key: 'performance', value: false }));
  }, [wsId, days]);

  const perf = state.performance;

  return (
    <div className="fade-in">
      <div className="flex-between mb-md">
        <div />
        <div style={{ display: 'flex', gap: 6 }}>
          {[7, 14, 30, 90].map(d => (
            <button key={d} className={`btn btn-sm ${days === d ? 'btn-primary' : ''}`}
              onClick={() => setDays(d)}>{d}d</button>
          ))}
        </div>
      </div>

      {/* Citation Rate Over Time */}
      <div className="card mb-lg">
        <div className="card-header">
          <span className="card-title">Citation Rate Over Time</span>
          <span className="badge blue">{perf?.citation_series?.length || 0} days</span>
        </div>
        <div className="card-body">
          <BarChart
            data={(perf?.citation_series || []).map(d => ({
              date: d.date, value: (d.avg_citation_rate || 0) * 100
            }))}
            color="blue" height={180} labelKey="date" valueKey="value" />
        </div>
      </div>

      <div className="grid-2 mb-lg">
        {/* Total Citations */}
        <div className="card">
          <div className="card-header"><span className="card-title">Total Citations</span></div>
          <div className="card-body">
            <BarChart
              data={(perf?.citation_series || []).map(d => ({ date: d.date, value: d.total_citations || 0 }))}
              color="emerald" height={140} labelKey="date" valueKey="value" />
          </div>
        </div>

        {/* API Cost */}
        <div className="card">
          <div className="card-header"><span className="card-title">API Cost ($)</span></div>
          <div className="card-body">
            <BarChart
              data={(perf?.cost_series || []).map(d => ({ date: d.date, value: d.total_cost || 0 }))}
              color="amber" height={140} labelKey="date" valueKey="value" />
          </div>
        </div>
      </div>

      {/* Top Performing URLs */}
      <div className="card">
        <div className="card-header">
          <span className="card-title">Top Performing URLs</span>
        </div>
        <div className="card-body" style={{ padding: 0 }}>
          {perf?.top_urls?.length ? (
            <table className="data-table">
              <thead><tr><th>URL</th><th>Avg Rate</th><th>Citations</th><th>Measurements</th></tr></thead>
              <tbody>
                {perf.top_urls.map((u, i) => (
                  <tr key={i}>
                    <td style={{ maxWidth: 340, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--blue)' }}>
                      {u.url}
                    </td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{((u.avg_rate || 0) * 100).toFixed(1)}%</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{u.total_citations}</td>
                    <td style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>{u.measurements}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty-state"><h4>No URL data yet</h4></div>
          )}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// CONTENT DELTAS PAGE
// ═══════════════════════════════════════════════════════════════

function DeltasPage({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;

  useEffect(() => {
    if (!wsId) return;
    api(`/api/dashboard/deltas/${wsId}`, {}, state.token)
      .then(res => { if (res.success) dispatch({ type: 'SET_DELTAS', data: res.data }); })
      .catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const d = state.deltas;
  const s = d?.summary || {};

  return (
    <div className="fade-in">
      <div className="metrics-grid mb-lg">
        <MetricCard label="URLs Tracked" value={s.urls_tracked || 0} color="blue" />
        <MetricCard label="Citation Lift" value={s.total_citation_lift || 0} color="emerald"
          delta={s.total_citation_lift > 0 ? `+${s.total_citation_lift}` : `${s.total_citation_lift || 0}`}
          deltaDir={s.total_citation_lift > 0 ? 'up' : s.total_citation_lift < 0 ? 'down' : 'flat'} />
        <MetricCard label="Avg Rate Before" value={`${((s.avg_rate_before || 0) * 100).toFixed(1)}%`} color="amber" />
        <MetricCard label="Avg Rate After" value={`${((s.avg_rate_after || 0) * 100).toFixed(1)}%`} color="emerald"
          delta={`${s.avg_rate_lift > 0 ? '+' : ''}${((s.avg_rate_lift || 0) * 100).toFixed(1)}pp`}
          deltaDir={s.avg_rate_lift > 0 ? 'up' : s.avg_rate_lift < 0 ? 'down' : 'flat'} />
      </div>

      <div className="card">
        <div className="card-header">
          <span className="card-title">Before / After Comparison</span>
          <span className="badge blue">{d?.deltas?.length || 0} URLs</span>
        </div>
        <div className="card-body" style={{ padding: 0 }}>
          {d?.deltas?.length ? (
            <table className="data-table">
              <thead><tr>
                <th>URL</th><th>Rate Before</th><th>Rate After</th><th>{'\u0394'} Rate</th>
                <th>Citations Before</th><th>Citations After</th><th>{'\u0394'}</th>
              </tr></thead>
              <tbody>
                {d.deltas.map((row, i) => (
                  <tr key={i}>
                    <td style={{ maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--blue)' }}>
                      {row.url}
                    </td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{((row.before_rate || 0) * 100).toFixed(1)}%</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{((row.after_rate || 0) * 100).toFixed(1)}%</td>
                    <td style={{ fontFamily: 'var(--font-mono)', color: row.delta_rate > 0 ? 'var(--emerald)' : row.delta_rate < 0 ? 'var(--rose)' : 'var(--text-muted)' }}>
                      {row.delta_rate > 0 ? '+' : ''}{((row.delta_rate || 0) * 100).toFixed(1)}pp
                    </td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{row.before_citations}</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{row.after_citations}</td>
                    <td style={{ fontFamily: 'var(--font-mono)', color: row.delta_citations > 0 ? 'var(--emerald)' : row.delta_citations < 0 ? 'var(--rose)' : 'var(--text-muted)' }}>
                      {row.delta_citations > 0 ? '+' : ''}{row.delta_citations}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty-state">
              <div className="empty-icon">{'\u0394'}</div>
              <h4>No deltas yet</h4>
              <p style={{ fontSize: 12 }}>Publish content and run re-measurements to see before/after comparisons.</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// MODEL BREAKDOWN PAGE
// ═══════════════════════════════════════════════════════════════

function ModelsPage({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;
  const perf = state.performance;

  useEffect(() => {
    if (!wsId || perf) return;
    api(`/api/dashboard/performance/${wsId}?days=30`, {}, state.token)
      .then(res => { if (res.success) dispatch({ type: 'SET_PERFORMANCE', data: res.data }); })
      .catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const models = perf?.model_breakdown || [];
  const modelColors = {
    ChatGPT: 'var(--emerald)', Perplexity: 'var(--blue)', Gemini: 'var(--amber)',
    Claude: 'var(--purple)', Copilot: 'var(--cyan)', Other: 'var(--text-muted)',
  };

  return (
    <div className="fade-in">
      {models.length ? (
        <>
          <div className="grid-2 mb-lg">
            <div className="card">
              <div className="card-header"><span className="card-title">Citation Distribution by Model</span></div>
              <div className="card-body" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 32 }}>
                <DonutChart segments={models.map(m => ({
                  value: m.total_citations || 0,
                  color: modelColors[m.model_source] || 'var(--text-muted)',
                }))} size={160} />
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {models.map((m, i) => (
                    <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 12 }}>
                      <div style={{ width: 10, height: 10, borderRadius: 2, background: modelColors[m.model_source] || 'var(--text-muted)' }} />
                      <span style={{ width: 80 }}>{m.model_source}</span>
                      <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>{m.total_citations}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div className="card">
              <div className="card-header"><span className="card-title">Avg Citation Rate by Model</span></div>
              <div className="card-body">
                {models.map((m, i) => {
                  const rate = (m.avg_citation_rate || 0) * 100;
                  return (
                    <div key={i} style={{ marginBottom: 12 }}>
                      <div className="flex-between" style={{ marginBottom: 4 }}>
                        <span style={{ fontSize: 12 }}>{m.model_source}</span>
                        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 12 }}>{rate.toFixed(1)}%</span>
                      </div>
                      <div className="progress-bar">
                        <div className="progress-fill" style={{
                          width: `${Math.min(rate, 100)}%`,
                          background: modelColors[m.model_source] || 'var(--text-muted)',
                        }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-header"><span className="card-title">Model Comparison Table</span></div>
            <div className="card-body" style={{ padding: 0 }}>
              <table className="data-table">
                <thead><tr><th>Model</th><th>Citations</th><th>Avg Rate</th><th>Avg Visibility</th><th>Measurements</th></tr></thead>
                <tbody>
                  {models.map((m, i) => (
                    <tr key={i}>
                      <td><span className="badge" style={{ background: `${modelColors[m.model_source]}22`, color: modelColors[m.model_source] }}>{m.model_source}</span></td>
                      <td style={{ fontFamily: 'var(--font-mono)' }}>{m.total_citations}</td>
                      <td style={{ fontFamily: 'var(--font-mono)' }}>{((m.avg_citation_rate || 0) * 100).toFixed(1)}%</td>
                      <td style={{ fontFamily: 'var(--font-mono)' }}>{(m.avg_visibility || 0).toFixed(3)}</td>
                      <td style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>{m.cnt}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      ) : (
        <div className="empty-state">
          <div className="empty-icon">{'\u2630'}</div>
          <h4>No model data</h4>
          <p style={{ fontSize: 12 }}>Run Peec measurements to see performance across AI models.</p>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// PIPELINE ACTIVITY PAGE
// ═══════════════════════════════════════════════════════════════

function PipelineActivityPage({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;

  useEffect(() => {
    if (!wsId) return;
    api(`/api/dashboard/pipeline-activity/${wsId}?days=7`, {}, state.token)
      .then(res => { if (res.success) dispatch({ type: 'SET_ACTIVITY', data: res.data }); })
      .catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const act = state.activity || {};

  const allEvents = [
    ...(act.imports || []).map(e => ({ type: 'import', time: e.imported_at, text: `Imported ${e.record_count} records`, color: 'blue' })),
    ...(act.scrapes || []).filter(s => s.scraped_at).map(e => ({ type: 'scrape', time: e.scraped_at, text: `Scraped ${e.url}`, color: 'purple', status: e.scrape_status })),
    ...(act.analyses || []).map(e => ({ type: 'analysis', time: e.created_at, text: `Analyzed cluster "${e.cluster_name}"`, color: 'cyan' })),
    ...(act.drafts || []).map(e => ({ type: 'draft', time: e.created_at, text: `Generated ${e.template_type} draft for "${e.cluster_name}" (${e.word_count} words)`, color: 'amber', status: e.status })),
    ...(act.exports || []).map(e => ({ type: 'export', time: e.created_at, text: `Exported "${e.cluster_name}" as ${e.format}`, color: 'emerald' })),
  ].sort((a, b) => (b.time || '').localeCompare(a.time || ''));

  return (
    <div className="fade-in">
      <div className="metrics-grid mb-lg">
        <MetricCard label="Imports" value={act.imports?.length || 0} color="blue" />
        <MetricCard label="Scrapes" value={act.scrapes?.length || 0} color="purple" />
        <MetricCard label="Analyses" value={act.analyses?.length || 0} color="cyan" />
        <MetricCard label="Drafts" value={act.drafts?.length || 0} color="amber" />
        <MetricCard label="Exports" value={act.exports?.length || 0} color="emerald" />
        <MetricCard label="Batch Jobs" value={act.batch_jobs?.length || 0} color="rose" />
      </div>

      <div className="card">
        <div className="card-header">
          <span className="card-title">Activity Feed (Last 7 Days)</span>
          <span className="badge blue">{allEvents.length} events</span>
        </div>
        <div className="card-body">
          {allEvents.length ? allEvents.slice(0, 50).map((evt, i) => (
            <div key={i} className="activity-item">
              <div className={`activity-dot ${evt.color}`} />
              <div>
                <div className="activity-text">{evt.text}</div>
                <div className="activity-time">{evt.time?.replace('T', ' ').slice(0, 19)}</div>
              </div>
              {evt.status && (
                <span className={`badge ${evt.status === 'approved' ? 'emerald' : evt.status === 'rejected' ? 'rose' : 'amber'}`}
                  style={{ marginLeft: 'auto', alignSelf: 'center' }}>{evt.status}</span>
              )}
            </div>
          )) : (
            <div className="empty-state">
              <div className="empty-icon">{'\u25B6'}</div>
              <h4>No recent activity</h4>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// CAMPAIGNS PAGE
// ═══════════════════════════════════════════════════════════════

function CampaignsPage({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState('');
  const [desc, setDesc] = useState('');

  useEffect(() => {
    if (!wsId) return;
    api(`/api/workspaces/${wsId}/campaigns`, {}, state.token)
      .then(res => { if (res.success) dispatch({ type: 'SET_CAMPAIGNS', data: res.data }); })
      .catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const handleCreate = async () => {
    if (!name.trim()) return;
    try {
      await api(`/api/workspaces/${wsId}/campaigns`, {
        method: 'POST', body: JSON.stringify({ name, description: desc }),
      }, state.token);
      setShowCreate(false); setName(''); setDesc('');
      const res = await api(`/api/workspaces/${wsId}/campaigns`, {}, state.token);
      if (res.success) dispatch({ type: 'SET_CAMPAIGNS', data: res.data });
    } catch (err) { dispatch({ type: 'SET_ERROR', error: err.message }); }
  };

  const statusColors = { active: 'emerald', paused: 'amber', completed: 'blue', archived: 'rose' };

  return (
    <div className="fade-in">
      <div className="flex-between mb-lg">
        <div />
        <button className="btn btn-primary" onClick={() => setShowCreate(true)}>+ New Campaign</button>
      </div>

      {state.campaigns.length ? (
        <div className="card">
          <div className="card-body" style={{ padding: 0 }}>
            <table className="data-table">
              <thead><tr><th>Campaign</th><th>Status</th><th>Start</th><th>End</th><th>Created</th></tr></thead>
              <tbody>
                {state.campaigns.map(c => (
                  <tr key={c.id}>
                    <td>
                      <div style={{ fontWeight: 500, color: 'var(--text-primary)' }}>{c.name}</div>
                      {c.description && <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>{c.description}</div>}
                    </td>
                    <td><span className={`badge ${statusColors[c.status] || 'blue'}`}>{c.status}</span></td>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{c.start_date || '\u2014'}</td>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{c.end_date || '\u2014'}</td>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-muted)' }}>{c.created_at?.slice(0, 10)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : (
        <div className="empty-state">
          <div className="empty-icon">{'\u2691'}</div>
          <h4>No campaigns</h4>
          <p style={{ fontSize: 12 }}>Create a campaign to organize your GEO work.</p>
        </div>
      )}

      {showCreate && (
        <div className="modal-overlay" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h3>New Campaign</h3>
              <button className="modal-close" onClick={() => setShowCreate(false)}>{'\u00D7'}</button>
            </div>
            <div className="modal-body">
              <div className="form-group">
                <label className="form-label">Campaign Name</label>
                <input className="form-input" value={name} onChange={e => setName(e.target.value)} placeholder="Q2 Gut Health Push" />
              </div>
              <div className="form-group">
                <label className="form-label">Description</label>
                <textarea className="form-textarea" value={desc} onChange={e => setDesc(e.target.value)} placeholder="Campaign goals and scope..." />
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn" onClick={() => setShowCreate(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleCreate}>Create Campaign</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// REPORTS PAGE
// ═══════════════════════════════════════════════════════════════

function ReportsPage({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;
  const [showGenerate, setShowGenerate] = useState(false);
  const [periodStart, setPeriodStart] = useState('');
  const [periodEnd, setPeriodEnd] = useState('');
  const [selectedReport, setSelectedReport] = useState(null);

  useEffect(() => {
    if (!wsId) return;
    api(`/api/dashboard/reports/${wsId}`, {}, state.token)
      .then(res => { if (res.success) dispatch({ type: 'SET_REPORTS', data: res.data }); })
      .catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const handleGenerate = async () => {
    if (!periodStart || !periodEnd) return;
    try {
      const res = await api(`/api/dashboard/reports/${wsId}`, {
        method: 'POST', body: JSON.stringify({ period_start: periodStart, period_end: periodEnd }),
      }, state.token);
      setShowGenerate(false);
      // Refresh list
      const list = await api(`/api/dashboard/reports/${wsId}`, {}, state.token);
      if (list.success) dispatch({ type: 'SET_REPORTS', data: list.data });
      if (res.success) setSelectedReport(res.data);
    } catch (err) { dispatch({ type: 'SET_ERROR', error: err.message }); }
  };

  return (
    <div className="fade-in">
      <div className="flex-between mb-lg">
        <div />
        <button className="btn btn-primary" onClick={() => setShowGenerate(true)}>+ Generate Report</button>
      </div>

      <div className="grid-1-2">
        {/* Report list */}
        <div className="card">
          <div className="card-header"><span className="card-title">Saved Reports</span></div>
          <div className="card-body" style={{ padding: 0 }}>
            {state.reports.length ? state.reports.map(r => (
              <div key={r.id} style={{
                padding: '12px 18px', borderBottom: '1px solid var(--border-subtle)',
                cursor: 'pointer', background: selectedReport?.id === r.id ? 'var(--blue-dim)' : 'transparent',
              }} onClick={() => setSelectedReport(r)}>
                <div style={{ fontWeight: 500, fontSize: 12 }}>{r.report_type} Report</div>
                <div style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>
                  {r.period_start?.slice(0, 10)} {'\u2192'} {r.period_end?.slice(0, 10)}
                </div>
              </div>
            )) : (
              <div className="empty-state"><h4>No reports</h4></div>
            )}
          </div>
        </div>

        {/* Report detail */}
        <div className="card">
          <div className="card-header"><span className="card-title">Report Detail</span></div>
          <div className="card-body">
            {selectedReport?.metrics ? (
              <div>
                <div className="metrics-grid" style={{ marginBottom: 16 }}>
                  <MetricCard label="Measurements" value={selectedReport.metrics.measurement_count || 0} color="blue" />
                  <MetricCard label="Citations" value={selectedReport.metrics.total_citations || 0} color="emerald" />
                  <MetricCard label="Avg Rate" value={`${((selectedReport.metrics.avg_citation_rate || 0) * 100).toFixed(1)}%`} color="purple" />
                  <MetricCard label="Drafts Created" value={selectedReport.metrics.drafts_created || 0} color="amber" />
                  <MetricCard label="Drafts Approved" value={selectedReport.metrics.drafts_approved || 0} color="emerald" />
                  <MetricCard label="API Cost" value={`$${(selectedReport.metrics.total_cost || 0).toFixed(2)}`} color="rose" />
                </div>
                {selectedReport.metrics.model_breakdown && Object.keys(selectedReport.metrics.model_breakdown).length > 0 && (
                  <div>
                    <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-tertiary)', textTransform: 'uppercase', marginBottom: 8 }}>
                      By Model
                    </div>
                    {Object.entries(selectedReport.metrics.model_breakdown).map(([model, stats]) => (
                      <div key={model} className="flex-between" style={{ padding: '6px 0', borderBottom: '1px solid var(--border-subtle)', fontSize: 12 }}>
                        <span>{model}</span>
                        <span style={{ fontFamily: 'var(--font-mono)' }}>
                          {stats.citations} citations | {((stats.avg_rate || 0) * 100).toFixed(1)}% avg
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            ) : (
              <div className="empty-state"><h4>Select a report</h4></div>
            )}
          </div>
        </div>
      </div>

      {showGenerate && (
        <div className="modal-overlay" onClick={() => setShowGenerate(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Generate Report</h3>
              <button className="modal-close" onClick={() => setShowGenerate(false)}>{'\u00D7'}</button>
            </div>
            <div className="modal-body">
              <div className="form-group">
                <label className="form-label">Period Start</label>
                <input className="form-input" type="date" value={periodStart}
                  onChange={e => setPeriodStart(e.target.value)} />
              </div>
              <div className="form-group">
                <label className="form-label">Period End</label>
                <input className="form-input" type="date" value={periodEnd}
                  onChange={e => setPeriodEnd(e.target.value)} />
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn" onClick={() => setShowGenerate(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleGenerate}>Generate</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// WORKSPACES PAGE
// ═══════════════════════════════════════════════════════════════

function WorkspacesPage({ state, dispatch }) {
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm] = useState({ name: '', slug: '', brand_name: '', brand_voice: '', compliance_rules: '' });
  const [detail, setDetail] = useState(null);
  const [edit, setEdit] = useState(null);  // mirror of detail while editing
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveMsg, setSaveMsg] = useState('');

  const loadWorkspaces = async () => {
    const res = await api('/api/workspaces/', {}, state.token);
    if (res.success) dispatch({ type: 'SET_WORKSPACES', workspaces: res.data });
  };

  const loadDetail = async (wsId) => {
    const res = await api(`/api/workspaces/${wsId}`, {}, state.token);
    if (res.success) {
      setDetail(res.data);
      setEdit(_workspaceToEdit(res.data));
      setEditing(false); setSaveMsg('');
    }
  };

  const saveDetail = async () => {
    if (!edit || !detail) return;
    setSaving(true); setSaveMsg('');
    try {
      const body = {
        name: edit.name,
        brand_name: edit.brand_name,
        domains: _csvToArray(edit.domains),
        target_countries: _csvToArray(edit.target_countries),
        target_languages: _csvToArray(edit.target_languages),
        target_models: _csvToArray(edit.target_models),
        brand_voice: edit.brand_voice,
        compliance_rules: edit.compliance_rules,
        color_primary: edit.color_primary || undefined,
        color_accent: edit.color_accent || undefined,
      };
      const r = await api(`/api/workspaces/${detail.id}`, {
        method: 'PUT', body: JSON.stringify(body),
      }, state.token);
      if (r.success === false) {
        setSaveMsg('Save failed: ' + (r.error || 'unknown'));
      } else {
        setSaveMsg('Saved.');
        await loadWorkspaces();
        await loadDetail(detail.id);
        setEditing(false);
      }
    } catch (e) {
      setSaveMsg('Save failed: ' + e.message);
    }
    setSaving(false);
  };

  const handleCreate = async () => {
    if (!form.name || !form.slug) return;
    try {
      await api('/api/workspaces/', { method: 'POST', body: JSON.stringify(form) }, state.token);
      setShowCreate(false);
      setForm({ name: '', slug: '', brand_name: '', brand_voice: '', compliance_rules: '' });
      loadWorkspaces();
    } catch (err) { dispatch({ type: 'SET_ERROR', error: err.message }); }
  };

  return (
    <div className="fade-in">
      <div className="flex-between mb-lg">
        <div />
        <button className="btn btn-primary" onClick={() => setShowCreate(true)}>+ New Workspace</button>
      </div>

      <div className="grid-1-2">
        <div className="card">
          <div className="card-header"><span className="card-title">Client Workspaces</span></div>
          <div className="card-body" style={{ padding: 0 }}>
            {state.workspaces.map(ws => (
              <div key={ws.id} style={{
                padding: '14px 18px', borderBottom: '1px solid var(--border-subtle)',
                cursor: 'pointer', background: detail?.id === ws.id ? 'var(--blue-dim)' : 'transparent',
              }} onClick={() => loadDetail(ws.id)}>
                <div style={{ fontWeight: 500 }}>{ws.name}</div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>{ws.slug}</div>
                {ws.brand_name && <div style={{ fontSize: 11, color: 'var(--text-tertiary)', marginTop: 2 }}>{ws.brand_name}</div>}
              </div>
            ))}
          </div>
        </div>

        <div className="card">
          <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span className="card-title">Workspace Details</span>
            {detail && (
              <div style={{ display: 'flex', gap: 6 }}>
                {!editing && <button className="btn btn-sm" onClick={() => { setEditing(true); setSaveMsg(''); }}>Edit</button>}
                {editing && <>
                  <button className="btn btn-sm btn-primary" onClick={saveDetail} disabled={saving}>{saving ? 'Saving...' : 'Save Workspace'}</button>
                  <button className="btn btn-sm" onClick={() => { setEditing(false); setEdit(_workspaceToEdit(detail)); setSaveMsg(''); }}>Cancel</button>
                </>}
              </div>
            )}
          </div>
          <div className="card-body">
            {detail ? (
              <div>
                {saveMsg && <div style={{ fontSize: 11, color: saveMsg.includes('failed') ? 'var(--rose)' : 'var(--emerald)', marginBottom: 8 }}>{saveMsg}</div>}
                {!editing ? <>
                  <div style={{ fontSize: 16, fontWeight: 600, marginBottom: 4 }}>{detail.name}</div>
                  <div style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', marginBottom: 16 }}>{detail.slug}</div>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
                    <div><div className="form-label">Brand</div><div style={{ fontSize: 13 }}>{detail.brand_name || '\u2014'}</div></div>
                    <div><div className="form-label">Domains</div><div style={{ fontSize: 12, fontFamily: 'var(--font-mono)' }}>{tryParse(detail.domains)?.join(', ') || '\u2014'}</div></div>
                    <div><div className="form-label">Countries</div><div style={{ fontSize: 12 }}>{tryParse(detail.target_countries)?.join(', ') || '\u2014'}</div></div>
                    <div><div className="form-label">Languages</div><div style={{ fontSize: 12 }}>{tryParse(detail.target_languages)?.join(', ') || '\u2014'}</div></div>
                    <div><div className="form-label">Target Models</div><div style={{ fontSize: 12 }}>{tryParse(detail.target_models)?.join(', ') || '\u2014'}</div></div>
                  </div>
                  {detail.brand_voice && (
                    <div style={{ marginBottom: 12 }}>
                      <div className="form-label">Brand Voice Rules</div>
                      <div style={{ fontSize: 12, background: 'var(--bg-raised)', padding: 10, borderRadius: 6, whiteSpace: 'pre-wrap' }}>{detail.brand_voice}</div>
                    </div>
                  )}
                  {detail.compliance_rules && (
                    <div style={{ marginBottom: 12 }}>
                      <div className="form-label">Compliance / Forbidden Claims</div>
                      <div style={{ fontSize: 12, background: 'var(--bg-raised)', padding: 10, borderRadius: 6, whiteSpace: 'pre-wrap', color: 'var(--rose)' }}>{detail.compliance_rules}</div>
                    </div>
                  )}
                </> : <>
                  <div className="form-group"><label className="form-label">Name</label>
                    <input className="form-input" value={edit?.name || ''} onChange={e => setEdit({ ...edit, name: e.target.value })} />
                  </div>
                  <div className="form-group"><label className="form-label">Brand name</label>
                    <input className="form-input" value={edit?.brand_name || ''} onChange={e => setEdit({ ...edit, brand_name: e.target.value })} placeholder="e.g. Aesthetic Klinika" />
                  </div>
                  <div className="form-group"><label className="form-label">Domains (comma-separated)</label>
                    <input className="form-input" value={edit?.domains || ''} onChange={e => setEdit({ ...edit, domains: e.target.value })} placeholder="aestheticklinika.hu, www.aestheticklinika.hu" style={{ fontFamily: 'var(--font-mono)' }} />
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 8 }}>
                    <div className="form-group"><label className="form-label">Countries</label>
                      <input className="form-input" value={edit?.target_countries || ''} onChange={e => setEdit({ ...edit, target_countries: e.target.value })} placeholder="HU, AT" />
                    </div>
                    <div className="form-group"><label className="form-label">Languages</label>
                      <input className="form-input" value={edit?.target_languages || ''} onChange={e => setEdit({ ...edit, target_languages: e.target.value })} placeholder="hu, en" />
                    </div>
                    <div className="form-group"><label className="form-label">Target models</label>
                      <input className="form-input" value={edit?.target_models || ''} onChange={e => setEdit({ ...edit, target_models: e.target.value })} placeholder="ChatGPT, Perplexity, Gemini" />
                    </div>
                  </div>
                  <div className="form-group"><label className="form-label">Brand voice rules</label>
                    <textarea className="form-textarea" rows={4} value={edit?.brand_voice || ''} onChange={e => setEdit({ ...edit, brand_voice: e.target.value })} placeholder="Professional, evidence-based, empathetic..." />
                  </div>
                  <div className="form-group"><label className="form-label">Compliance / forbidden claims</label>
                    <textarea className="form-textarea" rows={3} value={edit?.compliance_rules || ''} onChange={e => setEdit({ ...edit, compliance_rules: e.target.value })} placeholder="No medical claims without citations..." />
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                    <div className="form-group"><label className="form-label">Primary color</label>
                      <input className="form-input" value={edit?.color_primary || ''} onChange={e => setEdit({ ...edit, color_primary: e.target.value })} placeholder="#2563EB" />
                    </div>
                    <div className="form-group"><label className="form-label">Accent color</label>
                      <input className="form-input" value={edit?.color_accent || ''} onChange={e => setEdit({ ...edit, color_accent: e.target.value })} placeholder="#10B981" />
                    </div>
                  </div>
                </>}

                <div style={{ marginTop: 16 }}>
                  <div className="form-label" style={{ marginBottom: 8 }}>Team Members ({detail.members?.length || 0})</div>
                  {detail.members?.map(m => (
                    <div key={m.id} className="flex-between" style={{ padding: '6px 0', borderBottom: '1px solid var(--border-subtle)', fontSize: 12 }}>
                      <div>
                        <span style={{ fontWeight: 500 }}>{m.name || m.email}</span>
                        {m.name && <span style={{ color: 'var(--text-muted)', marginLeft: 6 }}>{m.email}</span>}
                      </div>
                      <span className="badge blue">{m.workspace_role}</span>
                    </div>
                  ))}
                </div>

                {detail.campaigns?.length > 0 && (
                  <div style={{ marginTop: 16 }}>
                    <div className="form-label" style={{ marginBottom: 8 }}>Campaigns ({detail.campaigns.length})</div>
                    {detail.campaigns.map(c => (
                      <div key={c.id} className="flex-between" style={{ padding: '6px 0', borderBottom: '1px solid var(--border-subtle)', fontSize: 12 }}>
                        <span style={{ fontWeight: 500 }}>{c.name}</span>
                        <span className={`badge ${c.status === 'active' ? 'emerald' : 'amber'}`}>{c.status}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            ) : (
              <div className="empty-state"><h4>Select a workspace</h4></div>
            )}
          </div>
        </div>
      </div>

      {showCreate && (
        <div className="modal-overlay" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h3>New Workspace</h3>
              <button className="modal-close" onClick={() => setShowCreate(false)}>{'\u00D7'}</button>
            </div>
            <div className="modal-body">
              <div className="form-group">
                <label className="form-label">Name</label>
                <input className="form-input" value={form.name} onChange={e => setForm({...form, name: e.target.value})}
                  placeholder="Acme Health Co." />
              </div>
              <div className="form-group">
                <label className="form-label">Slug</label>
                <input className="form-input" value={form.slug} onChange={e => setForm({...form, slug: e.target.value})}
                  placeholder="acme-health" style={{ fontFamily: 'var(--font-mono)' }} />
              </div>
              <div className="form-group">
                <label className="form-label">Brand Name</label>
                <input className="form-input" value={form.brand_name} onChange={e => setForm({...form, brand_name: e.target.value})}
                  placeholder="Acme Health" />
              </div>
              <div className="form-group">
                <label className="form-label">Brand Voice Rules</label>
                <textarea className="form-textarea" value={form.brand_voice} onChange={e => setForm({...form, brand_voice: e.target.value})}
                  placeholder="Professional, evidence-based, empathetic..." />
              </div>
              <div className="form-group">
                <label className="form-label">Compliance Rules</label>
                <textarea className="form-textarea" value={form.compliance_rules} onChange={e => setForm({...form, compliance_rules: e.target.value})}
                  placeholder="No medical claims without citations..." />
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn" onClick={() => setShowCreate(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleCreate}>Create Workspace</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function tryParse(val) {
  if (Array.isArray(val)) return val;
  if (!val) return [];
  try { return JSON.parse(val); } catch { return []; }
}

function _workspaceToEdit(d) {
  if (!d) return null;
  const arr = (x) => (Array.isArray(x) ? x : (tryParse(x) || []));
  return {
    name: d.name || '',
    brand_name: d.brand_name || '',
    domains: arr(d.domains).join(', '),
    target_countries: arr(d.target_countries).join(', '),
    target_languages: arr(d.target_languages).join(', '),
    target_models: arr(d.target_models).join(', '),
    brand_voice: d.brand_voice || '',
    compliance_rules: d.compliance_rules || '',
    color_primary: d.color_primary || '',
    color_accent: d.color_accent || '',
  };
}

function _csvToArray(s) {
  if (!s) return [];
  return s.split(',').map(x => x.trim()).filter(Boolean);
}

// ═══════════════════════════════════════════════════════════════
// TEAM / USERS PAGE
// ═══════════════════════════════════════════════════════════════

function UsersPage({ state, dispatch }) {
  const [showInvite, setShowInvite] = useState(false);
  const [email, setEmail] = useState('');
  const [name, setName] = useState('');
  const [role, setRole] = useState('editor');
  const [inviteResult, setInviteResult] = useState(null);

  useEffect(() => {
    api('/api/dashboard/users', {}, state.token)
      .then(res => { if (res.success) dispatch({ type: 'SET_USERS', data: res.data }); })
      .catch(err => console.warn('API:', err.message));
  }, []);

  const handleInvite = async () => {
    if (!email) return;
    try {
      const res = await api('/api/auth/invite', {
        method: 'POST', body: JSON.stringify({ email, name, role }),
      }, state.token);
      if (res.success) {
        setInviteResult(res.data);
        const list = await api('/api/dashboard/users', {}, state.token);
        if (list.success) dispatch({ type: 'SET_USERS', data: list.data });
      }
    } catch (err) { dispatch({ type: 'SET_ERROR', error: err.message }); }
  };

  const roleColors = { superadmin: 'rose', admin: 'purple', editor: 'blue', reviewer: 'amber', client: 'emerald' };

  return (
    <div className="fade-in">
      <div className="flex-between mb-lg">
        <div />
        <button className="btn btn-primary" onClick={() => { setShowInvite(true); setInviteResult(null); }}>+ Invite User</button>
      </div>

      <div className="card">
        <div className="card-body" style={{ padding: 0 }}>
          <table className="data-table">
            <thead><tr><th>User</th><th>Email</th><th>Role</th><th>Status</th><th>Last Login</th></tr></thead>
            <tbody>
              {state.users.map(u => (
                <tr key={u.id}>
                  <td style={{ fontWeight: 500 }}>{u.name || '\u2014'}</td>
                  <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{u.email}</td>
                  <td><span className={`badge ${roleColors[u.role] || 'blue'}`}>{u.role}</span></td>
                  <td>
                    <span className={`badge ${u.is_active ? 'emerald' : 'rose'}`}>
                      {u.is_active ? 'active' : 'disabled'}
                    </span>
                  </td>
                  <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-muted)' }}>
                    {u.last_login?.replace('T', ' ').slice(0, 16) || 'Never'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {showInvite && (
        <div className="modal-overlay" onClick={() => setShowInvite(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Invite User</h3>
              <button className="modal-close" onClick={() => setShowInvite(false)}>{'\u00D7'}</button>
            </div>
            <div className="modal-body">
              {inviteResult ? (
                <div>
                  <div style={{ background: 'var(--emerald-dim)', color: 'var(--emerald)', padding: 12, borderRadius: 6, marginBottom: 14, fontSize: 12 }}>
                    User created successfully!
                  </div>
                  <div className="form-group">
                    <label className="form-label">Email</label>
                    <div style={{ fontFamily: 'var(--font-mono)', fontSize: 13 }}>{inviteResult.user.email}</div>
                  </div>
                  <div className="form-group">
                    <label className="form-label">Temporary Password</label>
                    <div style={{ fontFamily: 'var(--font-mono)', fontSize: 13, background: 'var(--bg-raised)', padding: '8px 12px', borderRadius: 6 }}>
                      {inviteResult.temp_password}
                    </div>
                  </div>
                </div>
              ) : (
                <>
                  <div className="form-group">
                    <label className="form-label">Email</label>
                    <input className="form-input" type="email" value={email} onChange={e => setEmail(e.target.value)} placeholder="user@company.com" />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Name</label>
                    <input className="form-input" value={name} onChange={e => setName(e.target.value)} placeholder="Jane Doe" />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Role</label>
                    <select className="form-input" value={role} onChange={e => setRole(e.target.value)}>
                      <option value="editor">Editor</option>
                      <option value="reviewer">Reviewer</option>
                      <option value="admin">Admin</option>
                      <option value="client">Client (Read-only)</option>
                    </select>
                  </div>
                </>
              )}
            </div>
            <div className="modal-footer">
              <button className="btn" onClick={() => setShowInvite(false)}>Close</button>
              {!inviteResult && <button className="btn btn-primary" onClick={handleInvite}>Send Invite</button>}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// AUDIT LOG PAGE
// ═══════════════════════════════════════════════════════════════

function AuditPage({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;

  useEffect(() => {
    if (!wsId) return;
    api(`/api/dashboard/audit/${wsId}`, {}, state.token)
      .then(res => { if (res.success) dispatch({ type: 'SET_AUDIT', data: res.data }); })
      .catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const actionColors = {
    login: 'blue', logout: 'purple', create: 'emerald', update: 'amber', delete: 'rose', export: 'cyan', publish: 'emerald',
  };

  return (
    <div className="fade-in">
      <div className="card">
        <div className="card-header">
          <span className="card-title">Audit Log</span>
          <span className="badge blue">{state.auditLog.length} entries</span>
        </div>
        <div className="card-body" style={{ padding: 0 }}>
          {state.auditLog.length ? (
            <table className="data-table">
              <thead><tr><th>Time</th><th>User</th><th>Action</th><th>Resource</th><th>Details</th></tr></thead>
              <tbody>
                {state.auditLog.map(log => (
                  <tr key={log.id}>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>
                      {log.created_at?.replace('T', ' ').slice(0, 19)}
                    </td>
                    <td style={{ fontSize: 12 }}>{log.user_name || log.user_email || log.user_id}</td>
                    <td><span className={`badge ${actionColors[log.action] || 'blue'}`}>{log.action}</span></td>
                    <td style={{ fontSize: 11 }}>
                      {log.resource_type && <span style={{ color: 'var(--text-tertiary)' }}>{log.resource_type}</span>}
                      {log.resource_id && <span style={{ fontFamily: 'var(--font-mono)', marginLeft: 4, color: 'var(--text-muted)' }}>{log.resource_id.slice(0, 12)}</span>}
                    </td>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {log.details !== '{}' ? log.details : ''}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty-state"><h4>No audit entries</h4></div>
          )}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// WORKSPACE SETTINGS PAGE
// ═══════════════════════════════════════════════════════════════

function SettingsPage({ state, dispatch }) {
  const ws = state.activeWorkspace;
  const [form, setForm] = useState({});
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [addMemberEmail, setAddMemberEmail] = useState('');
  const [addMemberRole, setAddMemberRole] = useState('editor');
  const [detail, setDetail] = useState(null);

  useEffect(() => {
    if (!ws) return;
    api(`/api/workspaces/${ws.id}`, {}, state.token)
      .then(res => {
        if (res.success) {
          const d = res.data;
          setDetail(d);
          setForm({
            name: d.name || '',
            brand_name: d.brand_name || '',
            domains: tryParse(d.domains).join(', '),
            target_countries: tryParse(d.target_countries).join(', '),
            target_languages: tryParse(d.target_languages).join(', '),
            target_models: tryParse(d.target_models).join(', '),
            brand_voice: d.brand_voice || '',
            compliance_rules: d.compliance_rules || '',
            color_primary: d.color_primary || '#2563EB',
            color_accent: d.color_accent || '#10B981',
          });
        }
      }).catch(err => console.warn('API:', err.message));
  }, [ws?.id]);

  const handleSave = async () => {
    setSaving(true);
    try {
      await api(`/api/workspaces/${ws.id}`, {
        method: 'PUT',
        body: JSON.stringify({
          name: form.name,
          brand_name: form.brand_name,
          domains: form.domains.split(',').map(s => s.trim()).filter(Boolean),
          target_countries: form.target_countries.split(',').map(s => s.trim()).filter(Boolean),
          target_languages: form.target_languages.split(',').map(s => s.trim()).filter(Boolean),
          target_models: form.target_models.split(',').map(s => s.trim()).filter(Boolean),
          brand_voice: form.brand_voice,
          compliance_rules: form.compliance_rules,
          color_primary: form.color_primary,
          color_accent: form.color_accent,
        }),
      }, state.token);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (err) { dispatch({ type: 'SET_ERROR', error: err.message }); }
    setSaving(false);
  };

  const handleAddMember = async () => {
    if (!addMemberEmail) return;
    try {
      await api(`/api/workspaces/${ws.id}/members`, {
        method: 'POST', body: JSON.stringify({ email: addMemberEmail, role: addMemberRole }),
      }, state.token);
      setAddMemberEmail('');
      // Reload detail
      const res = await api(`/api/workspaces/${ws.id}`, {}, state.token);
      if (res.success) setDetail(res.data);
    } catch (err) { dispatch({ type: 'SET_ERROR', error: err.message }); }
  };

  if (!ws) return <div className="empty-state"><h4>No workspace selected</h4></div>;

  return (
    <div className="fade-in">
      <div className="grid-2" style={{ alignItems: 'start' }}>
        {/* Settings form */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Workspace Settings</span>
            {saved && <span className="badge emerald">Saved</span>}
          </div>
          <div className="card-body">
            <div className="grid-2 gap-sm">
              <div className="form-group">
                <label className="form-label">Workspace Name</label>
                <input className="form-input" value={form.name || ''} onChange={e => setForm({...form, name: e.target.value})} />
              </div>
              <div className="form-group">
                <label className="form-label">Brand Name</label>
                <input className="form-input" value={form.brand_name || ''} onChange={e => setForm({...form, brand_name: e.target.value})} />
              </div>
            </div>

            <div className="form-group">
              <label className="form-label">Website Domains (comma-separated)</label>
              <input className="form-input" value={form.domains || ''} onChange={e => setForm({...form, domains: e.target.value})}
                placeholder="example.com, blog.example.com" style={{ fontFamily: 'var(--font-mono)', fontSize: 12 }} />
            </div>

            <div className="grid-2 gap-sm">
              <div className="form-group">
                <label className="form-label">Target Countries</label>
                <input className="form-input" value={form.target_countries || ''} onChange={e => setForm({...form, target_countries: e.target.value})}
                  placeholder="US, UK, DE" />
              </div>
              <div className="form-group">
                <label className="form-label">Target Languages</label>
                <input className="form-input" value={form.target_languages || ''} onChange={e => setForm({...form, target_languages: e.target.value})}
                  placeholder="en, de, es" />
              </div>
            </div>

            <div className="form-group">
              <label className="form-label">Target AI Models</label>
              <input className="form-input" value={form.target_models || ''} onChange={e => setForm({...form, target_models: e.target.value})}
                placeholder="ChatGPT, Perplexity, Gemini, Claude, Copilot" />
            </div>

            <div className="form-group">
              <label className="form-label">Brand Voice Rules</label>
              <textarea className="form-textarea" value={form.brand_voice || ''} onChange={e => setForm({...form, brand_voice: e.target.value})}
                placeholder="Professional, evidence-based, warm but authoritative..." />
            </div>

            <div className="form-group">
              <label className="form-label">Compliance / Forbidden Claims</label>
              <textarea className="form-textarea" value={form.compliance_rules || ''} onChange={e => setForm({...form, compliance_rules: e.target.value})}
                style={{ color: 'var(--rose)' }}
                placeholder="No medical diagnosis claims. No guaranteed results. All statistics must be cited..." />
            </div>

            <div className="grid-2 gap-sm mb-md">
              <div className="form-group">
                <label className="form-label">Primary Color</label>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <input type="color" value={form.color_primary || '#2563EB'} onChange={e => setForm({...form, color_primary: e.target.value})}
                    style={{ width: 32, height: 32, border: 'none', cursor: 'pointer', borderRadius: 4 }} />
                  <input className="form-input" value={form.color_primary || ''} onChange={e => setForm({...form, color_primary: e.target.value})}
                    style={{ fontFamily: 'var(--font-mono)', fontSize: 12 }} />
                </div>
              </div>
              <div className="form-group">
                <label className="form-label">Accent Color</label>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <input type="color" value={form.color_accent || '#10B981'} onChange={e => setForm({...form, color_accent: e.target.value})}
                    style={{ width: 32, height: 32, border: 'none', cursor: 'pointer', borderRadius: 4 }} />
                  <input className="form-input" value={form.color_accent || ''} onChange={e => setForm({...form, color_accent: e.target.value})}
                    style={{ fontFamily: 'var(--font-mono)', fontSize: 12 }} />
                </div>
              </div>
            </div>

            <button className="btn btn-primary" onClick={handleSave} disabled={saving} style={{ width: '100%', justifyContent: 'center' }}>
              {saving ? 'Saving...' : 'Save Settings'}
            </button>
          </div>
        </div>

        {/* Members */}
        <div>
          <div className="card mb-md">
            <div className="card-header"><span className="card-title">Team Members</span></div>
            <div className="card-body">
              {detail?.members?.map(m => (
                <div key={m.id} className="flex-between" style={{ padding: '8px 0', borderBottom: '1px solid var(--border-subtle)' }}>
                  <div>
                    <div style={{ fontWeight: 500, fontSize: 13 }}>{m.name || m.email}</div>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>{m.email}</div>
                  </div>
                  <span className="badge blue">{m.workspace_role}</span>
                </div>
              ))}

              <div style={{ marginTop: 14, paddingTop: 14, borderTop: '1px solid var(--border-default)' }}>
                <div className="form-label" style={{ marginBottom: 6 }}>Add Member</div>
                <div style={{ display: 'flex', gap: 6 }}>
                  <input className="form-input" placeholder="email@..." value={addMemberEmail}
                    onChange={e => setAddMemberEmail(e.target.value)} style={{ flex: 1, fontSize: 12 }} />
                  <select className="form-input" value={addMemberRole} onChange={e => setAddMemberRole(e.target.value)}
                    style={{ width: 90, fontSize: 11 }}>
                    <option value="editor">Editor</option>
                    <option value="reviewer">Reviewer</option>
                    <option value="admin">Admin</option>
                    <option value="client">Client</option>
                  </select>
                  <button className="btn btn-sm btn-primary" onClick={handleAddMember}>Add</button>
                </div>
              </div>
            </div>
          </div>

          {/* Metric Targets */}
          <MetricTargets state={state} dispatch={dispatch} />
        </div>
      </div>
    </div>
  );
}

function MetricTargets({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;
  const [metricName, setMetricName] = useState('citation_rate');
  const [target, setTarget] = useState('');
  const [baseline, setBaseline] = useState('');

  useEffect(() => {
    if (!wsId) return;
    api(`/api/dashboard/metrics/${wsId}`, {}, state.token)
      .then(res => { if (res.success) dispatch({ type: 'SET_METRICS', data: res.data }); })
      .catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const handleAdd = async () => {
    if (!target) return;
    try {
      await api(`/api/dashboard/metrics/${wsId}`, {
        method: 'POST',
        body: JSON.stringify({ metric_name: metricName, target_value: parseFloat(target), baseline_value: parseFloat(baseline) || 0 }),
      }, state.token);
      setTarget(''); setBaseline('');
      const res = await api(`/api/dashboard/metrics/${wsId}`, {}, state.token);
      if (res.success) dispatch({ type: 'SET_METRICS', data: res.data });
    } catch (err) {}
  };

  return (
    <div className="card">
      <div className="card-header"><span className="card-title">Success Metrics</span></div>
      <div className="card-body">
        {state.metrics.map(m => (
          <MetricProgress key={m.id} metric={m} />
        ))}

        <div style={{ marginTop: 14, paddingTop: 14, borderTop: '1px solid var(--border-default)' }}>
          <div className="form-label" style={{ marginBottom: 6 }}>Add Metric Target</div>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            <select className="form-input" value={metricName} onChange={e => setMetricName(e.target.value)}
              style={{ width: 140, fontSize: 11 }}>
              <option value="citation_rate">Citation Rate</option>
              <option value="citation_count">Citations</option>
              <option value="visibility">Visibility</option>
              <option value="brand_mentions">Brand Mentions</option>
              <option value="position">Position</option>
            </select>
            <input className="form-input" placeholder="Baseline" value={baseline}
              onChange={e => setBaseline(e.target.value)} style={{ width: 80, fontSize: 11 }} type="number" step="0.01" />
            <input className="form-input" placeholder="Target" value={target}
              onChange={e => setTarget(e.target.value)} style={{ width: 80, fontSize: 11 }} type="number" step="0.01" />
            <button className="btn btn-sm btn-primary" onClick={handleAdd}>Set</button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// MAIN APP
// ═══════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════
// TASK BOARD (Kanban)
// ═══════════════════════════════════════════════════════════════

function TaskBoardPage({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;
  const [board, setBoard] = useState(null);
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm] = useState({ title: '', task_type: 'analysis', priority: 'medium', description: '' });

  const loadBoard = () => {
    if (!wsId) return;
    api(`/api/ops/tasks/${wsId}`, {}, state.token)
      .then(res => { if (res.success) setBoard(res.data); })
      .catch(err => console.warn('API:', err.message));
  };
  useEffect(loadBoard, [wsId]);

  const handleCreate = async () => {
    if (!form.title) return;
    await api(`/api/ops/tasks/${wsId}`, { method: 'POST', body: JSON.stringify(form) }, state.token);
    setShowCreate(false); setForm({ title: '', task_type: 'analysis', priority: 'medium', description: '' });
    loadBoard();
  };

  const moveTask = async (taskId, newStatus) => {
    await api(`/api/ops/tasks/${wsId}/${taskId}`, { method: 'PUT', body: JSON.stringify({ status: newStatus }) }, state.token);
    loadBoard();
  };

  const columns = [
    { key: 'todo', label: 'To Do', color: 'var(--text-muted)' },
    { key: 'in_progress', label: 'In Progress', color: 'var(--blue)' },
    { key: 'review', label: 'Review', color: 'var(--amber)' },
    { key: 'done', label: 'Done', color: 'var(--emerald)' },
  ];

  const priorityColors = { urgent: 'rose', high: 'amber', medium: 'blue', low: 'purple' };
  const typeIcons = { analysis: '\u{1F50D}', writing: '\u270F', review: '\u2714', publishing: '\u{1F4E4}', measurement: '\u{1F4CA}', onboarding: '\u{1F44B}', other: '\u25A3' };

  return (
    <div className="fade-in">
      <div className="flex-between mb-md">
        <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>
          {board ? `${Object.values(board).flat().length} tasks` : ''}
        </div>
        <button className="btn btn-primary" onClick={() => setShowCreate(true)}>+ New Task</button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: `repeat(${columns.length}, 1fr)`, gap: 12, minHeight: 400 }}>
        {columns.map(col => (
          <div key={col.key} style={{ background: 'var(--bg-surface)', borderRadius: 8, border: '1px solid var(--border-default)', overflow: 'hidden' }}>
            <div style={{ padding: '10px 14px', borderBottom: `2px solid ${col.color}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span style={{ fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--text-secondary)' }}>{col.label}</span>
              <span style={{ fontSize: 10, fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>{board?.[col.key]?.length || 0}</span>
            </div>
            <div style={{ padding: 8, minHeight: 100 }}>
              {(board?.[col.key] || []).map(task => (
                <div key={task.id} style={{ background: 'var(--bg-raised)', borderRadius: 6, padding: '10px 12px', marginBottom: 8, border: '1px solid var(--border-subtle)', cursor: 'pointer' }}>
                  <div style={{ fontSize: 12, fontWeight: 500, marginBottom: 4 }}>{task.title}</div>
                  {task.description && <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 6, lineHeight: 1.4 }}>{task.description.slice(0, 80)}</div>}
                  <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', alignItems: 'center' }}>
                    <span className={`badge ${priorityColors[task.priority] || 'blue'}`}>{task.priority}</span>
                    <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{task.task_type}</span>
                    {task.due_date && <span style={{ fontSize: 9, fontFamily: 'var(--font-mono)', color: 'var(--text-muted)', marginLeft: 'auto' }}>{task.due_date.slice(0, 10)}</span>}
                  </div>
                  <div style={{ display: 'flex', gap: 4, marginTop: 6 }}>
                    {columns.filter(c => c.key !== col.key).map(c => (
                      <button key={c.key} className="btn btn-sm" style={{ fontSize: 9, padding: '2px 6px' }}
                        onClick={(e) => { e.stopPropagation(); moveTask(task.id, c.key); }}>
                        {'\u2192'} {c.label}
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>

      {showCreate && (
        <div className="modal-overlay" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header"><h3>New Task</h3><button className="modal-close" onClick={() => setShowCreate(false)}>{'\u00D7'}</button></div>
            <div className="modal-body">
              <div className="form-group"><label className="form-label">Title</label><input className="form-input" value={form.title} onChange={e => setForm({...form, title: e.target.value})} placeholder="Analyze gut health cluster" /></div>
              <div className="form-group"><label className="form-label">Description</label><textarea className="form-textarea" value={form.description} onChange={e => setForm({...form, description: e.target.value})} /></div>
              <div className="grid-2 gap-sm">
                <div className="form-group"><label className="form-label">Type</label>
                  <select className="form-input" value={form.task_type} onChange={e => setForm({...form, task_type: e.target.value})}>
                    <option value="analysis">Analysis</option><option value="writing">Writing</option><option value="review">Review</option>
                    <option value="publishing">Publishing</option><option value="measurement">Measurement</option><option value="onboarding">Onboarding</option><option value="other">Other</option>
                  </select></div>
                <div className="form-group"><label className="form-label">Priority</label>
                  <select className="form-input" value={form.priority} onChange={e => setForm({...form, priority: e.target.value})}>
                    <option value="urgent">Urgent</option><option value="high">High</option><option value="medium">Medium</option><option value="low">Low</option>
                  </select></div>
              </div>
            </div>
            <div className="modal-footer"><button className="btn" onClick={() => setShowCreate(false)}>Cancel</button><button className="btn btn-primary" onClick={handleCreate}>Create Task</button></div>
          </div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// PLAYBOOKS PAGE
// ═══════════════════════════════════════════════════════════════

function PlaybooksPage({ state }) {
  const [playbooks, setPlaybooks] = useState([]);
  const [selected, setSelected] = useState(null);

  useEffect(() => {
    api('/api/ops/playbooks', {}, state.token)
      .then(res => { if (res.success) setPlaybooks(res.data); })
      .catch(err => console.warn('API:', err.message));
  }, []);

  const catColors = { onboarding: 'emerald', generation: 'blue', reporting: 'amber', general: 'purple' };

  return (
    <div className="fade-in">
      <div className="grid-1-2">
        <div className="card">
          <div className="card-header"><span className="card-title">SOP Library</span></div>
          <div className="card-body" style={{ padding: 0 }}>
            {playbooks.map(pb => (
              <div key={pb.id} style={{ padding: '14px 18px', borderBottom: '1px solid var(--border-subtle)', cursor: 'pointer',
                background: selected?.id === pb.id ? 'var(--blue-dim)' : 'transparent' }}
                onClick={() => setSelected(pb)}>
                <div style={{ fontWeight: 500, fontSize: 13 }}>{pb.name}</div>
                <div style={{ display: 'flex', gap: 8, marginTop: 4 }}>
                  <span className={`badge ${catColors[pb.category] || 'blue'}`}>{pb.category}</span>
                  <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{pb.steps?.length || 0} steps</span>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="card">
          <div className="card-header"><span className="card-title">{selected?.name || 'Select a playbook'}</span></div>
          <div className="card-body">
            {selected ? (
              <div>
                <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 16 }}>{selected.description}</div>
                {selected.steps?.map((step, i) => (
                  <div key={i} style={{ marginBottom: 16, paddingBottom: 16, borderBottom: '1px solid var(--border-subtle)' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
                      <div style={{ width: 24, height: 24, borderRadius: '50%', background: 'var(--blue-dim)', color: 'var(--blue)',
                        display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 600, fontFamily: 'var(--font-mono)' }}>
                        {step.order}
                      </div>
                      <span style={{ fontWeight: 500, fontSize: 13 }}>{step.title}</span>
                      {step.est_minutes && <span style={{ marginLeft: 'auto', fontSize: 10, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>{step.est_minutes}min</span>}
                    </div>
                    <div style={{ paddingLeft: 34, fontSize: 12, color: 'var(--text-secondary)', marginBottom: 8 }}>{step.description}</div>
                    {step.checklist?.length > 0 && (
                      <div style={{ paddingLeft: 34 }}>
                        {step.checklist.map((item, j) => (
                          <div key={j} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: 'var(--text-muted)', padding: '2px 0' }}>
                            <span style={{ opacity: 0.5 }}>{'\u25A1'}</span> {item}
                          </div>
                        ))}
                      </div>
                    )}
                    {step.assignee_role && <div style={{ paddingLeft: 34, marginTop: 4 }}><span className="badge blue" style={{ fontSize: 9 }}>{step.assignee_role}</span></div>}
                  </div>
                ))}
              </div>
            ) : (
              <div className="empty-state"><h4>Select a playbook to view steps</h4></div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// COMPETITORS PAGE
// ═══════════════════════════════════════════════════════════════

function CompetitorsPage({ state, dispatch }) {
  const wsId = state.activeWorkspace?.id;
  const [competitors, setCompetitors] = useState([]);
  const [domain, setDomain] = useState('');
  const [name, setName] = useState('');

  const load = () => {
    if (!wsId) return;
    api(`/api/ops/competitors/${wsId}`, {}, state.token)
      .then(res => { if (res.success) setCompetitors(res.data); })
      .catch(err => console.warn('API:', err.message));
  };
  useEffect(load, [wsId]);

  const handleAdd = async () => {
    if (!domain) return;
    await api(`/api/ops/competitors/${wsId}`, { method: 'POST', body: JSON.stringify({ domain, name }) }, state.token);
    setDomain(''); setName(''); load();
  };

  return (
    <div className="fade-in">
      <div className="card mb-lg">
        <div className="card-header"><span className="card-title">Track Competitor</span></div>
        <div className="card-body">
          <div style={{ display: 'flex', gap: 8 }}>
            <input className="form-input" placeholder="competitor.com" value={domain} onChange={e => setDomain(e.target.value)} style={{ flex: 2 }} />
            <input className="form-input" placeholder="Display name" value={name} onChange={e => setName(e.target.value)} style={{ flex: 1 }} />
            <button className="btn btn-primary" onClick={handleAdd}>Add</button>
            <button className="btn" onClick={() => api(`/api/ops/competitors/${wsId}/refresh`, { method: 'POST' }, state.token).then(load)}>Refresh Stats</button>
          </div>
        </div>
      </div>

      <div className="card">
        <div className="card-header"><span className="card-title">Tracked Competitors</span><span className="badge blue">{competitors.length}</span></div>
        <div className="card-body" style={{ padding: 0 }}>
          {competitors.length ? (
            <table className="data-table">
              <thead><tr><th>Domain</th><th>Name</th><th>Citations</th><th>Avg Rate</th><th>Last Checked</th></tr></thead>
              <tbody>
                {competitors.map(c => (
                  <tr key={c.id}>
                    <td style={{ fontFamily: 'var(--font-mono)', color: 'var(--blue)' }}>{c.domain}</td>
                    <td>{c.name || '\u2014'}</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{c.citation_count}</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{((c.avg_citation_rate || 0) * 100).toFixed(1)}%</td>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-muted)' }}>{c.last_checked?.slice(0, 16) || 'Never'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty-state"><div className="empty-icon">{'\u2694'}</div><h4>No competitors tracked</h4><p style={{ fontSize: 12 }}>Add competitor domains to track their AI citation performance.</p></div>
          )}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// COMPETITOR PROFILES PAGE (Phase 3)
// ═══════════════════════════════════════════════════════════════

function CompetitorProfilesPage({ state, dispatch }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [profiles, setProfiles] = useState([]);
  const [newDomain, setNewDomain] = useState('');
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState('');
  const [loading, setLoading] = useState(false);
  const [refreshingDomain, setRefreshingDomain] = useState(null);

  const load = () => {
    if (!wsId) return;
    setLoading(true);
    api(`/api/competitors/${wsId}/tracked`, {}, token)
      .then(r => setProfiles(r.data || r || []))
      .catch(e => setMsg('Load failed: ' + e.message))
      .finally(() => setLoading(false));
  };
  useEffect(load, [wsId]);

  const trackNew = async () => {
    const d = newDomain.trim();
    if (!d) return;
    setBusy(true); setMsg('');
    try {
      await api(`/api/competitors/${wsId}/${encodeURIComponent(d)}/track`, { method: 'POST' }, token);
      setNewDomain('');
      setMsg(`Tracking ${d} ✓`);
      load();
    } catch (e) { setMsg('Track failed: ' + e.message); }
    setBusy(false);
  };

  const refreshProfile = async (d) => {
    setRefreshingDomain(d); setMsg('');
    try {
      const r = await api(`/api/competitors/${wsId}/${encodeURIComponent(d)}/profile`, {}, token);
      const next = r.data || r;
      setProfiles(ps => ps.map(p => (p.domain === d ? { ...p, ...next } : p)));
    } catch (e) { setMsg('Refresh failed: ' + e.message); }
    setRefreshingDomain(null);
  };

  const pushAttack = async (d) => {
    setMsg('');
    try {
      const r = await api(`/api/attack-map/${wsId}/${encodeURIComponent(d)}/push-actions`, { method: 'POST' }, token);
      const dd = r.data || r;
      const n = dd.created ?? (dd.action_ids || []).length ?? 0;
      setMsg(`${n} attack actions queued for ${d} ✓`);
    } catch (e) { setMsg('Push failed: ' + e.message); }
  };

  const eur = v => `€${Math.round(Number(v) || 0).toLocaleString()}`;
  const stageColor = { decision: 'rose', trust: 'amber', comparison: 'purple', objection: 'blue', solution: 'emerald', problem: 'blue', awareness: 'gray' };

  const movementChip = (m) => {
    const v = Number(m);
    if (!m && m !== 0) return <span style={{ color: 'var(--text-muted)' }}>—</span>;
    const isNum = !isNaN(v);
    const up = isNum ? v > 0 : /up|gain|rise|\+/i.test(String(m));
    const down = isNum ? v < 0 : /down|loss|drop|-/i.test(String(m));
    const arrow = up ? '↑' : down ? '↓' : '→';
    const color = up ? 'var(--rose)' : down ? 'var(--emerald)' : 'var(--text-muted)';
    const label = isNum ? `${arrow} ${v > 0 ? '+' : ''}${v}` : `${arrow} ${m}`;
    return (
      <span style={{
        display: 'inline-block', padding: '1px 7px', borderRadius: 9,
        fontSize: 10, fontWeight: 600, fontFamily: 'var(--font-mono)',
        color, background: `${color}22`, whiteSpace: 'nowrap',
      }}>{label}</span>
    );
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header">Competitor Profiles — full intelligence</div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          Track competitors to capture prompts-won, € revenue captured, strongest stage/topic, weakest factor, citation sources, and a suggested attack.
        </p>
        <div style={{ display: 'flex', gap: 6, marginTop: 6 }}>
          <input className="form-input" placeholder="Track new competitor (e.g. medicover.hu)"
            value={newDomain} onChange={e => setNewDomain(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter') trackNew(); }}
            style={{ flex: 1 }} />
          <button className="btn btn-primary" onClick={trackNew} disabled={busy || !newDomain.trim()}>
            {busy ? 'Tracking…' : 'Track competitor'}
          </button>
          <button className="btn" onClick={load} disabled={loading}>{loading ? 'Loading…' : 'Refresh'}</button>
        </div>
        {msg && <div style={{ fontSize: 11, color: msg.toLowerCase().includes('failed') ? 'var(--rose)' : 'var(--emerald)', marginTop: 6 }}>{msg}</div>}
      </div>

      {profiles.length === 0 ? (
        <div className="empty-state">
          ☉<br/>
          No competitors tracked yet. Click "Track Competitor" on the Attack Map page to add one.
          <div style={{ marginTop: 12 }}>
            <button className="btn btn-sm btn-primary" onClick={() => dispatch && dispatch({ type: 'SET_VIEW', view: 'attack_map' })}>
              Go to Attack Map
            </button>
          </div>
        </div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(420px, 1fr))', gap: 16 }}>
          {profiles.map(p => {
            const promptsWon = Number(p.prompts_won || 0);
            const hvWon = Number(p.high_value_prompts_won || 0);
            const avgPos = p.avg_position != null
              ? Number(p.avg_position).toFixed(1)
              : (p.average_position != null ? Number(p.average_position).toFixed(1) : '—');
            return (
              <div className="card" key={p.domain}>
                <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--blue)' }}>{p.domain}</span>
                    <span className="badge emerald">tracked</span>
                    <ConfidenceBadge level={p.confidence || 'estimated'} />
                  </div>
                  <button className="btn btn-sm" onClick={() => refreshProfile(p.domain)} disabled={refreshingDomain === p.domain}>
                    {refreshingDomain === p.domain ? '…' : 'Refresh'}
                  </button>
                </div>

                {/* 6-stat strip */}
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 6, marginBottom: 10 }}>
                  <div className="metric-card" style={{ padding: 8 }}>
                    <div className="metric-label">PROMPTS WON</div>
                    <div className="metric-value" style={{ fontSize: 20 }}>{promptsWon}</div>
                  </div>
                  <div className="metric-card" style={{ padding: 8 }}>
                    <div className="metric-label">HIGH-VALUE WON</div>
                    <div className="metric-value" style={{ fontSize: 20, color: 'var(--rose)' }}>{hvWon}</div>
                  </div>
                  <div className="metric-card" style={{ padding: 8 }}>
                    <div className="metric-label">€ REVENUE CAPTURED</div>
                    <div className="metric-value" style={{ fontSize: 18, color: 'var(--amber)' }}>{eur(p.est_revenue_captured)}</div>
                  </div>
                  <div className="metric-card" style={{ padding: 8 }}>
                    <div className="metric-label">AVG POSITION</div>
                    <div className="metric-value" style={{ fontSize: 20 }}>{avgPos}</div>
                  </div>
                  <div className="metric-card" style={{ padding: 8 }}>
                    <div className="metric-label">STRONGEST STAGE</div>
                    <div style={{ marginTop: 4 }}>
                      {p.strongest_stage
                        ? <span className={`badge ${stageColor[p.strongest_stage] || 'gray'}`}>{p.strongest_stage}</span>
                        : <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>—</span>}
                    </div>
                  </div>
                  <div className="metric-card" style={{ padding: 8 }}>
                    <div className="metric-label">STRONGEST TOPIC</div>
                    <div style={{ marginTop: 4, fontSize: 12, fontWeight: 600, color: 'var(--cyan)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                         title={p.strongest_topic || ''}>
                      {p.strongest_topic || '—'}
                    </div>
                  </div>
                </div>

                {/* Weakest + movement */}
                <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap', marginBottom: 10 }}>
                  <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Weakest factor:</span>
                  {p.weakest_factor
                    ? <span className="badge rose">{p.weakest_factor}</span>
                    : <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>—</span>}
                  <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginLeft: 8 }}>30d movement:</span>
                  {movementChip(p.recent_30d_movement)}
                </div>

                {/* Citation sources */}
                {(p.citation_sources || []).length > 0 && (
                  <div style={{ marginBottom: 10 }}>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Citation sources (top 5)</div>
                    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                      {p.citation_sources.slice(0, 5).map((s, i) => (
                        <span key={i} className="badge">
                          {typeof s === 'string' ? s : (s.source || s.name || s.domain || '?')}
                          {s && typeof s === 'object' && s.count != null ? ` · ${s.count}` : ''}
                        </span>
                      ))}
                    </div>
                  </div>
                )}

                {/* Suggested attack */}
                {p.suggested_attack && (
                  <div style={{ borderLeft: '2px solid var(--emerald)', paddingLeft: 10, marginBottom: 10 }}>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 3 }}>Suggested attack</div>
                    <div style={{ fontSize: 12, fontStyle: 'italic', color: 'var(--text-secondary)', lineHeight: 1.5 }}>
                      {p.suggested_attack}
                    </div>
                  </div>
                )}

                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderTop: '1px solid var(--border-subtle)', paddingTop: 8, gap: 6, flexWrap: 'wrap' }}>
                  <span style={{ fontSize: 10, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                    {p.last_analyzed ? `Updated ${_relTime(p.last_analyzed)}` : ''}
                  </span>
                  <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                    <button className="btn btn-sm" onClick={() => dispatch && dispatch({ type: 'SET_VIEW', view: 'attack_map' })}>
                      View Attack Map row
                    </button>
                    <button className="btn btn-sm btn-primary" onClick={() => pushAttack(p.domain)}>
                      Push Attack to Action Engine
                    </button>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// RECOMMENDATIONS PAGE
// ═══════════════════════════════════════════════════════════════

function RecommendationsPage({ state }) {
  const wsId = state.activeWorkspace?.id;
  const [recs, setRecs] = useState([]);
  const [filter, setFilter] = useState('new');
  const [scanning, setScanning] = useState(false);
  const [scanResult, setScanResult] = useState(null);

  const load = () => {
    if (!wsId) return;
    api(`/api/ops/recommendations/${wsId}?status=${filter}`, {}, state.token)
      .then(res => { if (res.success) setRecs(res.data); })
      .catch(err => console.warn('API:', err.message));
  };
  useEffect(load, [wsId, filter]);

  const updateStatus = async (id, status) => {
    await api(`/api/ops/recommendations/${wsId}/${id}?status=${status}`, { method: 'PUT' }, state.token);
    load();
  };

  const runScan = async () => {
    if (!wsId) return;
    setScanning(true);
    setScanResult(null);
    try {
      const r = await api(`/api/insights/scan/${wsId}`, { method: 'POST' }, state.token);
      const d = r.data || r;
      setScanResult({
        success: r.success !== false,
        msg: `Scan complete — ${d.sentiment_alerts || 0} sentiment, ${d.competitor_alerts || 0} competitor, ${d.emerging_topics || 0} emerging. ${d.recommendations_created || 0} new recs.`,
      });
      load();
    } catch (e) {
      setScanResult({ success: false, msg: typeof e.message === 'string' ? e.message : JSON.stringify(e.message) });
    }
    setScanning(false);
  };

  const typeColors = { new_topic: 'emerald', refresh: 'amber', gap: 'rose', opportunity: 'blue', competitor_alert: 'purple' };

  return (
    <div className="fade-in">
      <div className="flex-between mb-md">
        <div style={{ display: 'flex', gap: 6 }}>
          {['new', 'accepted', 'dismissed', 'completed'].map(s => (
            <button key={s} className={`btn btn-sm ${filter === s ? 'btn-primary' : ''}`} onClick={() => setFilter(s)}>{s}</button>
          ))}
        </div>
        <button className="btn btn-sm btn-primary" onClick={runScan} disabled={scanning}>
          {scanning ? 'Scanning...' : 'Run Insights Scan'}
        </button>
      </div>
      {scanResult && (
        <div className="card mb-md" style={{ borderLeft: `3px solid var(--${scanResult.success ? 'emerald' : 'rose'})` }}>
          <div style={{ color: scanResult.success ? 'var(--emerald)' : 'var(--rose)', padding: 8, fontSize: 12 }}>
            {scanResult.msg}
          </div>
        </div>
      )}

      {recs.length ? recs.map(rec => (
        <div key={rec.id} className="card mb-md">
          <div className="card-body">
            <div className="flex-between" style={{ marginBottom: 8 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                <span className={`badge ${typeColors[rec.rec_type] || 'blue'}`}>{rec.rec_type.replace('_', ' ')}</span>
                <span style={{ fontWeight: 600, fontSize: 14 }}>{rec.title}</span>
              </div>
              <div style={{ fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--amber)' }}>Score: {rec.priority_score}</div>
            </div>
            {rec.description && <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 10 }}>{rec.description}</div>}
            {filter === 'new' && (
              <div style={{ display: 'flex', gap: 6 }}>
                <button className="btn btn-sm btn-primary" onClick={() => updateStatus(rec.id, 'accepted')}>Accept</button>
                <button className="btn btn-sm" onClick={() => updateStatus(rec.id, 'dismissed')}>Dismiss</button>
              </div>
            )}
          </div>
        </div>
      )) : (
        <div className="empty-state"><div className="empty-icon">{'\u2605'}</div><h4>No {filter} recommendations</h4>
          <p style={{ fontSize: 12 }}>Recommendations appear as the system analyzes your data.</p></div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// ONBOARDING PAGE
// ═══════════════════════════════════════════════════════════════

function OnboardingPage({ state }) {
  const wsId = state.activeWorkspace?.id;
  const [ob, setOb] = useState(null);
  const [form, setForm] = useState({});

  useEffect(() => {
    if (!wsId) return;
    api(`/api/ops/onboarding/${wsId}`, {}, state.token)
      .then(res => {
        if (res.success && res.data) { setOb(res.data); setForm(res.data); }
        else setForm({ status: 'pending', domain_info: '', target_topics: [], competitors: [], brand_voice_notes: '', cms_type: '', cms_access: 0, approval_workflow: '', notes: '' });
      }).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const handleSave = async () => {
    const payload = { ...form };
    if (typeof payload.target_topics === 'string') payload.target_topics = payload.target_topics.split(',').map(s => s.trim()).filter(Boolean);
    if (typeof payload.competitors === 'string') payload.competitors = payload.competitors.split(',').map(s => s.trim()).filter(Boolean);
    await api(`/api/ops/onboarding/${wsId}`, { method: 'PUT', body: JSON.stringify(payload) }, state.token);
  };

  const steps = ['Domain info', 'Target topics', 'Brand voice', 'Peec integration', 'Workspace config', 'CMS access', 'Approval workflow', 'First analysis'];
  const completed = form.completed_steps || [];

  return (
    <div className="fade-in">
      <div className="grid-2" style={{ alignItems: 'start' }}>
        <div className="card">
          <div className="card-header"><span className="card-title">Onboarding Checklist</span><span className={`badge ${form.status === 'completed' ? 'emerald' : 'amber'}`}>{form.status || 'pending'}</span></div>
          <div className="card-body">
            {steps.map((step, i) => {
              const done = completed.includes(step);
              return (
                <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 0', borderBottom: '1px solid var(--border-subtle)', cursor: 'pointer' }}
                  onClick={() => {
                    const newCompleted = done ? completed.filter(s => s !== step) : [...completed, step];
                    setForm({...form, completed_steps: newCompleted});
                  }}>
                  <div style={{ width: 20, height: 20, borderRadius: 4, border: `2px solid ${done ? 'var(--emerald)' : 'var(--border-strong)'}`,
                    background: done ? 'var(--emerald-dim)' : 'transparent', display: 'flex', alignItems: 'center', justifyContent: 'center',
                    fontSize: 12, color: 'var(--emerald)' }}>
                    {done ? '\u2714' : ''}
                  </div>
                  <span style={{ fontSize: 13, color: done ? 'var(--text-muted)' : 'var(--text-primary)', textDecoration: done ? 'line-through' : 'none' }}>{step}</span>
                </div>
              );
            })}
            <div style={{ marginTop: 14, fontSize: 12, color: 'var(--text-muted)' }}>
              {completed.length}/{steps.length} completed
            </div>
            <div className="progress-bar" style={{ marginTop: 6 }}>
              <div className="progress-fill emerald" style={{ width: `${(completed.length / steps.length) * 100}%` }} />
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-header"><span className="card-title">Client Details</span></div>
          <div className="card-body">
            <div className="form-group"><label className="form-label">Domain Info</label>
              <input className="form-input" value={form.domain_info || ''} onChange={e => setForm({...form, domain_info: e.target.value})} placeholder="example.com" /></div>
            <div className="form-group"><label className="form-label">Target Topics (comma-separated)</label>
              <textarea className="form-textarea" value={Array.isArray(form.target_topics) ? form.target_topics.join(', ') : form.target_topics || ''} onChange={e => setForm({...form, target_topics: e.target.value})} placeholder="gut health, probiotics, microbiome..." /></div>
            <div className="form-group"><label className="form-label">Competitors (comma-separated)</label>
              <textarea className="form-textarea" value={Array.isArray(form.competitors) ? form.competitors.join(', ') : form.competitors || ''} onChange={e => setForm({...form, competitors: e.target.value})} placeholder="competitor1.com, competitor2.com" /></div>
            <div className="form-group"><label className="form-label">Brand Voice Notes</label>
              <textarea className="form-textarea" value={form.brand_voice_notes || ''} onChange={e => setForm({...form, brand_voice_notes: e.target.value})} /></div>
            <div className="grid-2 gap-sm">
              <div className="form-group"><label className="form-label">CMS Type</label>
                <select className="form-input" value={form.cms_type || ''} onChange={e => setForm({...form, cms_type: e.target.value})}>
                  <option value="">Select...</option><option value="wordpress">WordPress</option><option value="webflow">Webflow</option>
                  <option value="shopify">Shopify</option><option value="headless">Headless CMS</option><option value="none">None</option>
                </select></div>
              <div className="form-group"><label className="form-label">CMS Access?</label>
                <select className="form-input" value={form.cms_access || 0} onChange={e => setForm({...form, cms_access: parseInt(e.target.value)})}>
                  <option value={0}>No</option><option value={1}>Yes</option>
                </select></div>
            </div>
            <div className="form-group"><label className="form-label">Approval Workflow</label>
              <input className="form-input" value={form.approval_workflow || ''} onChange={e => setForm({...form, approval_workflow: e.target.value})} placeholder="Editor -> Reviewer -> Client" /></div>
            <div className="form-group"><label className="form-label">Notes</label>
              <textarea className="form-textarea" value={form.notes || ''} onChange={e => setForm({...form, notes: e.target.value})} /></div>
            <button className="btn btn-primary" onClick={handleSave} style={{ width: '100%', justifyContent: 'center' }}>Save Onboarding Info</button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// BILLING PAGE
// ═══════════════════════════════════════════════════════════════

function BillingPage({ state }) {
  const wsId = state.activeWorkspace?.id;
  const [records, setRecords] = useState([]);
  const [start, setStart] = useState('');
  const [end, setEnd] = useState('');

  const load = () => {
    if (!wsId) return;
    api(`/api/ops/billing/${wsId}`, {}, state.token)
      .then(res => { if (res.success) setRecords(res.data); })
      .catch(err => console.warn('API:', err.message));
  };
  useEffect(load, [wsId]);

  const generate = async () => {
    if (!start || !end) return;
    await api(`/api/ops/billing/${wsId}`, { method: 'POST', body: JSON.stringify({ period_start: start, period_end: end }) }, state.token);
    load();
  };

  return (
    <div className="fade-in">
      <div className="card mb-lg">
        <div className="card-header"><span className="card-title">Generate Billing Record</span></div>
        <div className="card-body">
          <div style={{ display: 'flex', gap: 8, alignItems: 'end' }}>
            <div className="form-group" style={{ marginBottom: 0 }}><label className="form-label">Period Start</label><input className="form-input" type="date" value={start} onChange={e => setStart(e.target.value)} /></div>
            <div className="form-group" style={{ marginBottom: 0 }}><label className="form-label">Period End</label><input className="form-input" type="date" value={end} onChange={e => setEnd(e.target.value)} /></div>
            <button className="btn btn-primary" onClick={generate}>Generate</button>
          </div>
        </div>
      </div>

      <div className="card">
        <div className="card-header"><span className="card-title">Billing History</span></div>
        <div className="card-body" style={{ padding: 0 }}>
          {records.length ? (
            <table className="data-table">
              <thead><tr><th>Period</th><th>API Cost</th><th>Scrapes</th><th>Content</th><th>Published</th><th>Total</th></tr></thead>
              <tbody>
                {records.map(r => (
                  <tr key={r.id}>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{r.period_start?.slice(0, 10)} {'\u2192'} {r.period_end?.slice(0, 10)}</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>${(r.token_cost || 0).toFixed(2)}</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{r.scrape_count}</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{r.content_count}</td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{r.publish_count}</td>
                    <td style={{ fontFamily: 'var(--font-mono)', fontWeight: 600 }}>${(r.total_cost || 0).toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty-state"><h4>No billing records</h4></div>
          )}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// JOB QUEUE PAGE
// ═══════════════════════════════════════════════════════════════

function JobQueuePage({ state }) {
  const wsId = state.activeWorkspace?.id;
  const [jobs, setJobs] = useState([]);
  const [stats, setStats] = useState(null);
  const [filter, setFilter] = useState('');

  const load = () => {
    if (!wsId) return;
    api(`/api/ops/jobs/${wsId}?status=${filter}`, {}, state.token).then(res => { if (res.success) setJobs(res.data); }).catch(err => console.warn('API:', err.message));
    api(`/api/ops/jobs/${wsId}/stats`, {}, state.token).then(res => { if (res.success) setStats(res.data); }).catch(err => console.warn('API:', err.message));
  };
  useEffect(load, [wsId, filter]);

  const statusColors = { queued: 'blue', running: 'amber', completed: 'emerald', failed: 'rose', cancelled: 'purple', retrying: 'amber' };

  return (
    <div className="fade-in">
      {stats && (
        <div className="metrics-grid mb-lg">
          {Object.entries(stats.status_counts || {}).map(([status, cnt]) => (
            <MetricCard key={status} label={status.toUpperCase()} value={cnt} color={statusColors[status] || 'blue'} />
          ))}
        </div>
      )}

      <div className="flex-between mb-md">
        <div style={{ display: 'flex', gap: 6 }}>
          {['', 'queued', 'running', 'completed', 'failed'].map(s => (
            <button key={s} className={`btn btn-sm ${filter === s ? 'btn-primary' : ''}`} onClick={() => setFilter(s)}>
              {s || 'All'}
            </button>
          ))}
        </div>
      </div>

      <div className="card">
        <div className="card-body" style={{ padding: 0 }}>
          {jobs.length ? (
            <table className="data-table">
              <thead><tr><th>ID</th><th>Type</th><th>Status</th><th>Priority</th><th>Retries</th><th>Created</th><th>Duration</th></tr></thead>
              <tbody>
                {jobs.map(j => (
                  <tr key={j.id}>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 10 }}>{j.id.slice(0, 12)}</td>
                    <td><span className="badge blue">{j.job_type}</span></td>
                    <td><span className={`badge ${statusColors[j.status] || 'blue'}`}>{j.status}</span></td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{j.priority}</td>
                    <td style={{ fontFamily: 'var(--font-mono)', color: j.retry_count > 0 ? 'var(--amber)' : 'var(--text-muted)' }}>{j.retry_count}/{j.max_retries}</td>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)' }}>{j.created_at?.replace('T', ' ').slice(0, 19)}</td>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)' }}>
                      {j.started_at && j.completed_at ? `${Math.round((new Date(j.completed_at) - new Date(j.started_at)) / 1000)}s` : j.started_at ? 'running...' : '\u2014'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty-state"><h4>No jobs {filter ? `with status "${filter}"` : ''}</h4></div>
          )}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// AUTOMATIONS PAGE
// ═══════════════════════════════════════════════════════════════

function AutomationsPage({ state }) {
  const wsId = state.activeWorkspace?.id;
  const [rules, setRules] = useState([]);
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm] = useState({ name: '', trigger_event: 'csv_uploaded', action_type: 'create_job' });

  const load = () => {
    if (!wsId) return;
    api(`/api/ops/automations/${wsId}`, {}, state.token).then(res => { if (res.success) setRules(res.data); }).catch(err => console.warn('API:', err.message));
  };
  useEffect(load, [wsId]);

  const handleCreate = async () => {
    if (!form.name) return;
    await api(`/api/ops/automations/${wsId}`, { method: 'POST', body: JSON.stringify(form) }, state.token);
    setShowCreate(false); load();
  };

  const toggle = async (ruleId) => {
    await api(`/api/ops/automations/${wsId}/${ruleId}/toggle`, { method: 'PUT' }, state.token);
    load();
  };

  const triggerEvents = ['csv_uploaded', 'scrape_complete', 'analysis_complete', 'draft_approved', 'performance_drop', 'job_created', 'schedule'];
  const actionTypes = ['create_job', 'notify', 'run_scrape', 'run_analysis', 'run_generation', 'publish', 'measure'];

  return (
    <div className="fade-in">
      <div className="flex-between mb-lg">
        <div />
        <button className="btn btn-primary" onClick={() => setShowCreate(true)}>+ New Rule</button>
      </div>

      <div className="card">
        <div className="card-header"><span className="card-title">Automation Rules</span><span className="badge blue">{rules.length}</span></div>
        <div className="card-body" style={{ padding: 0 }}>
          {rules.length ? (
            <table className="data-table">
              <thead><tr><th>Name</th><th>Trigger</th><th>Action</th><th>Active</th><th>Triggered</th><th>Last Run</th></tr></thead>
              <tbody>
                {rules.map(r => (
                  <tr key={r.id}>
                    <td style={{ fontWeight: 500 }}>{r.name}</td>
                    <td><span className="badge purple">{r.trigger_event}</span></td>
                    <td><span className="badge blue">{r.action_type}</span></td>
                    <td>
                      <button className={`btn btn-sm ${r.is_active ? 'btn-primary' : ''}`} onClick={() => toggle(r.id)}>
                        {r.is_active ? 'ON' : 'OFF'}
                      </button>
                    </td>
                    <td style={{ fontFamily: 'var(--font-mono)' }}>{r.trigger_count}x</td>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)' }}>{r.last_triggered?.slice(0, 16) || 'Never'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty-state"><div className="empty-icon">{'\u26A1'}</div><h4>No automation rules</h4>
              <p style={{ fontSize: 12 }}>Create rules to automate pipeline steps (e.g., auto-analyze when scrape completes).</p></div>
          )}
        </div>
      </div>

      {showCreate && (
        <div className="modal-overlay" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header"><h3>New Automation Rule</h3><button className="modal-close" onClick={() => setShowCreate(false)}>{'\u00D7'}</button></div>
            <div className="modal-body">
              <div className="form-group"><label className="form-label">Rule Name</label><input className="form-input" value={form.name} onChange={e => setForm({...form, name: e.target.value})} placeholder="Auto-analyze after scrape" /></div>
              <div className="grid-2 gap-sm">
                <div className="form-group"><label className="form-label">When (Trigger Event)</label>
                  <select className="form-input" value={form.trigger_event} onChange={e => setForm({...form, trigger_event: e.target.value})}>
                    {triggerEvents.map(e => <option key={e} value={e}>{e.replace(/_/g, ' ')}</option>)}
                  </select></div>
                <div className="form-group"><label className="form-label">Then (Action)</label>
                  <select className="form-input" value={form.action_type} onChange={e => setForm({...form, action_type: e.target.value})}>
                    {actionTypes.map(a => <option key={a} value={a}>{a.replace(/_/g, ' ')}</option>)}
                  </select></div>
              </div>
            </div>
            <div className="modal-footer"><button className="btn" onClick={() => setShowCreate(false)}>Cancel</button><button className="btn btn-primary" onClick={handleCreate}>Create Rule</button></div>
          </div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// DATA IMPORT PAGE (Peec Ingestion Layer)
// ═══════════════════════════════════════════════════════════════

function DataImportPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [records, setRecords] = useState([]);
  const [fieldMapping, setFieldMapping] = useState(null);
  const [csvText, setCsvText] = useState('');
  const [importing, setImporting] = useState(false);
  const [apiSyncing, setApiSyncing] = useState(false);
  const [mcpSyncing, setMcpSyncing] = useState(false);
  const [mcpStatus, setMcpStatus] = useState(null);
  const [mcpState, setMcpState] = useState(null);
  const [importResult, setImportResult] = useState(null);
  const [tab, setTab] = useState('upload');
  const recPag = usePagination(records, 50);

  useEffect(() => {
    if (!wsId) return;
    api('/api/peec/records/' + wsId, {}, token).then(r => setRecords(r.data || r.records || [])).catch(err => console.warn('API:', err.message));
    api('/api/peec/field-mapping', {}, token).then(r => setFieldMapping(r.data || r)).catch(err => console.warn('API:', err.message));
    refreshMcpStatus();
    api('/api/peec/mcp/sync/' + wsId + '/state', {}, token).then(r => setMcpState(r.data || r)).catch(() => {});
  }, [wsId]);

  const refreshMcpStatus = () => {
    if (!wsId) return;
    api('/api/peec/mcp/status?ws_id=' + wsId, {}, token).then(r => setMcpStatus(r.data || r)).catch(() => {});
  };

  const handleConnectPeec = async () => {
    if (!wsId) return;
    setImportResult(null);
    try {
      const r = await api('/api/peec/mcp/auth/start/' + wsId, { method: 'POST' }, token);
      const url = (r.data || r).authorize_url;
      if (!url) {
        setImportResult({ success: false, msg: r.error || 'Could not start OAuth flow' });
        return;
      }
      const w = window.open(url, 'peec-oauth', 'width=600,height=720');
      const onMessage = (ev) => {
        if (!ev.data || ev.data.type !== 'peec-oauth') return;
        window.removeEventListener('message', onMessage);
        if (ev.data.ok) {
          setImportResult({ success: true, msg: 'Peec connected. Token will auto-refresh.' });
          refreshMcpStatus();
        } else {
          setImportResult({ success: false, msg: 'Peec auth failed: ' + (ev.data.msg || 'unknown error') });
        }
      };
      window.addEventListener('message', onMessage);
      // Poll fallback in case the popup blocks postMessage
      const pollT = setInterval(() => {
        if (w && w.closed) { clearInterval(pollT); refreshMcpStatus(); }
      }, 1000);
    } catch (e) {
      setImportResult({ success: false, msg: typeof e.message === 'string' ? e.message : 'OAuth start failed' });
    }
  };

  const handleDisconnectPeec = async () => {
    if (!wsId) return;
    try {
      await api('/api/peec/mcp/auth/disconnect/' + wsId, { method: 'POST' }, token);
      refreshMcpStatus();
      setImportResult({ success: true, msg: 'Peec disconnected. Click Connect to re-authorize.' });
    } catch (e) {
      setImportResult({ success: false, msg: 'Disconnect failed' });
    }
  };

  const handleMcpSync = async () => {
    if (!wsId) {
      setImportResult({ success: false, msg: 'No active workspace.' });
      return;
    }
    setMcpSyncing(true);
    setImportResult(null);
    try {
      const r = await api('/api/peec/mcp/sync/' + wsId + '?since_hours=168&limit=500', { method: 'POST' }, token);
      if (r.success === false) {
        setImportResult({ success: false, msg: r.error || 'Peec MCP sync failed' });
      } else {
        const d = r.data || r;
        setImportResult({
          success: true,
          msg: `MCP sync: ${d.mentions || 0} mentions, ${d.sources || 0} sources, ${d.competitors || 0} competitors. Insights: ${d.insights?.recommendations_created || 0} new.`,
        });
        api('/api/peec/records/' + wsId, {}, token).then(r => setRecords(r.data || r.records || [])).catch(() => {});
        api('/api/peec/mcp/sync/' + wsId + '/state', {}, token).then(r => setMcpState(r.data || r)).catch(() => {});
      }
    } catch (e) {
      setImportResult({ success: false, msg: typeof e.message === 'string' ? e.message : JSON.stringify(e.message) });
    }
    setMcpSyncing(false);
  };

  const handleCsvImport = async () => {
    if (!csvText.trim()) {
      setImportResult({ success: false, msg: 'CSV is empty — paste or drop a file first.' });
      return;
    }
    if (!wsId) {
      setImportResult({ success: false, msg: 'No active workspace. Create or select a workspace first.' });
      return;
    }
    setImporting(true);
    setImportResult(null);
    try {
      const blob = new Blob([csvText], { type: 'text/csv' });
      const formData = new FormData();
      formData.append('file', blob, 'import.csv');
      formData.append('project_id', wsId);
      const r = await api('/api/peec/import/csv', {
        method: 'POST', body: formData
      }, token);
      if (r.success === false) {
        const v = r.data || {};
        const warnings = v.warnings || [];
        setImportResult({ success: false, msg: r.error || 'Import failed', warnings });
      } else {
        const d = r.data || r;
        // Detect prompts-export response (has prompts_upserted) vs the
        // legacy citations-export response (has records/imported).
        if (d.prompts_upserted != null || d.observations != null) {
          setImportResult({
            success: true,
            count: d.prompts_upserted || 0,
            msg: `Peec Prompts Export detected — imported ${d.prompts_upserted || 0} prompts and ${d.observations || 0} observations (our brand surfaced in ${d.our_brand_hits || 0}). Open GEO Conquest → Prompt Battlefield to see them.`,
          });
        } else {
          const imported = d.records || d.imported || 0;
          setImportResult({
            success: true,
            count: imported,
            msg: `Imported ${imported} records, ${d.sources || 0} sources, ${d.clusters || 0} clusters`,
          });
          // Refresh records list
          try {
            const fresh = await api('/api/peec/records/' + wsId, {}, token);
            setRecords(fresh.data || fresh.records || []);
          } catch (err) { console.warn('API:', err.message); }
          // Auto-switch to Records tab so user sees what landed
          if (imported > 0) setTab('records');
        }
        // Clear the text area so user can upload another file
        setCsvText('');
      }
    } catch (e) {
      const msg = typeof e.message === 'string' ? e.message : JSON.stringify(e.message);
      setImportResult({ success: false, msg });
    }
    setImporting(false);
  };

  const handleApiSync = async () => {
    setApiSyncing(true);
    setImportResult(null);
    try {
      const r = await api('/api/peec/import/api', {
        method: 'POST', body: JSON.stringify({ api_key: 'from-settings', project_id: wsId })
      }, token);
      if (r.success === false) {
        setImportResult({ success: false, msg: r.error || 'Peec API sync failed. Use CSV import instead.' });
      } else {
        setImportResult({ success: true, count: r.data?.imported || 0, msg: 'Peec API sync completed' });
        api('/api/peec/records/' + wsId, {}, token).then(r => setRecords(r.data || r.records || [])).catch(err => console.warn('API:', err.message));
      }
    } catch (e) {
      const msg = typeof e.message === 'string' ? e.message : JSON.stringify(e.message);
      setImportResult({ success: false, msg });
    }
    setApiSyncing(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    const file = e.dataTransfer?.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (ev) => setCsvText(ev.target.result);
      reader.readAsText(file);
    }
  };

  return (
    <div style={{ display: 'grid', gap: 16 }}>
      <div style={{ display: 'flex', gap: 8 }}>
        {['upload', 'mcp', 'api', 'records', 'mapping'].map(t => (
          <button key={t} className={`btn ${tab === t ? 'btn-primary' : ''}`} onClick={() => setTab(t)}>
            {t === 'upload' ? 'CSV Upload' : t === 'mcp' ? 'Peec MCP (live)' : t === 'api' ? 'Peec API Sync' : t === 'records' ? `Records (${records.length})` : 'Field Mapping'}
          </button>
        ))}
      </div>

      {importResult && (
        <div className="card" style={{ borderLeft: `3px solid var(--${importResult.success ? 'emerald' : 'rose'})` }}>
          <div style={{ color: importResult.success ? 'var(--emerald)' : 'var(--rose)' }}>
            {importResult.msg}{importResult.count ? ` — ${importResult.count} records imported` : ''}
          </div>
        </div>
      )}

      {tab === 'upload' && (
        <div className="card">
          <div className="card-header">Import Peec CSV Export</div>
          <div onDrop={handleDrop} onDragOver={e => e.preventDefault()}
            style={{ border: '2px dashed var(--border)', borderRadius: 8, padding: 32, textAlign: 'center', marginBottom: 12, cursor: 'pointer' }}
            onClick={() => document.getElementById('csv-file-input')?.click()}>
            <div style={{ fontSize: 32, marginBottom: 8 }}>{'\u21E9'}</div>
            <div>Drag & drop CSV file here or click to browse</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>Supports Peec export formats with auto-detection</div>
            <input id="csv-file-input" type="file" accept=".csv,.tsv" style={{ display: 'none' }} onChange={e => {
              const file = e.target.files[0];
              if (file) { const r = new FileReader(); r.onload = ev => setCsvText(ev.target.result); r.readAsText(file); }
            }} />
          </div>
          <textarea className="form-input" value={csvText} onChange={e => setCsvText(e.target.value)}
            placeholder="Or paste CSV content here..." rows={8} style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }} />
          <div style={{ marginTop: 12, display: 'flex', gap: 8 }}>
            <button className="btn btn-primary" onClick={handleCsvImport} disabled={importing || !csvText.trim()}>
              {importing ? 'Importing...' : 'Import CSV'}
            </button>
            <button className="btn" onClick={() => setCsvText('')}>Clear</button>
          </div>
        </div>
      )}

      {tab === 'mcp' && (
        <div className="card">
          <div className="card-header">Peec MCP — Live Citation Stream</div>
          <p style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 16 }}>
            Pulls fresh mentions, sources, competitors, and sentiment directly from Peec's Model Context Protocol endpoint —
            no CSV exports, no rate-limited REST loops. Each sync also runs the Insights engine to surface
            sentiment drops, competitor moves, and untapped clusters. Authentication is per-workspace via OAuth — login once, refresh forever.
          </p>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12, marginBottom: 16 }}>
            <div className="card" style={{ padding: 12 }}>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Endpoint</div>
              <div style={{ fontSize: 12, fontWeight: 600, marginTop: 4, fontFamily: 'var(--font-mono)', wordBreak: 'break-all' }}>
                {mcpStatus?.url || '—'}
              </div>
              <div style={{ fontSize: 11, color: mcpStatus?.mcp_package_installed ? 'var(--emerald)' : 'var(--rose)' }}>
                {mcpStatus?.mcp_package_installed ? 'mcp package ready' : 'pip install mcp>=1.2.0'}
              </div>
            </div>
            <div className="card" style={{ padding: 12 }}>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>OAuth status</div>
              <div style={{ fontSize: 14, fontWeight: 600, marginTop: 4, color: mcpStatus?.oauth?.connected ? 'var(--emerald)' : 'var(--amber)' }}>
                {mcpStatus?.oauth?.connected ? 'Connected' : (mcpStatus?.oauth?.registered ? 'Registered, not authorized' : 'Not connected')}
              </div>
              <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
                {mcpStatus?.oauth?.expires_at ? `token exp: ${new Date(mcpStatus.oauth.expires_at).toLocaleTimeString()}` : '—'}
              </div>
            </div>
            <div className="card" style={{ padding: 12 }}>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Auto-sync</div>
              <div style={{ fontSize: 14, fontWeight: 600, marginTop: 4 }}>
                {mcpStatus?.auto_sync_minutes ? `every ${mcpStatus.auto_sync_minutes}m` : 'manual only'}
              </div>
              <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
                redirect: <span style={{ fontFamily: 'var(--font-mono)' }}>{mcpStatus?.public_base_url || '—'}/api/peec/mcp/auth/callback</span>
              </div>
            </div>
            <div className="card" style={{ padding: 12 }}>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Last sync</div>
              <div style={{ fontSize: 14, fontWeight: 600, marginTop: 4 }}>
                {mcpState?.last_sync_at ? new Date(mcpState.last_sync_at).toLocaleString() : '—'}
              </div>
              <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
                {mcpState?.last_status || 'never run'}
              </div>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {mcpStatus?.oauth?.connected ? (
              <>
                <button className="btn btn-primary" onClick={handleMcpSync} disabled={mcpSyncing || !mcpStatus?.mcp_package_installed}>
                  {mcpSyncing ? 'Syncing via MCP...' : 'Sync via Peec MCP'}
                </button>
                <button className="btn" onClick={handleDisconnectPeec}>Disconnect Peec</button>
              </>
            ) : (
              <button className="btn btn-primary" onClick={handleConnectPeec} disabled={!mcpStatus?.mcp_package_installed}>
                Connect Peec (OAuth)
              </button>
            )}
            <button className="btn btn-sm" onClick={refreshMcpStatus}>Refresh status</button>
          </div>
        </div>
      )}

      {tab === 'api' && (
        <div className="card">
          <div className="card-header">Peec API Integration</div>
          <p style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 16 }}>
            Sync citation data directly from the Peec API. Requires API key configured in backend settings.
          </p>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 16 }}>
            <div className="card" style={{ padding: 12 }}>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Sync Mode</div>
              <div style={{ fontSize: 14, fontWeight: 600, marginTop: 4 }}>Full Import</div>
              <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>Fetches all URL reports, prompts, and measurements</div>
            </div>
            <div className="card" style={{ padding: 12 }}>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Rate Limiting</div>
              <div style={{ fontSize: 14, fontWeight: 600, marginTop: 4 }}>Automatic</div>
              <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>Respects API rate limits with exponential backoff</div>
            </div>
          </div>
          <button className="btn btn-primary" onClick={handleApiSync} disabled={apiSyncing}>
            {apiSyncing ? 'Syncing...' : 'Start Peec API Sync'}
          </button>
        </div>
      )}

      {tab === 'records' && (
        <div className="card">
          <div className="card-header">Imported Records ({records.length})</div>
          {records.length === 0 ? (
            <div className="empty-state">{'\u21E9'}<br/>No records imported yet. Upload a CSV or sync from the Peec API.</div>
          ) : (
            <><table className="data-table"><thead><tr>
              <th>Title</th><th>URL</th><th>Model</th><th>Citations</th><th>Rate</th><th>Sentiment</th><th>Usage</th><th>Topic</th><th>Imported</th>
            </tr></thead><tbody>{recPag.paged.map((r, i) => {
              const sLabel = r.sentiment;
              const sScore = r.sentiment_score;
              const sColor = sLabel === 'positive' ? 'var(--emerald)' : sLabel === 'negative' ? 'var(--rose)' : sLabel === 'neutral' ? 'var(--text-muted)' : 'var(--text-muted)';
              return (
              <tr key={i}>
                <td style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>{r.title || r.prompt || r.query || '—'}</td>
                <td style={{ maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', fontFamily: 'var(--font-mono)', fontSize: 10 }}>{r.url || '—'}</td>
                <td><span className="badge">{r.model_source || r.model || r.platform || '—'}</span></td>
                <td style={{ fontWeight: 600, color: 'var(--emerald)' }}>{r.citation_count ?? '—'}</td>
                <td>{r.citation_rate != null ? (r.citation_rate * 100).toFixed(1) + '%' : '—'}</td>
                <td style={{ color: sColor, fontSize: 11 }}>
                  {sLabel ? `${sLabel}${sScore != null ? ` (${Number(sScore).toFixed(2)})` : ''}` : '—'}
                </td>
                <td>{r.usage_count ?? '—'}</td>
                <td>{r.topic || (Array.isArray(r.tags) ? r.tags[0] : r.tag) || '—'}</td>
                <td style={{ fontSize: 10, color: 'var(--text-muted)' }}>{r.imported_at ? new Date(r.imported_at).toLocaleDateString() : '—'}</td>
              </tr>
            );})}</tbody></table>
            <PaginationBar {...recPag} label="records" /></>
          )}
        </div>
      )}

      {tab === 'mapping' && (
        <div className="card">
          <div className="card-header">Field Mapping Configuration</div>
          <p style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 12 }}>
            Maps raw Peec CSV columns to the internal schema. Auto-detected on import, configurable below.
          </p>
          <table className="data-table"><thead><tr>
            <th>Internal Field</th><th>Expected CSV Column</th><th>Type</th><th>Required</th>
          </tr></thead><tbody>
            {(fieldMapping?.fields || [
              { field: 'prompt', columns: 'prompt, query, search_query', type: 'text', required: true },
              { field: 'url', columns: 'url, cited_url, source_url', type: 'url', required: true },
              { field: 'model', columns: 'model, platform, ai_model', type: 'text', required: false },
              { field: 'citation_count', columns: 'citation_count, citations, count', type: 'integer', required: false },
              { field: 'citation_rate', columns: 'citation_rate, rate, frequency', type: 'float', required: false },
              { field: 'visibility', columns: 'visibility, visibility_score', type: 'float', required: false },
              { field: 'position', columns: 'position, rank, avg_position', type: 'float', required: false },
              { field: 'topic', columns: 'topic, tag, category, cluster', type: 'text', required: false },
              { field: 'date', columns: 'date, timestamp, measured_at', type: 'date', required: false },
            ]).map((f, i) => (
              <tr key={i}>
                <td><code style={{ color: 'var(--blue)' }}>{f.field}</code></td>
                <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{f.columns}</td>
                <td><span className="badge">{f.type}</span></td>
                <td>{f.required ? '\u2714' : '\u2014'}</td>
              </tr>
            ))}
          </tbody></table>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// SOURCES PAGE (Source Discovery & Ranking)
// ═══════════════════════════════════════════════════════════════

function SourcesPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [sources, setSources] = useState([]);
  const [clusters, setClusters] = useState([]);
  const [sortBy, setSortBy] = useState('citation_count');
  const [filterTopic, setFilterTopic] = useState('all');

  useEffect(() => {
    if (!wsId) return;
    api('/api/sources/' + wsId, {}, token).then(r => setSources(r.data || r.sources || [])).catch(err => console.warn('API:', err.message));
    api('/api/clusters/' + wsId, {}, token).then(r => setClusters(r.data || r.clusters || [])).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  // Backend column is total_citation_count on sources; fall back to citation_count for
  // measurement-table rows that use the legacy name.
  const sortKey = sortBy === 'citation_count' ? 'total_citation_count' : sortBy;
  const getCite = (s) => s.total_citation_count ?? s.citation_count ?? 0;
  const sorted = [...sources].sort((a, b) => (b[sortKey] || 0) - (a[sortKey] || 0));
  const filtered = filterTopic === 'all' ? sorted : sorted.filter(s => s.topic === filterTopic || (Array.isArray(s.topics) && s.topics.includes(filterTopic)));
  const srcPag = usePagination(filtered, 50);
  const topics = [...new Set(sources.flatMap(s => {
    if (Array.isArray(s.topics)) return s.topics;
    if (typeof s.topics === 'string' && s.topics.startsWith('[')) { try { return JSON.parse(s.topics); } catch { return []; } }
    return s.topic ? [s.topic] : [];
  }).filter(Boolean))];
  const domains = {};
  sources.forEach(s => {
    try { const d = new URL(s.url).hostname; domains[d] = (domains[d] || 0) + getCite(s); } catch {}
  });
  const topDomains = Object.entries(domains).sort((a, b) => b[1] - a[1]).slice(0, 10);

  return (
    <div style={{ display: 'grid', gap: 16 }}>
      <div className="metrics-grid">
        <div className="metric-card"><div className="metric-label">TOTAL SOURCES</div><div className="metric-value">{sources.length}</div></div>
        <div className="metric-card"><div className="metric-label">UNIQUE DOMAINS</div><div className="metric-value">{Object.keys(domains).length}</div></div>
        <div className="metric-card"><div className="metric-label">TOPIC CLUSTERS</div><div className="metric-value">{clusters.length}</div></div>
        <div className="metric-card"><div className="metric-label">TOTAL CITATIONS</div><div className="metric-value">{sources.reduce((sum, r) => sum + (r.total_citation_count ?? r.citation_count ?? 0), 0)}</div></div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 300px', gap: 16 }}>
        <div className="card">
          <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span>Source Rankings</span>
            <div style={{ display: 'flex', gap: 8 }}>
              <select className="form-input" style={{ width: 'auto', fontSize: 11 }} value={sortBy} onChange={e => setSortBy(e.target.value)}>
                <option value="citation_count">Citations</option>
                <option value="citation_rate">Citation Rate</option>
                <option value="visibility">Visibility</option>
                <option value="quality_score">Quality Score</option>
              </select>
              <select className="form-input" style={{ width: 'auto', fontSize: 11 }} value={filterTopic} onChange={e => setFilterTopic(e.target.value)}>
                <option value="all">All Topics</option>
                {topics.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
          </div>
          {filtered.length === 0 ? (
            <div className="empty-state">{'\u25C9'}<br/>No sources discovered yet. Import Peec data to populate sources.</div>
          ) : (
            <><table className="data-table"><thead><tr>
              <th>#</th><th>URL</th><th>Citations</th><th>Rate</th><th>Sentiment</th><th>Visibility</th><th>Quality</th><th>Models</th>
            </tr></thead><tbody>{srcPag.paged.map((s, i) => (
              <tr key={i}>
                <td style={{ color: 'var(--text-muted)' }}>{srcPag.page * 50 + i + 1}</td>
                <td style={{ maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', fontFamily: 'var(--font-mono)', fontSize: 11 }}>{s.url}</td>
                <td style={{ fontWeight: 600, color: 'var(--emerald)' }}>{getCite(s)}</td>
                <td>{(s.max_citation_rate ?? s.citation_rate) != null ? ((s.max_citation_rate ?? s.citation_rate) * 100).toFixed(1) + '%' : '—'}</td>
                <td style={{ fontSize: 11, color: s.avg_sentiment_score == null ? 'var(--text-muted)' : s.avg_sentiment_score > 0.1 ? 'var(--emerald)' : s.avg_sentiment_score < -0.1 ? 'var(--rose)' : 'var(--text-secondary)' }}>
                  {s.avg_sentiment_score != null ? (s.avg_sentiment_score >= 0 ? '+' : '') + Number(s.avg_sentiment_score).toFixed(2) : '—'}
                </td>
                <td>{s.visibility != null ? Number(s.visibility).toFixed(2) : '—'}</td>
                <td>{s.quality_score != null ? Number(s.quality_score).toFixed(1) : '—'}</td>
                <td style={{ fontSize: 10 }}>{(() => {
                  const ms = s.model_sources;
                  if (Array.isArray(ms)) return ms.join(', ');
                  if (typeof ms === 'string' && ms.startsWith('[')) { try { return JSON.parse(ms).join(', '); } catch { return ms; } }
                  return s.models_cited_in || ms || '—';
                })()}</td>
              </tr>
            ))}</tbody></table>
            <PaginationBar {...srcPag} label="sources" /></>
          )}
        </div>

        <div style={{ display: 'grid', gap: 16, alignContent: 'start' }}>
          <div className="card">
            <div className="card-header">Top Domains</div>
            {topDomains.length === 0 ? <div style={{ color: 'var(--text-muted)', fontSize: 12, padding: 8 }}>No domain data</div> :
              topDomains.map(([d, c], i) => (
                <div key={d} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '6px 0', borderBottom: '1px solid var(--border-subtle)' }}>
                  <span style={{ fontSize: 12, fontFamily: 'var(--font-mono)' }}>{d}</span>
                  <span className="badge">{c}</span>
                </div>
              ))
            }
          </div>
          <div className="card">
            <div className="card-header">Topic Clusters</div>
            {clusters.length === 0 ? <div style={{ color: 'var(--text-muted)', fontSize: 12, padding: 8 }}>No clusters</div> :
              clusters.slice(0, 10).map((c, i) => (
                <div key={i} style={{ display: 'flex', justifyContent: 'space-between', padding: '6px 0', borderBottom: '1px solid var(--border-subtle)' }}>
                  <span style={{ fontSize: 12 }}>{c.label || c.name || `Cluster ${c.id}`}</span>
                  <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{c.url_count ?? c.record_count ?? 0} URLs</span>
                </div>
              ))
            }
          </div>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// SCRAPER PAGE (Content Extraction Engine)
// ═══════════════════════════════════════════════════════════════

function ScraperPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [urls, setUrls] = useState('');
  const [scraping, setScraping] = useState(false);
  const [results, setResults] = useState([]);
  const [selectedResult, setSelectedResult] = useState(null);
  const [batchStatus, setBatchStatus] = useState(null);

  const handleScrape = async () => {
    const urlList = urls.split('\n').map(u => u.trim()).filter(Boolean);
    if (urlList.length === 0) return;
    setScraping(true);
    try {
      if (urlList.length > 5) {
        const r = await api('/api/batch/scrape', {
          method: 'POST', body: JSON.stringify({ project_id: wsId, urls: urlList })
        }, token);
        setBatchStatus(r.data || r);
      } else {
        const r = await api('/api/sources/scrape', {
          method: 'POST', body: JSON.stringify({ project_id: wsId, urls: urlList })
        }, token);
        const d = r.data || r;
        setResults(d.results || (Array.isArray(d) ? d : []));
      }
    } catch (e) { setResults([{ url: 'Error', error: e.message }]); }
    setScraping(false);
  };

  return (
    <div style={{ display: 'grid', gap: 16 }}>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
        <div className="card">
          <div className="card-header">URL Input</div>
          <textarea className="form-input" value={urls} onChange={e => setUrls(e.target.value)}
            placeholder="Enter URLs to scrape, one per line..." rows={10}
            style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }} />
          <div style={{ marginTop: 12, display: 'flex', gap: 8, alignItems: 'center' }}>
            <button className="btn btn-primary" onClick={handleScrape} disabled={scraping || !urls.trim()}>
              {scraping ? 'Scraping...' : `Scrape ${urls.split('\n').filter(u => u.trim()).length} URLs`}
            </button>
            <button className="btn" onClick={() => setUrls('')}>Clear</button>
            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>
              {'>'}5 URLs uses batch queue mode
            </span>
          </div>
        </div>

        <div className="card">
          <div className="card-header">Scraper Settings</div>
          <div style={{ display: 'grid', gap: 12 }}>
            <div><label className="form-label">Timeout (seconds)</label><input className="form-input" type="number" defaultValue={30} /></div>
            <div><label className="form-label">Max Retries</label><input className="form-input" type="number" defaultValue={2} /></div>
            <div><label className="form-label">Concurrency</label><input className="form-input" type="number" defaultValue={3} /></div>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              <span className="badge" style={{ background: 'var(--emerald)', color: '#000' }}>Clean Extraction</span>
              <span className="badge" style={{ background: 'var(--blue)', color: '#000' }}>Noise Removal</span>
              <span className="badge">FAQ Detection</span>
              <span className="badge">Schema.org</span>
              <span className="badge">Language Detection</span>
            </div>
          </div>
        </div>
      </div>

      {batchStatus && (
        <div className="card">
          <div className="card-header">Batch Job: {batchStatus.job_id || 'queued'}</div>
          <div>Status: <span className="badge">{batchStatus.status || 'queued'}</span></div>
        </div>
      )}

      {results.length > 0 && (
        <div style={{ display: 'grid', gridTemplateColumns: '300px 1fr', gap: 16 }}>
          <div className="card">
            <div className="card-header">Results ({results.length})</div>
            {results.map((r, i) => (
              <div key={i} className={`sidebar-item ${selectedResult === i ? 'active' : ''}`}
                onClick={() => setSelectedResult(i)} style={{ cursor: 'pointer', padding: '8px 12px' }}>
                <span style={{ fontSize: 11, display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {r.title || r.url}
                </span>
                <span className={`badge ${r.error ? 'badge-rose' : 'badge-emerald'}`} style={{ fontSize: 9, marginTop: 2 }}>
                  {r.error ? 'FAILED' : `${r.quality_score?.toFixed(0) || '—'}% quality`}
                </span>
              </div>
            ))}
          </div>
          {selectedResult != null && results[selectedResult] && (
            <div className="card">
              <div className="card-header">{results[selectedResult].title || 'Scraped Content'}</div>
              {results[selectedResult].error ? (
                <div style={{ color: 'var(--rose)' }}>{results[selectedResult].error}</div>
              ) : (
                <div style={{ display: 'grid', gap: 12 }}>
                  <div><strong style={{ fontSize: 10, color: 'var(--text-muted)' }}>META</strong>
                    <div style={{ fontSize: 12 }}>{results[selectedResult].meta_description || '—'}</div></div>
                  <div><strong style={{ fontSize: 10, color: 'var(--text-muted)' }}>HEADINGS</strong>
                    {(results[selectedResult].headings || []).map((h, j) => (
                      <div key={j} style={{ fontSize: 12, paddingLeft: (h.level - 1) * 12 }}>{h.level ? `H${h.level}: ` : ''}{h.text}</div>
                    ))}</div>
                  <div><strong style={{ fontSize: 10, color: 'var(--text-muted)' }}>BODY ({(results[selectedResult].body_text || '').length} chars)</strong>
                    <div style={{ fontSize: 11, maxHeight: 200, overflow: 'auto', fontFamily: 'var(--font-mono)', whiteSpace: 'pre-wrap' }}>
                      {(results[selectedResult].body_text || '').substring(0, 2000)}
                    </div></div>
                  {(results[selectedResult].faqs || []).length > 0 && (
                    <div><strong style={{ fontSize: 10, color: 'var(--text-muted)' }}>FAQs ({results[selectedResult].faqs.length})</strong>
                      {results[selectedResult].faqs.map((f, j) => (
                        <div key={j} style={{ fontSize: 11, padding: '4px 0', borderBottom: '1px solid var(--border-subtle)' }}>
                          <div style={{ fontWeight: 600 }}>Q: {f.question}</div>
                          <div style={{ color: 'var(--text-secondary)' }}>A: {f.answer?.substring(0, 200)}</div>
                        </div>
                      ))}
                    </div>
                  )}
                  <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                    {results[selectedResult].language && <span className="badge">Lang: {results[selectedResult].language}</span>}
                    {results[selectedResult].schema_types && <span className="badge">Schema: {results[selectedResult].schema_types}</span>}
                    <span className="badge">Confidence: {results[selectedResult].extraction_confidence || '—'}</span>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {results.length === 0 && !batchStatus && (
        <div className="card">
          <div className="empty-state">
            {'\u2692'}<br/>
            Enter URLs above to scrape. The engine extracts titles, headings, body text, FAQs, schema markup, and more.<br/>
            <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Supports retry logic, timeout control, noise removal, and language detection.</span>
          </div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// ANALYSIS PAGE (AI Orchestration Layer)
// ═══════════════════════════════════════════════════════════════

function AnalysisPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [clusters, setClusters] = useState([]);
  const [selectedCluster, setSelectedCluster] = useState(null);
  const [analyses, setAnalyses] = useState([]);
  const [briefs, setBriefs] = useState([]);
  const [running, setRunning] = useState(false);
  const [genBrief, setGenBrief] = useState(false);

  useEffect(() => {
    if (!wsId) return;
    api('/api/clusters/' + wsId, {}, token).then(r => setClusters(r.data || r.clusters || [])).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  useEffect(() => {
    if (!selectedCluster) return;
    api('/api/analyze/' + selectedCluster.id, {}, token).then(r => setAnalyses(r.data || r.analyses || [])).catch(err => console.warn('API:', err.message));
    api('/api/briefs/' + selectedCluster.id, {}, token).then(r => setBriefs(r.data || r.briefs || [])).catch(err => console.warn('API:', err.message));
  }, [selectedCluster]);

  const runAnalysis = async () => {
    if (!selectedCluster) return;
    setRunning(true);
    try {
      const r = await api('/api/analyze', {
        method: 'POST', body: JSON.stringify({ cluster_id: selectedCluster.id, project_id: wsId })
      }, token);
      setAnalyses(prev => [r.data || r, ...prev]);
    } catch (e) { alert(e.message); }
    setRunning(false);
  };

  const generateBrief = async (analysisId) => {
    setGenBrief(true);
    try {
      const r = await api('/api/analyze/brief', {
        method: 'POST', body: JSON.stringify({ analysis_id: analysisId })
      }, token);
      setBriefs(prev => [r.data || r, ...prev]);
    } catch (e) { alert(e.message); }
    setGenBrief(false);
  };

  return (
    <div style={{ display: 'grid', gap: 16 }}>
      <div className="metrics-grid">
        <div className="metric-card"><div className="metric-label">CLUSTERS</div><div className="metric-value">{clusters.length}</div></div>
        <div className="metric-card"><div className="metric-label">ANALYSES RUN</div><div className="metric-value">{analyses.length}</div></div>
        <div className="metric-card"><div className="metric-label">BRIEFS GENERATED</div><div className="metric-value">{briefs.length}</div></div>
        <div className="metric-card"><div className="metric-label">PIPELINE STEP</div><div className="metric-value" style={{ fontSize: 14 }}>Source → Analyze → Brief</div></div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '280px 1fr', gap: 16 }}>
        <div className="card">
          <div className="card-header">Topic Clusters</div>
          {clusters.length === 0 ? (
            <div style={{ color: 'var(--text-muted)', fontSize: 12, padding: 12 }}>No clusters. Import data first.</div>
          ) : clusters.map(c => (
            <div key={c.id} className={`sidebar-item ${selectedCluster?.id === c.id ? 'active' : ''}`}
              onClick={() => setSelectedCluster(c)} style={{ cursor: 'pointer', padding: '8px 12px' }}>
              <div style={{ fontSize: 12 }}>{c.label || c.name || `Cluster ${c.id}`}</div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>
                {c.url_count ?? c.record_count ?? 0} URLs · {c.prompt_count ?? 0} prompts · {c.total_citations ?? 0} citations
              </div>
            </div>
          ))}
        </div>

        <div>
          {!selectedCluster ? (
            <div className="card"><div className="empty-state">{'\u2B21'}<br/>Select a topic cluster to run analysis</div></div>
          ) : (
            <div style={{ display: 'grid', gap: 16 }}>
              <div className="card">
                <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span>Cluster: {selectedCluster.label || selectedCluster.name}</span>
                  <button className="btn btn-primary" onClick={runAnalysis} disabled={running}>
                    {running ? 'Analyzing...' : '\u2B21 Run Claude Analysis'}
                  </button>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 8 }}>
                  <div style={{ fontSize: 11 }}><span style={{ color: 'var(--text-muted)' }}>URLs:</span> {selectedCluster.url_count ?? 0}</div>
                  <div style={{ fontSize: 11 }}><span style={{ color: 'var(--text-muted)' }}>Prompts:</span> {selectedCluster.prompt_count ?? 0}</div>
                  <div style={{ fontSize: 11 }}><span style={{ color: 'var(--text-muted)' }}>Citations:</span> {selectedCluster.total_citations ?? 0}</div>
                </div>
              </div>

              {analyses.length > 0 && (
                <div className="card">
                  <div className="card-header">Analysis Results</div>
                  {analyses.map((a, i) => (
                    <div key={i} style={{ padding: '12px 0', borderBottom: '1px solid var(--border-subtle)' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{new Date(a.created_at).toLocaleString()}</span>
                        <button className="btn" style={{ fontSize: 10, padding: '2px 8px' }} onClick={() => generateBrief(a.id)} disabled={genBrief}>
                          {genBrief ? '...' : 'Generate Brief →'}
                        </button>
                      </div>
                      <div style={{ fontSize: 12, whiteSpace: 'pre-wrap', maxHeight: 200, overflow: 'auto' }}>
                        {typeof a.result === 'string' ? a.result : JSON.stringify(a.result, null, 2)}
                      </div>
                      {a.tokens_used && <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4 }}>Tokens: {a.tokens_used} · Cost: ${a.cost?.toFixed(4) || '—'}</div>}
                    </div>
                  ))}
                </div>
              )}

              {briefs.length > 0 && (
                <div className="card">
                  <div className="card-header">Content Briefs</div>
                  {briefs.map((b, i) => (
                    <div key={i} style={{ padding: '12px 0', borderBottom: '1px solid var(--border-subtle)' }}>
                      <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 4 }}>{b.title || `Brief ${i + 1}`}</div>
                      <div style={{ fontSize: 12, whiteSpace: 'pre-wrap', maxHeight: 200, overflow: 'auto' }}>
                        {typeof b.content === 'string' ? b.content : JSON.stringify(b.content, null, 2)}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// CONTENT STUDIO PAGE (Content Planning + Generation + Editorial)
// ═══════════════════════════════════════════════════════════════

function ContentStudioPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [tab, setTab] = useState('drafts');
  const [drafts, setDrafts] = useState([]);
  const [selectedDraft, setSelectedDraft] = useState(null);
  const [draftDetail, setDraftDetail] = useState(null);
  const [statusFilter, setStatusFilter] = useState('all');
  const [generating, setGenerating] = useState(false);
  const [reviewAction, setReviewAction] = useState(null);
  const [reviewNote, setReviewNote] = useState('');
  const statusColors = { draft: 'var(--amber)', pending_review: 'var(--blue)', revision_requested: 'var(--rose)',
    approved: 'var(--emerald)', rejected: 'var(--rose)', published: 'var(--purple)' };
  const statuses = ['all', 'draft', 'pending_review', 'revision_requested', 'approved', 'rejected', 'published'];

  useEffect(() => {
    if (!wsId) return;
    const params = statusFilter !== 'all' ? `?status=${statusFilter}` : '';
    api(`/api/drafts/${wsId}${params}`, {}, token).then(r => setDrafts(r.data || r.drafts || [])).catch(err => console.warn('API:', err.message));
  }, [wsId, statusFilter]);

  useEffect(() => {
    if (!selectedDraft) return;
    api('/api/drafts/detail/' + selectedDraft, {}, token).then(r => setDraftDetail(r.data || r)).catch(err => console.warn('API:', err.message));
  }, [selectedDraft]);

  const handleReview = async (action) => {
    if (!selectedDraft) return;
    try {
      await api('/api/review/' + selectedDraft, {
        method: 'PUT', body: JSON.stringify({ status: action, notes: reviewNote })
      }, token);
      setReviewNote('');
      setReviewAction(null);
      api('/api/drafts/detail/' + selectedDraft, {}, token).then(r => setDraftDetail(r.data || r)).catch(err => console.warn('API:', err.message));
      const params = statusFilter !== 'all' ? `?status=${statusFilter}` : '';
      api(`/api/drafts/${wsId}${params}`, {}, token).then(r => setDrafts(r.data || r.drafts || [])).catch(err => console.warn('API:', err.message));
    } catch (e) { alert(e.message); }
  };

  const contentTypes = ['blog_article', 'faq_page', 'landing_page', 'knowledge_base', 'comparison_page', 'glossary', 'location_page', 'category_page'];

  return (
    <div style={{ display: 'grid', gap: 16 }}>
      <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
        {['drafts', 'templates', 'briefs'].map(t => (
          <button key={t} className={`btn ${tab === t ? 'btn-primary' : ''}`} onClick={() => setTab(t)}>
            {t === 'drafts' ? 'Editorial Queue' : t === 'templates' ? 'Template Library' : 'Content Briefs'}
          </button>
        ))}
      </div>

      {tab === 'templates' && (
        <div className="card">
          <div className="card-header">Content Type Templates</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
            {contentTypes.map(ct => (
              <div key={ct} className="card" style={{ padding: 16, textAlign: 'center', cursor: 'pointer' }}
                onMouseOver={e => e.currentTarget.style.borderColor = 'var(--blue)'} onMouseOut={e => e.currentTarget.style.borderColor = ''}>
                <div style={{ fontSize: 24, marginBottom: 8 }}>
                  {ct === 'blog_article' ? '\u270E' : ct === 'faq_page' ? '\u2753' : ct === 'landing_page' ? '\u2B50' :
                   ct === 'knowledge_base' ? '\u{1F4DA}' : ct === 'comparison_page' ? '\u2696' : ct === 'glossary' ? '\u{1F4D6}' :
                   ct === 'location_page' ? '\u{1F4CD}' : '\u2630'}
                </div>
                <div style={{ fontSize: 13, fontWeight: 600, textTransform: 'capitalize' }}>{ct.replace(/_/g, ' ')}</div>
                <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4 }}>
                  {ct === 'blog_article' ? 'Long-form articles with SEO structure' :
                   ct === 'faq_page' ? 'Structured Q&A with schema markup' :
                   ct === 'landing_page' ? 'Conversion-focused pages' :
                   ct === 'comparison_page' ? 'Side-by-side product/service comparison' :
                   'Optimized for AI citation'}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {tab === 'briefs' && (
        <div className="card">
          <div className="card-header">Brief Builder</div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div><label className="form-label">Target Topic</label><input className="form-input" placeholder="e.g. best CRM software" /></div>
            <div><label className="form-label">Intent</label>
              <select className="form-input"><option>informational</option><option>comparison</option><option>transactional</option><option>educational</option><option>problem-solution</option></select></div>
            <div><label className="form-label">Target Audience</label><input className="form-input" placeholder="e.g. small business owners" /></div>
            <div><label className="form-label">Content Type</label>
              <select className="form-input">{contentTypes.map(ct => <option key={ct} value={ct}>{ct.replace(/_/g, ' ')}</option>)}</select></div>
            <div style={{ gridColumn: '1/-1' }}><label className="form-label">Angle / Positioning</label><input className="form-input" placeholder="Unique angle for this content" /></div>
            <div style={{ gridColumn: '1/-1' }}><label className="form-label">Source Summary</label><textarea className="form-input" rows={3} placeholder="Key findings from source analysis..." /></div>
          </div>
          <div style={{ marginTop: 12 }}><button className="btn btn-primary">Generate Brief with Claude</button></div>
        </div>
      )}

      {tab === 'drafts' && (
        <div>
          <div style={{ display: 'flex', gap: 6, marginBottom: 12 }}>
            {statuses.map(s => (
              <button key={s} className={`btn ${statusFilter === s ? 'btn-primary' : ''}`} style={{ fontSize: 10, padding: '4px 10px' }}
                onClick={() => setStatusFilter(s)}>{s.replace(/_/g, ' ')}</button>
            ))}
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '300px 1fr', gap: 16 }}>
            <div className="card">
              <div className="card-header">Drafts ({drafts.length})</div>
              {drafts.length === 0 ? (
                <div style={{ color: 'var(--text-muted)', fontSize: 12, padding: 12 }}>No drafts. Generate content from the Analysis page.</div>
              ) : drafts.map(d => (
                <div key={d.id} className={`sidebar-item ${selectedDraft === d.id ? 'active' : ''}`}
                  onClick={() => setSelectedDraft(d.id)} style={{ cursor: 'pointer', padding: '8px 12px' }}>
                  <div style={{ fontSize: 12, fontWeight: selectedDraft === d.id ? 600 : 400 }}>{d.title || `Draft ${d.id?.slice(0, 8)}`}</div>
                  <div style={{ display: 'flex', gap: 4, marginTop: 4 }}>
                    <span className="badge" style={{ background: statusColors[d.status] || 'var(--surface-2)', fontSize: 9 }}>{d.status?.replace(/_/g, ' ')}</span>
                    <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>v{d.version || 1}</span>
                  </div>
                </div>
              ))}
            </div>
            <div>
              {!draftDetail ? (
                <div className="card"><div className="empty-state">{'\u270E'}<br/>Select a draft to review</div></div>
              ) : (
                <div style={{ display: 'grid', gap: 12 }}>
                  <div className="card">
                    <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
                      <span>{draftDetail.title || 'Draft'}</span>
                      <div style={{ display: 'flex', gap: 4 }}>
                        <span className="badge" style={{ background: statusColors[draftDetail.status] }}>{draftDetail.status?.replace(/_/g, ' ')}</span>
                        <span className="badge">v{draftDetail.version || 1}</span>
                      </div>
                    </div>
                    <div style={{ fontSize: 12, whiteSpace: 'pre-wrap', lineHeight: 1.6, maxHeight: 400, overflow: 'auto', padding: '8px 0' }}>
                      {draftDetail.content || draftDetail.body || 'No content'}
                    </div>
                    {draftDetail.meta_title && <div style={{ fontSize: 11, color: 'var(--blue)', marginTop: 8 }}>Meta: {draftDetail.meta_title} — {draftDetail.meta_description}</div>}
                  </div>
                  <div className="card">
                    <div className="card-header">Review Actions</div>
                    <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
                      <button className="btn" style={{ background: 'var(--emerald)', color: '#000' }} onClick={() => handleReview('approved')}>Approve</button>
                      <button className="btn" style={{ background: 'var(--amber)', color: '#000' }} onClick={() => handleReview('revision_requested')}>Request Revision</button>
                      <button className="btn" style={{ background: 'var(--rose)', color: '#fff' }} onClick={() => handleReview('rejected')}>Reject</button>
                    </div>
                    <textarea className="form-input" value={reviewNote} onChange={e => setReviewNote(e.target.value)} placeholder="Internal review notes..." rows={2} />
                  </div>
                  {(draftDetail.versions || []).length > 1 && (
                    <div className="card">
                      <div className="card-header">Version History</div>
                      {draftDetail.versions.map((v, i) => (
                        <div key={i} style={{ padding: '6px 0', borderBottom: '1px solid var(--border-subtle)', fontSize: 12, display: 'flex', justifyContent: 'space-between' }}>
                          <span>v{v.version} — {v.status}</span>
                          <span style={{ color: 'var(--text-muted)', fontSize: 10 }}>{new Date(v.created_at).toLocaleString()}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// PUBLISHING PAGE (Publishing & Delivery Layer)
// ═══════════════════════════════════════════════════════════════

function PublishingPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [exports, setExports] = useState([]);
  const [drafts, setDrafts] = useState([]);
  const [selectedDraft, setSelectedDraft] = useState(null);
  const [exportFormat, setExportFormat] = useState('markdown');
  const [cms, setCms] = useState('wordpress');
  const [publishing, setPublishing] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [cmsStatus, setCmsStatus] = useState({});
  const [exportResult, setExportResult] = useState(null);

  useEffect(() => {
    if (!wsId) return;
    api(`/api/drafts/${wsId}?status=approved`, {}, token).then(r => setDrafts(r.data || r.drafts || [])).catch(err => console.warn('API:', err.message));
    api('/api/exports/' + wsId, {}, token).then(r => setExports(r.data || r.exports || [])).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const checkCms = async (cmsType) => {
    try {
      const r = await api('/api/publish/check/' + cmsType, { method: 'POST' }, token);
      const d = r.data || r;
      setCmsStatus(prev => ({ ...prev, [cmsType]: d.connected ? 'connected' : 'failed' }));
    } catch { setCmsStatus(prev => ({ ...prev, [cmsType]: 'failed' })); }
  };

  const handleExport = async () => {
    if (!selectedDraft) return;
    setExporting(true);
    try {
      const r = await api('/api/export', {
        method: 'POST', body: JSON.stringify({ draft_id: selectedDraft, format: exportFormat, project_id: wsId })
      }, token);
      setExportResult(r.data || r);
      api('/api/exports/' + wsId, {}, token).then(r => setExports(r.data || r.exports || [])).catch(err => console.warn('API:', err.message));
    } catch (e) { alert(e.message); }
    setExporting(false);
  };

  const handlePublish = async () => {
    if (!selectedDraft) return;
    setPublishing(true);
    try {
      const r = await api('/api/publish', {
        method: 'POST', body: JSON.stringify({ draft_id: selectedDraft, cms, project_id: wsId })
      }, token);
      const d = r.data || r;
      alert('Published successfully! URL: ' + (d.url || d.published_url || 'N/A'));
    } catch (e) { alert(e.message); }
    setPublishing(false);
  };

  return (
    <div style={{ display: 'grid', gap: 16 }}>
      <div className="metrics-grid">
        <div className="metric-card"><div className="metric-label">APPROVED DRAFTS</div><div className="metric-value">{drafts.length}</div></div>
        <div className="metric-card"><div className="metric-label">EXPORTS</div><div className="metric-value">{exports.length}</div></div>
        <div className="metric-card"><div className="metric-label">PUBLISHED</div><div className="metric-value">{exports.filter(e => e.status === 'published').length}</div></div>
        <div className="metric-card"><div className="metric-label">FORMATS</div><div className="metric-value" style={{ fontSize: 14 }}>MD · HTML · TXT · CMS</div></div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
        <div className="card">
          <div className="card-header">Export Content</div>
          <div style={{ marginBottom: 12 }}>
            <label className="form-label">Select Approved Draft</label>
            <select className="form-input" value={selectedDraft || ''} onChange={e => setSelectedDraft(e.target.value)}>
              <option value="">Choose draft...</option>
              {drafts.map(d => <option key={d.id} value={d.id}>{d.title || d.id}</option>)}
            </select>
          </div>
          <div style={{ marginBottom: 12 }}>
            <label className="form-label">Format</label>
            <div style={{ display: 'flex', gap: 8 }}>
              {['markdown', 'html', 'plaintext'].map(f => (
                <button key={f} className={`btn ${exportFormat === f ? 'btn-primary' : ''}`} onClick={() => setExportFormat(f)}>{f.toUpperCase()}</button>
              ))}
            </div>
          </div>
          <button className="btn btn-primary" onClick={handleExport} disabled={exporting || !selectedDraft}>
            {exporting ? 'Exporting...' : 'Export'}
          </button>
          {exportResult && (
            <div style={{ marginTop: 12, padding: 12, background: 'var(--surface-1)', borderRadius: 6, maxHeight: 200, overflow: 'auto' }}>
              <pre style={{ fontSize: 11, fontFamily: 'var(--font-mono)', whiteSpace: 'pre-wrap' }}>{exportResult.content || JSON.stringify(exportResult, null, 2)}</pre>
            </div>
          )}
        </div>

        <div className="card">
          <div className="card-header">Publish to CMS</div>
          <div style={{ marginBottom: 12, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
            {['wordpress', 'webflow', 'shopify', 'headless'].map(c => (
              <div key={c} className="card" style={{ padding: 12, cursor: 'pointer', border: cms === c ? '1px solid var(--blue)' : '' }}
                onClick={() => setCms(c)}>
                <div style={{ fontSize: 13, fontWeight: 600, textTransform: 'capitalize' }}>{c}</div>
                <div style={{ fontSize: 10, color: cmsStatus[c] === 'connected' ? 'var(--emerald)' : cmsStatus[c] === 'failed' ? 'var(--rose)' : 'var(--text-muted)' }}>
                  {cmsStatus[c] || 'Not checked'}
                </div>
              </div>
            ))}
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <button className="btn" onClick={() => checkCms(cms)}>Test Connection</button>
            <button className="btn btn-primary" onClick={handlePublish} disabled={publishing || !selectedDraft}>
              {publishing ? 'Publishing...' : 'Publish'}
            </button>
          </div>
        </div>
      </div>

      <div className="card">
        <div className="card-header">Export History</div>
        {exports.length === 0 ? (
          <div className="empty-state">{'\u21EA'}<br/>No exports yet. Approve drafts and export them here.</div>
        ) : (
          <table className="data-table"><thead><tr>
            <th>Draft</th><th>Format</th><th>Status</th><th>CMS</th><th>Published URL</th><th>Date</th>
          </tr></thead><tbody>{exports.map((e, i) => (
            <tr key={i}>
              <td style={{ fontSize: 11 }}>{e.draft_title || e.draft_id?.slice(0, 12)}</td>
              <td><span className="badge">{e.format || '—'}</span></td>
              <td><span className="badge" style={{ background: e.status === 'published' ? 'var(--emerald)' : 'var(--amber)', color: '#000' }}>{e.status}</span></td>
              <td>{e.cms || '—'}</td>
              <td style={{ fontFamily: 'var(--font-mono)', fontSize: 10 }}>{e.published_url || '—'}</td>
              <td style={{ fontSize: 10, color: 'var(--text-muted)' }}>{e.created_at ? new Date(e.created_at).toLocaleString() : '—'}</td>
            </tr>
          ))}</tbody></table>
        )}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// PROMPT TEMPLATES PAGE (AI Orchestration - Prompt Management)
// ═══════════════════════════════════════════════════════════════

function PromptTemplatesPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [templates, setTemplates] = useState([]);
  const [selected, setSelected] = useState(null);
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm] = useState({ name: '', type: 'source_analysis', template: '', model: 'claude-sonnet-4-5-20250514', temperature: 0.3, max_tokens: 4096 });
  const types = ['source_analysis', 'structure_extraction', 'brief_generation', 'content_generation', 'qa_factuality', 'tone_adaptation', 'localization'];

  useEffect(() => {
    if (!wsId) return;
    api('/api/prompts/' + wsId, {}, token).then(r => setTemplates(r.data || r.templates || [])).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const handleSave = async () => {
    try {
      await api('/api/prompts', {
        method: 'POST', body: JSON.stringify({ project_id: wsId, ...form })
      }, token);
      setShowCreate(false);
      setForm({ name: '', type: 'source_analysis', template: '', model: 'claude-sonnet-4-5-20250514', temperature: 0.3, max_tokens: 4096 });
      api('/api/prompts/' + wsId, {}, token).then(r => setTemplates(r.data || r.templates || [])).catch(err => console.warn('API:', err.message));
    } catch (e) { alert(e.message); }
  };

  return (
    <div style={{ display: 'grid', gap: 16 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div style={{ display: 'flex', gap: 8 }}>{types.map(t => (
          <span key={t} className="badge" style={{ fontSize: 9 }}>{t.replace(/_/g, ' ')}</span>
        ))}</div>
        <button className="btn btn-primary" onClick={() => setShowCreate(true)}>+ New Template</button>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '300px 1fr', gap: 16 }}>
        <div className="card">
          <div className="card-header">Templates ({templates.length})</div>
          {templates.length === 0 ? (
            <div style={{ color: 'var(--text-muted)', fontSize: 12, padding: 12 }}>No custom templates. Using system defaults.</div>
          ) : templates.map((t, i) => (
            <div key={i} className={`sidebar-item ${selected === i ? 'active' : ''}`}
              onClick={() => setSelected(i)} style={{ cursor: 'pointer', padding: '8px 12px' }}>
              <div style={{ fontSize: 12 }}>{t.name || t.type}</div>
              <div style={{ display: 'flex', gap: 4, marginTop: 2 }}>
                <span className="badge" style={{ fontSize: 9 }}>{t.type?.replace(/_/g, ' ')}</span>
                <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>v{t.version || 1}</span>
              </div>
            </div>
          ))}
          <div style={{ borderTop: '1px solid var(--border)', padding: '12px 0', marginTop: 8 }}>
            <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 8 }}>System Defaults</div>
            {['Source Analysis', 'Brief Generation', 'Content Generation'].map(n => (
              <div key={n} style={{ fontSize: 12, padding: '4px 12px', color: 'var(--text-secondary)' }}>{n} (built-in)</div>
            ))}
          </div>
        </div>

        <div>
          {selected != null && templates[selected] ? (
            <div className="card">
              <div className="card-header">{templates[selected].name || templates[selected].type}</div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 8, marginBottom: 12 }}>
                <div style={{ fontSize: 11 }}><span style={{ color: 'var(--text-muted)' }}>Model:</span> {templates[selected].model || 'default'}</div>
                <div style={{ fontSize: 11 }}><span style={{ color: 'var(--text-muted)' }}>Temp:</span> {templates[selected].temperature ?? '—'}</div>
                <div style={{ fontSize: 11 }}><span style={{ color: 'var(--text-muted)' }}>Max Tokens:</span> {templates[selected].max_tokens || '—'}</div>
              </div>
              <div style={{ fontFamily: 'var(--font-mono)', fontSize: 11, whiteSpace: 'pre-wrap', background: 'var(--surface-1)', padding: 12, borderRadius: 6, maxHeight: 400, overflow: 'auto' }}>
                {templates[selected].template || 'No template content'}
              </div>
            </div>
          ) : (
            <div className="card"><div className="empty-state">{'\u2756'}<br/>Select a template to view or create a new one.</div></div>
          )}
        </div>
      </div>

      {showCreate && (
        <div className="modal-overlay" onClick={() => setShowCreate(false)}>
          <div className="modal" onClick={e => e.stopPropagation()}>
            <div className="modal-header">New Prompt Template</div>
            <div className="modal-body">
              <div style={{ display: 'grid', gap: 12 }}>
                <div><label className="form-label">Name</label><input className="form-input" value={form.name} onChange={e => setForm({...form, name: e.target.value})} placeholder="e.g. GEO Blog Analysis v2" /></div>
                <div><label className="form-label">Type</label>
                  <select className="form-input" value={form.type} onChange={e => setForm({...form, type: e.target.value})}>
                    {types.map(t => <option key={t} value={t}>{t.replace(/_/g, ' ')}</option>)}
                  </select></div>
                <div><label className="form-label">Model</label>
                  <select className="form-input" value={form.model} onChange={e => setForm({...form, model: e.target.value})}>
                    <option value="claude-sonnet-4-5-20250514">Claude Sonnet 4.5</option>
                    <option value="claude-opus-4-6">Claude Opus 4.6</option>
                    <option value="claude-haiku-4-5-20251001">Claude Haiku 4.5</option>
                  </select></div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                  <div><label className="form-label">Temperature</label><input className="form-input" type="number" step="0.1" min="0" max="1" value={form.temperature} onChange={e => setForm({...form, temperature: parseFloat(e.target.value)})} /></div>
                  <div><label className="form-label">Max Tokens</label><input className="form-input" type="number" value={form.max_tokens} onChange={e => setForm({...form, max_tokens: parseInt(e.target.value)})} /></div>
                </div>
                <div><label className="form-label">Prompt Template</label>
                  <textarea className="form-input" value={form.template} onChange={e => setForm({...form, template: e.target.value})}
                    rows={10} placeholder="Enter your prompt template. Use {{variables}} for dynamic content..." style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }} /></div>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn" onClick={() => setShowCreate(false)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleSave}>Save Template</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// MONITORING PAGE (System Health & Observability)
// ═══════════════════════════════════════════════════════════════

function MonitoringPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [health, setHealth] = useState(null);
  const [jobStats, setJobStats] = useState(null);
  const [usage, setUsage] = useState(null);
  const [usageHistory, setUsageHistory] = useState([]);
  const [config, setConfig] = useState(null);

  useEffect(() => {
    api('/api/health', {}, token).then(r => setHealth(r)).catch(err => console.warn('API:', err.message));
    api('/api/config', {}, token).then(r => setConfig(r.data || r)).catch(err => console.warn('API:', err.message));
    if (!wsId) return;
    api('/api/ops/jobs/' + wsId + '/stats', {}, token).then(r => setJobStats(r.data || r)).catch(err => console.warn('API:', err.message));
    api('/api/usage/' + wsId, {}, token).then(r => setUsage(r.data || r)).catch(err => console.warn('API:', err.message));
    api('/api/usage/' + wsId + '/history', {}, token).then(r => setUsageHistory(r.data || r.history || [])).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  return (
    <div style={{ display: 'grid', gap: 16 }}>
      <div className="metrics-grid">
        <div className="metric-card">
          <div className="metric-label">SYSTEM STATUS</div>
          <div className="metric-value" style={{ color: health?.status === 'ok' ? 'var(--emerald)' : 'var(--rose)' }}>
            {health?.status === 'ok' ? 'HEALTHY' : 'CHECKING...'}
          </div>
        </div>
        <div className="metric-card">
          <div className="metric-label">QUEUE HEALTH</div>
          <div className="metric-value">{jobStats?.queued || 0} queued</div>
          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>{jobStats?.running || 0} running · {jobStats?.failed || 0} failed</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">DAILY COST</div>
          <div className="metric-value">${usage?.daily_cost?.toFixed(2) || '0.00'}</div>
          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Limit: ${usage?.daily_limit || '50.00'}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">MONTHLY COST</div>
          <div className="metric-value">${usage?.monthly_cost?.toFixed(2) || '0.00'}</div>
          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Limit: ${usage?.monthly_limit || '500.00'}</div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
        <div className="card">
          <div className="card-header">System Features</div>
          {health?.features ? Object.entries(health.features).map(([k, v]) => (
            <div key={k} style={{ display: 'flex', justifyContent: 'space-between', padding: '6px 0', borderBottom: '1px solid var(--border-subtle)' }}>
              <span style={{ fontSize: 12, textTransform: 'capitalize' }}>{k.replace(/_/g, ' ')}</span>
              <span className="badge" style={{ background: v ? 'var(--emerald)' : 'var(--surface-2)', color: v ? '#000' : 'var(--text-muted)' }}>
                {v ? 'ENABLED' : 'DISABLED'}
              </span>
            </div>
          )) : <div style={{ color: 'var(--text-muted)', fontSize: 12, padding: 12 }}>Loading...</div>}
        </div>

        <div className="card">
          <div className="card-header">Job Queue Status</div>
          {jobStats ? (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 8 }}>
              {Object.entries(jobStats).filter(([k]) => !k.startsWith('_')).map(([k, v]) => (
                <div key={k} style={{ padding: 8, background: 'var(--surface-1)', borderRadius: 6 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>{k.replace(/_/g, ' ')}</div>
                  <div style={{ fontSize: 18, fontWeight: 700 }}>{typeof v === 'number' ? v : JSON.stringify(v)}</div>
                </div>
              ))}
            </div>
          ) : <div style={{ color: 'var(--text-muted)', fontSize: 12, padding: 12 }}>Loading...</div>}
        </div>
      </div>

      <div className="card">
        <div className="card-header">API Usage History</div>
        {usageHistory.length === 0 ? (
          <div className="empty-state">{'\u2261'}<br/>No usage data yet. Run analyses or generate content to track costs.</div>
        ) : (
          <table className="data-table"><thead><tr>
            <th>Date</th><th>Input Tokens</th><th>Output Tokens</th><th>Total Tokens</th><th>Cost</th><th>Calls</th>
          </tr></thead><tbody>{usageHistory.slice(0, 30).map((h, i) => (
            <tr key={i}>
              <td>{h.date}</td>
              <td>{h.input_tokens?.toLocaleString() || 0}</td>
              <td>{h.output_tokens?.toLocaleString() || 0}</td>
              <td>{h.total_tokens?.toLocaleString() || 0}</td>
              <td style={{ color: 'var(--amber)' }}>${h.cost?.toFixed(4) || '0'}</td>
              <td>{h.call_count || 0}</td>
            </tr>
          ))}</tbody></table>
        )}
      </div>

      <div className="card">
        <div className="card-header">Configuration</div>
        {config ? (
          <div style={{ fontFamily: 'var(--font-mono)', fontSize: 11, whiteSpace: 'pre-wrap', background: 'var(--surface-1)', padding: 12, borderRadius: 6, maxHeight: 300, overflow: 'auto' }}>
            {JSON.stringify(config, null, 2)}
          </div>
        ) : <div style={{ color: 'var(--text-muted)', fontSize: 12, padding: 12 }}>Loading...</div>}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// GEO CONQUEST — 11 engines
// ═══════════════════════════════════════════════════════════════

// 1. Prompt Battlefield
function PromptBattlefieldPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [bf, setBf] = useState(null);
  const [prompts, setPrompts] = useState([]);
  const [filterStage, setFilterStage] = useState('');
  const [newText, setNewText] = useState('');
  const [busy, setBusy] = useState(false);
  const [tracking, setTracking] = useState(false);
  const [msg, setMsg] = useState('');
  const [importing, setImporting] = useState(false);
  const [emergingMap, setEmergingMap] = useState({}); // prompt_id -> { subtype, why }
  const [trackingSource, setTrackingSource] = useState(null); // latest data_source
  const [emergingHover, setEmergingHover] = useState(null); // prompt_id of hovered row
  const [comparePromptId, setComparePromptId] = useState(null);

  const load = () => {
    if (!wsId) return;
    api(`/api/prompts/${wsId}/battlefield`, {}, token).then(r => setBf(r.data || r)).catch(() => {});
    api(`/api/prompts/${wsId}${filterStage ? '?stage=' + filterStage : ''}`, {}, token)
      .then(r => setPrompts(r.data || [])).catch(() => {});
    api(`/api/emerging/${wsId}`, {}, token)
      .then(r => {
        const list = r.data || r || [];
        const m = {};
        list.forEach(it => { if (it.prompt_id) m[it.prompt_id] = { subtype: it.subtype, why: it.why, confidence: it.confidence }; });
        setEmergingMap(m);
      })
      .catch(() => {});
    api(`/api/tracking/${wsId}/runs`, {}, token)
      .then(r => {
        const runs = r.data || r || [];
        setTrackingSource(runs[0]?.data_source || null);
      })
      .catch(() => {});
  };
  useEffect(load, [wsId, filterStage]);

  const importCsv = async (file) => {
    if (!file || !wsId) return;
    setImporting(true); setMsg('');
    try {
      const fd = new FormData();
      fd.append('file', file);
      fd.append('classify', 'false');
      const r = await api(`/api/prompts/${wsId}/import-csv`, { method: 'POST', body: fd }, token);
      if (r.success === false) {
        setMsg('Import failed: ' + (r.error || 'unknown'));
      } else {
        const d = r.data || r;
        setMsg(`Imported ${d.prompts_upserted} prompts, ${d.observations} observations, our brand in ${d.our_brand_hits}.`);
        load();
      }
    } catch (e) { setMsg('Import failed: ' + e.message); }
    setImporting(false);
  };

  const addPrompt = async () => {
    if (!newText.trim()) return;
    setBusy(true);
    try {
      await api(`/api/prompts/${wsId}`, { method: 'POST', body: JSON.stringify({ text: newText }) }, token);
      setNewText(''); setMsg('Prompt added.'); load();
    } catch (e) { setMsg('Failed: ' + e.message); }
    setBusy(false);
  };
  const trackAll = async () => {
    setTracking(true); setMsg('');
    try {
      const r = await api(`/api/prompts/${wsId}/track`, { method: 'POST', body: JSON.stringify({}) }, token);
      const d = r.data || r;
      setMsg(`Tracked ${d.tracked} prompts; our brand surfaced in ${d.with_our_brand}.`);
      load();
    } catch (e) { setMsg('Track failed: ' + e.message); }
    setTracking(false);
  };

  const stageColor = { decision: 'rose', trust: 'amber', comparison: 'purple', objection: 'blue', solution: 'emerald', problem: 'blue', awareness: 'gray' };
  const isReplay = trackingSource === 'peec_replay';

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header">Prompt Battlefield — what AI says about you</div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          The real GEO battlefield. Each prompt = one question AI answers. Win the ones with revenue intent.
        </p>
        {bf && (
          <div className="metrics-grid" style={{ marginTop: 8 }}>
            <div className="metric-card"><div className="metric-label">HIGH-VALUE PROMPTS</div><div className="metric-value">{bf.high_value_prompts || 0}</div></div>
            <div className="metric-card"><div className="metric-label">OWNED</div><div className="metric-value" style={{ color: 'var(--emerald)' }}>{bf.owned || 0}</div></div>
            <div className="metric-card"><div className="metric-label">LOST</div><div className="metric-value" style={{ color: 'var(--rose)' }}>{bf.lost || 0}</div></div>
            <div className="metric-card"><div className="metric-label">EMERGING</div><div className="metric-value" style={{ color: 'var(--amber)' }}>{bf.emerging || 0}</div></div>
            <div className="metric-card"><div className="metric-label">€ AT STAKE</div><div className="metric-value">€{Math.round((bf.estimated_revenue_at_stake_eur || 0)).toLocaleString()}</div></div>
          </div>
        )}
        {bf?.top_dominators?.length > 0 && (
          <div style={{ marginTop: 12 }}>
            <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Top dominators</div>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {bf.top_dominators.map(d => (
                <span key={d.domain} className="badge rose">{d.domain} · {d.lost_prompts} lost</span>
              ))}
            </div>
          </div>
        )}
      </div>

      <EmergingPanelCard wsId={wsId} />

      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 16 }}>
        <div className="card">
          <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span>Prompts ({prompts.length})</span>
            <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
              <select className="form-input" style={{ width: 'auto', fontSize: 11 }} value={filterStage} onChange={e => setFilterStage(e.target.value)}>
                <option value="">All stages</option>
                {['awareness','problem','solution','comparison','trust','objection','decision'].map(s => <option key={s} value={s}>{s}</option>)}
              </select>
              <button className="btn btn-sm btn-primary" onClick={trackAll} disabled={tracking}>{tracking ? 'Tracking...' : 'Track All (high-value)'}</button>
              {isReplay && (
                <span title="No live model API keys — using cached Peec data" style={{ fontSize: 10, color: 'var(--amber)', fontStyle: 'italic' }}>
                  Tracking will use Peec replay (no model API keys configured)
                </span>
              )}
              <button className="btn btn-sm" onClick={async () => { setBusy(true); setMsg(''); try { const r = await api(`/api/prompts/${wsId}/reclassify?max_n=40`, { method: 'POST' }, token); setMsg(`Claude classified ${r.data?.reclassified || 0} prompts.`); load(); } catch (e) { setMsg('Reclassify failed: ' + e.message); } setBusy(false); }} disabled={busy}>Reclassify (Claude)</button>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
            <input className="form-input" placeholder='Add a prompt: e.g. "best private orthopedic surgeon Budapest"'
              value={newText} onChange={e => setNewText(e.target.value)} style={{ flex: 1 }} />
            <button className="btn btn-primary" onClick={addPrompt} disabled={busy}>{busy ? 'Adding...' : 'Add Prompt'}</button>
            <label className="btn" style={{ cursor: 'pointer' }}>
              {importing ? 'Importing...' : 'Import Peec Prompts CSV'}
              <input type="file" accept=".csv,.tsv" style={{ display: 'none' }}
                disabled={importing}
                onChange={e => { const f = e.target.files?.[0]; if (f) importCsv(f); e.target.value = ''; }} />
            </label>
          </div>
          {msg && <div style={{ fontSize: 11, color: 'var(--emerald)', marginBottom: 6 }}>{msg}</div>}
          {prompts.length === 0 ? (
            <div className="empty-state">⚑<br/>No prompts yet. Add the questions you want AI to recommend you for.</div>
          ) : (
            <table className="data-table"><thead><tr>
              <th>Prompt</th><th>Type</th><th>Stage</th>
              <th>Revenue <MetricTooltip metricKey="prompt_revenue" /></th>
              <th>Ownership <MetricTooltip metricKey="prompt_ownership_level" /></th>
              <th>Source</th><th>Confidence</th><th>Status</th><th>Action</th>
            </tr></thead><tbody>{prompts.slice(0, 200).map(p => {
              const em = emergingMap[p.id];
              return (
                <tr key={p.id}>
                  <td style={{ maxWidth: 360, overflow: 'hidden', textOverflow: 'ellipsis', position: 'relative' }}>
                    <span>{p.text}</span>
                    {em && (
                      <span
                        onMouseEnter={() => setEmergingHover(p.id)}
                        onMouseLeave={() => setEmergingHover(null)}
                        title={em.why || ''}
                        style={{
                          marginLeft: 6, display: 'inline-block', padding: '1px 7px',
                          borderRadius: 9, fontSize: 9, fontWeight: 600,
                          color: 'var(--purple)', background: 'rgba(168,85,247,0.18)',
                          border: '1px solid rgba(168,85,247,0.35)',
                          fontFamily: 'var(--font-mono)', cursor: 'help',
                        }}
                      >Emerging · {em.subtype}</span>
                    )}
                    {em && emergingHover === p.id && em.why && (
                      <div style={{
                        position: 'absolute', top: '100%', left: 0, zIndex: 50,
                        marginTop: 4, padding: '6px 8px',
                        background: 'var(--bg-raised)', border: '1px solid var(--border-default)',
                        borderRadius: 4, fontSize: 11, color: 'var(--text-secondary)',
                        maxWidth: 320, lineHeight: 1.4, boxShadow: '0 4px 12px rgba(0,0,0,0.25)',
                      }}>{em.why}</div>
                    )}
                  </td>
                  <td><span className="badge">{p.prompt_type}</span></td>
                  <td><span className={`badge ${stageColor[p.buyer_stage] || 'gray'}`}>{p.buyer_stage}</span></td>
                  <td style={{ fontWeight: 600, color: p.revenue_score >= 70 ? 'var(--emerald)' : p.revenue_score >= 40 ? 'var(--amber)' : 'var(--text-muted)' }}>{Math.round(p.revenue_score || 0)}</td>
                  <td><OwnershipLevelBadge level={p.ownership_level} status={p.ownership_status} /></td>
                  <td>{p.source || '—'}</td>
                  <td><ConfidenceBadge level={p.confidence || 'estimated'} /></td>
                  <td><span className="badge">{p.status}</span></td>
                  <td><button className="btn btn-sm" onClick={() => setComparePromptId(p.id)} title="Compare vs winning competitor">Compare</button></td>
                </tr>
              );
            })}</tbody></table>
          )}
        </div>

        <div style={{ display: 'grid', gap: 16, alignContent: 'start' }}>
          <DominatorListCard wsId={wsId} limit={5} expandable={false} title="Top dominators" />
          <TrackingLogPanel wsId={wsId} onRefresh={load} />
        </div>
      </div>
      <PageCoverageCard wsId={wsId} />

      {comparePromptId && (
        <ComparativeDiagnosisModal
          wsId={wsId}
          promptId={comparePromptId}
          onClose={() => setComparePromptId(null)}
          onPushedToActions={() => { /* keep modal open with msg */ }}
        />
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// PAGE COVERAGE CARD — Phase 4
// ═══════════════════════════════════════════════════════════════

function PageCoverageCard({ wsId }) {
  const { token } = useContext(AuthContext);
  const [rows, setRows] = useState([]);
  const [onlyMissing, setOnlyMissing] = useState(false);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState('');
  const [briefFor, setBriefFor] = useState(null);

  const load = () => {
    if (!wsId) return;
    api(`/api/page-mapping/${wsId}?only_missing=${onlyMissing}`, {}, token)
      .then(r => setRows(r.data || r || []))
      .catch(e => setMsg('Load failed: ' + e.message));
  };
  useEffect(load, [wsId, onlyMissing]);

  const mapWorkspace = async () => {
    setBusy(true); setMsg('');
    try { const r = await api(`/api/page-mapping/${wsId}/map-workspace`, { method: 'POST' }, token); const d = r.data || r; setMsg(`Mapped ${d.mapped || 0} prompts; ${(d.missing_pages || []).length} missing pages.`); load(); }
    catch (e) { setMsg('Map workspace failed: ' + e.message); }
    setBusy(false);
  };

  const mapOne = async (promptId) => {
    setBusy(true); setMsg('');
    try { await api(`/api/page-mapping/${wsId}/${promptId}/map`, { method: 'POST' }, token); setMsg('Mapped.'); load(); }
    catch (e) { setMsg('Map failed: ' + e.message); }
    setBusy(false);
  };

  const linkify = url => {
    if (!url) return <span style={{ color: 'var(--text-muted)' }}>—</span>;
    let href = String(url);
    if (!/^https?:\/\//i.test(href)) href = 'https://' + href;
    return <a href={href} target="_blank" rel="noopener noreferrer" style={{ color: 'var(--blue)', fontFamily: 'var(--font-mono)', fontSize: 11, wordBreak: 'break-all' }}>{String(url).slice(0, 60)}</a>;
  };

  return (
    <>
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>Page coverage — prompt → page mapping</span>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <label style={{ fontSize: 11, display: 'flex', gap: 4, alignItems: 'center' }}>
              <input type="checkbox" checked={onlyMissing} onChange={e => setOnlyMissing(e.target.checked)} /> Only missing pages
            </label>
            <button className="btn btn-sm btn-primary" onClick={mapWorkspace} disabled={busy || !wsId}>{busy ? 'Working…' : 'Map Workspace'}</button>
          </div>
        </div>
        {msg && <div style={{ fontSize: 11, color: msg.includes('failed') ? 'var(--rose)' : 'var(--emerald)', marginBottom: 6 }}>{msg}</div>}
        {rows.length === 0 ? (
          <div className="empty-state" style={{ fontSize: 12 }}>◌<br/>No mappings yet. Click Map Workspace to scan prompts vs your pages.</div>
        ) : (
          <table className="data-table"><thead><tr>
            <th>Prompt</th><th>Page type</th><th>Target URL</th><th>Current best URL</th><th>Missing?</th><th>Competitor winning URL</th><th>Confidence</th><th></th>
          </tr></thead><tbody>{rows.map(r => (
            <tr key={r.id || r.prompt_id}>
              <td style={{ maxWidth: 280, overflow: 'hidden', textOverflow: 'ellipsis' }}>{r.prompt_text || r.prompt_id}</td>
              <td>{r.page_type ? <span className="badge">{r.page_type}</span> : <span style={{ color: 'var(--text-muted)' }}>—</span>}</td>
              <td>{linkify(r.target_url)}</td>
              <td>{linkify(r.current_best_url)}</td>
              <td>{r.missing_page ? <span className="badge rose">missing</span> : <span className="badge emerald">covered</span>}</td>
              <td>{linkify(r.competitor_winning_url)}</td>
              <td><ConfidenceBadge level={r.confidence || 'estimated'} /></td>
              <td style={{ whiteSpace: 'nowrap' }}>
                <button className="btn btn-sm" onClick={() => mapOne(r.prompt_id)} disabled={busy}>Map</button>
                {r.missing_page ? <button className="btn btn-sm btn-primary" style={{ marginLeft: 4 }} onClick={() => setBriefFor(r.prompt_id)}>Generate Brief</button> : null}
              </td>
            </tr>
          ))}</tbody></table>
        )}
      </div>
      {briefFor && <GeoBriefModal wsId={wsId} promptId={briefFor} onClose={() => setBriefFor(null)} />}
    </>
  );
}

// 2. Citation Intelligence
function CitationIntelPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [diags, setDiags] = useState([]);
  const [prompts, setPrompts] = useState([]);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState('');
  const [selPrompt, setSelPrompt] = useState('');
  const [competitor, setCompetitor] = useState('');
  const [manualUrl, setManualUrl] = useState('');
  const [cmpPromptId, setCmpPromptId] = useState(null);
  const [cmpCompetitor, setCmpCompetitor] = useState('');
  const [cmpManualUrl, setCmpManualUrl] = useState('');
  const [cmpSelect, setCmpSelect] = useState('');
  const [cmpPasteId, setCmpPasteId] = useState('');
  const [cmpPreloaded, setCmpPreloaded] = useState(null);
  const [cmpBusy, setCmpBusy] = useState(false);
  const [recentDiagnoses, setRecentDiagnoses] = useState([]);

  const load = () => {
    if (!wsId) return;
    api(`/api/intel/diagnostics/${wsId}`, {}, token).then(r => {
      const list = r.data || r || [];
      setDiags(list);
      setRecentDiagnoses(list);
    }).catch(() => {});
    api(`/api/prompts/${wsId}?min_revenue=40&limit=200`, {}, token).then(r => setPrompts(r.data || [])).catch(() => {});
  };
  useEffect(load, [wsId]);

  const runComparative = async () => {
    const pid = (cmpSelect || cmpPasteId || '').trim();
    if (!pid) { setMsg('Pick a prompt or paste a prompt ID first.'); return; }
    setCmpBusy(true); setMsg('');
    try {
      const body = {};
      if (cmpCompetitor.trim()) body.competitor_domain = cmpCompetitor.trim();
      if (cmpManualUrl.trim()) body.manual_competitor_url = cmpManualUrl.trim();
      const r = await api(`/api/intel/${wsId}/compare/${pid}`, {
        method: 'POST', body: JSON.stringify(body),
      }, token);
      const d = r.data || r;
      setCmpPreloaded(d);
      setCmpPromptId(pid);
    } catch (e) { setMsg('Comparative failed: ' + e.message); }
    setCmpBusy(false);
  };

  const openExistingDiagnosis = (d) => {
    // Map persisted diagnostic row -> ComparativeDiagnosisModal data shape (best-effort).
    const preloaded = {
      prompt_id: d.prompt_id || null,
      prompt_text: d.prompt_text || d.prompt || '',
      buyer_stage: d.buyer_stage || '',
      winning_competitor: d.competitor_domain || d.winning_competitor || '',
      competitor_url: d.analyzed_url || d.competitor_url || '',
      our_url: d.our_url || '',
      gaps: d.gaps || {
        why_they_win: d.diagnosis || '',
        what_they_have_we_lack: d.actions || [],
        content_gap: '', schema_gap: '', trust_gap: '',
        offsite_gap: '', citation_gap: '', decision_support_gap: '',
      },
      recommended_action: d.recommended_action || '',
      expected_impact: d.expected_impact || '',
      implementation_difficulty: d.implementation_difficulty || '',
      confidence: d.confidence || 'estimated',
      source_label: d.source_label || d.data_source || '',
      diagnosis_id: d.id || d.diagnosis_id,
      last_analyzed: d.analyzed_at || d.last_analyzed || null,
    };
    setCmpPreloaded(preloaded);
    setCmpPromptId(preloaded.prompt_id || d.id || 'existing');
  };

  const runWorkspace = async () => {
    setBusy(true); setMsg('');
    try {
      const r = await api(`/api/intel/diagnose/${wsId}?top_n=8`, { method: 'POST' }, token);
      setMsg(`Ran ${r.data?.diagnosed || 0} diagnoses.`); load();
    } catch (e) { setMsg('Failed: ' + e.message); }
    setBusy(false);
  };

  const runOne = async () => {
    if (!selPrompt) { setMsg('Pick a prompt first.'); return; }
    setBusy(true); setMsg('');
    try {
      const qs = competitor ? '?competitor_domain=' + encodeURIComponent(competitor) : '';
      const body = manualUrl.trim() ? JSON.stringify({ manual_urls: [manualUrl.trim()] }) : null;
      const opts = { method: 'POST' };
      if (body) opts.body = body;
      await api(`/api/intel/diagnose/${wsId}/${selPrompt}${qs}`, opts, token);
      setMsg('Diagnosis complete.'); load();
    } catch (e) { setMsg('Failed: ' + e.message); }
    setBusy(false);
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <CitationBreakdownCard wsId={wsId} />
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>Citation Intelligence — why are we losing <MetricTooltip metricKey="citation_quality" /></span>
          <button className="btn btn-sm btn-primary" onClick={runWorkspace} disabled={busy}>{busy ? 'Diagnosing...' : 'Diagnose Top 8 Losses'}</button>
        </div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          For every prompt where a competitor outranks us, we scrape their winning page and ask Claude WHY AI cites them. Diagnosis + actions persist below.
        </p>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr auto', gap: 6, marginTop: 8 }}>
          <select className="form-input" value={selPrompt} onChange={e => setSelPrompt(e.target.value)}>
            <option value="">— pick a prompt —</option>
            {prompts.map(p => <option key={p.id} value={p.id}>{p.text.slice(0, 80)}</option>)}
          </select>
          <input className="form-input" placeholder="competitor domain/brand" value={competitor} onChange={e => setCompetitor(e.target.value)} />
          <input className="form-input" placeholder="manual URL (optional)" value={manualUrl} onChange={e => setManualUrl(e.target.value)} />
          <button className="btn" onClick={runOne} disabled={busy}>Diagnose this</button>
        </div>
        {msg && <div style={{ fontSize: 11, color: msg.includes('Failed') ? 'var(--rose)' : 'var(--emerald)', marginTop: 6 }}>{msg}</div>}
      </div>

      {/* Phase 3: Comparative diagnosis (side-by-side) */}
      <div className="card">
        <div className="card-header">Diagnose this prompt vs winning competitor</div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          Side-by-side competitor vs us — content, schema, trust, offsite, citation, decision support. Pushes prioritized actions to the Action Engine.
        </p>
        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr 1fr 1fr auto', gap: 6, marginTop: 8 }}>
          <select className="form-input" value={cmpSelect} onChange={e => { setCmpSelect(e.target.value); if (e.target.value) setCmpPasteId(''); }}>
            <option value="">— pick a prompt —</option>
            {prompts.map(p => <option key={p.id} value={p.id}>{p.text.slice(0, 80)}</option>)}
          </select>
          <input className="form-input" placeholder="…or paste prompt ID" value={cmpPasteId} onChange={e => { setCmpPasteId(e.target.value); if (e.target.value) setCmpSelect(''); }} />
          <input className="form-input" placeholder="competitor domain (optional)" value={cmpCompetitor} onChange={e => setCmpCompetitor(e.target.value)} />
          <input className="form-input" placeholder="manual competitor URL (optional)" value={cmpManualUrl} onChange={e => setCmpManualUrl(e.target.value)} />
          <button className="btn btn-primary" onClick={runComparative} disabled={cmpBusy}>{cmpBusy ? 'Running…' : 'Run comparative'}</button>
        </div>
      </div>

      {/* Phase 3: Recent diagnoses table */}
      {recentDiagnoses.length > 0 && (
        <div className="card">
          <div className="card-header">Recent comparative diagnoses</div>
          <table className="data-table">
            <thead><tr>
              <th>Prompt</th><th>Competitor</th><th>Recommended action</th><th>Confidence</th><th></th>
            </tr></thead>
            <tbody>
              {recentDiagnoses.slice(0, 20).map(d => (
                <tr key={d.id}>
                  <td style={{ maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                      title={d.prompt_text || d.prompt || ''}>
                    {(d.prompt_text || d.prompt || '—').slice(0, 80)}
                  </td>
                  <td style={{ fontFamily: 'var(--font-mono)', color: 'var(--blue)' }}>{d.competitor_domain || '—'}</td>
                  <td style={{ maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                      title={d.recommended_action || d.diagnosis || ''}>
                    {(d.recommended_action || d.diagnosis || '—').slice(0, 90)}
                  </td>
                  <td><ConfidenceBadge level={d.confidence || 'estimated'} /></td>
                  <td><button className="btn btn-sm" onClick={() => openExistingDiagnosis(d)}>Open</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {diags.length === 0 ? (
        <div className="empty-state">◎<br/>No diagnostics yet. Run the diagnosis above.</div>
      ) : diags.map(d => (
        <div key={d.id} className="card">
          <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span>{d.competitor_domain} <span className="badge">{d.analyzed_url ? new URL(d.analyzed_url).pathname.slice(0, 40) : ''}</span></span>
            <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-muted)' }}>{new Date(d.analyzed_at).toLocaleDateString()}</span>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: 6, marginBottom: 10 }}>
            {['content','schema','authority','reddit','youtube','entity'].map(k => {
              const v = d[`${k}_score`] || 0;
              return (
                <div key={k} className="card" style={{ padding: 8, textAlign: 'center' }}>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase' }}>{k}</div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: v >= 70 ? 'var(--rose)' : v >= 40 ? 'var(--amber)' : 'var(--emerald)' }}>{Math.round(v)}</div>
                </div>
              );
            })}
          </div>
          <div style={{ fontSize: 13, lineHeight: 1.5, marginBottom: 8 }}>{d.diagnosis}</div>
          {(() => { try {
            const acts = typeof d.actions === 'string' ? JSON.parse(d.actions) : (d.actions || []);
            return acts.length ? (
              <ol style={{ fontSize: 12, paddingLeft: 18, color: 'var(--text-secondary)' }}>
                {acts.map((a, i) => (
                  <li key={i} style={{ marginBottom: 4 }}>
                    <span className={`badge ${a.priority === 'high' ? 'rose' : a.priority === 'medium' ? 'amber' : 'gray'}`}>{a.priority}</span>{' '}{a.step} {a.impact && <span style={{ color: 'var(--text-muted)' }}>→ {a.impact}</span>}
                  </li>
                ))}
              </ol>
            ) : null;
          } catch { return null; }})()}
        </div>
      ))}
      {cmpPromptId && (
        <ComparativeDiagnosisModal
          wsId={wsId}
          promptId={cmpPromptId}
          preloaded={cmpPreloaded}
          onClose={() => { setCmpPromptId(null); setCmpPreloaded(null); }}
          onPushedToActions={() => load()}
        />
      )}
    </div>
  );
}

// 3. Attack Map
function AttackMapPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [rows, setRows] = useState([]);
  const [movements, setMovements] = useState([]);
  const [domain, setDomain] = useState('');
  const [busy, setBusy] = useState(false);
  const [trackedSet, setTrackedSet] = useState({});
  const [rowBusy, setRowBusy] = useState({}); // domain -> 'analyze' | 'track' | 'push'
  const [toast, setToast] = useState('');

  const load = () => {
    if (!wsId) return;
    api(`/api/attack-map/${wsId}`, {}, token).then(r => setRows(r.data || [])).catch(() => {});
    api(`/api/attack-map/${wsId}/movements?days=14`, {}, token).then(r => setMovements(r.data || [])).catch(() => {});
    api(`/api/competitors/${wsId}/tracked`, {}, token)
      .then(r => {
        const list = r.data || r || [];
        const m = {};
        list.forEach(p => { if (p?.domain) m[p.domain] = true; });
        setTrackedSet(m);
      })
      .catch(() => {});
  };
  useEffect(load, [wsId]);

  const flashToast = (txt) => {
    setToast(txt);
    setTimeout(() => setToast(''), 4000);
  };

  const analyze = async () => {
    if (!domain.trim()) return;
    setBusy(true);
    try {
      await api(`/api/attack-map/${wsId}/analyze`, { method: 'POST', body: JSON.stringify({ competitor_domain: domain }) }, token);
      setDomain(''); load();
    } catch (e) { flashToast('Analyze failed: ' + e.message); }
    setBusy(false);
  };

  const analyzeAll = async () => {
    setBusy(true);
    try {
      await api(`/api/attack-map/${wsId}/analyze-all-known?max_competitors=12`, { method: 'POST' }, token);
      load();
    } catch (e) { flashToast('Analyze all failed: ' + e.message); }
    setBusy(false);
  };

  const analyzeRow = async (d) => {
    setRowBusy(s => ({ ...s, [d]: 'analyze' }));
    try {
      await api(`/api/attack-map/${wsId}/analyze`, { method: 'POST', body: JSON.stringify({ competitor_domain: d }) }, token);
      flashToast(`Analyzed ${d}`); load();
    } catch (e) { flashToast('Analyze failed: ' + e.message); }
    setRowBusy(s => ({ ...s, [d]: null }));
  };

  const trackRow = async (d) => {
    setRowBusy(s => ({ ...s, [d]: 'track' }));
    try {
      await api(`/api/competitors/${wsId}/${encodeURIComponent(d)}/track`, { method: 'POST' }, token);
      setTrackedSet(s => ({ ...s, [d]: true }));
      flashToast(`Tracking ${d} ✓`);
    } catch (e) { flashToast('Track failed: ' + e.message); }
    setRowBusy(s => ({ ...s, [d]: null }));
  };

  const pushAttackRow = async (d) => {
    setRowBusy(s => ({ ...s, [d]: 'push' }));
    try {
      const r = await api(`/api/attack-map/${wsId}/${encodeURIComponent(d)}/push-actions`, { method: 'POST' }, token);
      const dd = r.data || r;
      const n = dd.created ?? (dd.action_ids || []).length ?? 0;
      flashToast(`${n} attack actions queued for ${d} ✓`);
    } catch (e) { flashToast('Push failed: ' + e.message); }
    setRowBusy(s => ({ ...s, [d]: null }));
  };

  const AXES = ['schema','reddit','youtube','faq_depth','decision_support','review','entity_consistency','pr','local_authority'];

  const effortColors = { easy: 'var(--emerald)', medium: 'var(--amber)', hard: 'var(--rose)', low: 'var(--emerald)', high: 'var(--rose)' };
  const impactColors = { high: 'var(--emerald)', medium: 'var(--amber)', low: 'var(--text-muted)' };

  const truncate = (s, n) => {
    if (!s) return '';
    return s.length > n ? s.slice(0, n - 1) + '…' : s;
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header" style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span>GEO Attack Map — find the open flank</span>
          <MetricTooltip metricKey="dominator" />
        </div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          After your Peec CSV import, every competitor brand mentioned in your prompts gets a seeded
          capability row. Click <b>Analyze All Known</b> to refine them with page scrapes + Claude diagnosis,
          or add a specific competitor domain below.
        </p>
        <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
          <input className="form-input" placeholder="competitor domain or brand (e.g. medicover.hu or Aesthetica)" value={domain} onChange={e => setDomain(e.target.value)} style={{ flex: 1 }} />
          <button className="btn btn-primary" onClick={analyze} disabled={busy}>{busy ? 'Analyzing...' : 'Analyze Competitor'}</button>
          <button className="btn" onClick={analyzeAll} disabled={busy}>Analyze All Known</button>
        </div>
        {toast && <div style={{ fontSize: 11, color: toast.includes('failed') ? 'var(--rose)' : 'var(--emerald)', marginTop: 4 }}>{toast}</div>}
      </div>

      {rows.length > 0 && (
        <div className="card">
          <div className="card-header">Capability matrix</div>
          <div style={{ overflowX: 'auto' }}>
            <table className="data-table">
              <thead><tr>
                <th>Competitor</th>
                {AXES.map(a => <th key={a} style={{ fontSize: 10 }}>{a}</th>)}
                <th>Overall</th><th>Weakest</th>
                <th>Conf</th><th>Open flank</th><th>Recommended attack</th>
                <th>Effort</th><th>Impact</th><th>Last analyzed</th>
                <th>Actions</th>
              </tr></thead>
              <tbody>{rows.map(r => {
                const d = r.competitor_domain;
                const rb = rowBusy[d];
                const isTracked = !!trackedSet[d];
                const eff = String(r.effort || '').toLowerCase();
                const imp = String(r.potential_impact || '').toLowerCase();
                const effColor = effortColors[eff] || 'var(--text-muted)';
                const impColor = impactColors[imp] || 'var(--text-muted)';
                return (
                  <tr key={d}>
                    <td style={{ fontWeight: 600 }}>{d}</td>
                    {AXES.map(a => {
                      const v = r[`${a}_score`] || 0;
                      return <td key={a} style={{ background: `linear-gradient(90deg, rgba(${v >= 60 ? '244,63,94' : v >= 30 ? '245,158,11' : '16,185,129'},0.2) ${v}%, transparent ${v}%)`, fontWeight: 600 }}>{Math.round(v)}</td>;
                    })}
                    <td style={{ fontWeight: 700 }}>{Math.round(r.overall_strength || 0)}</td>
                    <td><span className="badge emerald">{r.weakest_axis} ({Math.round(r.weakest_axis_score || 0)})</span></td>
                    <td><ConfidenceBadge level={r.confidence || 'estimated'} /></td>
                    <td title={r.open_flank || ''} style={{ maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 11, color: 'var(--text-secondary)' }}>
                      {truncate(r.open_flank || '—', 28)}
                    </td>
                    <td title={r.recommended_attack || ''} style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 11, color: 'var(--text-secondary)' }}>
                      {truncate(r.recommended_attack || '—', 36)}
                    </td>
                    <td>{eff ? <span style={{
                      display: 'inline-block', padding: '1px 7px', borderRadius: 9,
                      fontSize: 10, fontWeight: 600, fontFamily: 'var(--font-mono)',
                      color: effColor, background: `${effColor}22`,
                    }}>{eff}</span> : <span style={{ color: 'var(--text-muted)' }}>—</span>}</td>
                    <td>{imp ? <span style={{
                      display: 'inline-block', padding: '1px 7px', borderRadius: 9,
                      fontSize: 10, fontWeight: 600, fontFamily: 'var(--font-mono)',
                      color: impColor, background: `${impColor}22`,
                    }}>{imp}</span> : <span style={{ color: 'var(--text-muted)' }}>—</span>}</td>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>
                      {_relTime(r.last_analyzed_at)}
                    </td>
                    <td>
                      <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                        <button className="btn btn-sm" disabled={!!rb} onClick={() => analyzeRow(d)}>
                          {rb === 'analyze' ? '…' : 'Analyze'}
                        </button>
                        <button className="btn btn-sm" disabled={!!rb || isTracked} onClick={() => trackRow(d)}>
                          {rb === 'track' ? '…' : (isTracked ? 'Tracked ✓' : 'Track')}
                        </button>
                        <button className="btn btn-sm btn-primary" disabled={!!rb} onClick={() => pushAttackRow(d)}>
                          {rb === 'push' ? '…' : 'Push attack to Actions'}
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}</tbody>
            </table>
          </div>
        </div>
      )}

      {movements.length > 0 && (
        <div className="card">
          <div className="card-header">Movements (last 14 days)</div>
          {movements.slice(0, 12).map(m => (
            <div key={m.id} style={{ display: 'flex', justifyContent: 'space-between', padding: '6px 0', borderBottom: '1px solid var(--border-subtle)', fontSize: 12 }}>
              <span>{m.competitor_domain} · {m.axis}</span>
              <span style={{ color: m.delta_vs_prev > 0 ? 'var(--rose)' : 'var(--emerald)' }}>{m.delta_vs_prev > 0 ? '+' : ''}{Math.round(m.delta_vs_prev)}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// 4. Revenue Priority
function RevenuePriorityPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [summary, setSummary] = useState(null);
  const [recs, setRecs] = useState([]);
  const [busy, setBusy] = useState(false);
  const [whyPrompt, setWhyPrompt] = useState(null);

  const load = () => {
    if (!wsId) return;
    api(`/api/revenue/${wsId}/summary`, {}, token).then(r => setSummary(r.data || r)).catch(() => {});
    api(`/api/revenue/${wsId}/priority?top_n=15`, {}, token).then(r => setRecs(r.data || [])).catch(() => {});
  };
  useEffect(load, [wsId]);

  const push = async () => {
    setBusy(true);
    try { await api(`/api/revenue/${wsId}/push-recs?top_n=5`, { method: 'POST' }, token); } catch {}
    setBusy(false);
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header">Revenue Priority — money over volume</div>
        {summary && (
          <div className="metrics-grid">
            <div className="metric-card"><div className="metric-label">€ PIPELINE AT STAKE <MetricTooltip metricKey="pipeline_at_stake" /></div><div className="metric-value">€{Math.round(summary.estimated_pipeline_eur || 0).toLocaleString()}</div></div>
            <div className="metric-card"><div className="metric-label">€ WON</div><div className="metric-value" style={{ color: 'var(--emerald)' }}>€{Math.round(summary.won_eur || 0).toLocaleString()}</div></div>
            <div className="metric-card"><div className="metric-label">€ LOST</div><div className="metric-value" style={{ color: 'var(--rose)' }}>€{Math.round(summary.lost_eur || 0).toLocaleString()}</div></div>
            <div className="metric-card"><div className="metric-label">PROMPTS WON</div><div className="metric-value">{summary.won_count || 0}</div></div>
            <div className="metric-card"><div className="metric-label">PROMPTS LOST</div><div className="metric-value">{summary.lost_count || 0}</div></div>
          </div>
        )}
        <button className="btn btn-primary" onClick={push} disabled={busy} style={{ marginTop: 8 }}>
          {busy ? 'Pushing...' : 'Push top 5 to Recommendations'}
        </button>
      </div>

      <div className="card">
        <div className="card-header">Do This Next — by revenue × ownership gap</div>
        {recs.length === 0 ? <div className="empty-state">€<br/>No prompts yet.</div> : recs.map((r, i) => (
          <div key={r.prompt_id} style={{ padding: 10, borderBottom: '1px solid var(--border-subtle)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
              <div style={{ fontWeight: 600 }}>{i + 1}. {r.text}</div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <button className="btn btn-sm" onClick={() => setWhyPrompt(r.prompt_id || r.id)}>Why?</button>
                <div style={{ fontFamily: 'var(--font-mono)', color: 'var(--amber)' }}>€{Math.round(r.estimated_value_eur).toLocaleString()}</div>
              </div>
            </div>
            <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginBottom: 4 }}>
              <span className="badge">{r.buyer_stage}</span> our: {Math.round(r.our_score)}/100 · leader: {r.leader_domain || '—'} · priority: {Math.round(r.priority)}
            </div>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)' }}>→ {r.next_action}</div>
          </div>
        ))}
      </div>
      {whyPrompt && <RevenueLogicModal wsId={wsId} promptId={whyPrompt} onClose={() => setWhyPrompt(null)} />}
    </div>
  );
}

// 5. Buyer Journey
function BuyerJourneyPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [data, setData] = useState(null);     // legacy /api/journey/{wsId}
  const [cov, setCov] = useState(null);       // /api/journey/{wsId}/coverage-map
  const [insight, setInsight] = useState(null); // /api/journey/{wsId}/insight
  const [covErr, setCovErr] = useState('');
  const [insightErr, setInsightErr] = useState('');
  const [busy, setBusy] = useState(false);

  const load = () => {
    if (!wsId) return;
    api(`/api/journey/${wsId}?refresh=false`, {}, token).then(r => setData(r.data || r)).catch(() => {});
    setCovErr(''); setInsightErr('');
    api(`/api/journey/${wsId}/coverage-map`, {}, token)
      .then(r => setCov(r.data || r))
      .catch(e => setCovErr(e.message || 'Failed to load'));
    api(`/api/journey/${wsId}/insight`, {}, token)
      .then(r => setInsight(r.data || r))
      .catch(e => setInsightErr(e.message || 'Failed to load'));
  };
  useEffect(load, [wsId]);

  const refresh = async () => {
    setBusy(true);
    try { const r = await api(`/api/journey/${wsId}?refresh=true`, {}, token); setData(r.data || r); } catch {}
    setBusy(false);
    load();
  };

  const CANONICAL_6 = ['awareness','consideration','comparison','trust','decision','purchase'];
  const LEGACY_STAGES = ['awareness','problem','solution','comparison','trust','objection','decision'];
  const sevColor = { critical: 'rose', medium: 'amber', low: 'amber', none: 'emerald' };

  const priorityColor = p => {
    const v = String(p || '').toLowerCase();
    if (v === 'high') return 'rose';
    if (v === 'mid' || v === 'medium') return 'amber';
    if (v === 'low') return 'emerald';
    return 'gray';
  };

  const eur = v => `€${Math.round(Number(v) || 0).toLocaleString('en-US')}`;

  const stageRow = name => {
    const s = (cov && cov.stages && cov.stages[name]) || {};
    return {
      name,
      prompt_count: s.prompt_count ?? 0,
      owned_count: s.owned_count ?? 0,
      lost_count: s.lost_count ?? 0,
      existing_pages: s.existing_pages ?? 0,
      missing_pages: s.missing_pages ?? 0,
      revenue_at_stake: s.revenue_at_stake ?? 0,
      priority: s.priority || 'low',
      confidence: s.confidence || 'estimated',
    };
  };

  const recoLine = r => {
    if (r.lost_count > 0 && r.missing_pages > 0) {
      return `Build ${r.missing_pages} ${r.name} page${r.missing_pages === 1 ? '' : 's'}; ${eur(r.revenue_at_stake)} at stake (${r.lost_count} prompts lost).`;
    }
    if (r.lost_count > 0) {
      return `Recover ${r.lost_count} lost ${r.name} prompt${r.lost_count === 1 ? '' : 's'}; ${eur(r.revenue_at_stake)} at stake.`;
    }
    if (r.missing_pages > 0) {
      return `Add ${r.missing_pages} ${r.name} page${r.missing_pages === 1 ? '' : 's'} to widen funnel coverage.`;
    }
    if (r.prompt_count === 0) return `No ${r.name} prompts tracked yet.`;
    return `${r.name} fully covered (${r.owned_count}/${r.prompt_count} owned).`;
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>

      {/* Insight card */}
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>Buyer Journey Insight</span>
          {insight && <ConfidenceBadge level={insight.confidence || 'estimated'} />}
        </div>
        {insightErr && <div style={{ fontSize: 12, color: 'var(--rose)' }}>{insightErr}</div>}
        {!insight && !insightErr && <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading insight…</div>}
        {insight && (
          <React.Fragment>
            <div style={{ fontSize: 16, lineHeight: 1.6, color: 'var(--text-primary)', marginBottom: 12 }}>
              {insight.summary || 'No summary available.'}
            </div>
            <div style={{ display: 'grid', gap: 6 }}>
              <div style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
                <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', minWidth: 100 }}>Over-covered</span>
                {(insight.over_covered || []).length === 0
                  ? <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>—</span>
                  : insight.over_covered.map(s => <span key={s} className="badge emerald">{s}</span>)}
              </div>
              <div style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
                <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', minWidth: 100 }}>Under-covered</span>
                {(insight.under_covered || []).length === 0
                  ? <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>—</span>
                  : insight.under_covered.map(s => <span key={s} className="badge rose">{s}</span>)}
              </div>
            </div>
          </React.Fragment>
        )}
      </div>

      {/* Coverage map table — canonical 6 */}
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>Coverage map — canonical 6 stages</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {cov && <ConfidenceBadge level={cov.confidence || 'estimated'} />}
            <button className="btn btn-sm btn-primary" onClick={refresh} disabled={busy}>{busy ? 'Re-classifying...' : 'Refresh'}</button>
          </div>
        </div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>Audit your content by buyer stage. Money is in the last three stages.</p>
        {covErr && <div style={{ fontSize: 12, color: 'var(--rose)' }}>{covErr}</div>}
        {!cov && !covErr && <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading coverage map…</div>}
        {cov && (
          <table className="data-table" style={{ fontSize: 12 }}>
            <thead>
              <tr>
                <th>Stage<MetricTooltip metricKey="buyer_journey_stage" /></th>
                <th style={{ textAlign: 'right' }}>Prompts</th>
                <th style={{ textAlign: 'right' }}>Owned</th>
                <th style={{ textAlign: 'right' }}>Lost</th>
                <th style={{ textAlign: 'right' }}>Existing Pages</th>
                <th style={{ textAlign: 'right' }}>Missing Pages</th>
                <th style={{ textAlign: 'right' }}>Revenue at Stake</th>
                <th>Priority</th>
                <th>Conf.</th>
              </tr>
            </thead>
            <tbody>
              {CANONICAL_6.map(stageRow).map(r => (
                <tr key={r.name}>
                  <td style={{ textTransform: 'capitalize', fontWeight: 600 }}>{r.name}</td>
                  <td style={{ textAlign: 'right', fontFamily: 'var(--font-mono)' }}>{r.prompt_count}</td>
                  <td style={{ textAlign: 'right', color: 'var(--emerald)', fontFamily: 'var(--font-mono)' }}>{r.owned_count}</td>
                  <td style={{ textAlign: 'right', color: r.lost_count > 0 ? 'var(--rose)' : 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>{r.lost_count}</td>
                  <td style={{ textAlign: 'right', fontFamily: 'var(--font-mono)' }}>{r.existing_pages}</td>
                  <td style={{ textAlign: 'right', color: r.missing_pages > 0 ? 'var(--amber)' : 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>{r.missing_pages}</td>
                  <td style={{ textAlign: 'right', color: 'var(--amber)', fontWeight: 600, fontFamily: 'var(--font-mono)' }}>{eur(r.revenue_at_stake)}</td>
                  <td><span className={`badge ${priorityColor(r.priority)}`}>{r.priority}</span></td>
                  <td><ConfidenceBadge level={r.confidence} /></td>
                </tr>
              ))}
              {cov.totals && (
                <tr style={{ borderTop: '2px solid var(--border-default)', background: 'var(--bg-raised)' }}>
                  <td style={{ fontWeight: 700 }}>Total</td>
                  <td style={{ textAlign: 'right', fontFamily: 'var(--font-mono)', fontWeight: 700 }}>{cov.totals.prompt_count ?? '—'}</td>
                  <td style={{ textAlign: 'right', fontFamily: 'var(--font-mono)', fontWeight: 700, color: 'var(--emerald)' }}>{cov.totals.owned_count ?? '—'}</td>
                  <td style={{ textAlign: 'right', fontFamily: 'var(--font-mono)', fontWeight: 700, color: 'var(--rose)' }}>{cov.totals.lost_count ?? '—'}</td>
                  <td style={{ textAlign: 'right', fontFamily: 'var(--font-mono)', fontWeight: 700 }}>{cov.totals.existing_pages ?? '—'}</td>
                  <td style={{ textAlign: 'right', fontFamily: 'var(--font-mono)', fontWeight: 700 }}>{cov.totals.missing_pages ?? '—'}</td>
                  <td style={{ textAlign: 'right', fontFamily: 'var(--font-mono)', fontWeight: 700, color: 'var(--amber)' }}>{cov.totals.revenue_at_stake != null ? eur(cov.totals.revenue_at_stake) : '—'}</td>
                  <td></td>
                  <td></td>
                </tr>
              )}
            </tbody>
          </table>
        )}
      </div>

      {/* Per-stage recommended actions */}
      {cov && (
        <div className="card">
          <div className="card-header">Per-stage recommended actions</div>
          <ol style={{ paddingLeft: 18, fontSize: 12, display: 'grid', gap: 6, margin: 0 }}>
            {CANONICAL_6.map(stageRow).map(r => (
              <li key={r.name} style={{ lineHeight: 1.5 }}>
                <span style={{ fontWeight: 600, textTransform: 'capitalize' }}>{r.name}:</span>{' '}
                <span style={{ color: 'var(--text-secondary)' }}>{recoLine(r)}</span>{' '}
                <span className={`badge ${priorityColor(r.priority)}`} style={{ marginLeft: 4 }}>{r.priority}</span>
              </li>
            ))}
          </ol>
        </div>
      )}

      {/* Legacy: keep old per-stage detail accessible if present */}
      {data && (data.stages || data.recommendation) && (
        <div className="card">
          <div className="card-header">Legacy stage detail (Phase 1)</div>
          {data && (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 6, marginBottom: 10 }}>
              {LEGACY_STAGES.map(s => {
                const x = (data.stages || {})[s] || {};
                const cv = x.coverage_score || 0;
                return (
                  <div key={s} className="card" style={{ padding: 10, textAlign: 'center' }}>
                    <div style={{ fontSize: 10, textTransform: 'uppercase', color: 'var(--text-muted)' }}>{s}</div>
                    <div style={{ fontSize: 22, fontWeight: 700, color: cv >= 60 ? 'var(--emerald)' : cv >= 30 ? 'var(--amber)' : 'var(--rose)' }}>{Math.round(cv)}</div>
                    <div style={{ fontSize: 10 }}>{x.page_count || 0} pages</div>
                    <div style={{ marginTop: 4 }}><span className={`badge ${sevColor[x.gap_severity] || 'gray'}`}>{x.gap_severity || 'none'}</span></div>
                  </div>
                );
              })}
            </div>
          )}
          {data && Object.entries(data.stages || {}).map(([s, x]) => (
            <div key={s} style={{ padding: '6px 0', borderBottom: '1px solid var(--border-subtle)' }}>
              <div style={{ fontWeight: 600, fontSize: 12, textTransform: 'capitalize' }}>{s} <span className="badge">{x.page_count} pages · {x.prompt_count} prompts</span></div>
              {x.recommendation && <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginTop: 3 }}>{x.recommendation}</div>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// 6. Reddit Command Center

const _RISK_COLORS = { low: 'emerald', med: 'amber', medium: 'amber', high: 'rose' };

function CommunityOpportunitiesPanel({ wsId }) {
  const { token } = useContext(AuthContext);
  const [data, setData] = useState(null);
  const [err, setErr] = useState('');

  useEffect(() => {
    if (!wsId) return;
    api(`/api/community/${wsId}/opportunities`, {}, token)
      .then(r => setData(r.data || r))
      .catch(e => setErr(e.message || 'Failed to load'));
  }, [wsId, token]);

  const opps = data?.opportunities || [];
  const disclaimer = data?.disclaimer;

  return (
    <div className="card" style={{ display: 'grid', gap: 12 }}>
      <div className="card-header">Community Opportunity Finder</div>

      {disclaimer && (
        <div style={{ border: '1px solid var(--amber)', background: 'rgba(245, 158, 11, 0.08)', borderRadius: 6, padding: '8px 10px', fontSize: 11, color: 'var(--amber)', display: 'flex', gap: 8, alignItems: 'flex-start' }}>
          <span style={{ fontSize: 13 }}>⚠</span>
          <span>{disclaimer}</span>
        </div>
      )}

      {err && <div style={{ fontSize: 11, color: 'var(--rose)' }}>{err}</div>}

      {opps.length === 0 ? (
        <div className="empty-state">☴<br/>No community opportunities yet. Harvest Reddit data first.</div>
      ) : opps.map((opp, i) => {
        const risk = String(opp.risk_level || '').toLowerCase();
        const riskCls = _RISK_COLORS[risk] || 'gray';
        return (
          <div key={i} style={{ border: '1px solid var(--border-subtle)', borderRadius: 6, padding: 12, background: 'var(--bg-raised)', display: 'grid', gap: 8 }}>
            <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
              <span className="badge purple">r/{opp.subreddit}</span>
              {opp.topic && <span style={{ fontWeight: 600 }}>{opp.topic}</span>}
              {risk && <span className={`badge ${riskCls}`}>risk: {opp.risk_level}</span>}
            </div>
            {opp.user_question && (
              <div style={{ fontStyle: 'italic', fontSize: 12, color: 'var(--text-secondary)' }}>"{opp.user_question}"</div>
            )}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 10 }}>
              <div>
                <div style={{ fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>Suggested content angle</div>
                <div style={{ fontSize: 12 }}>{opp.suggested_content_angle || '—'}</div>
              </div>
              <div>
                <div style={{ fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>Possible FAQ</div>
                {opp.possible_faq ? (
                  typeof opp.possible_faq === 'object' && (opp.possible_faq.q || opp.possible_faq.question) ? (
                    <div style={{ fontSize: 12 }}>
                      <div style={{ fontWeight: 700 }}>{opp.possible_faq.q || opp.possible_faq.question}</div>
                      <div style={{ color: 'var(--text-secondary)' }}>{opp.possible_faq.a || opp.possible_faq.answer || ''}</div>
                    </div>
                  ) : (
                    <div style={{ fontSize: 12, whiteSpace: 'pre-wrap' }}>{typeof opp.possible_faq === 'string' ? opp.possible_faq : JSON.stringify(opp.possible_faq)}</div>
                  )
                ) : <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>—</div>}
              </div>
              <div>
                <div style={{ fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>Intent</div>
                {opp.intent ? <span className="badge gray">{opp.intent}</span> : <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>—</span>}
              </div>
              <div>
                <div style={{ fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 3 }}>Recommended action</div>
                <div style={{ fontSize: 12 }}>{opp.recommended_action || '—'}</div>
              </div>
            </div>
            <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
              <ConfidenceBadge level={opp.confidence || 'estimated'} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

function RedditCommandPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [cc, setCc] = useState(null);
  const [intel, setIntel] = useState([]);
  const [onlyGaps, setOnlyGaps] = useState(true);
  const [busy, setBusy] = useState(false);
  const [subsText, setSubsText] = useState('plasticsurgery, AskDocs, hungary');
  const [msg, setMsg] = useState('');

  const load = () => {
    if (!wsId) return;
    api(`/api/reddit/${wsId}/command-center`, {}, token).then(r => setCc(r.data || r)).catch(() => {});
    api(`/api/reddit/${wsId}?only_gaps=${onlyGaps}`, {}, token).then(r => setIntel(r.data || [])).catch(() => {});
  };
  useEffect(load, [wsId, onlyGaps]);

  const harvest = async () => {
    setBusy(true); setMsg('');
    try { const r = await api(`/api/reddit/${wsId}/harvest`, { method: 'POST' }, token); const d = r.data || r; setMsg(`Harvested ${d.threads || 0} threads from existing Peec citations.`); load(); }
    catch (e) { setMsg('Harvest failed: ' + e.message); }
    setBusy(false);
  };

  const harvestSubs = async () => {
    const subs = subsText.split(',').map(s => s.trim()).filter(Boolean);
    if (subs.length === 0) return;
    setBusy(true); setMsg('');
    try {
      const r = await api(`/api/reddit/${wsId}/harvest-subreddits`, { method: 'POST', body: JSON.stringify({ subreddits: subs }) }, token);
      const d = r.data || r;
      setMsg(`Pulled ${d.threads || 0} hot threads across ${(d.subreddits || []).length} subreddits. ${d.opportunity_gaps || 0} opportunity gaps.`);
      load();
    } catch (e) { setMsg('Manual harvest failed: ' + e.message); }
    setBusy(false);
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <CommunityOpportunitiesPanel wsId={wsId} />
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>Reddit Command Center</span>
          <button className="btn btn-sm btn-primary" onClick={harvest} disabled={busy}>{busy ? 'Harvesting...' : 'Harvest from Peec citations'}</button>
        </div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>AI weighs Reddit as authority. Find threads where competitors are mentioned and you are not.</p>
        <div style={{ display: 'flex', gap: 6, marginTop: 8, marginBottom: 4 }}>
          <input className="form-input" placeholder="comma-separated subreddits (e.g. budapest, hungary, plasticsurgery)"
            value={subsText} onChange={e => setSubsText(e.target.value)} style={{ flex: 1 }} />
          <button className="btn" onClick={harvestSubs} disabled={busy}>Pull hot threads</button>
        </div>
        {msg && <div style={{ fontSize: 11, color: msg.includes('failed') ? 'var(--rose)' : 'var(--emerald)' }}>{msg}</div>}
        {cc && (
          <div className="metrics-grid" style={{ marginTop: 8 }}>
            <div className="metric-card"><div className="metric-label">THREADS TRACKED</div><div className="metric-value">{cc.thread_count || 0}</div></div>
            <div className="metric-card"><div className="metric-label">SUBREDDITS</div><div className="metric-value">{Object.keys(cc.subreddits || {}).length}</div></div>
            <div className="metric-card"><div className="metric-label">OPPORTUNITY GAPS</div><div className="metric-value" style={{ color: 'var(--rose)' }}>{cc.opportunity_gaps?.length || 0}</div></div>
          </div>
        )}
      </div>

      {cc?.top_brands?.length > 0 && (
        <div className="card">
          <div className="card-header">Top brands across Reddit</div>
          {cc.top_brands.map(b => (
            <div key={b.brand} style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', fontSize: 12 }}>
              <span>{b.brand}</span><span className="badge">{b.mentions}</span>
            </div>
          ))}
        </div>
      )}

      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>Threads ({intel.length})</span>
          <label style={{ fontSize: 12 }}><input type="checkbox" checked={onlyGaps} onChange={e => setOnlyGaps(e.target.checked)} /> only opportunity gaps</label>
        </div>
        {intel.length === 0 ? <div className="empty-state">☴<br/>No Reddit data yet. Harvest first.</div> : intel.map(r => (
          <div key={r.id} style={{ padding: 8, borderBottom: '1px solid var(--border-subtle)', fontSize: 12 }}>
            <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginBottom: 4 }}>
              <span className="badge purple">r/{r.subreddit}</span>
              <span className="badge">{r.discussion_type}</span>
              {r.is_opportunity_gap ? <span className="badge rose">gap</span> : <span className="badge emerald">we are present</span>}
              {r.sentiment_label && <span className={`badge ${r.sentiment_label === 'positive' ? 'emerald' : r.sentiment_label === 'negative' ? 'rose' : 'gray'}`}>{r.sentiment_label}</span>}
            </div>
            <div style={{ fontWeight: 600 }}>{r.thread_title || r.thread_url}</div>
            <a href={r.thread_url} target="_blank" rel="noreferrer" style={{ fontSize: 11, color: 'var(--blue)', wordBreak: 'break-all' }}>{r.thread_url}</a>
            <div style={{ marginTop: 4, color: 'var(--text-secondary)' }}>→ {r.suggested_action}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

// 7. Schema Engine
function SchemaEnginePage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [audits, setAudits] = useState([]);
  const [url, setUrl] = useState('');
  const [busy, setBusy] = useState(false);

  const load = () => {
    if (!wsId) return;
    api(`/api/schema/${wsId}`, {}, token).then(r => setAudits(r.data || [])).catch(() => {});
  };
  useEffect(load, [wsId]);

  const auditOne = async () => {
    if (!url.trim()) return;
    setBusy(true);
    try { await api(`/api/schema/${wsId}/audit`, { method: 'POST', body: JSON.stringify({ url }) }, token); setUrl(''); load(); } catch {}
    setBusy(false);
  };
  const auditWs = async () => {
    setBusy(true);
    try { await api(`/api/schema/${wsId}/audit-workspace?top_n=10`, { method: 'POST' }, token); load(); } catch {}
    setBusy(false);
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header">Schema Opportunity Engine — diagnosis, not generator</div>
        <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
          <input className="form-input" placeholder="https://example.com/page-to-audit" value={url} onChange={e => setUrl(e.target.value)} style={{ flex: 1 }} />
          <button className="btn btn-primary" onClick={auditOne} disabled={busy}>Audit URL</button>
          <button className="btn" onClick={auditWs} disabled={busy}>Audit top workspace pages</button>
        </div>
      </div>
      {audits.length === 0 ? <div className="empty-state">⌬<br/>No audits yet.</div> : audits.map(a => {
        let types = []; let missing = []; let recs = [];
        try { types = typeof a.schema_types === 'string' ? JSON.parse(a.schema_types) : (a.schema_types || []); } catch {}
        try { missing = typeof a.missing_critical === 'string' ? JSON.parse(a.missing_critical) : (a.missing_critical || []); } catch {}
        try { recs = typeof a.recommendations === 'string' ? JSON.parse(a.recommendations) : (a.recommendations || []); } catch {}
        return (
          <div key={a.id} className="card">
            <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{a.page_url}</span>
              <span>Depth {Math.round(a.schema_depth_score || 0)}/100 {a.is_competitor ? <span className="badge rose">competitor</span> : <span className="badge emerald">us</span>}</span>
            </div>
            <div style={{ fontSize: 11, marginBottom: 6 }}>Present: {types.length ? types.map(t => <span key={t} className="badge emerald" style={{ marginRight: 4 }}>{t}</span>) : '—'}</div>
            <div style={{ fontSize: 11, marginBottom: 6 }}>Missing: {missing.slice(0, 8).map(m => <span key={m.type} className={`badge ${m.severity === 'high' ? 'rose' : m.severity === 'medium' ? 'amber' : 'gray'}`} style={{ marginRight: 4 }} title={m.why_it_hurts}>{m.type}</span>)}</div>
            {a.diagnosis && <div style={{ fontSize: 13, marginBottom: 6 }}>{a.diagnosis}</div>}
            {recs.length > 0 && (
              <ol style={{ fontSize: 12, paddingLeft: 18, color: 'var(--text-secondary)' }}>
                {recs.map((r, i) => <li key={i}>{r.step || JSON.stringify(r)}</li>)}
              </ol>
            )}
          </div>
        );
      })}
    </div>
  );
}

// 8. AIO Overview
function AioOverviewPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [ov, setOv] = useState(null);
  const [losses, setLosses] = useState([]);
  const [busy, setBusy] = useState(false);

  const load = () => {
    if (!wsId) return;
    api(`/api/aio/${wsId}`, {}, token).then(r => setOv(r.data || r)).catch(() => {});
    api(`/api/aio/${wsId}/losses?limit=50`, {}, token).then(r => setLosses(r.data || [])).catch(() => {});
  };
  useEffect(load, [wsId]);

  const detect = async () => {
    setBusy(true);
    try { await api(`/api/aio/${wsId}/detect-movements`, { method: 'POST' }, token); load(); } catch {}
    setBusy(false);
  };

  const trackAll = async () => {
    setBusy(true);
    try { await api(`/api/aio/${wsId}/track-all?max_prompts=30`, { method: 'POST' }, token); load(); } catch {}
    setBusy(false);
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      {ov && !ov.tracker_configured && (
        <div className="card" style={{ borderLeft: '3px solid var(--amber)' }}>
          <div style={{ padding: 12, fontSize: 12, color: 'var(--amber)' }}>
            ⚠ Live AIO tracking is off: <code>SERPAPI_KEY</code> is not set. Add it in Railway → Variables to populate this page automatically. Without it, AIO observations only come from manual prompt tracks.
          </div>
        </div>
      )}
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>Google AI Overview Tracker</span>
          <div style={{ display: 'flex', gap: 6 }}>
            <button className="btn btn-sm btn-primary" onClick={trackAll} disabled={busy || !ov?.tracker_configured}>Track All High-Value</button>
            <button className="btn btn-sm" onClick={detect} disabled={busy}>Detect Movements</button>
          </div>
        </div>
        {ov && (
          <div className="metrics-grid" style={{ marginTop: 8 }}>
            <div className="metric-card"><div className="metric-label">PROMPTS TRACKED</div><div className="metric-value">{ov.tracked_prompts || 0}</div></div>
            <div className="metric-card"><div className="metric-label">WITH AIO</div><div className="metric-value">{ov.with_aio || 0}</div></div>
            <div className="metric-card"><div className="metric-label">US IN AIO</div><div className="metric-value" style={{ color: 'var(--emerald)' }}>{ov.with_our_brand || 0}</div></div>
            <div className="metric-card"><div className="metric-label">WITHOUT US</div><div className="metric-value" style={{ color: 'var(--rose)' }}>{ov.without_us || 0}</div></div>
          </div>
        )}
      </div>

      {ov?.top_publishers?.length > 0 && (
        <div className="card"><div className="card-header">Dominant publishers</div>
          {ov.top_publishers.map(p => <div key={p.domain} style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', fontSize: 12 }}><span>{p.domain}</span><span className="badge">{p.appearances}</span></div>)}
        </div>
      )}

      <div className="card">
        <div className="card-header">Losses — AIO present, our brand absent</div>
        {losses.length === 0 ? <div className="empty-state">◈<br/>No losses logged.</div> : losses.map(l => (
          <div key={l.id} style={{ padding: 8, borderBottom: '1px solid var(--border-subtle)', fontSize: 12 }}>
            <div style={{ fontWeight: 600 }}>{l.prompt_text}</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{new Date(l.observed_at).toLocaleString()} · stage {l.buyer_stage} · revenue {Math.round(l.revenue_score || 0)}</div>
            {l.delta_vs_prev && <div style={{ color: 'var(--amber)' }}>{l.delta_vs_prev}</div>}
          </div>
        ))}
      </div>
    </div>
  );
}

// 9. Metadata Studio
function MetadataStudioPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [pkgs, setPkgs] = useState([]);
  const [url, setUrl] = useState('');
  const [audit, setAudit] = useState(false);
  const [busy, setBusy] = useState(false);

  const load = () => {
    if (!wsId) return;
    api(`/api/metadata/${wsId}`, {}, token).then(r => setPkgs(r.data || [])).catch(() => {});
  };
  useEffect(load, [wsId]);

  const run = async () => {
    if (!url.trim()) return;
    setBusy(true);
    try { await api(`/api/metadata/${wsId}/url`, { method: 'POST', body: JSON.stringify({ url, audit }) }, token); setUrl(''); load(); } catch {}
    setBusy(false);
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header">Metadata + Snippet Studio</div>
        <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
          <input className="form-input" placeholder="https://example.com/page" value={url} onChange={e => setUrl(e.target.value)} style={{ flex: 1 }} />
          <label style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 12 }}><input type="checkbox" checked={audit} onChange={e => setAudit(e.target.checked)} /> audit existing</label>
          <button className="btn btn-primary" onClick={run} disabled={busy}>{audit ? 'Audit' : 'Generate'}</button>
        </div>
      </div>
      {pkgs.length === 0 ? <div className="empty-state">▧<br/>No metadata packages yet.</div> : pkgs.map(p => (
        <div key={p.id} className="card">
          <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{p.page_url || p.draft_id}</span>
            <span>AIO score {Math.round(p.aio_compatibility_score || 0)}/100</span>
          </div>
          <div style={{ fontSize: 12, marginBottom: 4 }}><b>Title:</b> {p.seo_title}</div>
          <div style={{ fontSize: 12, marginBottom: 4 }}><b>Meta:</b> {p.meta_description}</div>
          <div style={{ fontSize: 12, marginBottom: 4 }}><b>Snippet target:</b> {p.snippet_target}</div>
          {(() => {
            try {
              const faqs = typeof p.faq_extractions === 'string' ? JSON.parse(p.faq_extractions) : (p.faq_extractions || []);
              return faqs.length ? <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}><b>FAQs:</b> {faqs.map(f => f.q).join(' · ')}</div> : null;
            } catch { return null; }
          })()}
        </div>
      ))}
    </div>
  );
}

// 10. Authority Score
function AuthorityScorePage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [latest, setLatest] = useState([]);
  const [busy, setBusy] = useState(false);

  const load = () => {
    if (!wsId) return;
    api(`/api/authority/${wsId}/latest`, {}, token).then(r => setLatest((r.data || r).scores || [])).catch(() => {});
  };
  useEffect(load, [wsId]);

  const rebuild = async () => {
    setBusy(true);
    try { await api(`/api/authority/${wsId}/rebuild`, { method: 'POST' }, token); load(); } catch {}
    setBusy(false);
  };
  const compute = async () => {
    setBusy(true);
    try { await api(`/api/authority/${wsId}/compute`, { method: 'POST' }, token); load(); } catch {}
    setBusy(false);
  };

  const SUBS = ['citation_score','prompt_ownership_score','schema_score','offsite_score','reddit_score','entity_score','local_score'];

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>GEO Authority Score™ <MetricTooltip metricKey="authority_score" /></span>
          <div style={{ display: 'flex', gap: 6 }}>
            <button className="btn btn-sm btn-primary" onClick={compute} disabled={busy}>Compute Us</button>
            <button className="btn btn-sm" onClick={rebuild} disabled={busy}>Rebuild All (incl. competitors)</button>
          </div>
        </div>
      </div>
      <ScoreBreakdownCard wsId={wsId} />
      <CitationBreakdownCard wsId={wsId} />
      {latest.length === 0 ? <div className="empty-state">★<br/>No score yet. Click Compute.</div> : latest.map(s => (
        <div key={s.id} className="card">
          <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span>{s.subject_domain} {s.is_us ? <span className="badge emerald">us</span> : <span className="badge gray">competitor</span>}</span>
            <span style={{ fontSize: 24, fontWeight: 700, color: s.total_score >= 70 ? 'var(--emerald)' : s.total_score >= 40 ? 'var(--amber)' : 'var(--rose)' }}>{Math.round(s.total_score)}/100</span>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 6 }}>
            {SUBS.map(k => {
              const v = s[k] || 0;
              return (
                <div key={k} style={{ padding: 8, background: 'var(--bg-subtle)', borderRadius: 4, textAlign: 'center' }}>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase' }}>{k.replace('_score','').replace(/_/g, ' ')}</div>
                  <div style={{ fontSize: 16, fontWeight: 700, color: v >= 60 ? 'var(--emerald)' : v >= 30 ? 'var(--amber)' : 'var(--rose)' }}>{Math.round(v)}</div>
                </div>
              );
            })}
          </div>
          {s.rationale && (() => {
            try {
              const r = typeof s.rationale === 'string' ? JSON.parse(s.rationale) : s.rationale;
              return <div style={{ marginTop: 8, fontSize: 12, color: 'var(--text-secondary)' }}>Biggest lever: <b>{r.biggest_lever}</b> — {r.summary}</div>;
            } catch { return null; }
          })()}
        </div>
      ))}
    </div>
  );
}

// 11. YouTube GEO
function YouTubeGeoPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [assets, setAssets] = useState([]);
  const [topic, setTopic] = useState('');
  const [expert, setExpert] = useState('');
  const [service, setService] = useState('');
  const [goal, setGoal] = useState('trust');
  const [auditUrl, setAuditUrl] = useState('');
  const [busy, setBusy] = useState(false);

  const load = () => {
    if (!wsId) return;
    api(`/api/youtube/${wsId}`, {}, token).then(r => setAssets(r.data || [])).catch(() => {});
  };
  useEffect(load, [wsId]);

  const generate = async () => {
    if (!topic.trim()) return;
    setBusy(true);
    try {
      await api(`/api/youtube/${wsId}/generate`, { method: 'POST', body: JSON.stringify({ topic, expert_name: expert, connected_service: service, goal }) }, token);
      setTopic(''); load();
    } catch {}
    setBusy(false);
  };
  const audit = async () => {
    if (!auditUrl.trim()) return;
    setBusy(true);
    try { await api(`/api/youtube/${wsId}/audit`, { method: 'POST', body: JSON.stringify({ video_url: auditUrl, goal }) }, token); setAuditUrl(''); load(); } catch {}
    setBusy(false);
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header">YouTube GEO Optimizer</div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>Engineer videos as AI authority assets — title, description, chapters, FAQs, embed strategy, VideoObject schema.</p>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 6, marginBottom: 6 }}>
          <input className="form-input" placeholder="Topic" value={topic} onChange={e => setTopic(e.target.value)} />
          <input className="form-input" placeholder="Expert name" value={expert} onChange={e => setExpert(e.target.value)} />
          <input className="form-input" placeholder="Connected service" value={service} onChange={e => setService(e.target.value)} />
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          <select className="form-input" style={{ width: 'auto', fontSize: 11 }} value={goal} onChange={e => setGoal(e.target.value)}>
            {['trust','lead-gen','FAQ','comparison'].map(g => <option key={g} value={g}>{g}</option>)}
          </select>
          <button className="btn btn-primary" onClick={generate} disabled={busy}>Generate Package</button>
        </div>
        <div style={{ display: 'flex', gap: 6, marginTop: 8 }}>
          <input className="form-input" placeholder="Audit existing https://youtube.com/..." value={auditUrl} onChange={e => setAuditUrl(e.target.value)} style={{ flex: 1 }} />
          <button className="btn" onClick={audit} disabled={busy}>Audit existing</button>
        </div>
      </div>
      {assets.length === 0 ? <div className="empty-state">▶<br/>No assets yet.</div> : assets.map(a => (
        <div key={a.id} className="card">
          <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span>{a.optimized_title || a.topic}</span>
            <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{a.goal} · {new Date(a.generated_at).toLocaleDateString()}</span>
          </div>
          {a.video_url && <div style={{ fontSize: 11, color: 'var(--blue)', marginBottom: 6 }}>{a.video_url}</div>}
          <div style={{ fontSize: 12, marginBottom: 6, whiteSpace: 'pre-wrap' }}>{a.description}</div>
          {(() => {
            try {
              const ch = typeof a.chapters === 'string' ? JSON.parse(a.chapters) : (a.chapters || []);
              return ch.length ? (
                <div style={{ fontSize: 11, marginBottom: 6 }}>
                  <b>Chapters:</b><br/>
                  {ch.map((c, i) => <div key={i} style={{ fontFamily: 'var(--font-mono)' }}>{c.ts} {c.title}</div>)}
                </div>
              ) : null;
            } catch { return null; }
          })()}
          {a.embed_strategy && <div style={{ fontSize: 12, color: 'var(--text-secondary)' }}><b>Embed:</b> {a.embed_strategy}</div>}
        </div>
      ))}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// WAR ROOM — live landing
// ═══════════════════════════════════════════════════════════════

function WarRoomPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [bf, setBf] = useState(null);
  const [rev, setRev] = useState(null);
  const [auth, setAuth] = useState([]);
  const [recent, setRecent] = useState([]);
  const [dist, setDist] = useState(null);
  const [topDom, setTopDom] = useState([]);
  const [busy, setBusy] = useState(false);

  const load = () => {
    if (!wsId) return;
    api(`/api/prompts/${wsId}/battlefield`, {}, token).then(r => setBf(r.data || r)).catch(() => {});
    api(`/api/revenue/${wsId}/summary`, {}, token).then(r => setRev(r.data || r)).catch(() => {});
    api(`/api/authority/${wsId}/latest`, {}, token).then(r => setAuth((r.data || r).scores || [])).catch(() => {});
    api(`/api/alerts/${wsId}/events?limit=15`, {}, token).then(r => setRecent(r.data || [])).catch(() => {});
    api(`/api/ownership/${wsId}/distribution`, {}, token).then(r => setDist(r.data || r)).catch(() => {});
    api(`/api/dominators/${wsId}?limit=3`, {}, token).then(r => setTopDom(r.data || r || [])).catch(() => {});
  };
  useEffect(load, [wsId]);
  useEffect(() => {
    if (!wsId) return;
    const t = setInterval(load, 15000);  // live-ish refresh every 15s
    return () => clearInterval(t);
  }, [wsId]);

  const runEval = async () => {
    setBusy(true);
    try { await api(`/api/alerts/${wsId}/evaluate`, { method: 'POST' }, token); load(); } catch {}
    setBusy(false);
  };

  const us = auth.find(s => s.is_us) || auth[0];
  const totalScore = us ? Math.round(us.total_score || 0) : null;

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card" style={{ background: 'linear-gradient(135deg, rgba(99,102,241,0.08), rgba(16,185,129,0.04))' }}>
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ fontSize: 18, fontWeight: 700 }}>Live GEO War Room</span>
          <button className="btn btn-sm" onClick={runEval} disabled={busy}>{busy ? 'Evaluating...' : 'Run Alert Sweep'}</button>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
          <div className="metric-card" style={{ borderTop: '2px solid var(--emerald)' }}>
            <div className="metric-label">AUTHORITY SCORE</div>
            <div className="metric-value">{totalScore != null ? `${totalScore}/100` : '—'}</div>
            <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>{us?.subject_domain || 'not computed'}</div>
          </div>
          <div className="metric-card" style={{ borderTop: '2px solid var(--blue)' }}>
            <div className="metric-label">PROMPTS OWNED</div>
            <div className="metric-value">{bf?.owned ?? '—'} <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>/ {bf?.high_value_prompts ?? 0}</span></div>
          </div>
          <div className="metric-card" style={{ borderTop: '2px solid var(--rose)' }}>
            <div className="metric-label">PROMPTS LOST</div>
            <div className="metric-value" style={{ color: 'var(--rose)' }}>{bf?.lost ?? '—'}</div>
          </div>
          <div className="metric-card" style={{ borderTop: '2px solid var(--amber)' }}>
            <div className="metric-label">€ PIPELINE AT STAKE</div>
            <div className="metric-value">€{Math.round((rev?.estimated_pipeline_eur || 0)).toLocaleString()}</div>
            <div style={{ fontSize: 10, color: 'var(--emerald)' }}>€{Math.round(rev?.won_eur || 0).toLocaleString()} won</div>
          </div>
        </div>
        {topDom.length > 0 && (
          <div style={{ marginTop: 10, display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Top 3 dominators</span>
            {topDom.slice(0, 3).map(d => (
              <span key={d.domain} className="badge rose">{d.domain} · {Math.round(Number(d.dominator_score) || 0)}</span>
            ))}
          </div>
        )}
      </div>

      <div className="card">
        <div className="card-header">Ownership distribution</div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {[
            { key: 'not_visible', label: 'Not visible' },
            { key: 'mentioned', label: 'Mentioned' },
            { key: 'listed', label: 'Listed' },
            { key: 'top3', label: 'Top 3' },
            { key: 'recommended', label: 'Recommended' },
            { key: 'owned', label: 'Owned' },
            { key: 'co_owned', label: 'Co-owned' },
            { key: 'lost', label: 'Lost' },
            { key: 'volatile', label: 'Volatile' },
            { key: 'emerging', label: 'Emerging' },
          ].map(c => (
            <div key={c.key} className="metric-card" style={{ minWidth: 92, padding: 8, textAlign: 'center' }}>
              <div className="metric-label" style={{ fontSize: 9 }}>{c.label}</div>
              <div className="metric-value" style={{ fontSize: 18 }}>{(dist && (dist[c.key] ?? 0)) || 0}</div>
            </div>
          ))}
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 16 }}>
        <DominatorListCard wsId={wsId} limit={10} expandable={true} />
        <div className="card">
          <div className="card-header">Recent alerts</div>
          {recent.length === 0 ? (
            <div className="empty-state" style={{ fontSize: 11 }}>⚠<br/>No alerts. Configure rules in Alerts.</div>
          ) : (recent.slice(0, 8).map(a => (
            <div key={a.id} style={{ padding: '6px 0', borderBottom: '1px solid var(--border-subtle)', fontSize: 11 }}>
              <div style={{ fontWeight: 600 }}>{a.title}</div>
              <div style={{ color: 'var(--text-muted)' }}>{new Date(a.fired_at).toLocaleString()} · <span className={`badge ${a.severity === 'critical' ? 'rose' : a.severity === 'warning' ? 'amber' : 'gray'}`}>{a.severity}</span></div>
            </div>
          )))}
        </div>
      </div>

      <TrackingLogPanel wsId={wsId} onRefresh={load} />
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// METRIC DICTIONARY
// ═══════════════════════════════════════════════════════════════

function MetricDictionaryPage({ state }) {
  const { token } = useContext(AuthContext);
  const [metrics, setMetrics] = useState(null);
  const [error, setError] = useState('');
  const [q, setQ] = useState('');

  useEffect(() => {
    let cancelled = false;
    api(`/api/metrics/dictionary`, {}, token)
      .then(r => { if (!cancelled) setMetrics(r.data || r || []); })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load'); });
    return () => { cancelled = true; };
  }, [token]);

  const loading = metrics === null && !error;
  const filtered = (metrics || []).filter(m => {
    if (!q.trim()) return true;
    const hay = `${m.name} ${m.definition} ${m.metric_key}`.toLowerCase();
    return hay.includes(q.trim().toLowerCase());
  });

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>Metric Dictionary — what every number means</span>
          {metrics && <span className="badge blue">{metrics.length} metrics</span>}
        </div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          Plain-English definitions, formulas, data sources and the action to take when a score is low.
        </p>
        <input className="form-input" placeholder="Search metrics…" value={q} onChange={e => setQ(e.target.value)} style={{ marginTop: 8 }} />
      </div>

      {loading && <div className="card"><div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Loading metrics…</div></div>}
      {error && <div className="card"><div style={{ color: 'var(--rose)', fontSize: 12 }}>{error}</div></div>}
      {metrics && filtered.length === 0 && !loading && (
        <div className="empty-state">{'?'}<br/>No metrics match “{q}”.</div>
      )}

      {filtered.map(m => (
        <div key={m.metric_key} className="card">
          <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span>{m.name}</span>
            <ConfidenceBadge level={m.confidence} />
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.55 }}>{m.definition}</div>
          {m.formula && (
            <code style={{ display: 'block', marginTop: 8, fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--cyan)', background: 'var(--bg-raised)', padding: '6px 8px', borderRadius: 4, whiteSpace: 'pre-wrap' }}>{m.formula}</code>
          )}
          {(m.data_sources || []).length > 0 && (
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 8 }}>
              {m.data_sources.map(s => <span key={s} className="badge">{s}</span>)}
              {m.refresh_frequency && <span className="badge purple">{m.refresh_frequency}</span>}
            </div>
          )}
          {m.low_score_meaning && (
            <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 8 }}>
              <b style={{ color: 'var(--text-secondary)' }}>Low score:</b> {m.low_score_meaning}
            </div>
          )}
          {m.recommended_action && (
            <div style={{ fontSize: 12, color: 'var(--text-primary)', marginTop: 6, borderLeft: '2px solid var(--emerald)', paddingLeft: 10 }}>
              → {m.recommended_action}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// AUTHORITY GRAPH viz
// ═══════════════════════════════════════════════════════════════

function AuthorityGraphPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [summary, setSummary] = useState(null);
  const [graph, setGraph] = useState(null);
  const [insight, setInsight] = useState(null);
  const [busy, setBusy] = useState(false);
  const [relabeling, setRelabeling] = useState(false);
  const [msg, setMsg] = useState('');

  const load = () => {
    if (!wsId) return;
    api(`/api/graph/${wsId}/summary`, {}, token).then(r => setSummary(r.data || r)).catch(() => {});
    api(`/api/graph/${wsId}/json?top_n_nodes=80`, {}, token).then(r => setGraph(r.data || r)).catch(() => {});
    api(`/api/graph/${wsId}/insight`, {}, token).then(r => setInsight(r.data || r)).catch(() => {});
  };
  useEffect(load, [wsId]);

  const rebuild = async () => {
    setBusy(true); setMsg('');
    try { const r = await api(`/api/graph/${wsId}/rebuild`, { method: 'POST' }, token); setMsg(`Built ${r.data?.nodes || 0} nodes / ${r.data?.edges || 0} edges.`); load(); }
    catch (e) { setMsg('Rebuild failed: ' + e.message); }
    setBusy(false);
  };

  const relabelTopics = async () => {
    setRelabeling(true); setMsg('');
    try {
      const r = await api(`/api/graph/${wsId}/relabel-topics`, { method: 'POST' }, token);
      const d = r.data || r;
      setMsg(`${d.relabeled || 0} topics relabeled, ${d.skipped_manual || 0} manual labels preserved`);
      load();
    } catch (e) { setMsg('Relabel failed: ' + e.message); }
    setRelabeling(false);
  };

  const nodeColor = (type) => ({ brand: '#f43f5e', topic: '#3b82f6', prompt: '#10b981', model: '#a855f7', url: '#f59e0b', subreddit: '#06b6d4', doctor: '#ec4899' }[type] || '#94a3b8');

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>Authority Graph — entities + edges</span>
          <div style={{ display: 'flex', gap: 6 }}>
            <button className="btn btn-sm" onClick={relabelTopics} disabled={relabeling}>{relabeling ? 'Relabeling…' : 'Relabel topics'}</button>
            <button className="btn btn-sm btn-primary" onClick={rebuild} disabled={busy}>{busy ? 'Rebuilding...' : 'Rebuild Graph'}</button>
          </div>
        </div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
          Brand → Topic → Prompt → Model → URL → Subreddit. Edges weighted by citation count. Powers explainable Authority Score.
        </p>
        {msg && <div style={{ fontSize: 11, color: msg.toLowerCase().includes('failed') ? 'var(--rose)' : 'var(--emerald)' }}>{msg}</div>}
      </div>

      {summary && (
        <div className="card">
          <div className="card-header">Summary</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
            <div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Top brands by degree</div>
              {(summary.top_brands || []).slice(0, 8).map(b => (
                <div key={b.id || b.label} style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', fontSize: 11 }}>
                  <span>{b.label}</span><span className="badge">{b.degree || 0}</span>
                </div>
              ))}
            </div>
            <div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Top topics</div>
              {(summary.top_topics || []).slice(0, 8).map(t => (
                <div key={t.id || t.label} style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', fontSize: 11 }}>
                  <span>{t.label}</span><span className="badge">{t.degree || 0}</span>
                </div>
              ))}
            </div>
            <div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Isolated (opportunity)</div>
              {(summary.isolated || []).slice(0, 8).map(n => (
                <div key={n.id || n.label} style={{ padding: '3px 0', fontSize: 11 }}>{n.label}</div>
              ))}
            </div>
          </div>
        </div>
      )}

      {graph?.nodes?.length > 0 && (
        <div className="card">
          <div className="card-header">Force layout (top {graph.nodes.length} nodes / {graph.links?.length || 0} edges)</div>
          <svg width="100%" height="500" style={{ background: 'var(--bg-subtle)', borderRadius: 6 }}>
            {(graph.links || []).slice(0, 400).map((l, i) => {
              // crude grid layout — no D3
              const a = graph.nodes.findIndex(n => n.id === l.source);
              const b = graph.nodes.findIndex(n => n.id === l.target);
              if (a < 0 || b < 0) return null;
              const cols = Math.ceil(Math.sqrt(graph.nodes.length));
              const ax = 30 + (a % cols) * (1100 / cols);
              const ay = 30 + Math.floor(a / cols) * 60;
              const bx = 30 + (b % cols) * (1100 / cols);
              const by = 30 + Math.floor(b / cols) * 60;
              return <line key={i} x1={ax} y1={ay} x2={bx} y2={by} stroke="rgba(148,163,184,0.2)" strokeWidth={Math.min(2, (l.weight || 1) * 0.5)} />;
            })}
            {graph.nodes.map((n, i) => {
              const cols = Math.ceil(Math.sqrt(graph.nodes.length));
              const x = 30 + (i % cols) * (1100 / cols);
              const y = 30 + Math.floor(i / cols) * 60;
              return (
                <g key={n.id}>
                  <circle cx={x} cy={y} r={6} fill={nodeColor(n.node_type)} />
                  <text x={x + 8} y={y + 3} fill="var(--text-secondary)" fontSize="9">{(n.label || '').slice(0, 16)}</text>
                </g>
              );
            })}
          </svg>
          <div style={{ display: 'flex', gap: 12, fontSize: 11, marginTop: 8 }}>
            {['brand','topic','prompt','model','url','subreddit','doctor'].map(t => (
              <span key={t}><span style={{ display: 'inline-block', width: 10, height: 10, background: nodeColor(t), borderRadius: '50%', marginRight: 4 }}></span>{t}</span>
            ))}
          </div>
        </div>
      )}

      {insight && (
        <div className="card">
          <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span>Graph Insight</span>
            <ConfidenceBadge level={insight.confidence || 'estimated'} />
          </div>
          {insight.summary_text && (
            <p style={{ fontSize: 14, color: 'var(--text-primary)', lineHeight: 1.6, margin: '4px 0 14px' }}>
              {insight.summary_text}
            </p>
          )}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 14 }}>
            <div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Top brands by degree</div>
              {(() => {
                const tb = insight.top_brands_by_degree || [];
                const maxDeg = Math.max(1, ...tb.map(b => Number(b.degree) || 0));
                return tb.slice(0, 10).map((b, i) => {
                  const pct = Math.max(2, Math.round((Number(b.degree) || 0) * 100 / maxDeg));
                  return (
                    <div key={`${b.name}-${i}`} style={{ display: 'grid', gridTemplateColumns: '120px 1fr 40px', gap: 6, alignItems: 'center', marginBottom: 4, fontSize: 11 }}>
                      <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{b.name}</span>
                      <span style={{ height: 8, background: 'var(--bg-raised)', borderRadius: 4, overflow: 'hidden' }}>
                        <span style={{ display: 'block', height: '100%', width: pct + '%', background: 'var(--blue)' }}></span>
                      </span>
                      <span style={{ textAlign: 'right', fontFamily: 'var(--font-mono)', color: 'var(--text-muted)' }}>{b.degree}</span>
                    </div>
                  );
                });
              })()}
              {!(insight.top_brands_by_degree || []).length && <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>—</div>}
            </div>

            <div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Top topics</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                {(insight.top_topics || []).map((t, i) => (
                  <span key={`${t.label}-${i}`} className="badge blue" title={`${t.prompt_count || 0} prompts · ${t.competitor_count || 0} competitors`}>
                    {t.label} · {t.prompt_count || 0}
                  </span>
                ))}
                {!(insight.top_topics || []).length && <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>—</div>}
              </div>
            </div>
          </div>

          <div style={{ marginTop: 14, display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 14 }}>
            <div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Isolated opportunities</div>
              {(insight.isolated_opportunities || []).slice(0, 12).map((o, i) => (
                <div key={i} style={{ padding: '6px 8px', borderRadius: 4, background: 'rgba(244,63,94,0.10)', borderLeft: '2px solid var(--rose)', marginBottom: 4, fontSize: 11 }}>
                  <span style={{ fontWeight: 600, color: 'var(--rose)' }}>{o.topic_label}</span>
                  {o.reason && <span style={{ color: 'var(--text-secondary)' }}> — {o.reason}</span>}
                </div>
              ))}
              {!(insight.isolated_opportunities || []).length && <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>—</div>}
            </div>
            <div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Weakly defended topics</div>
              {(insight.weakly_defended_topics || []).slice(0, 12).map((w, i) => (
                <div key={i} style={{ padding: '6px 8px', borderRadius: 4, background: 'rgba(245,158,11,0.10)', borderLeft: '2px solid var(--amber)', marginBottom: 4, fontSize: 11 }}>
                  <span style={{ fontWeight: 600, color: 'var(--amber)' }}>{w.topic_label || w.label || w.name || '—'}</span>
                  {w.reason && <span style={{ color: 'var(--text-secondary)' }}> — {w.reason}</span>}
                </div>
              ))}
              {!(insight.weakly_defended_topics || []).length && <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>—</div>}
            </div>
          </div>

          {(insight.strongest_edges || []).length > 0 && (
            <div style={{ marginTop: 14 }}>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Strongest edges</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', fontSize: 11 }}>
                {insight.strongest_edges.slice(0, 10).map((e, i) => (
                  <span key={i} className="badge">{e.source || e.from || '?'} → {e.target || e.to || '?'}{e.weight ? ` (${e.weight})` : ''}</span>
                ))}
              </div>
            </div>
          )}

          {(insight.citation_heavy_nodes || []).length > 0 && (
            <div style={{ marginTop: 10 }}>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Citation-heavy nodes</div>
              <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', fontSize: 11 }}>
                {insight.citation_heavy_nodes.slice(0, 12).map((n, i) => (
                  <span key={i} className="badge purple">{n.label || n.name || n.id || '?'}{n.citations ? ` · ${n.citations}` : ''}</span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// BRAND MANAGER
// ═══════════════════════════════════════════════════════════════

function _relTime(iso) {
  if (!iso) return '—';
  const t = new Date(iso).getTime();
  if (isNaN(t)) return '—';
  const diff = Date.now() - t;
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  const d = Math.floor(h / 24);
  if (d < 30) return `${d}d ago`;
  const mo = Math.floor(d / 30);
  if (mo < 12) return `${mo}mo ago`;
  return `${Math.floor(mo / 12)}y ago`;
}

function ReviewIntelligencePanel({ wsId }) {
  const { token } = useContext(AuthContext);
  const [summary, setSummary] = useState(null);
  const [msg, setMsg] = useState('');
  const [form, setForm] = useState({ brand: '', platform: '', review_count: '', avg_rating: '', latest_review_at: '', sentiment_score: '', source_url: '' });

  const load = () => {
    if (!wsId) return;
    api(`/api/reviews/${wsId}/summary`, {}, token).then(r => setSummary(r.data || r)).catch(() => setSummary(null));
  };
  useEffect(load, [wsId]);

  const submit = async (e) => {
    e.preventDefault();
    if (!form.brand.trim() || !form.platform.trim()) { setMsg('Brand and platform are required.'); return; }
    setMsg('');
    try {
      const body = {
        brand: form.brand,
        platform: form.platform,
        review_count: form.review_count !== '' ? Number(form.review_count) : null,
        avg_rating: form.avg_rating !== '' ? Number(form.avg_rating) : null,
        latest_review_at: form.latest_review_at || null,
        sentiment_score: form.sentiment_score !== '' ? Number(form.sentiment_score) : null,
        source_url: form.source_url || null,
      };
      await api(`/api/reviews/${wsId}/signal`, { method: 'POST', body: JSON.stringify(body) }, token);
      setMsg('Saved.');
      setForm({ brand: '', platform: '', review_count: '', avg_rating: '', latest_review_at: '', sentiment_score: '', source_url: '' });
      load();
    } catch (err) { setMsg('Failed: ' + err.message); }
  };

  const brands = summary?.brands || [];
  const disclaimer = summary?.disclaimer;

  return (
    <div className="card" style={{ display: 'grid', gap: 12 }}>
      <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span>Review Intelligence</span>
        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
          {summary?.overall_confidence && <ConfidenceBadge level={summary.overall_confidence} />}
          <button className="btn btn-sm" onClick={load}>Refresh</button>
        </div>
      </div>

      {disclaimer && (
        <div style={{ border: '1px solid var(--amber)', background: 'rgba(245, 158, 11, 0.08)', borderRadius: 6, padding: '8px 10px', fontSize: 11, color: 'var(--amber)', display: 'flex', gap: 8, alignItems: 'flex-start' }}>
          <span style={{ fontSize: 13 }}>⚠</span>
          <span>{disclaimer}</span>
        </div>
      )}

      {brands.length === 0 ? (
        <div className="empty-state">★<br/>No review signals yet. Record one to start tracking review intelligence.</div>
      ) : (
        <div style={{ display: 'grid', gap: 8 }}>
          {brands.map((b, i) => {
            const t = b.totals || {};
            const sent = t.sentiment_score;
            const sentPct = sent != null ? Math.max(0, Math.min(100, Math.round(((Number(sent) + 1) / 2) * 100))) : null;
            const sentColor = sent == null ? 'var(--text-muted)' : sent > 0.2 ? 'var(--emerald)' : sent < -0.2 ? 'var(--rose)' : 'var(--amber)';
            return (
              <div key={b.brand || i} style={{ border: '1px solid var(--border-subtle)', borderRadius: 6, padding: 10, background: 'var(--bg-raised)', display: 'grid', gap: 6 }}>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                  <span style={{ fontWeight: 600 }}>{b.brand}</span>
                  {(b.platforms || []).map(p => <span key={p} className="badge gray" style={{ fontSize: 10 }}>{p}</span>)}
                  <ConfidenceBadge level={b.confidence || 'estimated'} />
                </div>
                <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap', fontSize: 12, color: 'var(--text-secondary)' }}>
                  <span><span style={{ color: 'var(--text-muted)' }}>reviews </span><span style={{ fontFamily: 'var(--font-mono)' }}>{t.review_count ?? '—'}</span></span>
                  <span><span style={{ color: 'var(--text-muted)' }}>avg </span>{t.avg_rating != null ? `${Number(t.avg_rating).toFixed(1)} ★` : '—'}</span>
                  <span><span style={{ color: 'var(--text-muted)' }}>latest </span>{_relTime(t.latest_review_at)}</span>
                </div>
                {sentPct != null && (
                  <div style={{ display: 'flex', gap: 6, alignItems: 'center', fontSize: 11 }}>
                    <span style={{ color: 'var(--text-muted)', minWidth: 60 }}>sentiment</span>
                    <div style={{ flex: 1, height: 6, background: 'var(--border-subtle)', borderRadius: 3, overflow: 'hidden' }}>
                      <div style={{ width: `${sentPct}%`, height: '100%', background: sentColor }} />
                    </div>
                    <span style={{ fontFamily: 'var(--font-mono)', color: sentColor, minWidth: 40, textAlign: 'right' }}>{Number(sent).toFixed(2)}</span>
                  </div>
                )}
                {b.recommended_review_strategy && (
                  <div style={{ fontStyle: 'italic', fontSize: 12, color: 'var(--text-secondary)' }}>{b.recommended_review_strategy}</div>
                )}
                {b.disclaimer && !disclaimer && (
                  <div style={{ fontSize: 10, color: 'var(--amber)' }}>⚠ {b.disclaimer}</div>
                )}
              </div>
            );
          })}
        </div>
      )}

      <form onSubmit={submit} style={{ borderTop: '1px solid var(--border-subtle)', paddingTop: 10, display: 'grid', gap: 6 }}>
        <div style={{ fontWeight: 600, fontSize: 12 }}>Record review signal</div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 6 }}>
          <input className="form-input" placeholder="Brand" value={form.brand} onChange={e => setForm(f => ({ ...f, brand: e.target.value }))} />
          <input className="form-input" placeholder="Platform (e.g. Google, Trustpilot)" value={form.platform} onChange={e => setForm(f => ({ ...f, platform: e.target.value }))} />
          <input className="form-input" type="number" placeholder="Review count" value={form.review_count} onChange={e => setForm(f => ({ ...f, review_count: e.target.value }))} />
          <input className="form-input" type="number" step="0.1" placeholder="Avg rating" value={form.avg_rating} onChange={e => setForm(f => ({ ...f, avg_rating: e.target.value }))} />
          <input className="form-input" type="date" placeholder="Latest review at" value={form.latest_review_at} onChange={e => setForm(f => ({ ...f, latest_review_at: e.target.value }))} />
          <input className="form-input" type="number" step="0.1" min="-1" max="1" placeholder="Sentiment (-1..1)" value={form.sentiment_score} onChange={e => setForm(f => ({ ...f, sentiment_score: e.target.value }))} />
          <input className="form-input" placeholder="Source URL" value={form.source_url} onChange={e => setForm(f => ({ ...f, source_url: e.target.value }))} style={{ gridColumn: '1 / -1' }} />
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <button type="submit" className="btn btn-sm btn-primary">Record signal</button>
          {msg && <span style={{ fontSize: 11, color: msg.includes('Failed') ? 'var(--rose)' : 'var(--emerald)' }}>{msg}</span>}
        </div>
      </form>
    </div>
  );
}

const _ECS_COLORS = { consistent: 'var(--emerald)', inconsistent: 'var(--rose)', missing: 'var(--text-muted)' };

function EntityConsistencyPanel({ wsId }) {
  const { token } = useContext(AuthContext);
  const [result, setResult] = useState(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState('');
  const [showRecent, setShowRecent] = useState(false);
  const [recent, setRecent] = useState(null);
  const [recentBusy, setRecentBusy] = useState(false);

  const runCheck = async () => {
    if (!wsId) return;
    setBusy(true); setMsg('');
    try {
      const r = await api(`/api/entity-consistency/${wsId}/check`, { method: 'POST' }, token);
      setResult(r.data || r);
    } catch (e) { setMsg('Check failed: ' + e.message); }
    setBusy(false);
  };

  const toggleRecent = async () => {
    const next = !showRecent;
    setShowRecent(next);
    if (next && recent == null && wsId) {
      setRecentBusy(true);
      try {
        const r = await api(`/api/entity-consistency/${wsId}/recent?limit=50`, {}, token);
        const rows = r.data || r;
        setRecent(Array.isArray(rows) ? rows : (rows?.rows || []));
      } catch (e) { setRecent([]); }
      setRecentBusy(false);
    }
  };

  const axes = result?.axes || [];

  return (
    <div className="card" style={{ display: 'grid', gap: 12 }}>
      <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span>Entity Consistency</span>
        <button className="btn btn-sm btn-primary" onClick={runCheck} disabled={busy || !wsId}>{busy ? 'Checking…' : 'Run Consistency Check'}</button>
      </div>

      {!result ? (
        <div className="empty-state">⌖<br/>Verify your brand name, address, hours, and phone are identical across every source AI uses to ground answers. Click <b>Run Consistency Check</b> to start.</div>
      ) : (
        <React.Fragment>
          <div style={{ display: 'flex', gap: 14, alignItems: 'center', flexWrap: 'wrap' }}>
            <div className="metric-card" style={{ minWidth: 140 }}>
              <div className="metric-label">OVERALL SCORE</div>
              <div className="metric-value">{result.overall_score != null ? Math.round(result.overall_score) : '—'}</div>
            </div>
            <ConfidenceBadge level={result.confidence || 'estimated'} />
            {result.observed_at && <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>observed {_relTime(result.observed_at)}</span>}
          </div>
          {msg && <div style={{ fontSize: 11, color: 'var(--rose)' }}>{msg}</div>}

          {axes.length > 0 && (
            <table className="data-table">
              <thead><tr><th>Axis</th><th>Sources</th><th>Recommended fix</th><th>Confidence</th></tr></thead>
              <tbody>
                {axes.map((ax, i) => (
                  <tr key={ax.axis || i}>
                    <td style={{ fontWeight: 600 }}>{ax.axis}</td>
                    <td>
                      <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                        {Object.entries(ax.statuses_per_source || {}).map(([src, status]) => {
                          const color = _ECS_COLORS[String(status).toLowerCase()] || 'var(--text-muted)';
                          return (
                            <span key={src} title={`${src}: ${status}`} style={{
                              display: 'inline-flex', alignItems: 'center', gap: 4, padding: '1px 7px',
                              borderRadius: 9, fontSize: 10, fontWeight: 600, fontFamily: 'var(--font-mono)',
                              color, background: `${color}22`, whiteSpace: 'nowrap',
                            }}>
                              <span style={{ width: 6, height: 6, borderRadius: '50%', background: color }} />
                              {src}
                            </span>
                          );
                        })}
                      </div>
                      {(ax.inconsistencies || []).length > 0 && (
                        <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4 }}>
                          {(ax.inconsistencies || []).map((inc, j) => <div key={j}>· {typeof inc === 'string' ? inc : JSON.stringify(inc)}</div>)}
                        </div>
                      )}
                    </td>
                    <td style={{ fontSize: 12 }}>{ax.recommended_fix || '—'}</td>
                    <td><ConfidenceBadge level={ax.confidence || 'estimated'} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}

          <div style={{ borderTop: '1px solid var(--border-subtle)', paddingTop: 8 }}>
            <button className="btn btn-sm" onClick={toggleRecent}>{showRecent ? '▾' : '▸'} Recent observations</button>
            {showRecent && (
              <div style={{ marginTop: 8 }}>
                {recentBusy ? <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Loading…</div>
                  : !recent || recent.length === 0 ? <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>No observations yet.</div>
                  : (
                    <table className="data-table">
                      <thead><tr><th>Axis</th><th>Source</th><th>Value</th><th>Status</th><th>Observed at</th></tr></thead>
                      <tbody>
                        {recent.slice(0, 50).map((row, i) => {
                          const color = _ECS_COLORS[String(row.status).toLowerCase()] || 'var(--text-muted)';
                          return (
                            <tr key={i}>
                              <td>{row.axis || '—'}</td>
                              <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{row.source || '—'}</td>
                              <td style={{ fontSize: 11 }}>{row.value || '—'}</td>
                              <td><span className="badge" style={{ color, background: `${color}22` }}>{row.status || '—'}</span></td>
                              <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{_relTime(row.observed_at)}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  )}
              </div>
            )}
          </div>
        </React.Fragment>
      )}
    </div>
  );
}

function BrandManagerPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [brands, setBrands] = useState([]);
  const [name, setName] = useState('');
  const [domain, setDomain] = useState('');
  const [msg, setMsg] = useState('');

  const load = () => {
    if (!wsId) return;
    api(`/api/brands/${wsId}`, {}, token).then(r => setBrands(r.data || [])).catch(() => {});
  };
  useEffect(load, [wsId]);

  const addOrSetUs = async (asUs) => {
    if (!name.trim()) return;
    try {
      if (asUs) {
        await api(`/api/brands/${wsId}/set-canonical-for-us`, { method: 'POST', body: JSON.stringify({ name, domain }) }, token);
      } else {
        await api(`/api/brands/${wsId}/canonicalize`, { method: 'POST', body: JSON.stringify({ name }) }, token);
      }
      setName(''); setDomain(''); setMsg('Saved.'); load();
    } catch (e) { setMsg('Failed: ' + e.message); }
  };

  const mergeInto = async (keep, mergeId) => {
    if (!confirm(`Merge brand ${mergeId} into ${keep.canonical_name}? Aliases will be reassigned.`)) return;
    try { await api(`/api/brands/${wsId}/merge`, { method: 'POST', body: JSON.stringify({ keep_id: keep.id, merge_id: mergeId }) }, token); load(); } catch {}
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header">Brand Manager — canonicalize aliases</div>
        <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>"Pasarét Klinika" vs "Pasaret Klinika" vs "pasarét klinika" should be ONE brand. Add yours below, then merge duplicates.</p>
        <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
          <input className="form-input" placeholder="Brand name" value={name} onChange={e => setName(e.target.value)} style={{ flex: 1 }} />
          <input className="form-input" placeholder="domain (optional)" value={domain} onChange={e => setDomain(e.target.value)} style={{ flex: 1 }} />
          <button className="btn" onClick={() => addOrSetUs(false)}>Add Brand</button>
          <button className="btn btn-primary" onClick={() => addOrSetUs(true)}>Mark as US</button>
        </div>
        {msg && <div style={{ fontSize: 11, color: 'var(--emerald)' }}>{msg}</div>}
      </div>
      <div className="card">
        <div className="card-header">All brands ({brands.length})</div>
        {brands.length === 0 ? <div className="empty-state">Ⓑ<br/>No brands yet. Import a Peec CSV or add manually.</div> : (
          <table className="data-table"><thead><tr>
            <th>Canonical</th><th>Display</th><th>Domain</th><th>Aliases</th><th>Us?</th><th></th>
          </tr></thead><tbody>{brands.map(b => (
            <tr key={b.id}>
              <td>{b.canonical_name}</td>
              <td>{b.display_name || '—'}</td>
              <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{b.domain || '—'}</td>
              <td style={{ fontSize: 11, color: 'var(--text-muted)' }}>{(b.aliases || []).length}</td>
              <td>{b.is_us ? <span className="badge emerald">us</span> : '—'}</td>
              <td><code style={{ fontSize: 10 }}>{b.id?.slice(0, 8)}</code></td>
            </tr>
          ))}</tbody></table>
        )}
      </div>
      <ReviewIntelligencePanel wsId={wsId} />
      <EntityConsistencyPanel wsId={wsId} />
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// ALERTS — rule CRUD + recent events
// ═══════════════════════════════════════════════════════════════

const TRIGGERS = [
  { id: 'ownership_drop', label: 'Ownership drop on a prompt', params: { min_revenue_score: 60, drop_delta: 0.10, window_hours: 168 } },
  { id: 'authority_drop', label: 'Authority score drop', params: { drop_delta: 5, window_days: 7 } },
  { id: 'competitor_aio_entry', label: 'New competitor in AIO', params: { min_revenue_score: 60 } },
  { id: 'we_dropped_from_aio', label: 'We dropped from AIO', params: { min_revenue_score: 60 } },
  { id: 'sentiment_drop', label: 'Sentiment drop', params: { window_hours: 168, min_negative_ratio: 0.3 } },
  { id: 'reddit_opportunity', label: 'Reddit opportunity gap', params: { subreddits: [], min_brands_mentioned: 2 } },
  { id: 'prompt_lost', label: 'Prompt lost to competitor', params: { min_revenue_score: 60 } },
  { id: 'capability_movement', label: 'Competitor capability movement', params: { axis: 'schema_score', delta: 10 } },
];

function AlertsPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [rules, setRules] = useState([]);
  const [events, setEvents] = useState([]);
  const [newRule, setNewRule] = useState({ name: '', trigger_type: 'ownership_drop', channels: 'inapp', cooldown_minutes: 60 });
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState('');

  const load = () => {
    if (!wsId) return;
    api(`/api/alerts/${wsId}/rules`, {}, token).then(r => setRules(r.data || [])).catch(() => {});
    api(`/api/alerts/${wsId}/events?limit=50`, {}, token).then(r => setEvents(r.data || [])).catch(() => {});
  };
  useEffect(load, [wsId]);

  const createRule = async () => {
    if (!newRule.name) { setMsg('Name required.'); return; }
    setBusy(true); setMsg('');
    try {
      const trig = TRIGGERS.find(t => t.id === newRule.trigger_type);
      await api(`/api/alerts/${wsId}/rules`, { method: 'POST', body: JSON.stringify({
        name: newRule.name,
        trigger_type: newRule.trigger_type,
        params: trig?.params || {},
        channels: newRule.channels.split(',').map(s => s.trim()).filter(Boolean),
        cooldown_minutes: parseInt(newRule.cooldown_minutes, 10) || 60,
      })}, token);
      setNewRule({ name: '', trigger_type: 'ownership_drop', channels: 'inapp', cooldown_minutes: 60 });
      load();
    } catch (e) { setMsg('Failed: ' + e.message); }
    setBusy(false);
  };

  const deleteRule = async (id) => {
    if (!confirm('Delete rule?')) return;
    try { await api(`/api/alerts/${wsId}/rules/${id}`, { method: 'DELETE' }, token); load(); } catch {}
  };

  const evalNow = async () => {
    setBusy(true);
    try { const r = await api(`/api/alerts/${wsId}/evaluate`, { method: 'POST' }, token); setMsg(`Fired ${r.data?.fired || 0} events.`); load(); }
    catch (e) { setMsg('Evaluate failed: ' + e.message); }
    setBusy(false);
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span>Alert Rules</span>
          <button className="btn btn-sm btn-primary" onClick={evalNow} disabled={busy}>{busy ? 'Evaluating...' : 'Evaluate Now'}</button>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 2fr 100px 100px', gap: 6, marginBottom: 8 }}>
          <input className="form-input" placeholder="Rule name" value={newRule.name} onChange={e => setNewRule({ ...newRule, name: e.target.value })} />
          <select className="form-input" value={newRule.trigger_type} onChange={e => setNewRule({ ...newRule, trigger_type: e.target.value })}>
            {TRIGGERS.map(t => <option key={t.id} value={t.id}>{t.label}</option>)}
          </select>
          <input className="form-input" placeholder="channels: inapp,slack:https://hooks...,webhook:https://..." value={newRule.channels} onChange={e => setNewRule({ ...newRule, channels: e.target.value })} />
          <input className="form-input" type="number" value={newRule.cooldown_minutes} onChange={e => setNewRule({ ...newRule, cooldown_minutes: e.target.value })} title="cooldown min" />
          <button className="btn btn-primary" onClick={createRule} disabled={busy}>Create</button>
        </div>
        {msg && <div style={{ fontSize: 11, color: msg.includes('failed') || msg.includes('required') ? 'var(--rose)' : 'var(--emerald)' }}>{msg}</div>}
        {rules.length === 0 ? <div className="empty-state">⚠<br/>No rules yet.</div> : (
          <table className="data-table"><thead><tr>
            <th>Name</th><th>Trigger</th><th>Channels</th><th>Cooldown</th><th>Enabled</th><th></th>
          </tr></thead><tbody>{rules.map(r => (
            <tr key={r.id}>
              <td style={{ fontWeight: 600 }}>{r.name}</td>
              <td><span className="badge">{r.trigger_type}</span></td>
              <td style={{ fontSize: 11, fontFamily: 'var(--font-mono)' }}>{Array.isArray(r.channels) ? r.channels.join(', ') : r.channels}</td>
              <td>{r.cooldown_minutes}m</td>
              <td>{r.enabled ? <span className="badge emerald">on</span> : <span className="badge gray">off</span>}</td>
              <td><button className="btn btn-sm" onClick={() => deleteRule(r.id)}>×</button></td>
            </tr>
          ))}</tbody></table>
        )}
      </div>

      <div className="card">
        <div className="card-header">Recent events ({events.length})</div>
        {events.length === 0 ? <div className="empty-state">⚠<br/>No alerts fired yet.</div> : events.map(e => (
          <div key={e.id} style={{ padding: 8, borderBottom: '1px solid var(--border-subtle)', fontSize: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span style={{ fontWeight: 600 }}>{e.title}</span>
              <span><span className={`badge ${e.severity === 'critical' ? 'rose' : e.severity === 'warning' ? 'amber' : 'gray'}`}>{e.severity}</span> · {new Date(e.fired_at).toLocaleString()}</span>
            </div>
            {e.message && <div style={{ color: 'var(--text-secondary)', marginTop: 4 }}>{e.message}</div>}
          </div>
        ))}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// COMPARATIVE REPORT
// ═══════════════════════════════════════════════════════════════

function ComparativeReportPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [competitors, setCompetitors] = useState([]);
  const [chosen, setChosen] = useState('');
  const [html, setHtml] = useState('');
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!wsId) return;
    api(`/api/attack-map/${wsId}`, {}, token).then(r => setCompetitors((r.data || []).map(x => x.competitor_domain))).catch(() => {});
  }, [wsId]);

  const generate = async () => {
    const list = chosen.split(',').map(s => s.trim()).filter(Boolean);
    if (list.length === 0) return;
    setBusy(true);
    try {
      const r = await api(`/api/reports/comparative/${wsId}`, { method: 'POST', body: JSON.stringify({ competitor_domains: list, include_prompts: true }) }, token);
      setHtml(r.data?.html || '');
    } catch (e) { setHtml('<p style="color:red">Failed: ' + e.message + '</p>'); }
    setBusy(false);
  };

  const openHtmlWindow = () => {
    const list = chosen.split(',').map(s => s.trim()).filter(Boolean);
    const url = `/api/reports/comparative/${wsId}/html?competitors=${encodeURIComponent(list.join(','))}`;
    window.open(url + (token ? `&token=${token}` : ''), '_blank');
  };

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header">Comparative Report — "us vs them" PDF-ready</div>
        <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
          <input className="form-input" placeholder="competitor domains comma-separated" value={chosen} onChange={e => setChosen(e.target.value)} style={{ flex: 1 }} />
          <button className="btn btn-primary" onClick={generate} disabled={busy}>{busy ? 'Generating...' : 'Generate'}</button>
          <button className="btn" onClick={openHtmlWindow}>Open Printable</button>
        </div>
        {competitors.length > 0 && (
          <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Known: {competitors.slice(0, 10).join(', ')}</div>
        )}
      </div>
      {html && (
        <div className="card" style={{ padding: 0, maxHeight: 800, overflow: 'auto' }}>
          <iframe srcDoc={html} style={{ width: '100%', height: 800, border: 'none', borderRadius: 6 }} title="report" />
        </div>
      )}

      <WorkspaceImpactReportCard wsId={state.activeWorkspace?.id} />
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// GEO BRIEF MODAL — Phase 4
// ═══════════════════════════════════════════════════════════════

function GeoBriefModal({ wsId, promptId, briefId: initialBriefId, readOnly, onClose }) {
  const { token } = useContext(AuthContext);
  const [brief, setBrief] = useState(null);
  const [error, setError] = useState('');
  const [pushing, setPushing] = useState(false);
  const [msg, setMsg] = useState('');
  const loading = !brief && !error;

  useEffect(() => {
    let cancelled = false;
    setBrief(null); setError('');
    const fetcher = initialBriefId
      ? api(`/api/briefs/${wsId}`, {}, token).then(r => {
          const list = r.data || r || [];
          const found = list.find(b => b.id === initialBriefId);
          return found ? { data: found } : { data: null };
        })
      : api(`/api/briefs/${wsId}/${promptId}/generate?push=false`, { method: 'POST' }, token);
    fetcher
      .then(r => { if (!cancelled) setBrief(r.data || r); })
      .catch(e => { if (!cancelled) setError(e.message || 'Failed to load brief'); });
    return () => { cancelled = true; };
  }, [wsId, promptId, initialBriefId, token]);

  useEffect(() => {
    const onKey = e => { if (e.key === 'Escape') onClose && onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const pushToActions = async () => {
    if (!brief?.id) return;
    setPushing(true); setMsg('');
    try {
      const r = await api(`/api/briefs/${brief.id}/push-to-actions`, { method: 'POST' }, token);
      const d = r.data || r;
      setMsg(`Action created (${d.action_id || 'ok'}). Closing…`);
      setTimeout(() => onClose && onClose(), 900);
    } catch (e) { setMsg('Push failed: ' + e.message); }
    setPushing(false);
  };

  const toMarkdown = (bj) => {
    if (!bj) return '';
    const lines = [];
    lines.push(`# ${bj.suggested_h1 || bj.target_prompt || 'GEO Brief'}`);
    if (bj.target_prompt) lines.push(`\n**Target prompt:** ${bj.target_prompt}`);
    if (bj.buyer_stage) lines.push(`**Buyer stage:** ${bj.buyer_stage}`);
    if (bj.search_intent) lines.push(`**Search intent:** ${bj.search_intent}`);
    if (bj.recommended_page_type) lines.push(`**Page type:** ${bj.recommended_page_type}`);
    if (Array.isArray(bj.suggested_h2_outline) && bj.suggested_h2_outline.length) {
      lines.push(`\n## H2 outline`);
      bj.suggested_h2_outline.forEach((h, i) => lines.push(`${i + 1}. ${h}`));
    }
    if (bj.direct_answer_block) lines.push(`\n## Direct answer\n> ${bj.direct_answer_block}`);
    if (Array.isArray(bj.decision_support_sections) && bj.decision_support_sections.length) {
      lines.push(`\n## Decision support`);
      bj.decision_support_sections.forEach(s => lines.push(`- ${s}`));
    }
    if (Array.isArray(bj.comparison_table) && bj.comparison_table.length) {
      lines.push(`\n## Comparison`);
      lines.push(`| Column | Us | Competitor |`);
      lines.push(`| --- | --- | --- |`);
      bj.comparison_table.forEach(row => lines.push(`| ${row.column || ''} | ${row.our || ''} | ${row.competitor || ''} |`));
    }
    if (Array.isArray(bj.faq_questions) && bj.faq_questions.length) {
      lines.push(`\n## FAQ`);
      bj.faq_questions.forEach(q => {
        if (typeof q === 'string') lines.push(`- ${q}`);
        else lines.push(`- **${q.q || q.question || ''}** — ${q.a || q.answer || ''}`);
      });
    }
    if (Array.isArray(bj.schema_type) && bj.schema_type.length) {
      lines.push(`\n**Schema:** ${bj.schema_type.join(', ')}`);
    }
    if (Array.isArray(bj.trust_proof_needed) && bj.trust_proof_needed.length) {
      lines.push(`\n## Trust proof needed`);
      bj.trust_proof_needed.forEach(t => lines.push(`- ${t}`));
    }
    if (Array.isArray(bj.internal_link_suggestions) && bj.internal_link_suggestions.length) {
      lines.push(`\n## Internal links`);
      bj.internal_link_suggestions.forEach(l => lines.push(`- ${l}`));
    }
    if (Array.isArray(bj.external_proof_suggestions) && bj.external_proof_suggestions.length) {
      lines.push(`\n## External proof`);
      bj.external_proof_suggestions.forEach(l => lines.push(`- ${l}`));
    }
    if (bj.cta_angle) lines.push(`\n## CTA angle\n${bj.cta_angle}`);
    if (Array.isArray(bj.competitor_pages_to_study) && bj.competitor_pages_to_study.length) {
      lines.push(`\n## Competitor pages to study`);
      bj.competitor_pages_to_study.forEach(l => lines.push(`- ${l}`));
    }
    if (bj.success_metric) lines.push(`\n**Success metric:** ${bj.success_metric}`);
    if (bj.re_track_schedule_days != null) lines.push(`**Re-track in:** ${bj.re_track_schedule_days}d`);
    return lines.join('\n');
  };

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(toMarkdown(brief?.brief_json));
      setMsg('Copied to clipboard.');
    } catch { setMsg('Copy failed.'); }
  };

  const bj = brief?.brief_json || {};
  const intentColor = { transactional: 'rose', commercial: 'amber', comparison: 'purple', informational: 'blue', navigational: 'gray' };

  const linkify = url => {
    if (!url) return null;
    let href = String(url);
    if (!/^https?:\/\//i.test(href)) href = 'https://' + href;
    return <a href={href} target="_blank" rel="noopener noreferrer" style={{ color: 'var(--blue)', fontFamily: 'var(--font-mono)', fontSize: 11, wordBreak: 'break-all' }}>{String(url).slice(0, 100)}</a>;
  };

  return (
    <div className="modal-overlay" onClick={() => onClose && onClose()}>
      <div className="modal" onClick={e => e.stopPropagation()} style={{ maxWidth: 920, width: '92vw' }}>
        <div className="modal-header">
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <h3 style={{ marginBottom: 0 }}>GEO Brief {readOnly ? '— read-only' : ''}</h3>
            {brief && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                {bj.target_prompt && <span style={{ fontSize: 12, color: 'var(--text-secondary)', maxWidth: 600 }}>{bj.target_prompt}</span>}
                {bj.recommended_page_type && <span className="badge purple">{bj.recommended_page_type}</span>}
                {bj.search_intent && <span className={`badge ${intentColor[bj.search_intent] || 'gray'}`}>{bj.search_intent}</span>}
                <ConfidenceBadge level={bj.confidence || 'estimated'} />
              </div>
            )}
          </div>
          <button className="modal-close" onClick={() => onClose && onClose()}>{'×'}</button>
        </div>
        <div className="modal-body">
          {loading && <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>Generating brief…</div>}
          {error && <div style={{ color: 'var(--rose)', fontSize: 12 }}>{error}</div>}
          {brief && (
            <div style={{ display: 'grid', gap: 14 }}>
              {bj.suggested_h1 && (
                <div className="card" style={{ padding: 12 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Suggested H1</div>
                  <div style={{ fontSize: 16, fontWeight: 700, color: 'var(--text-secondary)' }}>{bj.suggested_h1}</div>
                </div>
              )}

              {Array.isArray(bj.suggested_h2_outline) && bj.suggested_h2_outline.length > 0 && (
                <div className="card" style={{ padding: 12 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>H2 outline</div>
                  <ol style={{ margin: 0, paddingLeft: 18, fontSize: 12, lineHeight: 1.6 }}>
                    {bj.suggested_h2_outline.map((h, i) => <li key={i}>{h}</li>)}
                  </ol>
                </div>
              )}

              {bj.direct_answer_block && (
                <div className="card" style={{ padding: 12, borderLeft: '3px solid var(--emerald)', background: 'rgba(16,185,129,0.06)' }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Direct answer block</div>
                  <div style={{ fontStyle: 'italic', fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.55 }}>"{bj.direct_answer_block}"</div>
                </div>
              )}

              {Array.isArray(bj.decision_support_sections) && bj.decision_support_sections.length > 0 && (
                <div className="card" style={{ padding: 12 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Decision support sections</div>
                  <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12, lineHeight: 1.6 }}>
                    {bj.decision_support_sections.map((s, i) => <li key={i}>{s}</li>)}
                  </ul>
                </div>
              )}

              {Array.isArray(bj.comparison_table) && bj.comparison_table.length > 0 && (
                <div className="card" style={{ padding: 12 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Comparison table</div>
                  <table className="data-table"><thead><tr><th>Column</th><th>Us</th><th>Competitor</th></tr></thead><tbody>
                    {bj.comparison_table.map((row, i) => (
                      <tr key={i}><td style={{ fontWeight: 600 }}>{row.column || '—'}</td><td>{row.our || '—'}</td><td>{row.competitor || '—'}</td></tr>
                    ))}
                  </tbody></table>
                </div>
              )}

              {Array.isArray(bj.faq_questions) && bj.faq_questions.length > 0 && (
                <div className="card" style={{ padding: 12 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>FAQ</div>
                  <div style={{ display: 'grid', gap: 8 }}>
                    {bj.faq_questions.map((q, i) => (
                      <div key={i} style={{ fontSize: 12 }}>
                        {typeof q === 'string'
                          ? <div style={{ fontWeight: 600 }}>Q: {q}</div>
                          : (<><div style={{ fontWeight: 600 }}>Q: {q.q || q.question || ''}</div>{(q.a || q.answer) && <div style={{ color: 'var(--text-muted)', marginTop: 2 }}>A: {q.a || q.answer}</div>}</>)}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {Array.isArray(bj.schema_type) && bj.schema_type.length > 0 && (
                <div className="card" style={{ padding: 12 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Schema types</div>
                  <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                    {bj.schema_type.map((s, i) => <span key={i} className="badge cyan">{s}</span>)}
                  </div>
                </div>
              )}

              {Array.isArray(bj.trust_proof_needed) && bj.trust_proof_needed.length > 0 && (
                <div className="card" style={{ padding: 12 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Trust proof needed</div>
                  <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12, lineHeight: 1.6 }}>
                    {bj.trust_proof_needed.map((t, i) => <li key={i}>{t}</li>)}
                  </ul>
                </div>
              )}

              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                {Array.isArray(bj.internal_link_suggestions) && bj.internal_link_suggestions.length > 0 && (
                  <div className="card" style={{ padding: 12 }}>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Internal links</div>
                    <ul style={{ margin: 0, paddingLeft: 18, fontSize: 11, lineHeight: 1.5 }}>
                      {bj.internal_link_suggestions.map((l, i) => <li key={i}>{l}</li>)}
                    </ul>
                  </div>
                )}
                {Array.isArray(bj.external_proof_suggestions) && bj.external_proof_suggestions.length > 0 && (
                  <div className="card" style={{ padding: 12 }}>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>External proof</div>
                    <ul style={{ margin: 0, paddingLeft: 18, fontSize: 11, lineHeight: 1.5 }}>
                      {bj.external_proof_suggestions.map((l, i) => <li key={i}>{l}</li>)}
                    </ul>
                  </div>
                )}
              </div>

              {bj.cta_angle && (
                <div className="card" style={{ padding: 12, borderLeft: '3px solid var(--amber)', background: 'rgba(245,158,11,0.06)' }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>CTA angle</div>
                  <div style={{ fontSize: 12, color: 'var(--text-secondary)' }}>{bj.cta_angle}</div>
                </div>
              )}

              {Array.isArray(bj.competitor_pages_to_study) && bj.competitor_pages_to_study.length > 0 && (
                <div className="card" style={{ padding: 12 }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 6 }}>Competitor pages to study</div>
                  <div style={{ display: 'grid', gap: 4 }}>
                    {bj.competitor_pages_to_study.map((u, i) => <div key={i}>{linkify(u)}</div>)}
                  </div>
                </div>
              )}

              <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', fontSize: 11, color: 'var(--text-muted)' }}>
                {bj.success_metric && <span><b style={{ color: 'var(--text-secondary)' }}>Success metric:</b> {bj.success_metric}</span>}
                {bj.re_track_schedule_days != null && <span><b style={{ color: 'var(--text-secondary)' }}>Re-track:</b> {bj.re_track_schedule_days}d</span>}
              </div>
            </div>
          )}
        </div>
        {brief && (
          <div style={{ display: 'flex', gap: 8, padding: '12px 16px', borderTop: '1px solid var(--border-subtle)', flexWrap: 'wrap', alignItems: 'center' }}>
            <button className="btn btn-sm btn-primary" onClick={pushToActions} disabled={pushing || readOnly}>{pushing ? 'Pushing…' : 'Push to Action Engine'}</button>
            <button className="btn btn-sm" onClick={() => { setMsg('Switch to Content Studio to draft this brief.'); }}>Open Content Studio</button>
            <button className="btn btn-sm" onClick={copy}>Copy to clipboard</button>
            <button className="btn btn-sm" onClick={() => onClose && onClose()}>Close</button>
            {msg && <span style={{ fontSize: 11, color: msg.includes('fail') || msg.includes('Failed') ? 'var(--rose)' : 'var(--emerald)' }}>{msg}</span>}
          </div>
        )}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// WORKSPACE IMPACT REPORT CARD — Phase 4
// ═══════════════════════════════════════════════════════════════

function WorkspaceImpactReportCard({ wsId }) {
  const { token } = useContext(AuthContext);
  const [days, setDays] = useState(30);
  const [report, setReport] = useState(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState('');
  const [actionId, setActionId] = useState('');
  const [actionReport, setActionReport] = useState(null);
  const [actionBusy, setActionBusy] = useState(false);

  const run = async () => {
    if (!wsId) return;
    setBusy(true); setMsg('');
    try {
      const r = await api(`/api/reports/${wsId}/workspace?days=${days}`, {}, token);
      setReport(r.data || r);
    } catch (e) { setMsg('Report failed: ' + e.message); }
    setBusy(false);
  };

  const openHtml = () => {
    if (!wsId) return;
    const url = `${API}/api/reports/${wsId}/workspace.html?days=${days}${token ? `&token=${token}` : ''}`;
    window.open(url, '_blank');
  };

  const runActionReport = async () => {
    if (!actionId.trim()) return;
    setActionBusy(true);
    try {
      const r = await api(`/api/reports/action/${actionId.trim()}`, {}, token);
      setActionReport(r.data || r);
    } catch (e) { setActionReport({ error: e.message }); }
    setActionBusy(false);
  };

  const dColor = v => (v == null ? 'var(--text-muted)' : v > 0 ? 'var(--emerald)' : v < 0 ? 'var(--rose)' : 'var(--text-muted)');
  const fmt = v => v == null ? '—' : (v > 0 ? '+' : '') + (typeof v === 'number' ? v.toFixed(1) : v);

  return (
    <>
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>Workspace Impact Report — before/after deltas</span>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <select className="form-input" value={days} onChange={e => setDays(parseInt(e.target.value, 10))} style={{ width: 110 }}>
              <option value={7}>7 days</option>
              <option value={14}>14 days</option>
              <option value={30}>30 days</option>
              <option value={60}>60 days</option>
              <option value={90}>90 days</option>
            </select>
            <button className="btn btn-sm btn-primary" onClick={run} disabled={busy || !wsId}>{busy ? 'Running…' : 'Run Report'}</button>
            <button className="btn btn-sm" onClick={openHtml} disabled={!wsId}>Open HTML Report</button>
          </div>
        </div>
        {msg && <div style={{ fontSize: 11, color: 'var(--rose)' }}>{msg}</div>}
        {!report ? (
          <div className="empty-state" style={{ fontSize: 12 }}>◎<br/>Run a report to see workspace deltas over the chosen window.</div>
        ) : (
          <div style={{ display: 'grid', gap: 12, marginTop: 8 }}>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
              <div className="metric-card" style={{ borderTop: `2px solid ${dColor(report.authority_delta)}` }}>
                <div className="metric-label">AUTHORITY Δ</div>
                <div className="metric-value" style={{ color: dColor(report.authority_delta) }}>{fmt(report.authority_delta)}</div>
              </div>
              <div className="metric-card" style={{ borderTop: `2px solid ${dColor(report.ownership_delta)}` }}>
                <div className="metric-label">OWNERSHIP Δ</div>
                <div className="metric-value" style={{ color: dColor(report.ownership_delta) }}>{fmt(report.ownership_delta)}</div>
              </div>
              <div className="metric-card" style={{ borderTop: `2px solid ${dColor(report.aio_delta)}` }}>
                <div className="metric-label">AIO Δ</div>
                <div className="metric-value" style={{ color: dColor(report.aio_delta) }}>{fmt(report.aio_delta)}</div>
              </div>
              <div className="metric-card" style={{ borderTop: `2px solid ${dColor(report.revenue_at_stake_delta)}` }}>
                <div className="metric-label">€ AT STAKE Δ</div>
                <div className="metric-value" style={{ color: dColor(report.revenue_at_stake_delta) }}>{fmt(report.revenue_at_stake_delta)}</div>
              </div>
            </div>

            {report.narrative && (
              <div style={{ background: 'linear-gradient(135deg, rgba(16,185,129,0.08), rgba(99,102,241,0.04))', padding: 12, borderRadius: 6, fontStyle: 'italic', fontSize: 13, color: 'var(--text-secondary)' }}>{report.narrative}</div>
            )}

            {Array.isArray(report.competitor_movement) && report.competitor_movement.length > 0 && (
              <div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Competitor movement</div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {report.competitor_movement.map((c, i) => (
                    <span key={i} className={`badge ${(c.delta || 0) > 0 ? 'rose' : 'emerald'}`}>
                      {c.domain || c.name || c.competitor || 'competitor'} {(c.delta || 0) > 0 ? '+' : ''}{c.delta != null ? c.delta : ''}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {Array.isArray(report.completed_actions) && report.completed_actions.length > 0 && (
              <div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Completed actions in window</div>
                <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12, lineHeight: 1.6 }}>
                  {report.completed_actions.map((a, i) => <li key={i}>{a.title || a.id || JSON.stringify(a)}</li>)}
                </ul>
              </div>
            )}

            {Array.isArray(report.next_recommendations) && report.next_recommendations.length > 0 && (
              <div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Next recommendations</div>
                <ul style={{ margin: 0, paddingLeft: 18, fontSize: 12, lineHeight: 1.6 }}>
                  {report.next_recommendations.map((r, i) => <li key={i}>{typeof r === 'string' ? r : (r.title || r.text || JSON.stringify(r))}</li>)}
                </ul>
              </div>
            )}

            {report.disclaimer && (
              <div style={{ background: 'rgba(245,158,11,0.10)', border: '1px solid rgba(245,158,11,0.35)', color: 'var(--amber)', padding: '8px 12px', borderRadius: 6, fontSize: 11 }}>
                ⚠ {report.disclaimer}
              </div>
            )}
          </div>
        )}
      </div>

      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>Action Impact — single action before/after</span>
          <div style={{ display: 'flex', gap: 6 }}>
            <input className="form-input" placeholder="action_id" value={actionId} onChange={e => setActionId(e.target.value)} style={{ width: 260 }} />
            <button className="btn btn-sm btn-primary" onClick={runActionReport} disabled={actionBusy || !actionId.trim()}>{actionBusy ? 'Loading…' : 'Action Impact'}</button>
          </div>
        </div>
        {!actionReport ? (
          <div className="empty-state" style={{ fontSize: 12 }}>◎<br/>Paste an action_id to see its before/after impact.</div>
        ) : actionReport.error ? (
          <div style={{ color: 'var(--rose)', fontSize: 12 }}>{actionReport.error}</div>
        ) : (
          <div style={{ display: 'grid', gap: 10 }}>
            {actionReport.action && (
              <div style={{ fontSize: 13, fontWeight: 600 }}>{actionReport.action.title || actionReport.action.id}</div>
            )}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 10 }}>
              <div className="metric-card" style={{ borderTop: `2px solid ${dColor(actionReport.ownership_delta)}` }}>
                <div className="metric-label">OWNERSHIP Δ</div>
                <div className="metric-value" style={{ color: dColor(actionReport.ownership_delta) }}>{fmt(actionReport.ownership_delta)}</div>
              </div>
              <div className="metric-card" style={{ borderTop: `2px solid ${dColor(actionReport.citation_delta)}` }}>
                <div className="metric-label">CITATION Δ</div>
                <div className="metric-value" style={{ color: dColor(actionReport.citation_delta) }}>{fmt(actionReport.citation_delta)}</div>
              </div>
              <div className="metric-card" style={{ borderTop: `2px solid ${dColor(actionReport.authority_delta)}` }}>
                <div className="metric-label">AUTHORITY Δ</div>
                <div className="metric-value" style={{ color: dColor(actionReport.authority_delta) }}>{fmt(actionReport.authority_delta)}</div>
              </div>
              <div className="metric-card" style={{ borderTop: `2px solid ${dColor(actionReport.aio_delta)}` }}>
                <div className="metric-label">AIO Δ</div>
                <div className="metric-value" style={{ color: dColor(actionReport.aio_delta) }}>{fmt(actionReport.aio_delta)}</div>
              </div>
            </div>
            {actionReport.narrative && (
              <div style={{ fontStyle: 'italic', fontSize: 12, color: 'var(--text-secondary)' }}>{actionReport.narrative}</div>
            )}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              <div className="card" style={{ padding: 10 }}>
                <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Before</div>
                <pre style={{ fontFamily: 'var(--font-mono)', fontSize: 10, maxHeight: 200, overflow: 'auto', margin: 0, whiteSpace: 'pre-wrap' }}>{actionReport.before_snapshot ? JSON.stringify(actionReport.before_snapshot, null, 2) : '—'}</pre>
              </div>
              <div className="card" style={{ padding: 10 }}>
                <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>After</div>
                <pre style={{ fontFamily: 'var(--font-mono)', fontSize: 10, maxHeight: 200, overflow: 'auto', margin: 0, whiteSpace: 'pre-wrap' }}>{actionReport.after_snapshot ? JSON.stringify(actionReport.after_snapshot, null, 2) : '—'}</pre>
              </div>
            </div>
            {Array.isArray(actionReport.competitor_movement) && actionReport.competitor_movement.length > 0 && (
              <div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: 4 }}>Competitor movement</div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {actionReport.competitor_movement.map((c, i) => (
                    <span key={i} className="badge gray">{c.domain || c.name || 'competitor'} {c.delta != null ? `(${c.delta > 0 ? '+' : ''}${c.delta})` : ''}</span>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </>
  );
}

function ActionsPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [actions, setActions] = useState([]);
  const [summary, setSummary] = useState(null);
  const [workflow, setWorkflow] = useState(null);
  const [statusFilter, setStatusFilter] = useState('all');
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState('');
  const [expanded, setExpanded] = useState({});

  const load = () => {
    if (!wsId) return;
    const qs = statusFilter === 'all' ? '' : `?status=${statusFilter}`;
    api(`/api/actions/${wsId}${qs}${qs ? '&' : '?'}limit=200`, {}, token).then(r => setActions(r.data || [])).catch(() => {});
    api(`/api/actions/${wsId}/summary`, {}, token).then(r => setSummary(r.data || r)).catch(() => {});
    api(`/api/actions/${wsId}/workflow`, {}, token).then(r => setWorkflow(r.data || r)).catch(() => {});
  };
  useEffect(load, [wsId, statusFilter]);

  const beforeSnapshot = async (id) => {
    setBusy(true); setMsg('');
    try {
      await api(`/api/actions/${wsId}/${id}/before-snapshot`, { method: 'POST' }, token);
      const t = new Date();
      const hhmm = `${String(t.getHours()).padStart(2, '0')}:${String(t.getMinutes()).padStart(2, '0')}`;
      setMsg(`✓ snapshot at ${hhmm}`);
      setTimeout(() => setMsg(m => m.startsWith('✓ snapshot') ? '' : m), 3000);
      load();
    } catch (e) { setMsg('before snapshot failed: ' + e.message); }
    setBusy(false);
  };

  const afterSnapshot = async (id) => {
    setBusy(true); setMsg('');
    try {
      await api(`/api/actions/${wsId}/${id}/after-snapshot`, { method: 'POST' }, token);
      const t = new Date();
      const hhmm = `${String(t.getHours()).padStart(2, '0')}:${String(t.getMinutes()).padStart(2, '0')}`;
      setMsg(`✓ snapshot at ${hhmm}`);
      setTimeout(() => setMsg(m => m.startsWith('✓ snapshot') ? '' : m), 3000);
      load();
    } catch (e) { setMsg('after snapshot failed: ' + e.message); }
    setBusy(false);
  };

  const scheduleRetrack = async (id) => {
    setBusy(true); setMsg('');
    try {
      const r = await api(`/api/actions/${wsId}/${id}/schedule-retrack?days=7`, { method: 'POST' }, token);
      const date = r.data?.scheduled_at ? new Date(r.data.scheduled_at).toLocaleDateString() : '';
      setMsg(`✓ scheduled ${date}`);
      setTimeout(() => setMsg(m => m.startsWith('✓ scheduled') ? '' : m), 3000);
      load();
    } catch (e) { setMsg('Schedule re-track failed: ' + e.message); }
    setBusy(false);
  };

  const runDueRetracks = async () => {
    setBusy(true); setMsg('');
    try {
      const r = await api(`/api/actions/${wsId}/run-due-retracks`, { method: 'POST' }, token);
      const d = r.data || r;
      setMsg(`Ran ${d.ran || 0}, completed ${d.completed ?? d.ran ?? 0}`);
      load();
    } catch (e) { setMsg('Run due re-tracks failed: ' + e.message); }
    setBusy(false);
  };

  const harvest = async () => {
    setBusy(true); setMsg('');
    try { const r = await api(`/api/actions/${wsId}/harvest`, { method: 'POST' }, token); setMsg(`Harvested ${r.data?.created || 0} actions.`); load(); }
    catch (e) { setMsg('Harvest failed: ' + e.message); }
    setBusy(false);
  };

  const generate = async (id) => {
    setBusy(true); setMsg('');
    try { await api(`/api/actions/${wsId}/${id}/generate`, { method: 'POST' }, token); setExpanded(x => ({ ...x, [id]: true })); load(); }
    catch (e) { setMsg('Generate failed: ' + e.message); }
    setBusy(false);
  };

  const setStatus = async (id, status) => {
    setBusy(true); setMsg('');
    try { await api(`/api/actions/${wsId}/${id}/status?status=${status}`, { method: 'PUT' }, token); load(); }
    catch (e) { setMsg('Update failed: ' + e.message); }
    setBusy(false);
  };

  const priColor = (p) => p >= 70 ? 'rose' : p >= 40 ? 'amber' : 'gray';

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>Action Engine — turn diagnosis into done <MetricTooltip metricKey="prompt_priority" /></span>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <select className="form-input" value={statusFilter} onChange={e => setStatusFilter(e.target.value)} style={{ width: 140 }}>
              <option value="all">all</option>
              <option value="pending">pending</option>
              <option value="generated">generated</option>
              <option value="approved">approved</option>
              <option value="done">done</option>
              <option value="dismissed">dismissed</option>
            </select>
            <button className="btn btn-sm" onClick={runDueRetracks} disabled={busy || !wsId} title="Run all re-tracks that are due">Run due re-tracks</button>
            <button className="btn btn-sm btn-primary" onClick={harvest} disabled={busy || !wsId}>{busy ? 'Working...' : 'Harvest Actions'}</button>
          </div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
          <div className="metric-card" style={{ borderTop: '2px solid var(--rose)' }}>
            <div className="metric-label">OPEN ACTIONS</div>
            <div className="metric-value">{summary?.open_actions ?? '—'}</div>
          </div>
          <div className="metric-card" style={{ borderTop: '2px solid var(--blue)' }}>
            <div className="metric-label">TOTAL</div>
            <div className="metric-value">{summary?.total ?? '—'}</div>
          </div>
          <div className="metric-card" style={{ borderTop: '2px solid var(--amber)' }}>
            <div className="metric-label">PIPELINE VALUE</div>
            <div className="metric-value">{summary?.pipeline_value_score ?? '—'}</div>
          </div>
          <div className="metric-card" style={{ borderTop: '2px solid var(--emerald)' }}>
            <div className="metric-label">GENERATED</div>
            <div className="metric-value">{summary?.by_status?.generated ?? 0}</div>
          </div>
        </div>
        {msg && <div style={{ fontSize: 11, marginTop: 8, color: msg.includes('failed') ? 'var(--rose)' : 'var(--emerald)' }}>{msg}</div>}
      </div>

      {/* Workflow board — 6-stage strip */}
      {workflow && (
        <div className="card">
          <div className="card-header">Workflow board</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: 8 }}>
            {['detect','diagnose','prioritize','execute','publish','remeasure'].map(stage => {
              const count = workflow.by_stage?.[stage] || 0;
              const stageColors = { detect: 'var(--blue)', diagnose: 'var(--purple)', prioritize: 'var(--amber)', execute: 'var(--cyan)', publish: 'var(--emerald)', remeasure: 'var(--rose)' };
              const c = stageColors[stage];
              return (
                <div key={stage} style={{ border: `1px solid var(--border-subtle)`, borderTop: `3px solid ${c}`, borderRadius: 6, padding: 10, background: 'var(--bg-raised)' }}>
                  <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>{stage}</div>
                  <div style={{ fontSize: 22, fontWeight: 700, color: c, marginTop: 4 }}>{count}</div>
                </div>
              );
            })}
          </div>
          {workflow.by_status && (
            <div style={{ marginTop: 10, display: 'flex', gap: 6, flexWrap: 'wrap' }}>
              {Object.entries(workflow.by_status).map(([k, v]) => (
                <span key={k} className="badge gray" style={{ fontSize: 10 }}>{k.replace(/_/g, ' ')}: {v}</span>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Pending re-tracks */}
      {workflow?.pending_retracks && (
        <div className="card">
          <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span>Pending re-tracks ({workflow.pending_retracks.length})</span>
            <button className="btn btn-sm btn-primary" onClick={runDueRetracks} disabled={busy || !wsId}>{busy ? 'Working…' : 'Run Due Re-tracks'}</button>
          </div>
          {workflow.pending_retracks.length === 0 ? (
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>No re-tracks scheduled.</div>
          ) : (
            <table className="data-table"><thead><tr><th>Action</th><th>Scheduled at</th></tr></thead><tbody>
              {workflow.pending_retracks.map((p, i) => (
                <tr key={p.id || i}>
                  <td>{p.title || p.action_title || p.action_id || '—'}</td>
                  <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{p.scheduled_at ? new Date(p.scheduled_at).toLocaleString() : '—'}</td>
                </tr>
              ))}
            </tbody></table>
          )}
        </div>
      )}

      {actions.length === 0 ? (
        <div className="card"><div className="empty-state">⚙<br/>No actions yet. Click Harvest Actions to scan your diagnostics.</div></div>
      ) : actions.map(a => (
        <div className="card" key={a.id} style={{ display: 'grid', gap: 8 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 8 }}>
            <div style={{ flex: 1 }}>
              <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 4, flexWrap: 'wrap' }}>
                <span className={`badge ${priColor(a.priority || 0)}`}>{a.action_type}</span>
                <span className="badge gray">{a.status}</span>
                {a.confidence && <ConfidenceBadge level={a.confidence} />}
                {a.estimated_impact != null && <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>impact {a.estimated_impact}</span>}
                {(a.before_snapshot_at || a.after_snapshot_at) && (
                  <span style={{ fontSize: 10, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>
                    {a.before_snapshot_at && `before: ${new Date(a.before_snapshot_at).toLocaleDateString()}`}
                    {a.before_snapshot_at && a.after_snapshot_at && ' · '}
                    {a.after_snapshot_at && `after: ${new Date(a.after_snapshot_at).toLocaleDateString()}`}
                  </span>
                )}
              </div>
              <div style={{ fontWeight: 600 }}>{a.title}</div>
              {a.description && <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>{a.description}</div>}
            </div>
            <div style={{ display: 'flex', gap: 6, flexShrink: 0, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
              {a.status === 'pending' && <button className="btn btn-sm btn-primary" onClick={() => generate(a.id)} disabled={busy}>Generate</button>}
              {a.status === 'generated' && <>
                <button className="btn btn-sm btn-primary" onClick={() => setStatus(a.id, 'approved')} disabled={busy}>Approve</button>
                <button className="btn btn-sm" onClick={() => setStatus(a.id, 'dismissed')} disabled={busy}>Dismiss</button>
              </>}
              {a.status === 'approved' && <button className="btn btn-sm btn-primary" onClick={() => setStatus(a.id, 'done')} disabled={busy}>Mark Done</button>}
              <button className="btn btn-sm" onClick={() => beforeSnapshot(a.id)} disabled={busy} title="Snapshot baseline metrics now">Before snapshot</button>
              <button className="btn btn-sm" onClick={() => afterSnapshot(a.id)} disabled={busy} title="Snapshot post-action metrics">After snapshot</button>
              <button className="btn btn-sm" onClick={() => scheduleRetrack(a.id)} disabled={busy} title="Schedule a re-track in 7 days">Schedule re-track</button>
              {a.generated_output && <button className="btn btn-sm" onClick={() => setExpanded(x => ({ ...x, [a.id]: !x[a.id] }))}>{expanded[a.id] ? 'Hide' : 'View'} Output</button>}
            </div>
          </div>
          {a.generated_output && expanded[a.id] && (
            <pre style={{ fontFamily: 'var(--font-mono)', fontSize: 11, maxHeight: 240, overflow: 'auto', background: 'var(--surface-2)', padding: 10, borderRadius: 6, whiteSpace: 'pre-wrap', margin: 0 }}>{typeof a.generated_output === 'string' ? a.generated_output : JSON.stringify(a.generated_output, null, 2)}</pre>
          )}
        </div>
      ))}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// GEO SANDBOX — prove the ROI
// ═══════════════════════════════════════════════════════════════

function BacktestPage({ state }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [actionCount, setActionCount] = useState(3);
  const [horizon, setHorizon] = useState(90);
  const [result, setResult] = useState(null);
  const [series, setSeries] = useState([]);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState('');

  useEffect(() => {
    if (!wsId) return;
    setResult(null);
    api(`/api/backtest/${wsId}/series?months=6`, {}, token).then(r => setSeries(r.data || [])).catch(() => {});
  }, [wsId]);

  const project = async () => {
    setBusy(true); setMsg('');
    try {
      const r = await api(`/api/backtest/${wsId}/project?action_count=${actionCount}&horizon_days=${horizon}`, { method: 'POST' }, token);
      setResult(r.data || r);
    } catch (e) { setMsg('Projection failed: ' + e.message); }
    setBusy(false);
  };

  const maxScore = Math.max(1, ...series.map(s => s.total_score || 0));

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>GEO Sandbox — prove the ROI</span>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <input className="form-input" type="number" value={actionCount} min={1} onChange={e => setActionCount(parseInt(e.target.value, 10) || 1)} title="action count" style={{ width: 80 }} />
            <select className="form-input" value={horizon} onChange={e => setHorizon(parseInt(e.target.value, 10))} style={{ width: 110 }}>
              <option value={30}>30 days</option>
              <option value={60}>60 days</option>
              <option value={90}>90 days</option>
              <option value={180}>180 days</option>
            </select>
            <button className="btn btn-sm btn-primary" onClick={project} disabled={busy || !wsId}>{busy ? 'Running...' : 'Run Projection'}</button>
          </div>
        </div>
        {msg && <div style={{ fontSize: 11, color: 'var(--rose)' }}>{msg}</div>}
        {series.length > 0 && (
          <div style={{ marginTop: 8 }}>
            <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 4 }}>Historical authority ({series.length} pts)</div>
            <div style={{ display: 'flex', alignItems: 'flex-end', gap: 3, height: 60 }}>
              {series.map((s, i) => (
                <div key={i} title={`${s.date}: ${Math.round(s.total_score || 0)}${s.source !== 'measured' ? ' (~)' : ''}`}
                  style={{ flex: 1, height: `${((s.total_score || 0) / maxScore) * 100}%`, minHeight: 2, background: s.source === 'measured' ? 'var(--blue)' : 'var(--border-subtle)', borderRadius: 2 }} />
              ))}
            </div>
          </div>
        )}
      </div>

      {!result ? (
        <div className="card"><div className="empty-state">⌬<br/>Run a projection to model the ROI of your next actions.</div></div>
      ) : (
        <>
          <div className="card">
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
              <div className="metric-card" style={{ borderTop: '2px solid var(--blue)' }}>
                <div className="metric-label">BASELINE SCORE</div>
                <div className="metric-value">{Math.round(result.baseline_score || 0)}</div>
              </div>
              <div className="metric-card" style={{ borderTop: '2px solid var(--emerald)' }}>
                <div className="metric-label">PROJECTED SCORE</div>
                <div className="metric-value" style={{ color: 'var(--emerald)' }}>{Math.round(result.projected_score || 0)}</div>
              </div>
              <div className="metric-card" style={{ borderTop: '2px solid var(--emerald)' }}>
                <div className="metric-label">LIFT ({result.horizon_days || horizon}d)</div>
                <div className="metric-value" style={{ color: 'var(--emerald)' }}>+{(result.lift || 0).toFixed(1)}</div>
              </div>
            </div>
          </div>

          {result.narrative && (
            <div className="card" style={{ background: 'linear-gradient(135deg, rgba(16,185,129,0.08), rgba(99,102,241,0.04))' }}>
              <div style={{ fontStyle: 'italic', fontSize: 15, color: 'var(--text-secondary)' }}>{result.narrative}</div>
            </div>
          )}

          {Array.isArray(result.per_action) && result.per_action.length > 0 && (
            <div className="card">
              <div className="card-header">Per-action projection</div>
              <table className="data-table"><thead><tr>
                <th>Prompt</th><th>Current</th><th>Projected</th><th>€ Revenue</th><th>Contribution</th>
              </tr></thead><tbody>{result.per_action.map((p, i) => (
                <tr key={i}>
                  <td style={{ fontSize: 11 }}>{p.prompt_text}</td>
                  <td>{Math.round(p.current_our_score || 0)}</td>
                  <td style={{ color: 'var(--emerald)', fontWeight: 600 }}>{Math.round(p.projected_our_score || 0)}</td>
                  <td>€{Math.round(p.revenue_eur || 0).toLocaleString()}</td>
                  <td>{(p.contribution_to_lift || 0).toFixed(1)}</td>
                </tr>
              ))}</tbody></table>
            </div>
          )}

          {result.assumptions && (
            <div className="card">
              <div className="card-header">Assumptions</div>
              <ul style={{ margin: 0, paddingLeft: 18, fontSize: 11, color: 'var(--text-muted)' }}>
                {Object.entries(result.assumptions).map(([k, v]) => (
                  <li key={k}>{k}: {typeof v === 'object' ? JSON.stringify(v) : String(v)}</li>
                ))}
              </ul>
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// INTEGRATIONS & HEALTH
// ═══════════════════════════════════════════════════════════════

function IntegrationsPage() {
  const { token } = useContext(AuthContext);
  const [status, setStatus] = useState(null);
  const [health, setHealth] = useState(null);

  useEffect(() => {
    api(`/api/integrations/status`, {}, token).then(r => setStatus(r.data || r)).catch(() => {});
    api(`/api/health/deep`, {}, token).then(r => setHealth(r.data || r)).catch(() => {});
  }, []);

  const integrations = (status?.integrations || []).slice().sort((a, b) => {
    const aTop = a.required && !a.configured ? 0 : 1;
    const bTop = b.required && !b.configured ? 0 : 1;
    if (aTop !== bTop) return aTop - bTop;
    return (a.configured === b.configured) ? 0 : (a.configured ? 1 : -1);
  });
  const dbOk = health?.db === 'ok' || health?.db === true;
  const tracking = status?.tracking_mode;

  return (
    <div className="fade-in" style={{ display: 'grid', gap: 16 }}>
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>Integrations &amp; Health</span>
          {tracking && <span className={`badge ${tracking === 'live' ? 'emerald' : 'amber'}`}>{tracking}</span>}
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
          <div className="metric-card" style={{ borderTop: `2px solid ${dbOk ? 'var(--emerald)' : 'var(--rose)'}` }}>
            <div className="metric-label">DATABASE</div>
            <div className="metric-value" style={{ color: dbOk ? 'var(--emerald)' : 'var(--rose)', fontSize: 18 }}>{dbOk ? 'ok' : 'error'}</div>
            <div style={{ fontSize: 10, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', overflow: 'hidden', textOverflow: 'ellipsis' }}>{health?.db_path || '—'}</div>
          </div>
          <div className="metric-card" style={{ borderTop: '2px solid var(--blue)' }}>
            <div className="metric-label">TABLES</div>
            <div className="metric-value">{Array.isArray(health?.tables) ? health.tables.length : (health?.tables ?? '—')}</div>
          </div>
          <div className="metric-card" style={{ borderTop: `2px solid ${health?.db_writable ? 'var(--emerald)' : 'var(--amber)'}` }}>
            <div className="metric-label">WRITABLE</div>
            <div className="metric-value" style={{ fontSize: 18 }}>{health?.db_writable ? 'yes' : 'no'}</div>
          </div>
          <div className="metric-card" style={{ borderTop: '2px solid var(--emerald)' }}>
            <div className="metric-label">CONFIGURED</div>
            <div className="metric-value">{summaryConfigured(status)}</div>
          </div>
        </div>
        {status?.summary?.required_missing?.length > 0 && (
          <div style={{ fontSize: 12, color: 'var(--rose)', marginTop: 8 }}>⚠ Required missing: {status.summary.required_missing.join(', ')}</div>
        )}
      </div>

      {integrations.length === 0 ? (
        <div className="card"><div className="empty-state">☰<br/>No integration data.</div></div>
      ) : (
        <div className="card">
          <div className="card-header">Integrations</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: 12 }}>
            {integrations.map(it => (
              <div key={it.key} style={{ border: '1px solid var(--border-subtle)', borderRadius: 8, padding: 12 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                  <span style={{ fontWeight: 600 }}>{it.label}</span>
                  {it.required && <span className="badge amber" style={{ fontSize: 9 }}>required</span>}
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
                  <span style={{ width: 8, height: 8, borderRadius: '50%', background: it.configured ? 'var(--emerald)' : 'var(--text-muted)', display: 'inline-block' }} />
                  <span style={{ fontSize: 12, color: it.configured ? 'var(--emerald)' : 'var(--text-muted)' }}>{it.configured ? 'Connected' : 'Not connected'}</span>
                  {it.category && <span className="badge gray" style={{ fontSize: 9, marginLeft: 'auto' }}>{it.category}</span>}
                </div>
                {it.unlocks && <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>Unlocks: {Array.isArray(it.unlocks) ? it.unlocks.join(', ') : it.unlocks}</div>}
                {!it.configured && it.hint && <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4 }}>{it.hint}</div>}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function summaryConfigured(status) {
  if (!status?.summary) return '—';
  return `${status.summary.configured ?? 0} / ${status.summary.total ?? 0}`;
}


// ═══════════════════════════════════════════════════════════════
// MAIN APP
// ═══════════════════════════════════════════════════════════════

function App() {
  const [state, dispatch] = useReducer(reducer, initialState);

  // Wire up global 401 handler — auto-logout on expired tokens
  useEffect(() => {
    setAuthErrorHandler(() => {
      localStorage.removeItem('geo_token');
      dispatch({ type: 'LOGOUT' });
    });
    return () => setAuthErrorHandler(null);
  }, []);

  // Auto-login from stored token
  useEffect(() => {
    const stored = localStorage.getItem('geo_token');
    if (stored) {
      api('/api/auth/me', {}, stored)
        .then(res => {
          if (res.success) {
            dispatch({
              type: 'LOGIN',
              user: res.data.user,
              token: stored,
              workspaces: res.data.workspaces,
            });
          } else {
            localStorage.removeItem('geo_token');
          }
        })
        .catch(() => localStorage.removeItem('geo_token'));
    }
  }, []);

  const handleLogin = (data) => {
    localStorage.setItem('geo_token', data.token);
    dispatch({ type: 'LOGIN', user: data.user, token: data.token, workspaces: data.workspaces || [] });
  };

  if (!state.user) {
    return <LoginScreen onLogin={handleLogin} />;
  }

  const viewMap = {
    // ── War Room (default landing) ──
    warroom: WarRoomPage,
    battlefield: PromptBattlefieldPage,
    revenue: RevenuePriorityPage,
    authority: AuthorityScorePage,
    metrics: MetricDictionaryPage,
    // ── Intelligence ──
    citation_intel: CitationIntelPage,
    attack_map: AttackMapPage,
    graph: AuthorityGraphPage,
    journey: BuyerJourneyPage,
    aio: AioOverviewPage,
    // ── Action ──
    actions: ActionsPage,
    reddit: RedditCommandPage,
    schema_engine: SchemaEnginePage,
    metadata_studio: MetadataStudioPage,
    youtube: YouTubeGeoPage,
    content: ContentStudioPage,
    publishing: PublishingPage,
    // ── Ops ──
    import: DataImportPage,
    sources: SourcesPage,
    analysis: AnalysisPage,
    brands: BrandManagerPage,
    alerts: AlertsPage,
    backtest: BacktestPage,
    report: ComparativeReportPage,
    jobs: JobQueuePage,
    // ── Admin ──
    workspaces: WorkspacesPage,
    users: UsersPage,
    competitors: CompetitorsPage,
    competitor_profiles: CompetitorProfilesPage,
    integrations: IntegrationsPage,
    audit: AuditPage,
    settings: SettingsPage,
  };

  const ViewComponent = viewMap[state.view] || WarRoomPage;

  return (
    <AuthContext.Provider value={{ user: state.user, token: state.token }}>
      <div className="app-shell">
        <Sidebar state={state} dispatch={dispatch} />
        <div className="main-content">
          <Header state={state} />
          <div className="main-body">
            <ViewComponent state={state} dispatch={dispatch} />
          </div>
        </div>
      </div>

      {/* Global error toast */}
      {state.error && (
        <div style={{
          position: 'fixed', bottom: 20, right: 20, background: 'var(--rose-dim)',
          border: '1px solid var(--rose)', color: 'var(--rose)', padding: '10px 16px',
          borderRadius: 8, fontSize: 12, maxWidth: 360, zIndex: 9999,
          display: 'flex', alignItems: 'center', gap: 10,
        }}>
          <span>{state.error}</span>
          <button style={{ background: 'none', border: 'none', color: 'var(--rose)', cursor: 'pointer', fontSize: 16 }}
            onClick={() => dispatch({ type: 'SET_ERROR', error: null })}>{'\u00D7'}</button>
        </div>
      )}
    </AuthContext.Provider>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App />);
