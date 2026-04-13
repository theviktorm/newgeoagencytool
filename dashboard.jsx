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
  view: 'overview',
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
  { section: 'Analytics', items: [
    { id: 'overview', label: 'Overview', icon: '\u25A3' },
    { id: 'performance', label: 'Performance', icon: '\u2191' },
    { id: 'deltas', label: 'Content Deltas', icon: '\u0394' },
    { id: 'models', label: 'Model Breakdown', icon: '\u2630' },
  ]},
  { section: 'Pipeline', items: [
    { id: 'import', label: 'Data Import', icon: '\u21E9' },
    { id: 'sources', label: 'Sources', icon: '\u25C9' },
    { id: 'scraper', label: 'Scraper', icon: '\u2692' },
    { id: 'analysis', label: 'Analysis', icon: '\u2B21' },
    { id: 'content', label: 'Content Studio', icon: '\u270E' },
    { id: 'publishing', label: 'Publishing', icon: '\u21EA' },
  ]},
  { section: 'Operations', items: [
    { id: 'pipeline', label: 'Pipeline Activity', icon: '\u25B6' },
    { id: 'tasks', label: 'Task Board', icon: '\u2611' },
    { id: 'campaigns', label: 'Campaigns', icon: '\u2691' },
    { id: 'reports', label: 'Reports', icon: '\u2637' },
    { id: 'playbooks', label: 'Playbooks', icon: '\u2263' },
  ]},
  { section: 'Intelligence', items: [
    { id: 'competitors', label: 'Competitors', icon: '\u2694' },
    { id: 'recommendations', label: 'Recommendations', icon: '\u2605' },
    { id: 'onboarding', label: 'Onboarding', icon: '\u2714' },
  ]},
  { section: 'Administration', items: [
    { id: 'workspaces', label: 'Workspaces', icon: '\u2302' },
    { id: 'users', label: 'Team', icon: '\u263A' },
    { id: 'billing', label: 'Billing', icon: '\u20AC' },
    { id: 'jobs', label: 'Job Queue', icon: '\u21BB' },
    { id: 'automations', label: 'Automations', icon: '\u26A1' },
    { id: 'prompts', label: 'Prompts', icon: '\u2756' },
    { id: 'monitoring', label: 'Monitoring', icon: '\u2261' },
    { id: 'audit', label: 'Audit Log', icon: '\u2318' },
    { id: 'settings', label: 'Settings', icon: '\u2699' },
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

  const loadWorkspaces = async () => {
    const res = await api('/api/workspaces/', {}, state.token);
    if (res.success) dispatch({ type: 'SET_WORKSPACES', workspaces: res.data });
  };

  const loadDetail = async (wsId) => {
    const res = await api(`/api/workspaces/${wsId}`, {}, state.token);
    if (res.success) setDetail(res.data);
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
          <div className="card-header"><span className="card-title">Workspace Details</span></div>
          <div className="card-body">
            {detail ? (
              <div>
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
// RECOMMENDATIONS PAGE
// ═══════════════════════════════════════════════════════════════

function RecommendationsPage({ state }) {
  const wsId = state.activeWorkspace?.id;
  const [recs, setRecs] = useState([]);
  const [filter, setFilter] = useState('new');

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

  const typeColors = { new_topic: 'emerald', refresh: 'amber', gap: 'rose', opportunity: 'blue', competitor_alert: 'purple' };

  return (
    <div className="fade-in">
      <div className="flex-between mb-md">
        <div style={{ display: 'flex', gap: 6 }}>
          {['new', 'accepted', 'dismissed', 'completed'].map(s => (
            <button key={s} className={`btn btn-sm ${filter === s ? 'btn-primary' : ''}`} onClick={() => setFilter(s)}>{s}</button>
          ))}
        </div>
      </div>

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
  const [importResult, setImportResult] = useState(null);
  const [tab, setTab] = useState('upload');
  const recPag = usePagination(records, 50);

  useEffect(() => {
    if (!wsId) return;
    api('/api/peec/records/' + wsId, {}, token).then(r => setRecords(r.records || [])).catch(err => console.warn('API:', err.message));
    api('/api/peec/field-mapping', {}, token).then(r => setFieldMapping(r)).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const handleCsvImport = async () => {
    if (!csvText.trim()) return;
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
        setImportResult({ success: true, count: d.records || d.imported || 0, msg: `Imported ${d.records || 0} records, ${d.sources || 0} sources, ${d.clusters || 0} clusters` });
        api('/api/peec/records/' + wsId, {}, token).then(r => setRecords(r.data || r.records || [])).catch(err => console.warn('API:', err.message));
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
        {['upload', 'api', 'records', 'mapping'].map(t => (
          <button key={t} className={`btn ${tab === t ? 'btn-primary' : ''}`} onClick={() => setTab(t)}>
            {t === 'upload' ? 'CSV Upload' : t === 'api' ? 'Peec API Sync' : t === 'records' ? `Records (${records.length})` : 'Field Mapping'}
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
              <th>Prompt</th><th>URL</th><th>Model</th><th>Citations</th><th>Rate</th><th>Topic</th><th>Imported</th>
            </tr></thead><tbody>{recPag.paged.map((r, i) => (
              <tr key={i}>
                <td style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>{r.prompt || r.query || '—'}</td>
                <td style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>{r.url || '—'}</td>
                <td><span className="badge">{r.model || r.platform || '—'}</span></td>
                <td>{r.citation_count ?? '—'}</td>
                <td>{r.citation_rate != null ? (r.citation_rate * 100).toFixed(1) + '%' : '—'}</td>
                <td>{r.topic || r.tag || '—'}</td>
                <td style={{ fontSize: 10, color: 'var(--text-muted)' }}>{r.imported_at ? new Date(r.imported_at).toLocaleDateString() : '—'}</td>
              </tr>
            ))}</tbody></table>
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
    api('/api/sources/' + wsId, {}, token).then(r => setSources(r.sources || [])).catch(err => console.warn('API:', err.message));
    api('/api/clusters/' + wsId, {}, token).then(r => setClusters(r.clusters || [])).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const sorted = [...sources].sort((a, b) => (b[sortBy] || 0) - (a[sortBy] || 0));
  const filtered = filterTopic === 'all' ? sorted : sorted.filter(s => s.topic === filterTopic);
  const srcPag = usePagination(filtered, 50);
  const topics = [...new Set(sources.map(s => s.topic).filter(Boolean))];
  const domains = {};
  sources.forEach(s => {
    try { const d = new URL(s.url).hostname; domains[d] = (domains[d] || 0) + (s.citation_count || 0); } catch {}
  });
  const topDomains = Object.entries(domains).sort((a, b) => b[1] - a[1]).slice(0, 10);

  return (
    <div style={{ display: 'grid', gap: 16 }}>
      <div className="metrics-grid">
        <div className="metric-card"><div className="metric-label">TOTAL SOURCES</div><div className="metric-value">{sources.length}</div></div>
        <div className="metric-card"><div className="metric-label">UNIQUE DOMAINS</div><div className="metric-value">{Object.keys(domains).length}</div></div>
        <div className="metric-card"><div className="metric-label">TOPIC CLUSTERS</div><div className="metric-value">{clusters.length}</div></div>
        <div className="metric-card"><div className="metric-label">TOTAL CITATIONS</div><div className="metric-value">{sources.reduce((s, r) => s + (r.citation_count || 0), 0)}</div></div>
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
              <th>#</th><th>URL</th><th>Citations</th><th>Rate</th><th>Visibility</th><th>Quality</th><th>Models</th>
            </tr></thead><tbody>{srcPag.paged.map((s, i) => (
              <tr key={i}>
                <td style={{ color: 'var(--text-muted)' }}>{srcPag.page * 50 + i + 1}</td>
                <td style={{ maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', fontFamily: 'var(--font-mono)', fontSize: 11 }}>{s.url}</td>
                <td style={{ fontWeight: 600, color: 'var(--emerald)' }}>{s.citation_count || 0}</td>
                <td>{s.citation_rate != null ? (s.citation_rate * 100).toFixed(1) + '%' : '—'}</td>
                <td>{s.visibility != null ? s.visibility.toFixed(2) : '—'}</td>
                <td>{s.quality_score != null ? s.quality_score.toFixed(1) : '—'}</td>
                <td style={{ fontSize: 10 }}>{s.models_cited_in || '—'}</td>
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
                  <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{c.record_count || 0} records</span>
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
        setBatchStatus(r);
      } else {
        const r = await api('/api/sources/scrape', {
          method: 'POST', body: JSON.stringify({ project_id: wsId, urls: urlList })
        }, token);
        setResults(r.results || []);
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
    api('/api/clusters/' + wsId, {}, token).then(r => setClusters(r.clusters || [])).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  useEffect(() => {
    if (!selectedCluster) return;
    api('/api/analyze/' + selectedCluster.id, {}, token).then(r => setAnalyses(r.analyses || [])).catch(err => console.warn('API:', err.message));
    api('/api/briefs/' + selectedCluster.id, {}, token).then(r => setBriefs(r.briefs || [])).catch(err => console.warn('API:', err.message));
  }, [selectedCluster]);

  const runAnalysis = async () => {
    if (!selectedCluster) return;
    setRunning(true);
    try {
      const r = await api('/api/analyze', {
        method: 'POST', body: JSON.stringify({ cluster_id: selectedCluster.id, project_id: wsId })
      }, token);
      setAnalyses(prev => [r, ...prev]);
    } catch (e) { alert(e.message); }
    setRunning(false);
  };

  const generateBrief = async (analysisId) => {
    setGenBrief(true);
    try {
      const r = await api('/api/analyze/brief', {
        method: 'POST', body: JSON.stringify({ analysis_id: analysisId })
      }, token);
      setBriefs(prev => [r, ...prev]);
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
              <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>{c.record_count || 0} records · {c.intent || 'mixed'}</div>
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
                  <div style={{ fontSize: 11 }}><span style={{ color: 'var(--text-muted)' }}>Intent:</span> {selectedCluster.intent || '—'}</div>
                  <div style={{ fontSize: 11 }}><span style={{ color: 'var(--text-muted)' }}>Records:</span> {selectedCluster.record_count || 0}</div>
                  <div style={{ fontSize: 11 }}><span style={{ color: 'var(--text-muted)' }}>Sources:</span> {selectedCluster.source_count || 0}</div>
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
    api(`/api/drafts/${wsId}${params}`, {}, token).then(r => setDrafts(r.drafts || [])).catch(err => console.warn('API:', err.message));
  }, [wsId, statusFilter]);

  useEffect(() => {
    if (!selectedDraft) return;
    api('/api/drafts/detail/' + selectedDraft, {}, token).then(r => setDraftDetail(r)).catch(err => console.warn('API:', err.message));
  }, [selectedDraft]);

  const handleReview = async (action) => {
    if (!selectedDraft) return;
    try {
      await api('/api/review/' + selectedDraft, {
        method: 'PUT', body: JSON.stringify({ status: action, notes: reviewNote })
      }, token);
      setReviewNote('');
      setReviewAction(null);
      api('/api/drafts/detail/' + selectedDraft, {}, token).then(r => setDraftDetail(r)).catch(err => console.warn('API:', err.message));
      const params = statusFilter !== 'all' ? `?status=${statusFilter}` : '';
      api(`/api/drafts/${wsId}${params}`, {}, token).then(r => setDrafts(r.drafts || [])).catch(err => console.warn('API:', err.message));
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
    api(`/api/drafts/${wsId}?status=approved`, {}, token).then(r => setDrafts(r.drafts || [])).catch(err => console.warn('API:', err.message));
    api('/api/exports/' + wsId, {}, token).then(r => setExports(r.exports || [])).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const checkCms = async (cmsType) => {
    try {
      const r = await api('/api/publish/check/' + cmsType, { method: 'POST' }, token);
      setCmsStatus(prev => ({ ...prev, [cmsType]: r.connected ? 'connected' : 'failed' }));
    } catch { setCmsStatus(prev => ({ ...prev, [cmsType]: 'failed' })); }
  };

  const handleExport = async () => {
    if (!selectedDraft) return;
    setExporting(true);
    try {
      const r = await api('/api/export', {
        method: 'POST', body: JSON.stringify({ draft_id: selectedDraft, format: exportFormat, project_id: wsId })
      }, token);
      setExportResult(r);
      api('/api/exports/' + wsId, {}, token).then(r => setExports(r.exports || [])).catch(err => console.warn('API:', err.message));
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
      alert('Published successfully! URL: ' + (r.url || r.published_url || 'N/A'));
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
    api('/api/prompts/' + wsId, {}, token).then(r => setTemplates(r.templates || [])).catch(err => console.warn('API:', err.message));
  }, [wsId]);

  const handleSave = async () => {
    try {
      await api('/api/prompts', {
        method: 'POST', body: JSON.stringify({ project_id: wsId, ...form })
      }, token);
      setShowCreate(false);
      setForm({ name: '', type: 'source_analysis', template: '', model: 'claude-sonnet-4-5-20250514', temperature: 0.3, max_tokens: 4096 });
      api('/api/prompts/' + wsId, {}, token).then(r => setTemplates(r.templates || [])).catch(err => console.warn('API:', err.message));
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
    api('/api/config', {}, token).then(r => setConfig(r)).catch(err => console.warn('API:', err.message));
    if (!wsId) return;
    api('/api/ops/jobs/' + wsId + '/stats', {}, token).then(r => setJobStats(r)).catch(err => console.warn('API:', err.message));
    api('/api/usage/' + wsId, {}, token).then(r => setUsage(r)).catch(err => console.warn('API:', err.message));
    api('/api/usage/' + wsId + '/history', {}, token).then(r => setUsageHistory(r.history || [])).catch(err => console.warn('API:', err.message));
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
    overview: OverviewPage,
    performance: PerformancePage,
    deltas: DeltasPage,
    models: ModelsPage,
    import: DataImportPage,
    sources: SourcesPage,
    scraper: ScraperPage,
    analysis: AnalysisPage,
    content: ContentStudioPage,
    publishing: PublishingPage,
    pipeline: PipelineActivityPage,
    campaigns: CampaignsPage,
    reports: ReportsPage,
    workspaces: WorkspacesPage,
    users: UsersPage,
    audit: AuditPage,
    settings: SettingsPage,
    tasks: TaskBoardPage,
    playbooks: PlaybooksPage,
    competitors: CompetitorsPage,
    recommendations: RecommendationsPage,
    onboarding: OnboardingPage,
    billing: BillingPage,
    jobs: JobQueuePage,
    automations: AutomationsPage,
    prompts: PromptTemplatesPage,
    monitoring: MonitoringPage,
  };

  const ViewComponent = viewMap[state.view] || OverviewPage;

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
