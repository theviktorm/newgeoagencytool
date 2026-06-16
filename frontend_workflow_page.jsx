/*
 MOMENTUS AI — GEO WORKFLOW PAGE
 Central project lifecycle, import batches, entity onboarding, audits, publish cycles
 ═══════════════════════════════════════════════════════════════ */

function GeoWorkflowPage({ state, dispatch }) {
  const { token } = useContext(AuthContext);
  const wsId = state.activeWorkspace?.id;
  const [projects, setProjects] = useState([]);
  const [selectedProject, setSelectedProject] = useState(null);
  const [showCreateProject, setShowCreateProject] = useState(false);
  const [projectForm, setProjectForm] = useState({ name: '', description: '' });
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState('');
  const [tab, setTab] = useState('projects'); // projects | batches | entities | audits | cycles

  const loadProjects = async () => {
    if (!wsId) return;
    try {
      const r = await api(`/api/workflow/${wsId}/projects`, {}, token);
      setProjects(r.data || r);
    } catch (e) { setMsg('Load failed: ' + e.message); }
  };

  const loadProjectDetail = async (projId) => {
    if (!wsId) return;
    try {
      const r = await api(`/api/workflow/${wsId}/projects/${projId}/summary`, {}, token);
      setSelectedProject(r.data || r);
    } catch (e) { setMsg('Load failed: ' + e.message); }
  };

  useEffect(() => {
    if (wsId) {
      loadProjects();
    }
  }, [wsId]);

  const handleCreateProject = async (e) => {
    e.preventDefault();
    if (!projectForm.name.trim()) {
      setMsg('Project name required');
      return;
    }
    setLoading(true);
    setMsg('');
    try {
      const r = await api(`/api/workflow/${wsId}/projects`, {
        method: 'POST',
        body: JSON.stringify(projectForm),
      }, token);
      if (r.success) {
        setMsg('Project created ✓');
        setProjectForm({ name: '', description: '' });
        setShowCreateProject(false);
        loadProjects();
      } else {
        setMsg('Create failed: ' + (r.error || 'unknown'));
      }
    } catch (e) { setMsg('Create failed: ' + e.message); }
    setLoading(false);
  };

  const handleSelectProject = (proj) => {
    setSelectedProject(proj);
    loadProjectDetail(proj.id);
    setTab('projects');
  };

  // ─── Project List View ───
  if (!selectedProject) {
    return (
      <div className="card">
        <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>GEO Projects</span>
          <button className="btn btn-sm btn-primary" onClick={() => setShowCreateProject(true)}>
            + New Project
          </button>
        </div>

        {showCreateProject && (
          <form onSubmit={handleCreateProject} style={{ borderBottom: '1px solid var(--border)', paddingBottom: 12, marginBottom: 12 }}>
            <div style={{ display: 'grid', gap: 8 }}>
              <input className="form-input" placeholder="Project name" value={projectForm.name}
                onChange={e => setProjectForm(f => ({ ...f, name: e.target.value }))} />
              <textarea className="form-input" placeholder="Description (optional)" rows="2" value={projectForm.description}
                onChange={e => setProjectForm(f => ({ ...f, description: e.target.value }))} />
              <div style={{ display: 'flex', gap: 8 }}>
                <button type="submit" className="btn btn-sm btn-primary" disabled={loading}>
                  {loading ? 'Creating...' : 'Create'}
                </button>
                <button type="button" className="btn btn-sm" onClick={() => {
                  setShowCreateProject(false);
                  setProjectForm({ name: '', description: '' });
                }}>Cancel</button>
              </div>
            </div>
          </form>
        )}

        {msg && (
          <div style={{ fontSize: 12, color: msg.includes('failed') ? 'var(--rose)' : 'var(--emerald)', marginBottom: 12 }}>
            {msg}
          </div>
        )}

        {projects.length === 0 ? (
          <div className="empty-state">📋<br/>No GEO projects yet. Create one to begin the workflow.</div>
        ) : (
          <div style={{ display: 'grid', gap: 8 }}>
            {projects.map(proj => (
              <div key={proj.id} className="sidebar-item" onClick={() => handleSelectProject(proj)}
                style={{ cursor: 'pointer', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                  <div style={{ fontWeight: 600 }}>{proj.name}</div>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                    {proj.status} • {proj.stage}
                  </div>
                </div>
                <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
                  <span className="badge gray">{proj.status}</span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  // ─── Project Detail View ───
  const proj = selectedProject.project || selectedProject;
  const batches = selectedProject.import_batches || [];
  const entities = selectedProject.entities || [];
  const audits = selectedProject.audits || [];
  const cycles = selectedProject.publish_cycles || [];
  const gates = selectedProject.approval_gates || [];

  return (
    <div style={{ display: 'grid', gap: 12 }}>
      <div className="card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <div>
            <button className="btn btn-sm" onClick={() => setSelectedProject(null)}>← Back</button>
            <h3 style={{ marginTop: 8 }}>{proj.name}</h3>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
              {proj.description}
            </div>
          </div>
          <div style={{ textAlign: 'right' }}>
            <div className="badge gray" style={{ marginBottom: 8 }}>{proj.status}</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{proj.stage}</div>
          </div>
        </div>

        {msg && (
          <div style={{ fontSize: 12, color: msg.includes('failed') ? 'var(--rose)' : 'var(--emerald)', marginBottom: 12 }}>
            {msg}
          </div>
        )}

        {/* Stage Progress */}
        <div style={{ display: 'grid', gap: 8, marginBottom: 16 }}>
          <div style={{ fontSize: 11, fontWeight: 600, textTransform: 'uppercase', color: 'var(--text-muted)' }}>
            Workflow Progress
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: 8 }}>
            {[
              { label: 'Brand Setup', key: 'brand_setup', icon: '🏢' },
              { label: 'Import Peec', key: 'import_peec', icon: '📥' },
              { label: 'Entity Onboarding', key: 'entity_onboarding', icon: '🔍' },
              { label: 'Audit', key: 'audit', icon: '✓' },
              { label: 'Actions', key: 'actions', icon: '⚙' },
              { label: 'Publish', key: 'publish', icon: '📤' },
              { label: 'Retrack', key: 'retrack', icon: '📊' },
            ].map(stage => {
              const complete = selectedProject.stage_progress?.[stage.key];
              return (
                <div key={stage.key} style={{
                  padding: 8, borderRadius: 6, background: complete ? 'var(--emerald-dim)' : 'var(--border-subtle)',
                  border: `1px solid ${complete ? 'var(--emerald)' : 'var(--border)'}`, textAlign: 'center', fontSize: 11
                }}>
                  <div style={{ fontSize: 14, marginBottom: 4 }}>{stage.icon}</div>
                  <div style={{ color: complete ? 'var(--emerald)' : 'var(--text-secondary)' }}>{stage.label}</div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Tabs */}
        <div style={{ display: 'flex', gap: 8, borderBottom: '1px solid var(--border)', marginBottom: 12 }}>
          {[
            { id: 'projects', label: `Milestones (${proj.milestones?.length || 0})` },
            { id: 'batches', label: `Import Batches (${batches.length})` },
            { id: 'entities', label: `Entities (${entities.length})` },
            { id: 'audits', label: `Audits (${audits.length})` },
            { id: 'cycles', label: `Publish Cycles (${cycles.length})` },
            { id: 'gates', label: `Approvals (${gates.length})` },
          ].map(t => (
            <button key={t.id} className="btn btn-sm" onClick={() => setTab(t.id)}
              style={{ borderBottom: tab === t.id ? '2px solid var(--blue)' : 'none', borderRadius: 0 }}>
              {t.label}
            </button>
          ))}
        </div>

        {/* Milestones Tab */}
        {tab === 'projects' && (
          <div style={{ display: 'grid', gap: 8 }}>
            {proj.milestones?.length === 0 ? (
              <div className="empty-state">📌<br/>No milestones recorded yet.</div>
            ) : (
              proj.milestones?.map((m, i) => (
                <div key={m.id} style={{ padding: 10, background: 'var(--bg-raised)', borderRadius: 6, borderLeft: '3px solid var(--blue)' }}>
                  <div style={{ fontWeight: 600, fontSize: 12 }}>{m.milestone_type}</div>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>
                    {new Date(m.completed_at).toLocaleString()}
                  </div>
                </div>
              ))
            )}
          </div>
        )}

        {/* Import Batches Tab */}
        {tab === 'batches' && (
          <div style={{ display: 'grid', gap: 8 }}>
            {batches.length === 0 ? (
              <div className="empty-state">📥<br/>No import batches yet.</div>
            ) : (
              <table className="data-table">
                <thead><tr>
                  <th>Type</th><th>Status</th><th>Records</th><th>Imported</th><th>Errors</th><th>Date</th>
                </tr></thead>
                <tbody>
                  {batches.map(b => (
                    <tr key={b.id}>
                      <td>{b.batch_type}</td>
                      <td><span className="badge gray">{b.status}</span></td>
                      <td>{b.record_count}</td>
                      <td>{b.imported_count}</td>
                      <td style={{ color: b.error_count > 0 ? 'var(--rose)' : 'var(--text-secondary)' }}>{b.error_count}</td>
                      <td style={{ fontSize: 11 }}>{new Date(b.created_at).toLocaleDateString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}

        {/* Entities Tab */}
        {tab === 'entities' && (
          <div style={{ display: 'grid', gap: 8 }}>
            {entities.length === 0 ? (
              <div className="empty-state">🔍<br/>No entities onboarded yet.</div>
            ) : (
              <table className="data-table">
                <thead><tr>
                  <th>Type</th><th>Name</th><th>Crawl Status</th><th>Consistency</th><th>Date</th>
                </tr></thead>
                <tbody>
                  {entities.map(e => (
                    <tr key={e.id}>
                      <td>{e.entity_type}</td>
                      <td>{e.name}</td>
                      <td><span className="badge gray">{e.crawl_status}</span></td>
                      <td>{e.consistency_score ? `${Math.round(e.consistency_score)}%` : '—'}</td>
                      <td style={{ fontSize: 11 }}>{new Date(e.created_at).toLocaleDateString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}

        {/* Audits Tab */}
        {tab === 'audits' && (
          <div style={{ display: 'grid', gap: 8 }}>
            {audits.length === 0 ? (
              <div className="empty-state">✓<br/>No audits run yet.</div>
            ) : (
              audits.map(a => (
                <div key={a.id} style={{ padding: 10, background: 'var(--bg-raised)', borderRadius: 6, borderLeft: `3px solid ${a.critical_count > 0 ? 'var(--rose)' : 'var(--emerald)'}` }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                      <div style={{ fontWeight: 600 }}>{a.audit_type}</div>
                      <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                        {a.issue_count} issues • {a.critical_count} critical
                      </div>
                    </div>
                    <span className="badge gray">{a.status}</span>
                  </div>
                </div>
              ))
            )}
          </div>
        )}

        {/* Publish Cycles Tab */}
        {tab === 'cycles' && (
          <div style={{ display: 'grid', gap: 8 }}>
            {cycles.length === 0 ? (
              <div className="empty-state">📤<br/>No publish cycles yet.</div>
            ) : (
              cycles.map(c => (
                <div key={c.id} style={{ padding: 10, background: 'var(--bg-raised)', borderRadius: 6 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                    <div style={{ fontWeight: 600 }}>Cycle #{c.cycle_number}</div>
                    <div style={{ display: 'flex', gap: 4 }}>
                      <span className="badge gray">{c.publish_status}</span>
                      <span className="badge gray">{c.retrack_status}</span>
                    </div>
                  </div>
                  {c.improvement > 0 && (
                    <div style={{ fontSize: 12, color: 'var(--emerald)' }}>
                      ↑ {c.improvement.toFixed(1)}% improvement
                    </div>
                  )}
                </div>
              ))
            )}
          </div>
        )}

        {/* Approvals Tab */}
        {tab === 'gates' && (
          <div style={{ display: 'grid', gap: 8 }}>
            {gates.length === 0 ? (
              <div className="empty-state">🔐<br/>No approval gates pending.</div>
            ) : (
              gates.map(g => (
                <div key={g.id} style={{ padding: 10, background: 'var(--bg-raised)', borderRadius: 6, borderLeft: `3px solid ${g.status === 'approved' ? 'var(--emerald)' : g.status === 'rejected' ? 'var(--rose)' : 'var(--amber)'}` }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                      <div style={{ fontWeight: 600 }}>{g.gate_type}</div>
                      <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                        Requested by {g.requested_by}
                      </div>
                    </div>
                    <span className="badge gray">{g.status}</span>
                  </div>
                </div>
              ))
            )}
          </div>
        )}
      </div>
    </div>
  );
}
