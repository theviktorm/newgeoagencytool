import sys

def repair():
    path = '/home/ubuntu/momentus_production_auth_fix/dashboard.jsx'
    with open(path, 'r') as f:
        lines = f.readlines()
    
    # 1. Find the start of the App component to fix the unclosed brace and duplication
    # We want to keep everything up to the first handleLogin in App, then fix the end of App.
    
    # Let's find the App function start
    app_start = -1
    for i, line in enumerate(lines):
        if 'function App()' in line:
            app_start = i
            break
            
    if app_start == -1:
        print("Could not find App component")
        return

    # Keep everything before App
    new_lines = lines[:app_start]
    
    # Add the full GeoWorkflowPage before App (cleaner structure)
    with open('/home/ubuntu/momentus_production_auth_fix/frontend_workflow_page.jsx', 'r') as f:
        workflow_content = f.read()
    
    new_lines.append("\n" + workflow_content + "\n")
    
    # Now reconstruct the App component from a known good template based on previous reads
    # but using the existing state/context from the file where possible.
    # Actually, let's just use the existing App up to the duplicated if statement, then close it.
    
    # Find handleLogin and the first if (!state.user)
    handle_login_start = -1
    for i in range(app_start, len(lines)):
        if 'const handleLogin = (data) => {' in lines[i]:
            handle_login_start = i
            break
            
    if handle_login_start == -1:
        print("Could not find handleLogin")
        return

    # Find the first if (!state.user) after handleLogin
    first_user_guard = -1
    for i in range(handle_login_start, len(lines)):
        if 'if (!state.user) {' in lines[i]:
            first_user_guard = i
            break
            
    if first_user_guard == -1:
        print("Could not find user guard")
        return

    # Add App start and everything up to the first user guard
    new_lines.extend(lines[app_start:first_user_guard])
    
    # Add the corrected App tail
    app_tail = """  if (!state.user) {
    return <LoginPage onLogin={handleLogin} error={loginError} loading={loginLoading} />;
  }

  const Page = viewMap[state.view] || (() => <div className="card">Page Not Found</div>);

  return (
    <div className="app-container">
      <Sidebar state={state} dispatch={dispatch} onLogout={handleLogout} />
      <main className="main-content">
        <header className="top-bar">
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <h2 style={{ margin: 0, fontSize: 18, fontWeight: 600 }}>
              {NAV_ITEMS.find(i => i.id === state.view)?.label || 'Dashboard'}
            </h2>
            {state.activeWorkspace && (
              <span className="badge" style={{ background: 'var(--blue-dim)', color: 'var(--blue)', border: '1px solid var(--blue)' }}>
                {state.activeWorkspace.name}
              </span>
            )}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 13, fontWeight: 500 }}>{state.user.username}</div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{state.user.role}</div>
            </div>
            <div className="avatar" style={{ width: 32, height: 32, background: 'var(--blue)', color: 'white', display: 'flex', alignItems: 'center', justifyContent: 'center', borderRadius: '50%', fontWeight: 600, fontSize: 14 }}>
              {state.user.username[0].toUpperCase()}
            </div>
          </div>
        </header>
        <div className="page-content">
          <Page state={state} dispatch={dispatch} />
        </div>
      </main>
    </div>
  );
}
"""
    new_lines.append(app_tail)
    
    with open(path + '.fixed', 'w') as f:
        f.writelines(new_lines)
    print("Repaired file written to dashboard.jsx.fixed")

repair()
