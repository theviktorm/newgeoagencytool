// Vite entry point for the production bundle.
//
// 1. './globals.js' runs first (side-effect import) and installs React /
//    ReactDOM on window so the global-style dashboard.jsx resolves.
// 2. The dashboard file lives at the repo ROOT (one level up); we import it
//    here so there is a SINGLE source of truth — no duplicated copy. Its body
//    ends with `ReactDOM.createRoot(...).render(<App/>)`, so importing it for
//    its side effect is enough to boot the app.
import './globals.js';
import '../dashboard.jsx';
