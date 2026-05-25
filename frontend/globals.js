// ─────────────────────────────────────────────────────────────────────────
// Global shim for the Babel-free production bundle.
//
// dashboard.jsx (the ~5700-line single-file app) was written for the
// Babel-in-browser setup where React / ReactDOM are UMD globals loaded from
// unpkg. Its very first line does:
//
//     const { useState, useEffect, ... } = React;
//
// and it mounts with:
//
//     ReactDOM.createRoot(document.getElementById('root')).render(<App/>);
//
// To bundle it with Vite WITHOUT touching that file, we expose React and a
// React-18-compatible ReactDOM (legacy API + the createRoot client API) on
// `window` BEFORE the dashboard module body runs.
//
// IMPORTANT: this lives in its own module (not inline in main.jsx) because ES
// module `import` statements are hoisted above ordinary statements. A bare
// `window.React = React` in main.jsx would execute AFTER `import './dashboard'`
// and the dashboard's top-level `const {...} = React` would blow up. By making
// the assignment a side-effect import that main.jsx lists first, it is
// guaranteed to run before the dashboard module is evaluated.
// ─────────────────────────────────────────────────────────────────────────
import React from 'react';
import * as ReactDOMLegacy from 'react-dom';
import * as ReactDOMClient from 'react-dom/client';

// React (provides hooks via destructuring: `const { useState } = React`).
window.React = React;

// ReactDOM: merge the legacy namespace (render, unmountComponentAtNode, ...)
// with the React 18 client namespace (createRoot, hydrateRoot). dashboard.jsx
// uses `ReactDOM.createRoot`, which only exists on react-dom/client.
window.ReactDOM = { ...ReactDOMLegacy, ...ReactDOMClient };

// Defensive: also expose the common hooks as bare globals. dashboard.jsx pulls
// them off `React` via destructuring so this is not strictly required, but it
// makes the shim robust if any hook were ever referenced bare.
const {
  useState, useEffect, useContext, useReducer, useRef,
  useMemo, useCallback, useLayoutEffect, createContext,
} = React;
Object.assign(window, {
  useState, useEffect, useContext, useReducer, useRef,
  useMemo, useCallback, useLayoutEffect, createContext,
});
