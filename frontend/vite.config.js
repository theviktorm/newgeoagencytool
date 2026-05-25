import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Build the single-file React dashboard into a Babel-free, minified bundle
// with React bundled IN. Output is a stable, hash-free filename so index.html
// can reference it directly (and fall back to the Babel path when absent).
export default defineConfig({
  // The dashboard.jsx source lives at the repo root (one level above this
  // config). Tell esbuild to treat .js files as JSX too, since dashboard.jsx
  // imported transitively is already .jsx — but main.jsx / globals.js are the
  // entry chain. Root must include the parent so importing '../dashboard.jsx'
  // is allowed.
  root: __dirname,

  plugins: [
    // Automatic JSX runtime: dashboard.jsx does not need a React import for
    // JSX (the plugin injects react/jsx-runtime). Its top-level
    // `const { useState } = React` still resolves to window.React (set by
    // globals.js) at runtime.
    react(),
  ],

  // Allow importing files outside the config dir (../dashboard.jsx).
  build: {
    outDir: resolve(__dirname, 'dist'),
    emptyOutDir: true,
    minify: 'esbuild',
    // We are not building an HTML page; we build a single JS library-style
    // bundle that index.html loads with a plain <script src>.
    rollupOptions: {
      input: resolve(__dirname, 'main.jsx'),
      output: {
        format: 'iife',
        entryFileNames: 'dashboard.bundle.js',
        // Inline any tiny dynamic chunks so we emit exactly one file.
        inlineDynamicImports: true,
        // Keep CSS (none expected) out of the way.
        assetFileNames: 'dashboard.[ext]',
      },
      // React is bundled IN (NOT external) on purpose.
    },
    // Single self-contained file — no code splitting.
    cssCodeSplit: false,
    // Quiet the "chunk too large" warning; this app is intentionally one big
    // bundle.
    chunkSizeWarningLimit: 4000,
  },
});
