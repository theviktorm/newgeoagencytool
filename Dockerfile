# ─────────────────────────────────────────────────────────────────────────
# STAGE 1 — build the Babel-free Vite bundle (frontend/dist/dashboard.bundle.js)
#
# Best-effort: if this stage fails or the bundle is missing, the runtime
# index.html falls back to the in-browser React + Babel path, so the app still
# works exactly as before. React is bundled INTO dashboard.bundle.js here.
# ─────────────────────────────────────────────────────────────────────────
FROM node:20-slim AS frontend-build

WORKDIR /app

# Install JS deps first for better layer caching.
COPY frontend/package.json ./frontend/package.json
RUN cd frontend && (npm ci || npm install)

# The Vite entry (frontend/main.jsx) imports ../dashboard.jsx, so the dashboard
# source must sit one level above the frontend/ build dir, matching the repo.
COPY frontend/ ./frontend/
COPY dashboard.jsx ./dashboard.jsx

# Produce frontend/dist/dashboard.bundle.js. Always ensure the dist dir exists
# (even on build failure) so the runtime COPY --from below never breaks.
RUN cd frontend && (npm run build || echo "frontend build skipped — runtime falls back to Babel") && mkdir -p dist


# ─────────────────────────────────────────────────────────────────────────
# STAGE 2 — Python runtime (unchanged behavior + the built bundle)
# ─────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY backend/ ./backend/

# Copy frontend files
COPY index.html ./frontend/index.html
COPY dashboard.jsx ./frontend/dashboard.jsx

# Copy the prebuilt bundle from the build stage. If the bundle was not produced
# the dir is still present (created above) and the runtime index.html falls back
# to the Babel path.
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

# Railway injects PORT env var at runtime
EXPOSE 8100

# Use shell form so $PORT is expanded at runtime
CMD uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8100}
