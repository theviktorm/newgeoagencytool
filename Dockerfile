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

# Railway injects PORT env var at runtime
EXPOSE 8100

# Use shell form so $PORT is expanded at runtime
CMD uvicorn backend.main:app --host 0.0.0.0 --port ${PORT:-8100}
