FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY backend/ ./backend/

# Railway provides PORT env var
ENV PORT=8100
EXPOSE ${PORT}

CMD python -m uvicorn backend.main:app --host 0.0.0.0 --port ${PORT}
