# infra/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SECUREDATAOPS_STATE_DIR=/tmp/securedataops_state

WORKDIR /app

# OS certs (good for HTTPS)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Install Python deps first (better cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ONLY backend code (avoid dashboard, tests, etc.)
COPY api/ api/
COPY etl/ etl/
COPY detection/ detection/

# Ensure state dir exists
RUN mkdir -p $SECUREDATAOPS_STATE_DIR

EXPOSE 8000

# Start consumer + API
CMD sh -lc "python -m etl.consumer_kinesis & exec uvicorn api.app:app --host 0.0.0.0 --port 8000"
