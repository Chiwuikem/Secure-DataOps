# infra/Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SECUREDATAOPS_STATE_DIR=/tmp/securedataops_state

WORKDIR /app

# CA certs for TLS
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .
RUN mkdir -p $SECUREDATAOPS_STATE_DIR

EXPOSE 8000

# Start producer + consumer + API (producer can be disabled by DISABLE_PRODUCER=1)
CMD sh -lc "[ \"$DISABLE_PRODUCER\" = \"1\" ] || python etl/producer_crypto.py & \
             python -m etl.consumer_kinesis & \
             exec uvicorn api.app:app --host 0.0.0.0 --port 8000"
