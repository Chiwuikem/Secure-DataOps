# api/app.py
import os
import json
import time
import pathlib
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ---------- Config ----------
# Use a writable state dir everywhere (container-friendly).
STATE_DIR = pathlib.Path(
    os.getenv("SECUREDATAOPS_STATE_DIR", "/tmp/securedataops_state")
).expanduser()
STATE_DIR.mkdir(parents=True, exist_ok=True)

METRICS_PATH = STATE_DIR / "metrics.json"
ALERTS_PATH = STATE_DIR / "alerts.ndjson"

# Comma-separated list of allowed origins (no spaces).
# Example for prod:
#   API_ALLOW_ORIGINS=https://dliloa83h36vb.cloudfront.net,http://localhost:5173
_allow_origins_env = os.getenv("API_ALLOW_ORIGINS", "http://localhost:5173")
ALLOW_ORIGINS = [o.strip() for o in _allow_origins_env.split(",") if o.strip()]

# ---------- App ----------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Routes ----------
@app.get("/health")
def health():
    return {"ok": True, "time": int(time.time() * 1000)}

@app.get("/metrics")
def metrics():
    if not METRICS_PATH.exists():
        raise HTTPException(status_code=404, detail="metrics not ready")
    with METRICS_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)

@app.get("/alerts")
def alerts(limit: int = 50):
    if not ALERTS_PATH.exists():
        return []
    lines = ALERTS_PATH.read_text(encoding="utf-8").splitlines()
    out = [json.loads(x) for x in lines[-limit:]] if lines else []
    return out[::-1]  # newest first
