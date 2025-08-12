from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json, pathlib, time

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

STATE_DIR = pathlib.Path(".state")
METRICS_PATH = STATE_DIR / "metrics.json"
ALERTS_PATH = STATE_DIR / "alerts.ndjson"

@app.get("/health")
def health():
    return {"ok": True, "time": int(time.time()*1000)}

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
    lines = ALERTS_PATH.read_text(encoding="utf-8").strip().splitlines()
    out = [json.loads(x) for x in lines[-limit:]] if lines else []
    return out[::-1]  # newest first
