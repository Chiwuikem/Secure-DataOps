# etl/consumer_kinesis.py
import os
import sys
import time
import json
import math
import pathlib

import boto3
from dotenv import load_dotenv
from detection.spike import zscore_spike  # keep your spike.py in detection/

# ---------- Config ----------
load_dotenv()

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
STREAM = os.getenv("KINESIS_STREAM_NAME")
READ_FROM = os.getenv("KINESIS_READ_FROM", "LATEST")  # LATEST or TRIM_HORIZON

STATE_DIR = pathlib.Path(
    os.getenv("SECUREDATAOPS_STATE_DIR", "/tmp/securedataops_state")
).expanduser()
STATE_DIR.mkdir(parents=True, exist_ok=True)
METRICS_PATH = STATE_DIR / "metrics.json"
ALERTS_PATH = STATE_DIR / "alerts.ndjson"

# ---------- Helpers ----------
def write_json_atomic(path: pathlib.Path, obj: dict, retries: int = 10, backoff: float = 0.05):
    """
    Robust atomic write for Windows/Linux.
    """
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}.{int(time.time()*1e6)}")
    tmp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    for i in range(retries):
        try:
            os.replace(tmp, path)  # atomic if target isn't locked
            return
        except PermissionError:
            time.sleep(backoff * (2 ** i))  # exponential backoff
    # last resort
    path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    try:
        tmp.unlink()
    except Exception:
        pass

def append_alert(alert: dict):
    ALERTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with ALERTS_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(alert) + "\n")

# ---------- Boot ----------
print(f"[boot] region={REGION} stream={STREAM} read_from={READ_FROM}", flush=True)
if not STREAM:
    print("[error] KINESIS_STREAM_NAME not set", file=sys.stderr)
    sys.exit(1)

kinesis = boto3.client("kinesis", region_name=REGION)

def get_shard_iterator(stream_name: str) -> str:
    shards = kinesis.list_shards(StreamName=stream_name).get("Shards", [])
    if not shards:
        print("[error] no shards on stream", file=sys.stderr)
        sys.exit(2)
    shard_id = shards[0]["ShardId"]
    it = kinesis.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType=READ_FROM
    )
    print(f"[boot] using shard {shard_id}", flush=True)
    return it["ShardIterator"]

# ---------- Main ----------
def main():
    shard_it = get_shard_iterator(STREAM)
    counts_by_second = []  # list[int] of trades/sec
    current_second = None
    counter = 0
    last_heartbeat = time.time()

    print(f"[system] reading stream: {STREAM} ({REGION}) from {READ_FROM}", flush=True)

    while True:
        resp = kinesis.get_records(ShardIterator=shard_it, Limit=1000)
        shard_it = resp["NextShardIterator"]
        records = resp.get("Records", [])

        # heartbeat for API consumers when idle
        if not records and (time.time() - last_heartbeat) > 3:
            write_json_atomic(METRICS_PATH, {
                "last_updated_ms": int(time.time() * 1000),
                "trades_per_sec": 0,
                "window_size": 30,
                "z": 0.0,
                "is_spike": False
            })
            last_heartbeat = time.time()

        for r in records:
            try:
                rec = json.loads(r["Data"])
                ts_ms = int(rec.get("timestamp", 0))
                sec = ts_ms // 1000

                if current_second is None:
                    current_second = sec

                if sec != current_second:
                    # finalize the last second
                    counts_by_second.append(counter)
                    is_spike, z = zscore_spike(counts_by_second, window=30, threshold=3.0)

                    metrics = {
                        "last_updated_ms": int(time.time() * 1000),
                        "trades_per_sec": counter,
                        "window_size": 30,
                        "z": round(z, 4),
                        "is_spike": bool(is_spike),
                    }
                    write_json_atomic(METRICS_PATH, metrics)

                    if is_spike:
                        alert = {"ts_ms": metrics["last_updated_ms"], "count": counter, "z": metrics["z"]}
                        append_alert(alert)
                        print(f"[ALERT] trade spike detected: count={counter}, z={z:.2f}", flush=True)
                    else:
                        print(f"[tick] {counter} trades/sec", flush=True)

                    current_second = sec
                    counter = 0

                counter += 1

            except Exception as e:
                print("[warn] parse error:", repr(e), flush=True)

        time.sleep(0.5)

if __name__ == "__main__":
    main()
