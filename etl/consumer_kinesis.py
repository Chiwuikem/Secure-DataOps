import os, json, time, math, sys, pathlib
import boto3
from dotenv import load_dotenv
from detection.spike import zscore_spike  # <-- if you keep consumer under detection/, change to: from spike import zscore_spike

load_dotenv()
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
STREAM = os.getenv("KINESIS_STREAM_NAME")
READ_FROM = os.getenv("KINESIS_READ_FROM", "LATEST")

STATE_DIR = pathlib.Path(".state")
STATE_DIR.mkdir(exist_ok=True)
METRICS_PATH = STATE_DIR / "metrics.json"
ALERTS_PATH = STATE_DIR / "alerts.ndjson"

def write_json_atomic(path: pathlib.Path, obj: dict):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False))
    tmp.replace(path)

def append_alert(alert: dict):
    with ALERTS_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(alert) + "\n")

print(f"[boot] region={REGION} stream={STREAM} read_from={READ_FROM}", flush=True)
if not STREAM:
    print("[error] KINESIS_STREAM_NAME not set", file=sys.stderr)
    sys.exit(1)

k = boto3.client("kinesis", region_name=REGION)

def get_shard_iterator(stream):
    shards = k.list_shards(StreamName=stream).get("Shards", [])
    if not shards:
        print("[error] no shards on stream", file=sys.stderr); sys.exit(2)
    shard_id = shards[0]["ShardId"]
    it = k.get_shard_iterator(
        StreamName=stream, ShardId=shard_id, ShardIteratorType=READ_FROM
    )
    print(f"[boot] using shard {shard_id}", flush=True)
    return it["ShardIterator"]

def main():
    shard_it = get_shard_iterator(STREAM)
    counts_by_second = []
    current_second = None
    counter = 0
    last_log = time.time()

    print(f"[system] reading stream: {STREAM} ({REGION}) from {READ_FROM}", flush=True)

    while True:
        resp = k.get_records(ShardIterator=shard_it, Limit=1000)
        shard_it = resp["NextShardIterator"]
        recs = resp.get("Records", [])

        if not recs and (time.time() - last_log) > 3:
            # heartbeat for API consumers
            write_json_atomic(METRICS_PATH, {
                "last_updated_ms": int(time.time()*1000),
                "trades_per_sec": 0,
                "window_size": 30,
                "z": 0.0,
                "is_spike": False
            })
            last_log = time.time()

        for r in recs:
            try:
                rec = json.loads(r["Data"])
                ts_ms = int(rec.get("timestamp", 0))
                sec = ts_ms // 1000
                if current_second is None:
                    current_second = sec
                if sec != current_second:
                    counts_by_second.append(counter)
                    is_spike, z = zscore_spike(counts_by_second, window=30, threshold=3.0)
                    metrics = {
                        "last_updated_ms": int(time.time()*1000),
                        "trades_per_sec": counter,
                        "window_size": 30,
                        "z": round(z, 4),
                        "is_spike": bool(is_spike)
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
