import os, json, time, math
import boto3
from dotenv import load_dotenv

load_dotenv()
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
STREAM = os.getenv("KINESIS_STREAM_NAME")

k = boto3.client("kinesis", region_name=REGION)

def get_shard_iterator(stream):
    # prefer LATEST so we only read new records
    shard_resp = k.list_shards(StreamName=stream)
    if not shard_resp.get("Shards"):
        raise RuntimeError("No shards found on stream.")
    shard_id = shard_resp["Shards"][0]["ShardId"]
    it = k.get_shard_iterator(
        StreamName=stream,
        ShardId=shard_id,
        ShardIteratorType="LATEST"  # TRIM_HORIZON to read from the very beginning
    )
    return it["ShardIterator"]

def detect_spike(window_counts, window_size=30, z_threshold=3.0):
    # z-score on trades/second
    if len(window_counts) < window_size:
        return False, 0.0
    series = window_counts[-window_size:]
    mean = sum(series) / window_size
    var = sum((x - mean) ** 2 for x in series) / max(window_size - 1, 1)
    sd = math.sqrt(var) or 1.0
    z = (series[-1] - mean) / sd
    return (z >= z_threshold), z

def main():
    shard_it = get_shard_iterator(STREAM)
    counts_by_second = []  # trades per second
    current_second = None
    counter = 0

    print(f"[system] reading stream: {STREAM} ({REGION})")

    while True:
        resp = k.get_records(ShardIterator=shard_it, Limit=1000)
        shard_it = resp["NextShardIterator"]

        for r in resp.get("Records", []):
            try:
                rec = json.loads(r["Data"])
                # expect 'timestamp' in ms (producer writes this)
                ts_ms = int(rec.get("timestamp", 0))
                sec = ts_ms // 1000
                if current_second is None:
                    current_second = sec
                if sec != current_second:
                    counts_by_second.append(counter)
                    spike, z = detect_spike(counts_by_second)
                    if spike:
                        print(f"[ALERT] trade spike detected: count={counter}, z={z:.2f}")
                    else:
                        print(f"[tick] {counter} trades/sec")
                    current_second = sec
                    counter = 0
                counter += 1
            except Exception as e:
                print("parse error:", e)

        time.sleep(0.5)  # be nice to API

if __name__ == "__main__":
    main()
