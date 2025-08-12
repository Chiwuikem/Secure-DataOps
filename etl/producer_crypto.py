# etl/producer_crypto.py
import os
import json
import time
import signal
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from websocket import WebSocketApp

# ----------------------
# Config & AWS Clients
# ----------------------
load_dotenv()

REGION   = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
STREAM   = os.getenv("KINESIS_STREAM_NAME")
BUCKET   = os.getenv("S3_BUCKET")
PROVIDER = os.getenv("PROVIDER", "kraken").lower()
SYMBOL   = os.getenv("SYMBOL", "XBT/USD")  # Kraken pair format

if not STREAM or not BUCKET:
    raise RuntimeError("Missing KINESIS_STREAM_NAME or S3_BUCKET in environment (.env).")

kinesis = boto3.client("kinesis", region_name=REGION)
s3      = boto3.client("s3", region_name=REGION)

# ----------------------
# Graceful shutdown
# ----------------------
exit_flag = False

def signal_handler(sig, frame):
    """Set a flag so all loops exit cleanly on Ctrl+C."""
    global exit_flag
    print("\n[system] Ctrl+C detected. Shutting down gracefully...")
    exit_flag = True

signal.signal(signal.SIGINT, signal_handler)

# ----------------------
# Buffer / Flush config
# ----------------------
buffer = []
BATCH_SIZE = 10            # flush to S3 after N events
FLUSH_INTERVAL_SEC = 5     # or after N seconds
_last_flush = 0.0

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def flush_to_s3():
    """Write buffered events to S3 as NDJSON and clear the buffer."""
    global buffer, _last_flush
    if not buffer:
        return
    ts_path = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H/%M")
    key = f"{PROVIDER}/{SYMBOL.replace('/','_').lower()}/{ts_path}/{int(time.time())}.ndjson"
    body = "\n".join(json.dumps(rec) for rec in buffer).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    print(f"[flush] wrote {len(buffer)} events to s3://{BUCKET}/{key}")
    buffer = []
    _last_flush = time.time()

def send_to_kinesis(rec: dict):
    """Send a single record to Kinesis."""
    try:
        kinesis.put_record(
            StreamName=STREAM,
            Data=json.dumps(rec),
            PartitionKey=rec.get("symbol", "default")
        )
    except Exception as e:
        print("[kinesis] put_record error:", e)

# ----------------------
# Kraken handlers
# ----------------------
WS_URL = "wss://ws.kraken.com/"

def kraken_on_open(ws):
    sub = {
        "event": "subscribe",
        "pair": [SYMBOL],  # e.g., "XBT/USD"
        "subscription": {"name": "trade"}
    }
    ws.send(json.dumps(sub))
    print(f"[kraken] Subscribed to trades for {SYMBOL}")

def kraken_on_message(ws, message):
    global buffer, exit_flag, _last_flush
    if exit_flag:
        ws.close()
        return

    try:
        msg = json.loads(message)
        # Kraken trade messages look like:
        # [channelID, [ [price, volume, time, side, orderType, misc], ...], "trade", "XBT/USD"]
        if isinstance(msg, list) and len(msg) >= 4 and msg[2] == "trade":
            pair = msg[3]  # "XBT/USD"
            for t in msg[1]:
                price = float(t[0])
                qty   = float(t[1])
                tsecs = float(t[2])  # epoch seconds (float)
                side_raw = t[3]      # "b" = buy, "s" = sell
                order_type = t[4]    # "m" (market), "l" (limit), etc.

                rec = {
                    "source": "kraken",
                    "symbol": pair.replace("/", "_").lower(),   # xbt_usd
                    "price": price,
                    "qty": qty,
                    "timestamp": int(tsecs * 1000),
                    "ingest_ts": utc_now_iso(),
                    "side": "buy" if side_raw == "b" else "sell",
                    "order_type": order_type
                }

                # send to Kinesis
                send_to_kinesis(rec)
                # buffer for S3
                buffer.append(rec)

            # size-based flush
            if len(buffer) >= BATCH_SIZE:
                flush_to_s3()

            # time-based flush
            now = time.time()
            if _last_flush == 0.0:
                _last_flush = now
            elif now - _last_flush >= FLUSH_INTERVAL_SEC:
                flush_to_s3()

        # Otherwise: status, heartbeat, etc. — ignore
    except Exception as e:
        print("[kraken] on_message error:", e)

def kraken_on_error(ws, error):
    # Print and keep running unless exit_flag is set
    print("[kraken] error:", error)

def kraken_on_close(ws, code, msg):
    print("[kraken] closed:", code, msg)
    flush_to_s3()

# ----------------------
# Runner
# ----------------------
if __name__ == "__main__":
    # Simple reconnect loop that respects exit_flag
    while not exit_flag:
        try:
            ws = WebSocketApp(
                WS_URL,
                on_open=kraken_on_open,
                on_message=kraken_on_message,
                on_error=kraken_on_error,
                on_close=kraken_on_close
            )
            ws.run_forever()
        except Exception as e:
            if exit_flag:
                break
            print("[system] connection error, retrying in 5s:", e)
            time.sleep(5)

    # final flush on exit
    flush_to_s3()
    print("[system] exit complete.")
