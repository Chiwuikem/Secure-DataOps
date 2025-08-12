import os, json, time
from datetime import datetime
from dotenv import load_dotenv
import boto3
from websocket import WebSocketApp

load_dotenv()

REGION  = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
STREAM  = os.getenv("KINESIS_STREAM_NAME")
BUCKET  = os.getenv("S3_BUCKET")
SYMBOL  = os.getenv("SYMBOL", "XBT/USD")  # Kraken pair format
PROVIDER= os.getenv("PROVIDER", "kraken").lower()

kinesis = boto3.client("kinesis", region_name=REGION)
s3      = boto3.client("s3", region_name=REGION)

buffer = []
BATCH_SIZE = 100
PUT_RECORDS_SIZE = 50

def flush_to_s3():
    global buffer
    if not buffer:
        return
    ts = datetime.utcnow().strftime("%Y/%m/%d/%H/%M")
    key = f"{PROVIDER}/{SYMBOL.replace('/','_').lower()}/{ts}/{int(time.time())}.ndjson"
    body = "\n".join(json.dumps(rec) for rec in buffer).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    buffer = []

def send_batch_to_kinesis(batch):
    entries = [{"PartitionKey": rec.get("symbol", SYMBOL), "Data": json.dumps(rec)} for rec in batch]
    for i in range(0, len(entries), PUT_RECORDS_SIZE):
        chunk = entries[i:i+PUT_RECORDS_SIZE]
        kinesis.put_records(StreamName=STREAM, Records=chunk)

# ---------- Kraken ----------
def kraken_ws_url():
    return "wss://ws.kraken.com/"

def kraken_on_open(ws):
    # Subscribe to trades for the given pair
    sub = {
        "event": "subscribe",
        "pair": [SYMBOL],
        "subscription": {"name": "trade"}
    }
    ws.send(json.dumps(sub))
    print(f"[kraken] Subscribed to trades for {SYMBOL}")

def kraken_on_message(ws, message):
    global buffer
    try:
        msg = json.loads(message)
        # Kraken trade messages are arrays:
        # [channelID, [ [price, volume, time, side, orderType, misc], ...], "trade", "XBT/USD"]
        if isinstance(msg, list) and len(msg) >= 4 and msg[2] == "trade":
            pair = msg[3]
            trades = msg[1]
            for t in trades:
                price = float(t[0])
                qty   = float(t[1])
                tsec  = float(t[2])  # epoch seconds with milliseconds
                rec = {
                    "source": "kraken",
                    "symbol": pair.replace("/", "_").lower(),
                    "price": price,
                    "qty": qty,
                    "timestamp": int(tsec * 1000)
                }
                buffer.append(rec)
                send_batch_to_kinesis([rec])
                if len(buffer) >= BATCH_SIZE:
                    flush_to_s3()
        # Heartbeats and system status are dicts—ignore
    except Exception as e:
        print("kraken on_message error:", e)

# ---------- Bitstamp (optional fallback) ----------
def bitstamp_ws_url():
    return "wss://ws.bitstamp.net"

def bitstamp_on_open(ws):
    # SYMBOL like BTC/USD -> channel: live_trades_btcusd
    ch = f"live_trades_{SYMBOL.replace('/','').lower()}"
    sub = {"event":"bts:subscribe","data":{"channel": ch}}
    ws.send(json.dumps(sub))
    print(f"[bitstamp] Subscribed to {ch}")

def bitstamp_on_message(ws, message):
    global buffer
    try:
        msg = json.loads(message)
        if msg.get("event") == "trade" and "data" in msg:
            d = msg["data"]
            price = float(d["price"])
            qty   = float(d["amount"])
            ts    = int(d.get("timestamp", time.time()))
            rec = {
                "source": "bitstamp",
                "symbol": SYMBOL.replace("/", "_").lower(),
                "price": price,
                "qty": qty,
                "timestamp": ts * 1000
            }
            buffer.append(rec)
            send_batch_to_kinesis([rec])
            if len(buffer) >= BATCH_SIZE:
                flush_to_s3()
    except Exception as e:
        print("bitstamp on_message error:", e)

# ---------- Runner ----------
def run_ws():
    if PROVIDER == "kraken":
        url = kraken_ws_url()
        return WebSocketApp(
            url,
            on_open=kraken_on_open,
            on_message=kraken_on_message,
            on_error=lambda ws, err: print("[kraken] error:", err),
            on_close=lambda ws, code, msg: (print("[kraken] closed:", code, msg), flush_to_s3())
        )
    elif PROVIDER == "bitstamp":
        url = bitstamp_ws_url()
        return WebSocketApp(
            url,
            on_open=bitstamp_on_open,
            on_message=bitstamp_on_message,
            on_error=lambda ws, err: print("[bitstamp] error:", err),
            on_close=lambda ws, code, msg: (print("[bitstamp] closed:", code, msg), flush_to_s3())
        )
    else:
        raise ValueError(f"Unsupported PROVIDER '{PROVIDER}'. Use 'kraken' or 'bitstamp'.")

if __name__ == "__main__":
    while True:
        try:
            ws = run_ws()
            ws.run_forever()
        except KeyboardInterrupt:
            flush_to_s3()
            break
        except Exception as e:
            print("Connection error, retrying in 5s:", e)
            time.sleep(5)
