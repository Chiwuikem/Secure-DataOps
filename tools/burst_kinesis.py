import os, time, json, boto3, random
from datetime import datetime
STREAM = os.getenv("KINESIS_STREAM_NAME", "secure-dataops-trades")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
k = boto3.client("kinesis", region_name=REGION)

now_ms = lambda: int(time.time() * 1000)
ts0 = now_ms()
# send ~250 records as fast as possible
for i in range(250):
    rec = {
        "timestamp": now_ms(),
        "provider": "test",
        "symbol": "XBT/USD",
        "price": 50000 + random.random(),   # dummy price
        "volume": random.random()/10,
        "side": "b",
        "order_type": "m"
    }
    k.put_record(StreamName=STREAM, PartitionKey="burst", Data=json.dumps(rec))
print("sent 250 test records in", now_ms() - ts0, "ms")
