-- Run once (skip if already done)
CREATE DATABASE IF NOT EXISTS secure_dataops;

-- External table over your raw NDJSON (adjust columns if your JSON differs)
CREATE EXTERNAL TABLE IF NOT EXISTS secure_dataops.trades_json (
  provider   string,
  symbol     string,
  price      double,
  volume     double,
  timestamp  bigint,
  side       string,
  order_type string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://secure-dataops-raw-2025dev/kraken/xbt_usd/';
