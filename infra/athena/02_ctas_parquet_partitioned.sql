-- CTAS: write Parquet, partitioned by date (dt) and hour (hh)
-- Output goes to a new prefix; safe to re-run with WHERE for a specific day/hour to refresh.
CREATE TABLE secure_dataops.trades_parquet
WITH (
  format = 'PARQUET',
  external_location = 's3://secure-dataops-raw-2025dev/athena/parquet/kraken/xbt_usd/',
  partitioned_by = ARRAY['dt','hh']
) AS
SELECT
  provider, symbol, price, volume, timestamp, side, order_type,
  from_unixtime(timestamp/1000)               AS ts,
  date_format(from_unixtime(timestamp/1000), '%Y-%m-%d') AS dt,
  date_format(from_unixtime(timestamp/1000), '%H')       AS hh
FROM secure_dataops.trades_json
-- Optional: first run just the latest few hours to be quick
-- WHERE timestamp > (unix_millis(current_timestamp) - 6*60*60*1000)
;
