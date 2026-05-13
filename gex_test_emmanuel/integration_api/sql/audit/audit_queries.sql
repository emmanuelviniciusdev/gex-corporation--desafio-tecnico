-- Audit Queries for db_integration (MySQL 8+)
-- Notes:
--  - These queries are written to be sargable and leverage composite indexes.
--  - Time windows use NOW(6) (server time) and CURDATE() for day bucketing.
--  - Relevant indexes are added in migration 003_add_audit_indexes.sql.

/*
1) Average lag (in seconds) between orders.transaction_time and
   distribution_status.delivered_at for the SMS channel, grouped by gateway,
   over the last 24 hours.
*/
SELECT
  o.gateway,
  AVG(UNIX_TIMESTAMP(ds.delivered_at) - UNIX_TIMESTAMP(o.transaction_time)) AS avg_lag_seconds,
  COUNT(*) AS deliveries
FROM distribution_status AS ds
JOIN orders AS o
  ON o.id = ds.order_id
WHERE ds.channel = 'SMS'
  AND ds.status = 'delivered'
  AND ds.delivered_at >= FROM_UNIXTIME(UNIX_TIMESTAMP(NOW(6)) - 24*3600)
GROUP BY o.gateway
ORDER BY deliveries DESC;


/*
2) Number of leads with pending status for more than 5 minutes.
   List: order_id, channel, and record age (in seconds).

   -> Emmanuel's note: given the statement, I was unsure whether the "status" should come from "orders.payment_status"
      or "distribution_status.status", since each lead will always have four "distribution_status" records. By applying
      the filter on "orders.payment_status", the query would always return all four "distribution_status" records for
      each available "channel", producing a redundant result, and for this reason, I thought it would be valid to apply
      the filter on "distribution_status.status".
*/
SELECT
  ds.order_id,
  ds.channel,
  (UNIX_TIMESTAMP(NOW(6)) - UNIX_TIMESTAMP(ds.created_at)) AS age_seconds
FROM distribution_status AS ds
WHERE ds.status = 'pending'
  AND ds.created_at < FROM_UNIXTIME(UNIX_TIMESTAMP(NOW(6)) - 5*60)
ORDER BY ds.created_at ASC;


/*
3) Leads processing success rate for the SMS channel, by product and by hour,
   over the last 6 hours.
   - Attempts are counted from distribution_status rows (any status)
     created within the interval for channel = 'SMS'.
   - Success is status = 'delivered'.
*/
SELECT
  DATE_FORMAT(ds.created_at, '%Y-%m-%d %H:00:00') AS hour_bucket,
  o.product_id,
  o.product_name,
  SUM(ds.status = 'delivered') AS delivered_count,
  COUNT(*) AS attempts_count,
  ROUND(100 * SUM(ds.status = 'delivered') / NULLIF(COUNT(*), 0), 2) AS success_rate_pct
FROM distribution_status AS ds
JOIN orders AS o
  ON o.id = ds.order_id
WHERE ds.channel = 'SMS'
  AND ds.created_at >= FROM_UNIXTIME(UNIX_TIMESTAMP(NOW(6)) - 6*3600)
GROUP BY hour_bucket, o.product_id, o.product_name
ORDER BY hour_bucket DESC, attempts_count DESC;


/*
4) Number of approved leads in lead_events versus total leads delivered to the
   SMS channel, per day, over the last 7 days. Display percentage and absolute gap.

   Interpretation:
   - Day is taken from the delivery day (DATE(ds.delivered_at)).
   - We count approvals (event = 'order.approved') for orders that were delivered to
     SMS (status = 'delivered', channel = 'SMS').
*/
SELECT
  DATE_FORMAT(ds.delivered_at, '%Y-%m-%d') AS day_str,
  COUNT(*) AS delivered_sms,
  SUM(CASE WHEN le.id IS NOT NULL THEN 1 ELSE 0 END) AS approved_count,
  ROUND(100 * SUM(CASE WHEN le.id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS approved_rate_pct,
  (COUNT(*) - SUM(CASE WHEN le.id IS NOT NULL THEN 1 ELSE 0 END)) AS absolute_gap
FROM distribution_status AS ds
LEFT JOIN lead_events AS le
  ON le.order_id = ds.order_id
 AND le.event = 'order.approved'
WHERE ds.channel = 'SMS'
  AND ds.status = 'delivered'
  AND ds.delivered_at >= FROM_UNIXTIME(UNIX_TIMESTAMP(CURDATE()) - 7*86400)
GROUP BY day_str
ORDER BY day_str DESC;
