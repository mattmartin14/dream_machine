UNLOAD (
  SELECT
    order_id,
    CAST(from_iso8601_timestamp(order_ts) AS timestamp) AS order_ts,
    CAST(dt AS date) AS dt,
    sales_channel,
    customer_ref.customer_id AS customer_id,
    customer_ref.segment AS customer_segment,
    cardinality(line_items) AS item_count,
    totals.subtotal AS subtotal,
    totals.shipping AS shipping,
    totals.tax AS tax,
    totals.grand_total AS grand_total,
    payment.method AS payment_method,
    fulfillment.warehouse AS warehouse,
    fulfillment.ship_speed AS ship_speed,
    customer_profile IS NOT NULL AS has_customer_profile
  FROM __STAGING_DATABASE__.__JSON_TABLE__
  WHERE order_id IS NOT NULL
)
TO '__PARQUET_S3__'
WITH (
  format = 'PARQUET',
  compression = 'SNAPPY'
);
