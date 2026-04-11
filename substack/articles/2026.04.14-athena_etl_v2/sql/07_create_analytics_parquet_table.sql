CREATE EXTERNAL TABLE __ANALYTICS_DATABASE__.__PARQUET_TABLE__ (
  order_id string,
  order_ts timestamp,
  dt date,
  sales_channel string,
  customer_id string,
  customer_segment string,
  item_count int,
  subtotal double,
  shipping double,
  tax double,
  grand_total double,
  payment_method string,
  warehouse string,
  ship_speed string,
  has_customer_profile boolean
)
STORED AS PARQUET
LOCATION '__PARQUET_S3__';
