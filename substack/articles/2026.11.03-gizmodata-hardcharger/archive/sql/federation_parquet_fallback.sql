-- Fallback when the managed Hardcharger cluster cannot use the S3 Tables REST catalog.
-- The small synthetic reference Parquet file and DuckLake query still run in Hardcharger.
SELECT sales.order_date,
       sales.nationkey,
       segment.segment,
       SUM(sales.net_revenue) AS net_revenue
FROM analytics.main.daily_nation_sales AS sales
JOIN read_parquet('s3://<RAW_BUCKET>/tpch/reference/customer_segment/*.parquet') AS segment
  ON segment.nationkey = sales.nationkey
GROUP BY sales.order_date, sales.nationkey, segment.segment
ORDER BY sales.order_date, sales.nationkey;
