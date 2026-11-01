-- Optional integration query template. Configure both catalog names only after
-- GizmoData confirms support and the Hardcharger admin attaches the catalogs.
-- Required environment values: FEDERATION_DUCKLAKE_TABLE and FEDERATION_ICEBERG_TABLE.
SELECT sales.order_date,
       sales.nationkey,
       segment.segment,
       SUM(sales.net_revenue) AS net_revenue
FROM {{FEDERATION_DUCKLAKE_TABLE}} AS sales
JOIN {{FEDERATION_ICEBERG_TABLE}} AS segment
  ON segment.nationkey = sales.nationkey
GROUP BY sales.order_date, sales.nationkey, segment.segment
ORDER BY sales.order_date, sales.nationkey;
