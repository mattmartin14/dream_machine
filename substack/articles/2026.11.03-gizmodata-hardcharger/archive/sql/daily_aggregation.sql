-- Bind ? to an ISO-8601 order date. The Python ETL executes the same expressions
-- inside a transaction after deleting the existing date, preserving rerun safety.
WITH orders AS (
    SELECT o_orderkey, o_orderdate, o_custkey
    FROM main.tpch_orders
    WHERE o_orderdate = CAST(? AS DATE)
)
SELECT o.o_orderdate AS order_date,
       COUNT(DISTINCT o.o_orderkey) AS order_count,
       COUNT(*) AS line_item_count,
       SUM(l.l_extendedprice) AS gross_revenue,
       SUM(l.l_extendedprice * l.l_discount) AS discount_amount,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS net_revenue,
       CASE WHEN COUNT(DISTINCT o.o_orderkey) = 0 THEN 0
            ELSE SUM(l.l_extendedprice * (1 - l.l_discount))
                 / COUNT(DISTINCT o.o_orderkey)
       END AS average_order_value
FROM orders o
JOIN main.tpch_lineitem l ON l.l_orderkey = o.o_orderkey
GROUP BY o.o_orderdate;
