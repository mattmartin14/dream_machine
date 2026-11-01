-- Column-level security: readonly_analyst may read only the listed columns.
-- Replace the table-wide grant from step 01 with a column grant.
REVOKE SELECT ON TABLE order_stats.main.orders_summary FROM readonly_analyst;
GRANT SELECT (order_status, order_priority, order_count)
    ON TABLE order_stats.main.orders_summary TO readonly_analyst;

-- As ${READONLY_USERNAME}:
--   works:   SELECT order_status, order_priority, order_count FROM order_stats.main.orders_summary
--   refused: SELECT * FROM order_stats.main.orders_summary          (total_price, average_price)
--   refused: SELECT total_price FROM order_stats.main.orders_summary
