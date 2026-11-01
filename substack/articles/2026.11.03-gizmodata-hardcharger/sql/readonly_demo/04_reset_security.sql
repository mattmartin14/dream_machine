-- Back to the step 01 state: remove the row policy and restore the table-wide grant.
DROP ROW POLICY IF EXISTS open_orders_only ON order_stats.main.orders_summary;
REVOKE SELECT ON TABLE order_stats.main.orders_summary FROM readonly_analyst;
GRANT SELECT ON TABLE order_stats.main.orders_summary TO readonly_analyst;
