-- Remove the demo user and role entirely.
DROP ROW POLICY IF EXISTS open_orders_only ON order_stats.main.orders_summary;
DROP USER IF EXISTS ${READONLY_USERNAME};
DROP ROLE IF EXISTS readonly_analyst;
