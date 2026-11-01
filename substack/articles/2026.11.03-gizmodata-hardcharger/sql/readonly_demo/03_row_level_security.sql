-- Row-level security: members of readonly_analyst see only rows whose order_status
-- matches the order_status attribute set on their user. The administrator is never filtered.
ALTER USER ${READONLY_USERNAME} SET ATTRIBUTE order_status = 'O';

CREATE OR REPLACE ROW POLICY open_orders_only ON order_stats.main.orders_summary
    TO readonly_analyst
    USING (order_status = session_attribute('order_status'));

SHOW ROW POLICIES;

-- As ${READONLY_USERNAME}, only order_status = 'O' rows come back:
--   SELECT order_status, order_priority, order_count FROM order_stats.main.orders_summary
-- Change the attribute (as admin) to see a different slice; new sessions pick it up:
--   ALTER USER ${READONLY_USERNAME} SET ATTRIBUTE order_status = 'F';
