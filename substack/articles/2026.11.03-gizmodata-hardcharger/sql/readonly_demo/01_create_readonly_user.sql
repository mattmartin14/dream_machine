-- Run as the Hardcharger administrator (scripts/run_sql.py sends one statement at a time).
-- Creates a read-only role and user that can SELECT from one table and nothing else.
-- ${READONLY_USERNAME} and ${READONLY_PASSWORD} are filled in from HC_READONLY_USERNAME and
-- HC_READONLY_PASSWORD by run_sql.py.
CREATE ROLE readonly_analyst;
GRANT SELECT ON TABLE order_stats.main.orders_summary TO readonly_analyst;

CREATE USER ${READONLY_USERNAME} WITH PASSWORD '${READONLY_PASSWORD}';
GRANT readonly_analyst TO ${READONLY_USERNAME};

SHOW GRANTS;
