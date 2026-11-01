-- Execute each statement separately as the Hardcharger administrator.
-- Principals contain no embedded passwords. Set each user's credentials through
-- the verified Hardcharger authentication workflow, not in this SQL file.
CREATE ROLE etl_writer;
CREATE ROLE finance_admin;
CREATE ROLE regional_analyst;
CREATE ROLE restricted_reporting;

CREATE USER etl_service;
CREATE USER finance_user;
CREATE USER region0_user;
CREATE USER region3_user;
CREATE USER reporting_user;

GRANT etl_writer TO etl_service;
GRANT finance_admin TO finance_user;
GRANT regional_analyst TO region0_user;
GRANT regional_analyst TO region3_user;
GRANT restricted_reporting TO reporting_user;

GRANT READ FILESYSTEM TO etl_writer;
GRANT CREATE TABLE ON SCHEMA main TO etl_writer;
GRANT SELECT, INSERT, DELETE ON TABLE analytics.main.daily_sales TO etl_writer;
GRANT SELECT, INSERT, DELETE ON TABLE analytics.main.daily_nation_sales TO etl_writer;
GRANT SELECT ON SCHEMA main TO finance_admin;
GRANT SELECT ON TABLE analytics.main.daily_sales TO regional_analyst;
GRANT SELECT ON TABLE analytics.main.daily_nation_sales TO regional_analyst;
GRANT SELECT ON TABLE analytics.main.daily_sales TO restricted_reporting;
GRANT SELECT (c_custkey, c_name, c_address, c_phone)
    ON TABLE analytics.main.customer_security_demo TO restricted_reporting;

-- Use actual TPC-H regionkey values (0-4) via this string-valued session attribute.
-- TPC-H regionkey 0 and 3 are genuine region identifiers. Query tpch_region to
-- display their standard labels; do not relabel them as East/West.
ALTER USER region0_user SET ATTRIBUTE regionkey = '0';
ALTER USER region3_user SET ATTRIBUTE regionkey = '3';
