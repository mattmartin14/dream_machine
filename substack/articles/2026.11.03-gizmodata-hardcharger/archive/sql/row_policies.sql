-- The policy applies to regional_analyst role members, not to the administrator.
-- regionkey is stored as VARCHAR in daily_nation_sales so it matches the documented
-- string-valued session_attribute() interface without relying on an undocumented cast.
CREATE OR REPLACE ROW POLICY region_scope ON analytics.main.daily_nation_sales
    TO regional_analyst
    USING (regionkey = session_attribute('regionkey'));

-- Example user configuration, using TPC-H's real regionkey:
-- ALTER USER region_user SET ATTRIBUTE regionkey = '3';
-- Query SELECT DISTINCT regionkey, region_name FROM main.tpch_region to map keys.
