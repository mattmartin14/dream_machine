CREATE OR REPLACE MASKING POLICY mask_customer_name
    AS (val VARCHAR) RETURNS VARCHAR -> 'REDACTED';
CREATE OR REPLACE MASKING POLICY mask_customer_address
    AS (val VARCHAR) RETURNS VARCHAR -> 'REDACTED';
CREATE OR REPLACE MASKING POLICY mask_customer_phone
    AS (val VARCHAR) RETURNS VARCHAR -> 'REDACTED';

ALTER TABLE analytics.main.customer_security_demo MODIFY COLUMN c_name
    SET MASKING POLICY mask_customer_name EXEMPT ROLE finance_admin;
ALTER TABLE analytics.main.customer_security_demo MODIFY COLUMN c_address
    SET MASKING POLICY mask_customer_address EXEMPT ROLE finance_admin;
ALTER TABLE analytics.main.customer_security_demo MODIFY COLUMN c_phone
    SET MASKING POLICY mask_customer_phone EXEMPT ROLE finance_admin;

-- Run the same SELECT as finance_user and reporting_user:
-- SELECT c_custkey, c_name, c_address, c_phone
-- FROM analytics.main.customer_security_demo ORDER BY c_custkey LIMIT 5;
