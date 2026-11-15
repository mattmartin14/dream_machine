.bail on

INSTALL AWS; LOAD AWS;
INSTALL DUCKLAKE; LOAD DUCKLAKE;
INSTALL POSTGRES; LOAD POSTGRES;
INSTALL slack_msg from community; LOAD slack_msg;

create secret s3_creds (type s3, provider credential_chain);
create secret slack_creds (type slack, webhook_url getenv('SLACK_WEBHOOK'));
-- SELECT CASE WHEN coalesce(getenv('AURORA_HOST'), '') = ''
--     OR coalesce(getenv('AURORA_ADMIN_USER'), '') = ''
--     OR coalesce(getenv('AURORA_ADMIN_PWD'), '') = ''
--     OR coalesce(getenv('AURORA_SSL_ROOT_CERT'), '') = ''
--     THEN error('Set AURORA_HOST, AURORA_ADMIN_USER, AURORA_ADMIN_PWD, and AURORA_SSL_ROOT_CERT before running.')
--     ELSE true END;
create secret aurora_creds (
    type postgres,
    host getenv('AURORA_HOST'),
    port 5432,
    database 'ducklake_catalog',
    user getenv('AURORA_ADMIN_USER'),
    password getenv('AURORA_ADMIN_PWD'),
    sslmode 'verify-full',
    sslrootcert getenv('AURORA_SSL_ROOT_CERT')
);
SET VARIABLE ducklake_dw = 's3://matt-sbx-bucket-1-us-east-1/ducklake_dw';

ATTACH 'ducklake:postgres:' as ducklake_cat (META_SECRET 'aurora_creds', DATA_PATH $ducklake_dw);

CREATE SCHEMA IF NOT EXISTS ducklake_cat.tpch_agg;

CREATE TABLE ducklake_cat.tpch_agg.line_item_agg AS
SELECT l_orderkey, count(*) as row_cnt
FROM read_parquet('s3://matt-sbx-bucket-1-us-east-1/tpch/lineitem/lineitem.parquet')
GROUP BY l_orderkey;


CALL slack_post(secret_name => 'slack_creds', msg => 'Ducklake table load complete');