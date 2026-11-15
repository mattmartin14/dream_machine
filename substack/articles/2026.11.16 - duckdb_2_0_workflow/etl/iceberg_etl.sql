.bail on

-- drop/recreate iceberg db
-- .shell failures do not stop DuckDB, even with .bail on.
.shell sh ./etl/delete_glue_database_if_exists.sh ice_db
.shell aws glue create-database --database-input Name=ice_db


INSTALL AWS; LOAD AWS;
INSTALL ICEBERG; LOAD ICEBERG;
INSTALL slack_msg from community; LOAD slack_msg;

create secret s3_creds (type s3, provider credential_chain);
create secret slack_creds (type slack, webhook_url getenv('SLACK_WEBHOOK'));

.print 'starting load process'

attach getenv('AWS_ACCT_ID') as iceberg_cat (
    TYPE ICEBERG,
    ENDPOINT_TYPE GLUE
);

CREATE TABLE iceberg_cat.ice_db.line_item_agg WITH (
    'location' = 's3://matt-sbx-bucket-1-us-east-1/tpch_agg/lineitem'
) AS
SELECT l_orderkey, count(*) as row_cnt
FROM read_parquet('s3://matt-sbx-bucket-1-us-east-1/tpch/lineitem/lineitem.parquet')
GROUP BY l_orderkey;

.print 'table load complete';

CALL slack_post(secret_name => 'slack_creds', msg => 'Table load complete');
