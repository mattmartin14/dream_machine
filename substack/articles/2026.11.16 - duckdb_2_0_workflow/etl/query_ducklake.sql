INSTALL AWS; LOAD AWS;
INSTALL DUCKLAKE; LOAD DUCKLAKE;
INSTALL POSTGRES; LOAD POSTGRES;
INSTALL slack_msg from community; LOAD slack_msg;

create secret s3_creds (type s3, provider credential_chain);
create secret slack_creds (type slack, webhook_url getenv('SLACK_WEBHOOK'));

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

ATTACH 'ducklake:postgres:' as ducklake_cat (META_SECRET 'aurora_creds');

SELECT *
FROM ducklake_cat.tpch_agg.line_item_agg
LIMIT 5;

