INSTALL aws; LOAD aws;

CREATE SECRET s3_creds (TYPE S3, PROVIDER CREDENTIAL_CHAIN);

SET VARIABLE S3_VAR_PATH = 's3://matt-sbx-bucket-1-us-east-1/2_dot_0_benchmark/*.json';

SET VARIABLE DUCKDB_VERSION = version();
SELECT getvariable('DUCKDB_VERSION') AS duckdb_version;

.timer on

SELECT order_cnt: count(distinct order_number), line_cnt: count(*)
    ,avg_line_qty: avg(quantity)
FROM 
(
    SELECT order_number, l.line_number, l.product, l.quantity, l.unit_price
    FROM read_json_auto(getvariable('S3_VAR_PATH')), UNNEST(order_lines) AS t(l)
) AS sub
;

.timer off