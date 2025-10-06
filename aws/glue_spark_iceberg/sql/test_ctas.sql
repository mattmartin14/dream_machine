drop table if exists iceberg_catalog.{DB_NAME}.ctas1;

create or replace table iceberg_catalog.{DB_NAME}.ctas1 
using iceberg
location '{WAREHOUSE}/ctas1'
as
select * 
from iceberg_catalog.{DB_NAME}.test1
;