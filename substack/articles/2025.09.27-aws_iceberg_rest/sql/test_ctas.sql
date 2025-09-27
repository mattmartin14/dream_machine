drop table if exists iceberg_catalog.icebox1.ctas1;

create or replace table iceberg_catalog.icebox1.ctas1 
using iceberg
location 's3://{BUCKET}/iceberg/ctas1'
as
select * 
from iceberg_catalog.icebox1.test1
;