insert into iceberg_catalog.{DB_NAME}.test2 (id, data)
values (7, 'i'), (8, 'j'), (9, 'k')
;

insert into iceberg_catalog.{DB_NAME}.test1 (id, data)
select src.id, src.data
from iceberg_catalog.{DB_NAME}.test2 as src
    left join iceberg_catalog.{DB_NAME}.test1 as tgt
    on src.id = tgt.id
where tgt.id is null
;