insert into iceberg_catalog.icebox1.test2 (id, data)
values (7, 'i'), (8, 'j'), (9, 'k')
;

insert into iceberg_catalog.icebox1.test1 (id, data)
select src.id, src.data
from iceberg_catalog.icebox1.test2 as src   
    left join iceberg_catalog.icebox1.test1 as tgt
    on src.id = tgt.id
where tgt.id is null
;