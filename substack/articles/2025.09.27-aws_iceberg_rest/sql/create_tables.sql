create table if not exists iceberg_catalog.icebox1.test1 (
  id int,
  data string
) using iceberg
location 's3://{BUCKET}/icehouse/test1'
;

create table if not exists iceberg_catalog.icebox1.test2 (
  id int,
  data string
) using iceberg
location 's3://{BUCKET}/icehouse/test2'
;

insert into iceberg_catalog.icebox1.test1 values
  (1, 'a'),
  (2, 'b'),
  (3, 'c')
; 

insert into iceberg_catalog.icebox1.test2 values
  (3, 'x'),
  (4, 'y'),
  (5, 'z')
; 
