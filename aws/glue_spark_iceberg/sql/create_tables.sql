create table if not exists iceberg_catalog.{DB_NAME}.test1 (
  id int,
  data string
) using iceberg
location '{WAREHOUSE}/test1'
;

create table if not exists iceberg_catalog.{DB_NAME}.test2 (
  id int,
  data string
) using iceberg
location '{WAREHOUSE}/test2'
;

insert into iceberg_catalog.{DB_NAME}.test1 values
  (1, 'a'),
  (2, 'b'),
  (3, 'c')
; 

insert into iceberg_catalog.{DB_NAME}.test2 values
  (3, 'x'),
  (4, 'y'),
  (5, 'z')
; 
