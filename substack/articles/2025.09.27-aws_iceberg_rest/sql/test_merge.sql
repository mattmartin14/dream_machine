MERGE INTO iceberg_catalog.icebox1.test1 as tgt
    using iceberg_catalog.icebox1.test2 as src
        on tgt.id = src.id
    when matched and tgt.id between 2 and 4 then
      update set tgt.data = 'merge update between 2 and 4'
    when matched and tgt.id = 5 then
      delete
    when not matched then
      insert (id, data) values (src.id, src.data)
