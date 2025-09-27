update iceberg_catalog.icebox1.test1 as tgt
    set tgt.data = 'update join worked'
from iceberg_catalog.icebox1.test2 as src
where tgt.id = src.id;