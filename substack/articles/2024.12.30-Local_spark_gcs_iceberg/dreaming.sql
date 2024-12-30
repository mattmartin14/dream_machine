

COPY (SELECT * FROM DUCK_FRAME) 
    TO ICEBERG_WRITE('{catalog}.{namespace}.{table}', path='gs://some_bucket/some_warehouse')