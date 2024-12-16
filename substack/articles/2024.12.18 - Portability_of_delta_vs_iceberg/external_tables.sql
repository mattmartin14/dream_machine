

-- worked the first time
CREATE OR REPLACE EXTERNAL TABLE test_ds.orders_deltalake
WITH CONNECTION us.test_cn_matt
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://{bucket}/delta_warehouse/orders']
);

-- creates table but throws "invalid URI"...not portable
CREATE OR REPLACE EXTERNAL TABLE test_ds.orders_iceberg
WITH CONNECTION us.test_cn_matt
OPTIONS (
  format = 'ICEBERG',
  uris = ['gs://{bucket}/iceberg_warehouse/dummy_data.db/orders/metadata/00001-c03dc88c-5884-4029-ac5c-f387efaec652.metadata.json']
);



