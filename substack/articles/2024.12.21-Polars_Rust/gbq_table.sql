CREATE OR REPLACE EXTERNAL TABLE test_ds.dummy_rust_data
WITH CONNECTION us.test_cn_matt
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://matts-super-secret-rust-bucket-123/agg_dataset/*']
);
