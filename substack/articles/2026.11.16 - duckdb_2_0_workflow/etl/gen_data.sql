INSTALL AWS; LOAD AWS;
INSTALL TPCH; LOAD TPCH;
call dbgen(sf=.001);

create secret s3_creds (type s3, provider credential_chain);

COPY (FROM lineitem) to 's3://matt-sbx-bucket-1-us-east-1/tpch/lineitem/lineitem.parquet';
COPY (FROM orders) to 's3://matt-sbx-bucket-1-us-east-1/tpch/orders/orders.parquet';
COPY (FROM customer) to 's3://matt-sbx-bucket-1-us-east-1/tpch/customer/customer.parquet';

.print 'datasets loaded to s3'