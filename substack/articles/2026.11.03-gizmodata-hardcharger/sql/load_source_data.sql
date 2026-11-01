install aws; load aws;
install tpch; load tpch;

create secret aws_creds (type s3, provider credential_chain);

call dbgen(sf=.001);

copy(from orders) to 's3://matt-sbx-bucket-1-us-east-1/tpch/orders.parquet';
