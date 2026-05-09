import duckdb

def main():
    
    cn = duckdb.connect()

    s3_target_bucket = 's3-sales-agg-test'

    cn.execute("install aws; load aws;")
    cn.execute("install httpfs; load httpfs;")
    cn.execute("create secret aws_s3 (type s3, provider credential_chain);")
   
    s3_target_path = f"s3://{s3_target_bucket}/tpch/cust_agg/cust_agg.parquet"
    cn.sql(f"from read_parquet('{s3_target_path}') limit 5").show()

if __name__ == "__main__":
    main()