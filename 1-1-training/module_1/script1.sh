export AWS_BUCKET="my-training-bucket"
export ENVIRONMENT="dev"


duckdb -c "SELECT '$AWS_BUCKET' AS bucket_name;"