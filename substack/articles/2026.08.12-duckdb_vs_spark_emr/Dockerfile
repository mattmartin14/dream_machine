FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir boto3 duckdb==1.5.5

# Pre-install DuckDB S3 extension at build time so ECS tasks don't need to fetch
# extensions at runtime in restricted networking environments.
RUN python - <<'PY'
import duckdb
conn = duckdb.connect()
conn.install_extension("aws")
conn.install_extension("httpfs")
conn.load_extension("aws")
conn.load_extension("httpfs")
conn.close()
PY

COPY skew_demo ./skew_demo

ENTRYPOINT ["python", "-m", "skew_demo.duckdb_job"]