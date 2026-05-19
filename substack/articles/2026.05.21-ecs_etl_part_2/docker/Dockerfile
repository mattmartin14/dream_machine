FROM public.ecr.aws/amazonlinux/amazonlinux:2023

RUN dnf -y update \
    && dnf -y install python3.13 python3.13-pip \
    && dnf clean all

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python3.13 -m pip install --no-cache-dir --upgrade pip \
    && python3.13 -m pip install --no-cache-dir -r /app/requirements.txt

ENV DUCKDB_EXTENSION_DIR="/opt/duckdb/extensions"
RUN mkdir -p "${DUCKDB_EXTENSION_DIR}" \
    && python3.13 -c "import os, duckdb; con = duckdb.connect(':memory:'); con.execute(\"SET extension_directory='\" + os.environ['DUCKDB_EXTENSION_DIR'] + \"';\"); con.execute('INSTALL httpfs;'); con.execute('INSTALL aws;'); con.close()"

COPY app /app/runner
COPY helpers /app/helpers

ENV PYTHONPATH="/app:${PYTHONPATH}"
ENV PYTHONUNBUFFERED=1
CMD ["python3.13", "/app/runner/runner.py"]
