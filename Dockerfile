FROM apache/airflow:2.9.3

USER root
RUN apt-get -o Acquire::Max-FutureTime=86400 -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends git libpq-dev build-essential libcurl4-openssl-dev libssl-dev zlib1g-dev pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# The Shiny dashboard has its own image since 2026-10-19 (docker/shiny/Dockerfile);
# no DAG runs R, so this image carries Airflow only.

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
