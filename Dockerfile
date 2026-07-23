FROM apache/airflow:2.9.3

USER root
RUN apt-get -o Acquire::Max-FutureTime=86400 -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends git r-base libpq-dev build-essential libcurl4-openssl-dev libssl-dev zlib1g-dev pkg-config r-cran-shiny r-cran-jsonlite r-cran-dbi && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# later/httpuv/promises from CRAN, not apt: the r-cran-shiny bundle pins an old
# event-loop stack (later 1.3.0 / httpuv 1.6.9) that segfaults R in
# execCallbacks under reconnect churn; the three must be built together or a
# stale-ABI binary keeps crashing the loop
RUN Rscript -e "install.packages(c('later', 'httpuv', 'promises', 'RPostgres', 'plotly', 'htmlwidgets', 'jsonlite', 'DT', 'nanoparquet'), repos='http://cran.us.r-project.org')" && \
    Rscript -e "for (p in c('shiny','httpuv','promises','RPostgres','plotly','jsonlite','DT','nanoparquet')) stopifnot(require(p, character.only=TRUE))"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
