FROM apache/airflow:2.9.3

USER root
RUN apt-get -o Acquire::Max-FutureTime=86400 -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends git r-base libpq-dev build-essential libcurl4-openssl-dev libssl-dev pkg-config r-cran-shiny r-cran-jsonlite r-cran-dbi && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN Rscript -e "install.packages(c('RPostgres', 'plotly', 'htmlwidgets', 'jsonlite', 'DT', 'nanoparquet'), repos='http://cran.us.r-project.org')" && \
    Rscript -e "for (p in c('shiny','RPostgres','plotly','jsonlite','DT','nanoparquet')) stopifnot(require(p, character.only=TRUE))"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
