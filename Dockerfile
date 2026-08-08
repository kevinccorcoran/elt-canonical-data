FROM apache/airflow:2.9.3

USER root
RUN apt-get -o Acquire::Max-FutureTime=86400 -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends git r-base libpq-dev build-essential libcurl4-openssl-dev libssl-dev zlib1g-dev pkg-config r-cran-shiny r-cran-jsonlite r-cran-dbi && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# shiny/later/httpuv/promises from CRAN, not apt: the r-cran-shiny apt bundle
# pins an old event-loop stack (shiny 1.7.4 / later 1.3.0 / httpuv 1.6.9). A
# prior fix rebuilt later/httpuv/promises from CRAN but LEFT shiny at apt 1.7.4,
# and that skew (old shiny driving new later/httpuv) is what segfaults R in
# execCallbacks(...loop$id) after a render. All four event-loop packages must be
# the same CRAN generation or the loop keeps crashing at a nil address.
RUN Rscript -e "install.packages(c('shiny', 'later', 'httpuv', 'promises', 'RPostgres', 'plotly', 'htmlwidgets', 'jsonlite', 'DT', 'nanoparquet'), repos='http://cran.us.r-project.org')" && \
    Rscript -e "for (p in c('shiny','httpuv','promises','RPostgres','plotly','jsonlite','DT','nanoparquet')) stopifnot(require(p, character.only=TRUE))" && \
    Rscript -e "cat('shiny', as.character(packageVersion('shiny')), 'later', as.character(packageVersion('later')), 'httpuv', as.character(packageVersion('httpuv')), '\n')"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
