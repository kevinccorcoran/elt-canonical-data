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
# httpuv is built from PATCHED source, not installed stock. Stock httpuv carries
# the rstudio/httpuv#171 null-_env deref that segfaults R on aborted connections
# (gdb-confirmed 2026-07-22/23; see scripts/patch_httpuv_171.py) -- the crash
# that drops the dashboard to its grey disconnect overlay. Hand-patching a live
# container is wiped by every rebuild, so bake it into the image here. httpuv is
# pinned to the version the patch targets so its source anchors match; the patch
# script exits non-zero on a mismatch, failing the build loudly instead of
# shipping an unpatched (crash-prone) httpuv. Rcpp/R6/later/promises are
# pre-installed because `R CMD INSTALL` does not resolve deps like install.packages.
COPY scripts/patch_httpuv_171.py /tmp/patch_httpuv_171.py
RUN Rscript -e "install.packages(c('Rcpp', 'R6', 'later', 'promises', 'RPostgres', 'plotly', 'htmlwidgets', 'jsonlite', 'DT', 'nanoparquet'), repos='http://cran.us.r-project.org')" && \
    mkdir -p /tmp/hp && cd /tmp/hp && \
    ( curl -fsSL -o httpuv.tar.gz https://cran.r-project.org/src/contrib/httpuv_1.7.1.tar.gz || \
      curl -fsSL -o httpuv.tar.gz https://cran.r-project.org/src/contrib/Archive/httpuv/httpuv_1.7.1.tar.gz ) && \
    tar xf httpuv.tar.gz && \
    python3 /tmp/patch_httpuv_171.py httpuv && \
    R CMD INSTALL httpuv && \
    cd / && rm -rf /tmp/hp /tmp/patch_httpuv_171.py && \
    Rscript -e "for (p in c('shiny','httpuv','promises','RPostgres','plotly','jsonlite','DT','nanoparquet')) stopifnot(require(p, character.only=TRUE))"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
