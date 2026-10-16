FROM apache/airflow:2.9.3

USER root
RUN apt-get -o Acquire::Max-FutureTime=86400 -o Acquire::Check-Valid-Until=false update && \
    apt-get install -y --no-install-recommends git r-base libpq-dev build-essential libcurl4-openssl-dev libssl-dev zlib1g-dev pkg-config r-cran-shiny r-cran-jsonlite r-cran-dbi && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
# shiny/later/httpuv/promises from CRAN, not apt: the r-cran-shiny apt bundle
# pins an old event-loop stack (shiny 1.7.4 / later 1.3.0 / httpuv 1.6.9), and
# all four event-loop packages must be the same CRAN generation.
# Packages come from a dated CRAN snapshot (Posit Package Manager), the day the
# working image was built, so every rebuild installs the same versions (shiny
# 1.14.0, later 1.4.8, httpuv 1.6.17, ...). The old http://cran.us.r-project.org
# mirror now answers 301 and install.packages() fails on it.
ARG CRAN_SNAPSHOT=https://packagemanager.posit.co/cran/2026-08-08
RUN Rscript -e "install.packages(c('shiny', 'later', 'httpuv', 'promises', 'RPostgres', 'plotly', 'htmlwidgets', 'jsonlite', 'DT', 'nanoparquet'), repos='${CRAN_SNAPSHOT}')" && \
    Rscript -e "for (p in c('shiny','httpuv','promises','RPostgres','plotly','jsonlite','DT','nanoparquet')) stopifnot(require(p, character.only=TRUE))" && \
    Rscript -e "cat('shiny', as.character(packageVersion('shiny')), 'later', as.character(packageVersion('later')), 'httpuv', as.character(packageVersion('httpuv')), '\n')"

# The dashboard's execCallbacks segfault (exit 139, "address (nil)") is
# rstudio/httpuv#171, unfixed upstream through 1.6.17: a request the client
# abandons while R is busy reaches onHeaders with its R env never created, and
# requestToEnv dereferences NULL. The healthcheck and watchdog curls (8-10 s
# timeouts) abandon a request every time a Generate blocks R for longer than
# that. Caught under gdb on 2026-07-23 and again on 2026-10-15 (requestToEnv
# pEnv=0x0, webapplication.cpp:294). A stock CRAN install silently drops the
# fix, so httpuv is rebuilt here from its pinned source with the patch; the
# build fails if the patch stops applying, and the entrypoint preflight checks
# the DESCRIPTION marker on every start.
ARG HTTPUV_VERSION=1.6.17
COPY scripts/patch_httpuv_171.py /tmp/patch_httpuv_171.py
RUN cd /tmp && \
    (curl -fsSL -o httpuv.tar.gz "${CRAN_SNAPSHOT}/src/contrib/httpuv_${HTTPUV_VERSION}.tar.gz" || \
     curl -fsSL -o httpuv.tar.gz "https://cloud.r-project.org/src/contrib/Archive/httpuv/httpuv_${HTTPUV_VERSION}.tar.gz") && \
    tar xzf httpuv.tar.gz && \
    python3 /tmp/patch_httpuv_171.py httpuv && \
    printf 'Patched: rstudio/httpuv#171 (scripts/patch_httpuv_171.py)\n' >> httpuv/DESCRIPTION && \
    R CMD INSTALL --library=/usr/local/lib/R/site-library httpuv && \
    Rscript -e "stopifnot(packageVersion('httpuv') == '${HTTPUV_VERSION}', !is.null(packageDescription('httpuv')[['Patched']]), require(shiny))" && \
    rm -rf /tmp/httpuv /tmp/httpuv.tar.gz /tmp/patch_httpuv_171.py

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
