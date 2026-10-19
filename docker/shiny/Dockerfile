# AlphaStream dashboard (Shiny) image: R and the dashboard's packages only.
# Split out of the Airflow image on 2026-10-19. The dashboard used to be R 4.2.2
# (Debian) bolted onto apache/airflow, with every package compiled from source on
# each rebuild. Packages now come as Ubuntu binaries from a dated Posit Package
# Manager snapshot, so a rebuild installs the same versions in a few minutes.
# Build context: the repo root (docker/airflow/docker-compose.yml, airflow-shiny).
FROM rocker/r-ver:4.5.3

ARG PPM_SNAPSHOT=2026-10-15
ARG HTTPUV_VERSION=1.6.17

# curl: healthcheck and watchdog; python3: scripts/shiny_monitor.py and the
# httpuv patch; libpq5: RPostgres; zlib1g-dev: building httpuv
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl python3 libpq5 zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

# C collation, as the Airflow image had: R sorts tickers by byte order
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN echo "options(repos = c(CRAN = 'https://packagemanager.posit.co/cran/__linux__/noble/${PPM_SNAPSHOT}'))" \
      >> "$(R RHOME)/etc/Rprofile.site" \
 && Rscript -e "install.packages(c('shiny', 'later', 'httpuv', 'promises', 'Rcpp', 'DBI', 'RPostgres', 'plotly', 'htmlwidgets', 'jsonlite', 'DT', 'nanoparquet', 'ggplot2'))" \
 && Rscript -e "for (p in c('shiny', 'DBI', 'RPostgres', 'plotly', 'jsonlite', 'DT', 'nanoparquet', 'ggplot2')) stopifnot(requireNamespace(p, quietly = TRUE))"

# rstudio/httpuv#171, unfixed upstream through 1.6.17: a request the client
# abandons while R is busy reaches onHeaders with its R env never created, and
# requestToEnv dereferences NULL (the exit-139 execCallbacks segfaults). A stock
# install silently drops the fix, so httpuv is rebuilt here from its pinned
# source with the patch; the build fails if the patch stops applying, and
# scripts/shiny_entrypoint.sh checks the DESCRIPTION marker on every start.
COPY scripts/patch_httpuv_171.py /tmp/patch_httpuv_171.py
RUN cd /tmp \
 && (curl -fsSL -o httpuv.tar.gz "https://packagemanager.posit.co/cran/${PPM_SNAPSHOT}/src/contrib/httpuv_${HTTPUV_VERSION}.tar.gz" \
     || curl -fsSL -o httpuv.tar.gz "https://cloud.r-project.org/src/contrib/Archive/httpuv/httpuv_${HTTPUV_VERSION}.tar.gz") \
 && tar xzf httpuv.tar.gz \
 && python3 /tmp/patch_httpuv_171.py httpuv \
 && printf 'Patched: rstudio/httpuv#171 (scripts/patch_httpuv_171.py)\n' >> httpuv/DESCRIPTION \
 && R CMD INSTALL httpuv \
 && Rscript -e "stopifnot(packageVersion('httpuv') == '${HTTPUV_VERSION}', !is.null(packageDescription('httpuv')[['Patched']]), requireNamespace('shiny'))" \
 && rm -rf /tmp/httpuv /tmp/httpuv.tar.gz /tmp/patch_httpuv_171.py

# the Airflow image's user (uid 50000, gid 0), so files the app writes into the
# mounted scripts/ dir keep the ownership they always had
RUN useradd --uid 50000 --gid 0 --create-home --shell /bin/bash airflow
USER airflow
WORKDIR /opt/airflow
CMD ["bash", "/opt/airflow/scripts/shiny_entrypoint.sh"]
