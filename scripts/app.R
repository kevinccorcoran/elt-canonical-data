# AlphaStream dashboard (Shiny). The code lives in R/: the numbered files load in
# name order into the global environment (libraries, helpers, SQL, UI), and
# R/12_server.R evaluates R/server/* inside each session's server function.
# Split from one 11.5k-line app.R on 2026-10-19 without changing any code.
.app_dir <- local({
  has_parts <- function(d) length(d) == 1 && nzchar(d) && dir.exists(file.path(d, "R", "server"))
  f <- sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE))
  cands <- c(Sys.getenv("SHINY_APP_DIR"), if (length(f)) dirname(normalizePath(f[1])), getwd(),
             "/opt/airflow/scripts")
  Filter(has_parts, as.list(cands))[[1]]
})
for (.f in sort(list.files(file.path(.app_dir, "R"), pattern = "^[0-9].*[.]R$", full.names = TRUE)))
  source(.f, local = globalenv())

# Run the application
# Bind address/port are env-overridable (SHINY_HOST / SHINY_PORT) so the same
# script serves local and the prod server; defaults keep the historical 0.0.0.0:3838.
.shiny_port <- suppressWarnings(as.integer(Sys.getenv("SHINY_PORT", "3838")))
if (is.na(.shiny_port)) .shiny_port <- 3838L
runApp(list(ui = ui, server = server),
       host = Sys.getenv("SHINY_HOST", "0.0.0.0"), port = .shiny_port)
