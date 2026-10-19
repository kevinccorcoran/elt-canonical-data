# AlphaStream dashboard - Libraries and options. Part of app.R, split 2026-10-19: files load in name order (see app.R).
library(shiny)
library(DBI)
library(RPostgres)
library(plotly)
library(jsonlite)
library(nanoparquet)
library(DT)
library(ggplot2)

# TEMP DIAGNOSTIC 2026-07-22: full stack traces for the crash-loop hunt.
# Remove once the segfault/sprintf source is fixed.
options(shiny.fullstacktrace = TRUE)

