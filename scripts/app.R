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

# ─── QA history parquet (persists across runs, one file across DBs) ───
QA_HISTORY_DIR  <- "/opt/airflow/scripts/data"
QA_HISTORY_PATH <- file.path(QA_HISTORY_DIR, "ticker_count_history.parquet")

load_qa_history <- function() {
  if (!file.exists(QA_HISTORY_PATH)) return(NULL)
  tryCatch(as.data.frame(nanoparquet::read_parquet(QA_HISTORY_PATH)),
           error = function(e) NULL)
}

append_qa_history <- function(results, db_name, run_at = Sys.time()) {
  if (!dir.exists(QA_HISTORY_DIR)) dir.create(QA_HISTORY_DIR, recursive = TRUE)
  new_rows <- data.frame(
    run_at        = rep(run_at, nrow(results)),
    database_name = rep(db_name, nrow(results)),
    schema_name   = results$schema,
    table_name    = results$table,
    ticker_count  = as.numeric(results$ticker_count),
    stringsAsFactors = FALSE
  )
  existing <- load_qa_history()
  all_rows <- if (is.null(existing)) new_rows else rbind(existing, new_rows)
  nanoparquet::write_parquet(all_rows, QA_HISTORY_PATH)
  new_rows
}

latest_qa_run <- function(history) {
  if (is.null(history) || nrow(history) == 0) return(NULL)
  latest_ts <- max(history$run_at)
  history[history$run_at == latest_ts, , drop = FALSE]
}

# ─── Schema display order + lineage ranks from dbt manifests ───
SCHEMA_ORDER <- c("raw", "cdm", "metrics", "analysis", "inference")

compute_lineage_ranks <- function() {
  manifest_paths <- c(
    "/opt/elt-inference-models/target/manifest.json",
    "/opt/elt-canonical-data/dbt/src/app/target/manifest.json"
  )
  node_info <- list()
  for (p in manifest_paths) {
    if (!file.exists(p)) next
    m <- tryCatch(jsonlite::fromJSON(p, simplifyVector = FALSE),
                  error = function(e) NULL)
    if (is.null(m) || is.null(m$nodes)) next
    for (uid in names(m$nodes)) {
      n <- m$nodes[[uid]]
      rt <- n$resource_type
      if (isTRUE(rt == "model") || isTRUE(rt == "seed")) {
        node_info[[uid]] <- list(
          schema = tolower(n$schema),
          name   = tolower(n$name),
          deps   = unlist(n$depends_on$nodes)
        )
      }
    }
  }
  if (length(node_info) == 0) {
    return(data.frame(schema=character(), name=character(), rank=integer(),
                      stringsAsFactors = FALSE))
  }
  rank_of  <- new.env(hash = TRUE)
  visiting <- new.env(hash = TRUE)
  compute_rank <- function(uid) {
    cached <- rank_of[[uid]]
    if (!is.null(cached)) return(cached)
    if (!is.null(visiting[[uid]])) return(0L)
    visiting[[uid]] <- TRUE
    info <- node_info[[uid]]
    if (is.null(info) || length(info$deps) == 0) {
      r <- 0L
    } else {
      dep_ranks <- vapply(info$deps, function(d) {
        if (!is.null(node_info[[d]])) compute_rank(d) else 0L
      }, integer(1))
      r <- as.integer(max(dep_ranks)) + 1L
    }
    rm(list = uid, envir = visiting)
    rank_of[[uid]] <- r
    r
  }
  for (uid in names(node_info)) compute_rank(uid)
  data.frame(
    schema = vapply(node_info, `[[`, character(1), "schema"),
    name   = vapply(node_info, `[[`, character(1), "name"),
    rank   = vapply(names(node_info), function(u) rank_of[[u]], integer(1)),
    stringsAsFactors = FALSE
  )
}

LINEAGE_RANKS <- tryCatch(compute_lineage_ranks(),
  error = function(e) data.frame(schema=character(), name=character(),
                                 rank=integer(), stringsAsFactors = FALSE))

# Return sort order for parallel (schema, table) vectors: schema priority,
# then lineage rank, then alpha. Unknown schemas/tables go to the end.
lineage_order <- function(schemas, tables) {
  key <- data.frame(schema = tolower(schemas), name = tolower(tables),
                    stringsAsFactors = FALSE)
  key$idx <- seq_len(nrow(key))
  merged <- merge(key, LINEAGE_RANKS, by = c("schema","name"),
                  all.x = TRUE, sort = FALSE)
  merged <- merged[order(merged$idx), ]
  ranks <- merged$rank
  ranks[is.na(ranks)] <- .Machine$integer.max %/% 2L
  schema_prio <- match(merged$schema, SCHEMA_ORDER)
  schema_prio[is.na(schema_prio)] <- 100L
  order(schema_prio, ranks, merged$name)
}

# Custom CSS matching the returns_analyzer.html dark theme
custom_css <- "
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

body {
  background-color: #0f172a !important;
  background-image:
    radial-gradient(at 0% 0%, rgba(56, 189, 248, 0.15) 0px, transparent 50%),
    radial-gradient(at 100% 100%, rgba(239, 68, 68, 0.1) 0px, transparent 50%);
  color: #f8fafc !important;
  font-family: 'Inter', sans-serif !important;
  min-height: 100vh;
}

/* Tab strip: pin dark explicitly - the Bootstrap navbar otherwise follows
   the browser's light color scheme and renders a white bar. Selectors cover
   both BS3 (.navbar-default, li.active) and BS5 (.nav-link) variants. */
.navbar, .navbar-default, .navbar-static-top {
  background-color: rgba(15, 23, 42, 0.95) !important;
  border: none !important;
  box-shadow: 0 1px 0 rgba(255, 255, 255, 0.06);
}
.navbar .navbar-brand, .navbar-default .navbar-brand {
  color: #f8fafc !important; font-weight: 600;
}
.navbar-nav > li > a, .navbar-nav .nav-link {
  color: #94a3b8 !important; background: transparent !important;
}
.navbar-nav > li > a:hover, .navbar-nav .nav-link:hover {
  color: #f8fafc !important;
}
.navbar-nav > .active > a, .navbar-nav > .active > a:hover,
.navbar-nav > .active > a:focus, .navbar-nav .nav-link.active {
  color: #f8fafc !important;
  background-color: rgba(56, 189, 248, 0.12) !important;
}

h2 {
  font-size: 1.5rem !important;
  font-weight: 700 !important;
  letter-spacing: -0.025em;
  background: linear-gradient(to right, #38bdf8, #818cf8);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  margin-bottom: 1.5rem !important;
}

h4 {
  font-size: 0.875rem !important;
  font-weight: 600 !important;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: #94a3b8 !important;
}

.well {
  background: rgba(30, 41, 59, 0.7) !important;
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  border-radius: 1rem !important;
  backdrop-filter: blur(12px);
  box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5) !important;
  color: #f8fafc !important;
}

.main-card {
  background: rgba(30, 41, 59, 0.7);
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 1rem;
  padding: 1.5rem !important;
  backdrop-filter: blur(12px);
  box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.5);
}

label, .control-label {
  color: #94a3b8 !important;
  font-size: 0.75rem !important;
  font-weight: 500 !important;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.form-control {
  background: rgba(15, 23, 42, 0.5) !important;
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  border-radius: 0.5rem !important;
  color: #f8fafc !important;
  font-family: 'Inter', monospace !important;
  font-size: 0.875rem !important;
  padding: 0.5rem 0.75rem !important;
  transition: all 0.2s;
}

.form-control:focus {
  border-color: #38bdf8 !important;
  box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2) !important;
  outline: none !important;
}

textarea.form-control {
  font-family: 'JetBrains Mono', 'Fira Code', monospace !important;
  font-size: 0.8rem !important;
  line-height: 1.6;
}

.btn-primary, .btn-default.action-button {
  background: #38bdf8 !important;
  color: #000 !important;
  border: none !important;
  border-radius: 0.5rem !important;
  font-weight: 600 !important;
  padding: 0.75rem 1.5rem !important;
  cursor: pointer;
  transition: all 0.2s;
  font-family: 'Inter', sans-serif !important;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  font-size: 0.8rem !important;
}

.btn-primary:hover, .btn-default.action-button:hover {
  background: #7dd3fc !important;
  transform: translateY(-1px);
}

hr {
  border-color: rgba(255, 255, 255, 0.1) !important;
}

.help-block {
  color: #64748b !important;
  font-size: 0.8rem !important;
}

/* Select dropdown styling */
.selectize-input {
  background: rgba(15, 23, 42, 0.5) !important;
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  border-radius: 0.5rem !important;
  color: #f8fafc !important;
  font-family: 'Inter', sans-serif !important;
}

.selectize-input.focus {
  border-color: #38bdf8 !important;
  box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2) !important;
}

.selectize-dropdown {
  background: #1e293b !important;
  border: 1px solid rgba(255, 255, 255, 0.1) !important;
  color: #f8fafc !important;
}

.selectize-dropdown .active {
  background: rgba(56, 189, 248, 0.2) !important;
  color: #f8fafc !important;
}

.selectize-dropdown-content .option {
  color: #f8fafc !important;
}

/* Caveat / read-me boxes above charts. note = neutral doc, info = explanation,
   warning = interpretation caveat the reader must not skip. */
.caveat-note, .caveat-info, .caveat-warning {
  padding: 0.5rem 0.75rem;
  background: rgba(255,255,255,0.03);
  border-radius: 4px;
  color: #94a3b8;
  font-size: 0.75rem;
  line-height: 1.4;
  margin-bottom: 0.75rem;
}
.caveat-note    { border-left: 2px solid #64748b; }
.caveat-info    { border-left: 2px solid #38bdf8; }
.caveat-warning { border-left: 2px solid #f59e0b; }
"

# ─── Helper: sidebar panel for a given tab suffix ───
make_sidebar <- function(suffix, title, filter_widgets) {
  sidebarPanel(
    h4(title),
    selectInput(paste0("db_env", suffix), "Environment", choices = c("Production", "Staging", "Dev"), selected = "Production"),
    textInput(paste0("db_host", suffix), "Host", value = "host.docker.internal"),
    textInput(paste0("db_port", suffix), "Port", value = "5432"),
    textInput(paste0("db_user", suffix), "User", value = "postgres"),
    passwordInput(paste0("db_pass", suffix), "Password", value = ""),
    actionButton(paste0("connect_btn", suffix), "Connect", class = "btn-primary"),
    hr(),
    h5("Filters"),
    div(id = paste0("filter_panel", suffix), filter_widgets),
    hr(),
    actionButton(paste0("execute_", suffix), "Generate Chart", class = "btn-primary w-100", style = "margin-top: 1rem;"),
    hr(),
    textOutput(paste0("statusMessage", suffix))
  )
}

# ─── Shared plot/style helpers ───

# Credibility tier colors (cell_credibility.tier), used by every tier plot.
TIER_COLORS <- c(high = '#10b981', medium = '#f59e0b', noise = '#64748b',
                 thin = '#334155', anti = '#dc2626')

# Purple -> yellow -> teal diverging scale; midpoint = zero / coin flip.
DIVERGING_COLORSCALE <- list(
  c(0.00, '#762a83'), c(0.25, '#c2a5cf'), c(0.50, '#f7f7b6'),
  c(0.75, '#5ab4ac'), c(1.00, '#01665e'))

# Standard transparent-background placeholder shown before data loads or
# when a query matches nothing.
empty_plot <- function(msg) {
  plot_ly() %>% layout(
    title = list(text = msg, font = list(color = "#f8fafc")),
    paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)")
}

# Transparent dark-theme layout wrapper; pass axis/shape/margin args through.
dark_layout <- function(p, ...) {
  layout(p, paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)", ...)
}

# Postgres COUNT()/SUM()/bigint arrive as integer64 (bit64). Left uncast,
# bit64 arithmetic truncates fractions BEFORE dividing (hit_rate 0.75 -> 0
# inside weighted.mean), and sprintf("%d") errors. Cast to double up front.
coerce_numeric_cols <- function(df, cols) {
  for (col in intersect(cols, names(df))) df[[col]] <- as.numeric(df[[col]])
  df
}

# Row-separator shapes for rank-grouped horizontal bar charts. ids_display is
# the id per bar in DISPLAY (top-to-bottom) order; the categoryarray is the
# reverse, so boundaries are computed on the reversed vector. Thin line
# between every rank row, stronger line where the cluster id changes; the
# faint per-row lines are dropped beyond 120 bars to keep the DOM sane.
rank_sep_shapes <- function(ids_display) {
  # ids_display = cluster id per row, TOP to BOTTOM display order.
  # Lines use paper coords: category slots fill the plot area evenly, so the
  # boundary after k top rows sits at 1 - k/n. Numeric y0 on a category axis
  # is interpreted inconsistently by plotly.js (can stretch the axis with
  # phantom slots), so shapes never reference the category axis directly.
  n <- length(ids_display)
  if (n < 2) return(list())
  shapes <- lapply(seq_len(n - 1), function(k) {
    id_change <- !identical(ids_display[k], ids_display[k + 1])
    if (!id_change && n > 120) return(NULL)
    yy <- 1 - k / n
    list(type = "line", xref = "paper", x0 = 0, x1 = 1, yref = "paper",
         y0 = yy, y1 = yy,
         line = list(width = if (id_change) 1 else 0.5,
                     color = if (id_change) "rgba(255,255,255,0.35)"
                             else "rgba(255,255,255,0.12)"))
  })
  Filter(Negate(is.null), shapes)
}

# ─── Define UI ───
ui <- navbarPage(
  title = "Analysis Dashboard",
  tags$head(tags$style(HTML(custom_css))),

  # ── Tab 1: Transition Range ──
  tabPanel("Transition Range",
    sidebarLayout(
      make_sidebar("T", "Database Connection (Transition)", tagList(
        selectInput("id_valT", "ID", choices = c("Connect first..." = ""), selected = ""),
        selectInput("past_fib_lagT", "Fibonacci Lag Value", choices = c("Connect first..." = ""), selected = ""),
        selectInput("future_fib_lagT", "Future Fibonacci Lag", choices = c("Connect first..." = ""), selected = ""),
        radioButtons("transition_modeT", "View",
                     choices = c("Empirical (all buckets)" = "empirical",
                                 "Actionable (current tickers only)" = "actionable"),
                     selected = "empirical", inline = FALSE),
        tags$div(
          style = "margin-top: 1.5rem; padding: 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #64748b; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; font-family: 'Inter'; line-height: 1.5;",
          tags$style(HTML("
            details > summary { color: #f8fafc; font-weight: 600; margin-top: 0.6rem; margin-bottom: 0.3rem; cursor: pointer; list-style: none; }
            details > summary::-webkit-details-marker { display: none; }
            details > summary::before { content: '\\25B8 '; display: inline-block; transition: transform 0.15s; }
            details[open] > summary::before { content: '\\25BE '; }
            details > div { margin-left: 0.8rem; }
          ")),

          tags$details(open = NA,
            tags$summary("Distribution metrics"),
            tags$div(
              tags$div(HTML("<b>Return /mo</b> = future_median / future_lag")),
              tags$div(HTML("<b>Improv /mo</b> = future_median / future_lag &minus; past_median / past_lag")),
              tags$div(HTML("<b>Risk /mo</b> = (future_q3 &minus; future_q1) / &radic;future_lag")),
              tags$div(HTML("<b>Tail Risk /mo</b> = max(q1&minus;min, max&minus;q3) / &radic;future_lag"))
            )
          ),

          tags$details(
            tags$summary("Positive flags"),
            tags$div(
              tags$div(HTML("&times;3 future_median &gt; 0")),
              tags$div(HTML("&times;2 Return /mo &divide; Risk /mo &ge; 0.5")),
              tags$div(HTML("&times;1 future_q1 &gt; 0")),
              tags$div(HTML("&times;1 future_median &gt; past_median")),
              tags$div(HTML("&times;1 future IQR &lt; past IQR"))
            )
          ),

          tags$details(
            tags$summary("Negative flags"),
            tags$div(
              tags$div(HTML("&times;3 future_median &lt; 0")),
              tags$div(HTML("&times;3 future_q3 &lt; 0")),
              tags$div(HTML("&times;1 future_median &lt; past_median")),
              tags$div(HTML("&times;1 future_risk_score &gt; p90 cutoff"))
            )
          ),

          tags$details(
            tags$summary("Aggregates"),
            tags$div(
              tags$div(HTML("<b>net_score</b> = positive_score &minus; negative_score")),
              tags$div(HTML("<b>signal_score</b>: STRONG_BUY=+3, BUY=+2, HOLD=0, WATCH=&minus;1, AVOID=&minus;2, SELL=&minus;3")),
              tags$div(HTML("<b>combined_score</b> = signal_score + net_score")),
              tags$div(HTML("<b>recommendation</b> = combined_score tier (STRONG_PICK / BUY / HOLD / AVOID / OUTLIER_*)"))
            )
          )
        )
      )),
      mainPanel(div(class = "main-card", style = "height: calc(100vh - 4rem); display: flex; flex-direction: column;",
        h4("Transition range — scoring.return_cluster_cell_score_extended",
           style = "color: #f8fafc; margin-bottom: 0.5rem; font-weight: 600;"),
        uiOutput("transitionHeader"),
        div(style = "flex: 1; min-height: 0;", plotlyOutput("transitionPlot", height = "100%"))
      ))
    )
  ),

  # ── Tab 3: Data QA ──
  tabPanel("Data QA",
    sidebarLayout(
      make_sidebar("Q", "Database Connection (Data QA)", tagList(
        tags$div(
          style = "padding: 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #64748b; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; font-family: 'Inter'; line-height: 1.5;
                   margin-bottom: 0.75rem;",
          tags$div(style = "color: #f8fafc; font-weight: 600; margin-bottom: 0.4rem;", "Scan"),
          "Scans every table with a ", tags$code("ticker"),
          " column. Connect to load schemas, pick which to include, then Generate Chart."
        ),
        uiOutput("qaSchemasUI")
      )),
      mainPanel(
        tags$style(HTML("
          #qaHistoryTable table { width: 100%; color: #f8fafc; font-family: 'Inter'; font-size: 0.85rem; }
          #qaHistoryTable th { color: #94a3b8; border-bottom: 1px solid rgba(255,255,255,0.1); padding: 0.5rem; text-align: left; }
          #qaHistoryTable td { border-bottom: 1px solid rgba(255,255,255,0.05); padding: 0.5rem; font-family: 'JetBrains Mono', monospace; }
          #qaHistoryTable tr:hover td { background: rgba(56,189,248,0.08); }
        ")),
        div(class = "main-card",
          h4("Ticker counts per table (parquet history)",
             style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
          tags$style(HTML("
            .qa-filter-head { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 0.25rem; }
            .qa-filter-head .control-label { margin-bottom: 0 !important; }
            .qa-filter-actions a { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; }
            .qa-filter-actions a + a { margin-left: 0.5rem; }
            .qa-filter-actions a.qa-all   { color: #38bdf8; }
            .qa-filter-actions a.qa-clear { color: #64748b; }
            /* DT dark theme overrides */
            #qaHistoryTable .dataTables_wrapper,
            #qaHistoryTable .dataTables_info,
            #qaHistoryTable .dataTables_paginate { color: #94a3b8 !important; }
            #qaHistoryTable table.dataTable thead th {
              color: #94a3b8 !important;
              border-bottom: 1px solid rgba(255,255,255,0.1) !important;
              background: transparent !important; }
            #qaHistoryTable table.dataTable tbody td {
              color: #f8fafc !important;
              border-top: 1px solid rgba(255,255,255,0.05) !important;
              background: transparent !important;
              font-family: 'JetBrains Mono', monospace;
              font-size: 0.85rem; }
            #qaHistoryTable table.dataTable tbody tr:hover td { background: rgba(56,189,248,0.08) !important; }
            #qaHistoryTable table.dataTable tbody tr.selected td { background: rgba(56,189,248,0.25) !important; color: #f8fafc !important; }
            #qaHistoryTable .paginate_button { color: #94a3b8 !important; }
            #qaHistoryTable .paginate_button.current,
            #qaHistoryTable .paginate_button:hover { color: #000 !important; background: #38bdf8 !important; border-color: #38bdf8 !important; }
          ")),
          div(style = "display: flex; gap: 0.75rem; margin-bottom: 1rem; flex-wrap: wrap;",
            div(style = "flex: 1; min-width: 180px;",
              tags$div(class = "qa-filter-head",
                tags$label("Run at", class = "control-label"),
                tags$span(class = "qa-filter-actions",
                  actionLink("filterRunAtAllQ",   "all",   class = "qa-all"),
                  actionLink("filterRunAtClearQ", "clear", class = "qa-clear"))
              ),
              selectizeInput("filterRunAtQ", label = NULL, choices = NULL, multiple = TRUE,
                             options = list(placeholder = "All runs (newest first)",
                                            plugins = list("remove_button")))
            ),
            div(style = "flex: 1; min-width: 180px;",
              tags$div(class = "qa-filter-head",
                tags$label("Database", class = "control-label"),
                tags$span(class = "qa-filter-actions",
                  actionLink("filterDbAllQ",   "all",   class = "qa-all"),
                  actionLink("filterDbClearQ", "clear", class = "qa-clear"))
              ),
              selectizeInput("filterDbQ", label = NULL, choices = NULL, multiple = TRUE,
                             options = list(placeholder = "All databases",
                                            plugins = list("remove_button")))
            ),
            div(style = "flex: 1; min-width: 180px;",
              tags$div(class = "qa-filter-head",
                tags$label("Schema", class = "control-label"),
                tags$span(class = "qa-filter-actions",
                  actionLink("filterSchemaAllQ",   "all",   class = "qa-all"),
                  actionLink("filterSchemaClearQ", "clear", class = "qa-clear"))
              ),
              selectizeInput("filterSchemaQ", label = NULL, choices = NULL, multiple = TRUE,
                             options = list(placeholder = "All schemas",
                                            plugins = list("remove_button")))
            )
          ),
          div(style = "display: flex; gap: 0.75rem; margin-bottom: 0.75rem; align-items: center;",
            actionLink("qaSelectAllQ",    "Select all",    style = "font-size: 0.75rem; color: #38bdf8; font-weight: 600; text-transform: uppercase;"),
            actionLink("qaSelectNoneQ",   "Select none",   style = "font-size: 0.75rem; color: #64748b; font-weight: 600; text-transform: uppercase;"),
            actionLink("qaSelectInvertQ", "Invert",        style = "font-size: 0.75rem; color: #a855f7; font-weight: 600; text-transform: uppercase;"),
            tags$span(style = "flex: 1;"),
            actionButton("qaDeleteSelectedQ", "Delete selected",
              style = "background: transparent !important; color: #f87171 !important; border: 1px solid #f87171 !important; font-size: 0.75rem !important; padding: 0.4rem 0.9rem !important;")
          ),
          DT::DTOutput("qaHistoryTable")
        )
      )
    )
  ),

  # ── Tab 4: Ticker Coverage ──
  tabPanel("Ticker Coverage",
    sidebarLayout(
      make_sidebar("V", "Database Connection (Coverage)", tagList(
        tags$div(
          style = "padding: 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #64748b; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; font-family: 'Inter'; line-height: 1.5;",
          tags$div(style = "color: #f8fafc; font-weight: 600; margin-bottom: 0.4rem;", "What this shows"),
          "One horizontal bar per ticker in ", tags$code("cdm.ingest_combined"),
          ". Left edge = first observation, right edge = last observation. Dashed vertical line = today."
        )
      )),
      mainPanel(div(class = "main-card",
        h4("Ticker history coverage — cdm.ingest_combined",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        uiOutput("coveragePlotContainer")
      ))
    )
  ),

  # ── Tab: Clusters ──
  tabPanel("Clusters",
    sidebarLayout(
      make_sidebar("K", "Database Connection (Clusters)", tagList()),
      mainPanel(div(class = "main-card",
        h4("Per-ticker scatter — analysis.ticker_cluster_segments × clustering.ticker_cluster_volatility_summary",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        tags$div(style = "margin-bottom: 0.75rem;",
          actionButton("clusterShowAll", "Show all", class = "btn-primary",
                       style = "margin-right: 0.5rem;"),
          actionButton("clusterHideAll", "Hide all", class = "btn-primary")
        ),
        plotlyOutput("clusterPlot", height = "600px")
      ))
    )
  ),

  # ── Tab: Top Picks ──
  tabPanel("Top Picks",
    sidebarLayout(
      make_sidebar("P", "Database Connection (Top Picks)", tagList(
        selectInput("id_valP", "Cluster ID", choices = c("Connect first..." = ""), selected = ""),
        sliderInput("top_n_valP", "Top N tickers", min = 5, max = 400, value = 30, step = 5)
      )),
      mainPanel(div(class = "main-card",
        h4("Top picks across horizons — serving.return_cluster_ticker_summary_current x return_cluster_ticker_pair_current",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #38bdf8; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; line-height: 1.4;
                   margin-bottom: 0.75rem;",
          "Rows: top-N tickers by agg_rank in the selected cluster (rank 1 at top). ",
          "Columns: fut_lag (1 to 33 months). ",
          "Color: SIGNED RELIABILITY EDGE = (wilson_lower - 0.5) x direction, averaged over cells with a directional vote. ",
          "wilson_lower is the conservative lower bound of the walk-forward agreement rate, so thin-sample cells get penalized vs dense-sample cells with the same raw agreement. ",
          "Bright green = reliable BUY with dense evidence. Bright red = reliable AVOID with dense evidence. Yellow = vote near coin flip OR thin-evidence cell whose raw agreement is high but wilson_lower is modest. Blank = no directional vote. ",
          "Scale fixed at -0.3 to +0.3 (clips extreme cells but spans p90 of high-tier cells). Row label format: #rank ticker (n=coverage_cell_count). Hover shows raw wilson_lower and cell count."
        ),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #fbbf24; border-radius: 4px;
                   color: #f8fafc; font-size: 0.85rem; line-height: 1.4;
                   margin-bottom: 0.75rem; font-family: 'JetBrains Mono', monospace;",
          textOutput("clusterIcDisplayP")
        ),
        plotlyOutput("topPicksPlot", height = "900px")
      ))
    )
  ),

  # ── Tab: Rank Stability ──
  tabPanel("Rank Stability",
    sidebarLayout(
      make_sidebar("RS", "Database Connection (Rank Stability)", tagList(
        selectInput("id_valRS", "Cluster ID",
                    choices = c("Connect first..." = ""), selected = ""),
        sliderInput("top_n_valRS", "Max vingtile to display (1 = top 5%, 20 = bottom 5%)",
                    min = 5, max = 20, value = 20, step = 1),
        selectInput("metric_valRS", "Metric",
                    choices = c("Mean return" = "mean_return",
                                "Median return" = "median_return",
                                "Hit rate (% correct direction)" = "hit_rate",
                                "Sharpe-like (mean/sd)" = "sharpe_like",
                                "Combo: hit + median (quadrants)" = "combo_quadrants",
                                "Combo: hit + mean (quadrants)" = "combo_mean_quadrants"),
                    selected = "mean_return"),
        dateRangeInput("cutoff_rangeRS", "Cutoff range",
                       start = NULL, end = NULL,
                       startview = "year", separator = " to ")
      )),
      mainPanel(div(class = "main-card",
        h4("Rank stability across walk-forward cohorts - validation.walk_forward_ticker_rank",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #f59e0b; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; line-height: 1.4;
                   margin-bottom: 0.75rem;",
          tags$b(style = "color: #fbbf24;", "Caveat: "),
          "cluster_id is NOT identity-stable across walk-forward refits. ",
          "Treat the cluster selector as a ", tags$i("growth-tier alignment"),
          " filter (tickers that landed in the same growth-vol regime as the selected cluster id at each cutoff), ",
          "not as a fixed-membership cohort. ",
          "Slot performance is cluster-agnostic by design (aggregated across all 84 cohorts)."
        ),
        textOutput("cutoffCountRS"),
        tags$br(),
        div(style = "display: flex; align-items: center; gap: 0.75rem;",
            h5("Slot performance - mean realized forward return per rank slot",
               style = "color: #f8fafc; font-weight: 600; margin: 0; flex: 1;"),
            checkboxInput("show_slot_perfRS", "Show", value = TRUE)
        ),
        conditionalPanel(
          condition = "input.show_slot_perfRS",
          plotlyOutput("slotPerfPlotRS", height = "380px")
        ),
        tags$br(),
        div(style = "display: flex; align-items: center; gap: 0.75rem;",
            h5("Vingtile (5% bin) vs fut_lag heatmap - mean realized return at each (vingtile, horizon) across 84 cohorts. Rank normalized to within-cluster vingtile so different cluster sizes compare fairly.",
               style = "color: #f8fafc; font-weight: 600; margin: 0; flex: 1;"),
            checkboxInput("show_vingtile_heatmapRS", "Show", value = TRUE)
        ),
        conditionalPanel(
          condition = "input.show_vingtile_heatmapRS",
          plotlyOutput("stabilityHeatmapRS", height = "800px")
        ),
        tags$br(),
        actionButton("execute_all_RS", "Generate small-multiples for ALL ids",
                     class = "btn-primary", style = "margin-bottom: 1rem;"),
        h5("All 19 ids - green = model was right (sign-adjusted: longs up = green, shorts down = profitable short). Combo - longs (id 1-12): gray=neither, blue=hit only, gold=Bessembinder, green=both high. Shorts (id 13-19): gray=neither, light purple=hit only, medium purple=Bessembinder, deep purple=both high (shorting worked).",
           style = "color: #f8fafc; font-weight: 600;"),
        plotlyOutput("allIdsGridRS", height = "1200px")
      ))
    )
  ),

  # ── Tab 8: Model Validation ──
  tabPanel("Model Validation",
    sidebarLayout(
      make_sidebar("MV", "Database Connection (Model Validation)", tagList(
        selectInput("id_valMV", "Cluster ID",
                    choices = c("Connect first..." = ""), selected = ""),
        selectInput("past_lagMV", "Past lag (Wilson forest)",
                    choices = c("Connect first..." = ""), selected = ""),
        selectInput("fut_lagMV", "Future lag (Wilson forest)",
                    choices = c("Connect first..." = ""), selected = "")
      )),
      mainPanel(div(class = "main-card",
        h4("Walk-forward IC - validation.walk_forward_id_ic",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        plotlyOutput("icHeatmapMV", height = "600px"),
        tags$br(),
        h4("Holdout payoff backtest - validation.return_cluster_payoff_backtest",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #f59e0b; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; line-height: 1.4;
                   margin-bottom: 0.75rem;",
          tags$b(style = "color: #fbbf24;", "Caveat: "),
          "read matched short horizons only (past_lag = fut_lag, fut_lag <= 12); ",
          "longer horizons have overlapping holdout windows and read noisy."
        ),
        plotlyOutput("payoffScatterMV", height = "460px"),
        tags$br(),
        h5("Expectancy heatmap (past_lag x fut_lag) - select a specific id",
           style = "color: #f8fafc; font-weight: 600;"),
        plotlyOutput("payoffHeatmapMV", height = "420px"),
        tags$br(),
        h4("Cell credibility - validation.cell_credibility",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        h5("Cells by tier per id (high/medium = actionable, thin = n<10, anti = reliably wrong)",
           style = "color: #f8fafc; font-weight: 600;"),
        plotlyOutput("tierBarMV", height = "380px"),
        tags$br(),
        h5("Wilson forest - selected (id, past_lag, fut_lag); dashed line = coin flip 0.5",
           style = "color: #f8fafc; font-weight: 600;"),
        plotlyOutput("forestPlotMV", height = "460px")
      ))
    )
  ),

  # ── Tab 9: Buy List ──
  tabPanel("Buy List",
    sidebarLayout(
      make_sidebar("BL", "Database Connection (Buy List)", tagList(
        tags$label("As-of date", style = "color: #94a3b8; font-weight: 600;"),
        # splitLayout cells default to overflow:hidden, which clips the
        # selectize option list (year dropdown showed one cut-off row)
        tags$style(HTML(".shiny-split-layout > div { overflow: visible; }")),
        splitLayout(
          cellWidths = c("30%", "38%", "32%"),
          selectInput("asof_dayBL", NULL, choices = 1:31, selected = 1),
          selectInput("asof_monthBL", NULL,
                      choices = setNames(1:12, month.abb), selected = 1),
          selectInput("asof_yearBL", NULL,
                      choices = c("Connect first..." = ""), selected = "")
        ),
        actionButton("todayBtnBL", "Set current date",
                     class = "btn-primary", style = "margin-bottom: 0.75rem;"),
        tags$p(paste("Latest date = live BUY list with payoff evidence.",
                     "Dates since 2026-06-16 replay the ledger snapshot graded to today.",
                     "Earlier dates replay the walk-forward BACKTEST at the nearest",
                     "quarterly cutoff. Default view = the model's TOP PICKS from",
                     "clusters that had earned trust by that date, graded on real",
                     "12-month prices vs SPY; switch to every ranked ticker for the",
                     "full ladder (ids 13+ = short side there).",
                     "No live BUY gates existed then."),
               style = "color: #64748b; font-size: 0.7rem; margin-bottom: 0.75rem;"),
        # mode-aware controls: View only exists for the current date (replay
        # always draws prediction order); horizon only for backtest dates.
        # Showing an inert control read as "Generate is broken".
        conditionalPanel(
          condition = "output.blCtlMode == 'current'",
          selectInput("view_valBL", "View",
                      choices = c("Shortlist - rank bins that win (55%+)" = "shortlist",
                                  "All BUYs + young clusters"             = "all",
                                  "ALL tickers by cluster & rank"         = "rankall"),
                      selected = "shortlist")),
        conditionalPanel(
          condition = "output.blCtlMode == 'current' && input.view_valBL == 'all'",
          checkboxGroupInput("show_catBL", NULL,
                             choices = c("Regular entries (BUYs)"      = "regular",
                                         "Sparse entries (young ids)"  = "sparse"),
                             selected = c("regular", "sparse"))),
        selectInput("flt_id_valBL", "Cluster id filter",
                    choices = c("All" = "ALL", setNames(1:19, 1:19)),
                    selected = "ALL"),
        sliderInput("flt_rank_rangeBL", "Rank range (within cluster)",
                    min = 1, max = 600, value = c(1, 600), step = 1),
        conditionalPanel(
          condition = "output.blCtlMode == 'wf'",
          radioButtons("wf_view_modeBL", "Backtest view",
                       choices = c("Model's top picks (trust-gated)" = "picks",
                                   "All ranked tickers"              = "ranking"),
                       selected = "picks"),
          # horizon/depth only steer the full-ranking replay; picks are a
          # fixed top-10 slice graded at 12 months in the table itself
          conditionalPanel(
            condition = "input.wf_view_modeBL == 'ranking'",
            selectInput("wf_horizon_valBL", "Replay horizon (months)",
                        choices = c("4", "7", "12", "20", "33"), selected = "12"),
            # replay's shortlist: the slider can't express "top X% of EACH
            # cluster" when clusters of different sizes are loaded together
            selectInput("wf_depth_valBL", "Per-cluster depth",
                        choices = c("All ranks"               = "all",
                                    "Top 5% of each cluster"  = "p5",
                                    "Top 10% of each cluster" = "p10",
                                    "Top 20% of each cluster" = "p20"),
                        selected = "all")))
      )),
      mainPanel(div(class = "main-card",
        uiOutput("modeNoteBL"),
        uiOutput("selStatsBL"),
        uiOutput("buyChartContainerBL"),
        tags$br(),
        DT::DTOutput("buyTableBL")
      ))
    )
  )
)

# ─── Helper: create a DB connection ───
get_con <- function(input, suffix) {
  env   <- input[[paste0("db_env", suffix)]]
  db_string <- if (env == "Production") "prod" else if (env == "Staging") "staging" else "dev"

  # In-container DNS hiccups surface as "could not translate host name ...
  # Temporary failure in name resolution" (EAI_AGAIN). They are transient, so
  # retry a few times before surfacing a raw error to the user. Non-transient
  # failures (bad password, refused) are raised immediately.
  transient <- "name resolution|translate host name|EAI_AGAIN|could not connect|server closed the connection|connection timed out"
  attempts  <- 3
  con <- NULL
  for (i in seq_len(attempts)) {
    con <- tryCatch(
      dbConnect(RPostgres::Postgres(),
        dbname   = db_string,
        host     = input[[paste0("db_host", suffix)]],
        port     = as.integer(input[[paste0("db_port", suffix)]]),
        user     = input[[paste0("db_user", suffix)]],
        password = input[[paste0("db_pass", suffix)]],
        sslmode  = "prefer",
        connect_timeout = 10
      ),
      error = function(e) {
        if (i < attempts && grepl(transient, e$message, ignore.case = TRUE)) {
          Sys.sleep(1.5)   # let the resolver / DB recover, then retry
          return(NULL)
        }
        stop(e)            # non-transient, or out of retries
      }
    )
    if (!is.null(con)) break
  }
  # Hard cap so a slow or stuck query can never freeze the single-threaded app
  DBI::dbExecute(con, "SET statement_timeout = '60s'")
  con
}

# ─── Helper: wire up env-switcher for a given suffix ───
setup_env_switcher <- function(input, session, suffix) {
  observeEvent(input[[paste0("db_env", suffix)]], {
    env <- input[[paste0("db_env", suffix)]]
    if (env == "Production") {
      updateTextInput(session, paste0("db_host", suffix),
                      value = Sys.getenv("PROD_DB_HOST", "dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com"))
      updateTextInput(session, paste0("db_port", suffix), value = "25060")
      updateTextInput(session, paste0("db_user", suffix), value = "doadmin")
      updateTextInput(session, paste0("db_pass", suffix), value = Sys.getenv("PROD_DB_PASSWORD", ""))
    } else {
      updateTextInput(session, paste0("db_host", suffix), value = "host.docker.internal")
      updateTextInput(session, paste0("db_port", suffix), value = "5432")
      updateTextInput(session, paste0("db_user", suffix), value = "postgres")
      updateTextInput(session, paste0("db_pass", suffix), value = Sys.getenv("DB_PASSWORD", ""))
    }
  })
}

# ─── Helper: severity ring (Buy List) ───
# Outline color = how many checks the row fails: green 0, yellow 1, orange 2,
# red 3+. Counted checks: the cluster gate legs from serving
# (cluster_failed_checks = micro_population / young_history / no_wf_evidence /
# wf_below_bar) + sparse ticker tenure + the row's rank bin historically
# losing (win < 50 on n >= 100). Before the evidence-id rebuild lands the
# serving columns are NA -> falls back to the old amber-for-sparse ring.
severity_ring <- function(df) {
  base <- if ("cluster_failed_checks" %in% names(df))
    suppressWarnings(as.integer(df$cluster_failed_checks)) else rep(NA_integer_, nrow(df))
  extra <- ifelse(!is.na(df$evidence_status) &
                  df$evidence_status %in% c("sparse_ticker", "sparse_both"), 1L, 0L)
  if ("bin_win_pct" %in% names(df))
    extra <- extra + ifelse(!is.na(df$bin_win_pct) & !is.na(df$bin_n) &
                            df$bin_n >= 100 & df$bin_win_pct < 50, 1L, 0L)
  checks <- base + extra
  sparse <- !is.na(df$evidence_status) & df$evidence_status != "mature"
  list(
    checks = checks,
    col = ifelse(is.na(checks), ifelse(sparse, "#f59e0b", "rgba(0,0,0,0)"),
          ifelse(checks == 0L, "#059669",
          ifelse(checks == 1L, "#eab308",
          ifelse(checks == 2L, "#f97316", "#ef4444")))),
    w = ifelse(is.na(checks), ifelse(sparse, 1.5, 0), 1.5)
  )
}

# ─── Helper: standard boxplot + pct line ───
render_single_boxplot <- function(df, title, x_title, box_color = '#a855f7', fill_color = 'rgba(167, 139, 250, 0.4)') {
  df$pct <- (df$count / sum(df$count, na.rm = TRUE)) * 100

  fig <- plot_ly(df)

  fig <- fig %>% add_trace(
    type = 'box', name = 'Return Distribution',
    x = ~as.factor(bucket), q1 = ~q1, median = ~med, q3 = ~q3,
    lowerfence = ~lo, upperfence = ~hi,
    marker = list(color = box_color), line = list(color = box_color, width = 2),
    fillcolor = fill_color, hoverinfo = "y", offsetgroup = '1'
  )

  fig <- fig %>% add_markers(
    x = ~as.factor(bucket), y = ~med, name = 'Median',
    marker = list(color = '#ffffff', symbol = "line-ew", size = 45, line = list(color='#ffffff', width=3)),
    hoverinfo = "skip", showlegend = FALSE, offsetgroup = '1'
  )

  fig <- fig %>% add_trace(
    x = ~as.factor(bucket), y = ~pct, type = 'scatter', mode = 'lines+markers',
    fill = 'tozeroy', yaxis = 'y2', name = 'Record %',
    line = list(color = '#fbbf24', width = 3), marker = list(color = '#fbbf24', size = 8),
    fillcolor = 'rgba(251, 191, 36, 0.15)',
    hovertemplate = "Bucket: %{x}<br>Percentage: %{y:.2f}%<extra></extra>"
  )

  max_pct <- max(df$pct, na.rm = TRUE)
  fig %>% layout(
    title = list(text = title, font = list(color = "#f8fafc", family = "Inter", size = 18)),
    paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)", barmode = "group",
    xaxis = list(title = x_title, color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)", zerolinecolor = "rgba(255,255,255,0.1)"),
    yaxis = list(title = "Alpha (%)", color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)", zeroline = TRUE, zerolinewidth = 2, zerolinecolor = "rgba(255,255,255,0.2)"),
    yaxis2 = list(title = "Record Percentage (%)", color = "#fbbf24", gridcolor = "transparent", overlaying = "y", side = "right",
                  range = c(0, ifelse(is.infinite(max_pct) || is.na(max_pct), 100, max_pct * 1.5))),
    margin = list(l = 50, r = 60, b = 50, t = 50),
    showlegend = TRUE, legend = list(font = list(color = "#f8fafc"), orientation = "h", y = -0.2)
  )
}

# ─── Server ───
server <- function(input, output, session) {

  # allowReconnect("force") is deliberately OFF (2026-07-23): the forced
  # reconnect REPLAYS whole sessions into a busy event loop, and that churn
  # is where the native execCallbacks segfaults keep firing (they survived
  # the later/httpuv/promises rebuilds). Cost: after a server restart the
  # page shows a grey overlay and needs one manual reload instead of
  # auto-resuming. Re-enable only if the segfaults are truly gone.

  # ── TRANSITION: Reactive values ──
  app_dataT <- reactiveVal(NULL)
  status_msgT <- reactiveVal("Ready")
  output$statusMessageT <- renderText({ status_msgT() })
  setup_env_switcher(input, session, "T")

  # ── TRANSITION: Connect ──
  observeEvent(input$connect_btnT, {
    if (input$db_passT == "") { status_msgT("Error: Password is not set."); return() }
    status_msgT("Connecting...")
    tryCatch({
      con <- get_con(input, "T")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals         <- dbGetQuery(con, "SELECT DISTINCT id FROM scoring.return_cluster_lag_viability ORDER BY 1")
      past_fib_vals   <- dbGetQuery(con, "SELECT DISTINCT fibonacci_lag_value FROM scoring.return_cluster_lag_viability ORDER BY 1")
      future_fib_vals <- dbGetQuery(con, "SELECT DISTINCT future_fibonacci_lag_value FROM scoring.return_cluster_lag_viability ORDER BY 1")
      updateSelectInput(session, "id_valT", choices = id_vals[[1]], selected = id_vals[[1]][1])
      updateSelectInput(session, "past_fib_lagT", choices = past_fib_vals[[1]], selected = past_fib_vals[[1]][1])
      updateSelectInput(session, "future_fib_lagT", choices = future_fib_vals[[1]], selected = future_fib_vals[[1]][1])
      status_msgT("Filters loaded!")
    }, error = function(e) { status_msgT(paste("Error:", e$message)) })
  })

  # ── TRANSITION: Execute ──
  observeEvent(input$execute_T, {
    if (input$db_passT == "") { status_msgT("Error: Password is not set."); return() }
    if (input$id_valT == "" || input$past_fib_lagT == "" || input$future_fib_lagT == "") {
      status_msgT("Error: Select filters first."); return()
    }
    status_msgT("Running query...")
    actionable_clause <- if (isTRUE(input$transition_modeT == "actionable")) {
      "AND EXISTS (
            SELECT 1 FROM serving.return_cluster_ticker_pair_current tpc
            WHERE tpc.id = cs.id
              AND tpc.past_lag = cs.past_fibonacci_lag_value
              AND tpc.fut_lag = cs.future_fibonacci_lag_value
              AND tpc.bucket = cs.past_excess_return_z_bucket_num
        )"
    } else {
      ""
    }
    query <- sprintf("
      SELECT past_excess_return_z_bucket_num AS bucket,
             past_record_count AS past_count,
             past_lo, past_q1, past_median AS past_med, past_q3, past_hi,
             future_record_count AS future_count,
             future_lo, future_q1, future_median AS future_med, future_q3, future_hi,
             n_observations,
             future_confidence_score AS conf_score,
             future_improvement_score AS improv_score,
             future_risk_score AS risk_score,
             alpha_rate,
             signal,
             alpha_signal,
             positive_score,
             negative_score,
             net_score,
             combined_score,
             recommendation,
             recommendation_rank
      FROM scoring.return_cluster_cell_score_extended cs
      WHERE cs.past_fibonacci_lag_value = %s AND cs.future_fibonacci_lag_value = %s AND cs.id = %s
        %s
      ORDER BY past_excess_return_z_bucket_num;",
      input$past_fib_lagT, input$future_fib_lagT, input$id_valT, actionable_clause)
    tryCatch({
      con <- get_con(input, "T")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      res <- dbGetQuery(con, query)
      for(col in names(res)) { if(!(col %in% c("signal","alpha_signal","recommendation"))) res[[col]] <- as.numeric(res[[col]]) }
      app_dataT(res)
      status_msgT(paste("Loaded", nrow(res), "rows."))
    }, error = function(e) { app_dataT(NULL); status_msgT(paste("Error:", e$message)) })
  })

  # ── TRANSITION: Render ──
  output$transitionPlot <- renderPlotly({
    req(app_dataT())
    df <- app_dataT()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))

    tot_count <- sum(df$future_count, na.rm = TRUE)
    df$bucket_share <- if(tot_count > 0) (df$future_count / tot_count) * 100 else 0

    # conf_score comes directly from the DB (future_confidence_score)
    # Build custom hover tooltips
    df$past_hover <- sprintf(
      paste0(
        "<b>Past Distribution</b><br>",
        "Max:&nbsp;&nbsp;&nbsp;&nbsp;%7.2f%%<br>",
        "Q3:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%7.2f%%<br>",
        "Median:&nbsp;%7.2f%%<br>",
        "Q1:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%7.2f%%<br>",
        "Min:&nbsp;&nbsp;&nbsp;&nbsp;%7.2f%%<br>",
        "Records: %.0f"
      ),
      df$past_hi, df$past_q3, df$past_med, df$past_q1, df$past_lo, df$past_count)

    df$future_hover <- sprintf(
      paste0(
        "<b>Future Range</b><br>",
        "Max:&nbsp;&nbsp;&nbsp;&nbsp;%7.2f%%<br>",
        "Q3:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%7.2f%%<br>",
        "Median:&nbsp;%7.2f%%<br>",
        "Q1:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%7.2f%%<br>",
        "Min:&nbsp;&nbsp;&nbsp;&nbsp;%7.2f%%<br>",
        "Records: %.0f"
      ),
      df$future_hi, df$future_q3, df$future_med, df$future_q1, df$future_lo, df$future_count)

    df$conf_color <- ifelse(df$conf_score >= 0, '#34d399', '#f87171')
    df$improv_color <- ifelse(df$improv_score >= 0, '#60a5fa', '#f87171')
    df$risk_color <- ifelse(df$risk_score <= 10, '#34d399', ifelse(df$risk_score <= 30, '#fbbf24', '#f87171'))

    fig <- plot_ly(df)

    # Past boxplot (purple) — visual only, no hover
    fig <- fig %>% add_trace(type='box', name='Past Distribution', x=~as.factor(bucket),
      q1=~past_q1, median=~past_med, q3=~past_q3, lowerfence=~past_lo, upperfence=~past_hi,
      marker=list(color='#a855f7'), line=list(color='#a855f7', width=2),
      fillcolor='rgba(167,139,250,0.4)', hoverinfo="skip", offsetgroup='1')
    fig <- fig %>% add_markers(x=~as.factor(bucket), y=~past_med, name='Past Median',
      marker=list(color='#ffffff', symbol="line-ew", size=25, line=list(color='#ffffff', width=3)),
      hoverinfo="skip", showlegend=FALSE, offsetgroup='1')
    # Past hover catcher (invisible, fires custom tooltip anywhere in the box column)
    fig <- fig %>% add_trace(type='scatter', mode='markers', x=~as.factor(bucket), y=~past_med,
      marker=list(color='rgba(167,139,250,0)', size=60),
      text=~past_hover, hovertemplate="%{text}<extra></extra>",
      showlegend=FALSE, hoverlabel=list(font=list(family='Courier New, monospace', size=12)),
      offsetgroup='1')

    # Future boxplot (sky blue) — box body = IQR (Q1..Q3), whiskers = Min..Max
    fig <- fig %>% add_trace(type='box', name='Future Distribution', x=~as.factor(bucket),
      q1=~future_q1, median=~future_med, q3=~future_q3,
      lowerfence=~future_lo, upperfence=~future_hi,
      marker=list(color='#0ea5e9'), line=list(color='#0ea5e9', width=2),
      fillcolor='rgba(14,165,233,0.4)', hoverinfo="skip", offsetgroup='2')
    fig <- fig %>% add_markers(x=~as.factor(bucket), y=~future_med, name='Future Median',
      marker=list(color='#ffffff', symbol="line-ew", size=25, line=list(color='#ffffff', width=3)),
      hoverinfo="skip", showlegend=FALSE, offsetgroup='2')
    # Future hover catcher (invisible, fires custom tooltip anywhere in the box column)
    fig <- fig %>% add_trace(type='scatter', mode='markers', x=~as.factor(bucket), y=~future_med,
      marker=list(color='rgba(14,165,233,0)', size=60),
      text=~future_hover, hovertemplate="%{text}<extra></extra>",
      showlegend=FALSE, hoverlabel=list(font=list(family='Courier New, monospace', size=12)),
      offsetgroup='2')

    # Bucket share (green solid) — % of records in each bucket for this combo
    fig <- fig %>% add_trace(x=~as.factor(bucket), y=~bucket_share, type='scatter', mode='lines+markers',
      name='Bucket share (%)', yaxis='y2', line=list(color='#34d399', width=3, shape='spline', smoothing=1.0),
      marker=list(color='#34d399', size=8), hovertemplate="<b>Bucket: %{x}</b><br>Share: %{y:.2f}%<extra></extra>")

    max_pct <- max(df$bucket_share, na.rm = TRUE)

    max_pct_local <- max_pct  # capture for inner closure
    fig %>% layout(
      title = list(text = "Alpha Forecast", font = list(color = "#f8fafc", family = "Inter", size = 18)),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)", barmode = "group", boxmode = "group",
      xaxis = list(title = "Past Alpha Z-Bucket (SD)", color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)", zerolinecolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "Alpha (%)", color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)", zeroline = TRUE, zerolinewidth = 2, zerolinecolor = "rgba(255,255,255,0.2)"),
      yaxis2 = list(title = "Record Percentage (%)", color = "#f8fafc", gridcolor = "transparent", overlaying = "y", side = "right",
                    range = c(0, ifelse(is.infinite(max_pct) || is.na(max_pct), 100, max_pct * 1.5))),
      margin = list(l = 60, r = 60, b = 80, t = 50),
      showlegend = TRUE, legend = list(font = list(color = "#f8fafc"), orientation = "h", y = -0.2)
    )
  })

  # ── TRANSITION: Metric header table (replaces in-plot annotations) ──
  output$transitionHeader <- renderUI({
    req(app_dataT())
    df <- app_dataT()
    if (nrow(df) == 0) return(NULL)

    df$conf_color   <- ifelse(df$conf_score   >= 0, '#34d399', '#f87171')
    df$improv_color <- ifelse(df$improv_score >= 0, '#60a5fa', '#f87171')
    df$risk_color   <- ifelse(df$risk_score   <= 10, '#34d399',
                       ifelse(df$risk_score   <= 30, '#fbbf24', '#f87171'))

    rec_colors <- c(
      'STRONG_PICK'     = '#059669',
      'BUY'             = '#34d399',
      'SCORE_PICK'      = '#60a5fa',
      'HOLD'            = '#fbbf24',
      'SIGNAL_TRAP'     = '#f87171',
      'AVOID'           = '#f87171',
      'OUTLIER_BUY'     = '#86efac',
      'OUTLIER_AVOID'   = '#fca5a5',
      'OUTLIER_NEUTRAL' = '#94a3b8',
      'SKIP'            = '#64748b'
    )
    rec_display <- c(
      'STRONG_PICK'     = 'STRONG_PICK',
      'BUY'             = 'BUY',
      'SCORE_PICK'      = 'SCORE_PICK',
      'HOLD'            = 'HOLD',
      'SIGNAL_TRAP'     = 'SIGNAL_TRAP',
      'AVOID'           = 'AVOID',
      'OUTLIER_BUY'     = 'OUT_BUY',
      'OUTLIER_AVOID'   = 'OUT_AVOID',
      'OUTLIER_NEUTRAL' = 'OUT_NEUT',
      'SKIP'            = 'SKIP'
    )

    cell_style <- "padding: 6px 8px; text-align: center; font-family: 'Inter'; font-weight: 600;"
    label_style <- "padding: 6px 8px; text-align: right; color: #94a3b8; font-family: 'Inter'; font-size: 0.75rem; font-weight: 500;"

    make_row <- function(label, values, colors, fmt) {
      tags$tr(
        tags$td(label, style = label_style),
        lapply(seq_along(values), function(i) {
          tags$td(sprintf(fmt, values[i]),
                  style = sprintf("%s color: %s;", cell_style, colors[i]))
        })
      )
    }

    sig_colors_vec <- sapply(df$recommendation, function(s) {
      if (s %in% names(rec_colors)) rec_colors[[s]] else "#94a3b8"
    })
    sig_display_vec <- sapply(df$recommendation, function(s) {
      if (s %in% names(rec_display)) rec_display[[s]] else s
    })

    tags$table(
      style = "width: 100%; border-collapse: collapse; margin-bottom: 0.5rem; background: rgba(15, 23, 42, 0.4); border-radius: 6px;",
      tags$tr(
        tags$td("Signal", style = label_style),
        lapply(seq_along(sig_display_vec), function(i) {
          tags$td(
            HTML(sprintf("%s <span style=\"color:#94a3b8; font-size:0.7rem;\">#%d</span>",
                         sig_display_vec[i], as.integer(df$recommendation_rank[i]))),
            style = sprintf("%s color: %s; font-size: 0.9rem;", cell_style, sig_colors_vec[i])
          )
        })
      ),
      make_row("Return /mo", df$conf_score,   df$conf_color,   "%.2f"),
      make_row("Improv /mo", df$improv_score, df$improv_color, "%.2f"),
      make_row("Risk /mo",   df$risk_score,   df$risk_color,   "%.1f")
    )
  })

  # ── DATA QA: Reactive values ──
  # app_historyQ = full parquet (all runs, all databases)
  initial_hist <- load_qa_history()
  app_historyQ <- reactiveVal(initial_hist)
  status_msgQ <- reactiveVal(
    if (is.null(initial_hist) || nrow(initial_hist) == 0) "Ready"
    else sprintf("Loaded %d rows from parquet across %d runs.",
                 nrow(initial_hist), length(unique(initial_hist$run_at)))
  )
  output$statusMessageQ <- renderText({ status_msgQ() })
  setup_env_switcher(input, session, "Q")

  qa_schemasQ <- reactiveVal(NULL)

  # ── DATA QA: Connect — load list of schemas that have tables with a ticker column ──
  observeEvent(input$connect_btnQ, {
    if (input$db_passQ == "") { status_msgQ("Error: Password is not set."); return() }
    status_msgQ("Loading schemas...")
    tryCatch({
      con <- get_con(input, "Q")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      schemas <- dbGetQuery(con, "
        SELECT table_schema, COUNT(*) AS n_tables
        FROM information_schema.columns
        WHERE column_name = 'ticker'
          AND table_schema NOT IN ('pg_catalog','information_schema')
        GROUP BY table_schema
        ORDER BY table_schema")
      qa_schemasQ(schemas)
      status_msgQ(sprintf("Loaded %d schemas (%s tables total).",
                          nrow(schemas), sum(schemas$n_tables)))
    }, error = function(e) { status_msgQ(paste("Error:", e$message)) })
  })

  # ── DATA QA: Render the schema checkboxes dynamically ──
  output$qaSchemasUI <- renderUI({
    schemas <- qa_schemasQ()
    if (is.null(schemas) || nrow(schemas) == 0) {
      return(tags$div(style = "color: #64748b; font-size: 0.75rem; font-style: italic;",
                      "Connect to load schemas."))
    }
    # Reorder schemas: raw → cdm → metrics → analysis → inference, then others alpha
    prio <- match(schemas$table_schema, SCHEMA_ORDER)
    prio[is.na(prio)] <- 100L
    schemas <- schemas[order(prio, schemas$table_schema), ]
    labels <- sprintf("%s (%d)", schemas$table_schema, as.integer(schemas$n_tables))
    choices <- setNames(schemas$table_schema, labels)
    tagList(
      tags$label("Schemas to scan", class = "control-label"),
      checkboxGroupInput("schemasQ", label = NULL,
                         choices = choices, selected = schemas$table_schema)
    )
  })

  # ── DATA QA: Execute ──
  observeEvent(input$execute_Q, {
    if (input$db_passQ == "") { status_msgQ("Error: Password is not set."); return() }

    # First click: load schemas and show checkboxes, then stop and wait for user
    if (is.null(qa_schemasQ())) {
      status_msgQ("Loading schemas...")
      schemas <- tryCatch({
        con <- get_con(input, "Q")
        on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
        dbGetQuery(con, "
          SELECT table_schema, COUNT(*) AS n_tables
          FROM information_schema.columns
          WHERE column_name = 'ticker'
            AND table_schema NOT IN ('pg_catalog','information_schema')
          GROUP BY table_schema
          ORDER BY table_schema")
      }, error = function(e) { status_msgQ(paste("Error:", e$message)); NULL })
      if (is.null(schemas)) return()
      qa_schemasQ(schemas)
      status_msgQ(sprintf(
        "Loaded %d schemas — pick which to include and click Generate Chart again.",
        nrow(schemas)))
      return()
    }

    selected <- input$schemasQ
    if (is.null(selected) || length(selected) == 0) {
      status_msgQ("Error: Pick at least one schema.")
      return()
    }
    status_msgQ("Scanning tables...")
    tryCatch({
      con <- get_con(input, "Q")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)

      placeholders <- paste(as.character(DBI::dbQuoteString(con, selected)), collapse = ",")
      tables <- dbGetQuery(con, sprintf("
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name = 'ticker'
          AND table_schema IN (%s)
        ORDER BY table_schema, table_name", placeholders))

      if (nrow(tables) == 0) {
        status_msgQ("No tables with a ticker column.")
        return()
      }

      results <- data.frame(
        schema       = character(nrow(tables)),
        table        = character(nrow(tables)),
        ticker_count = integer(nrow(tables)),
        stringsAsFactors = FALSE
      )
      for (i in seq_len(nrow(tables))) {
        s <- tables$table_schema[i]; t <- tables$table_name[i]
        status_msgQ(sprintf("Scanning %d/%d: %s.%s", i, nrow(tables), s, t))
        q <- sprintf('SELECT COUNT(DISTINCT ticker) AS tc FROM %s.%s',
                     DBI::dbQuoteIdentifier(con, s),
                     DBI::dbQuoteIdentifier(con, t))
        counts <- tryCatch(dbGetQuery(con, q),
                           error = function(e) data.frame(tc = NA))
        results$schema[i]       <- s
        results$table[i]        <- t
        results$ticker_count[i] <- as.numeric(counts$tc[1])
      }
      results <- results[lineage_order(results$schema, results$table), ]

      db_name <- tryCatch(
        dbGetQuery(con, "SELECT current_database() AS n")$n[1],
        error = function(e) "unknown")
      new_rows <- tryCatch(
        append_qa_history(results, db_name = db_name),
        error = function(e) { status_msgQ(paste("Parquet save failed:", e$message)); NULL })
      if (is.null(new_rows)) return()

      full_hist <- load_qa_history()
      app_historyQ(full_hist)
      status_msgQ(sprintf(
        "Scanned %d tables (db=%s). Parquet now has %d rows across %d runs.",
        nrow(new_rows), db_name,
        nrow(full_hist), length(unique(full_hist$run_at))))
    }, error = function(e) { status_msgQ(paste("Error:", e$message)) })
  })

  # ── DATA QA: Keep filter dropdowns in sync with current parquet history ──
  qa_filter_choices <- reactive({
    df <- app_historyQ()
    if (is.null(df) || nrow(df) == 0) {
      return(list(run_at = character(0), db = character(0), schema = character(0)))
    }
    # Pair each run_at with its database for the dropdown label
    run_map <- unique(data.frame(run_at = df$run_at,
                                 db     = df$database_name,
                                 stringsAsFactors = FALSE))
    run_map <- run_map[order(run_map$run_at, decreasing = TRUE), , drop = FALSE]
    run_at_values <- format(run_map$run_at, "%Y-%m-%d %H:%M:%S")
    run_at_labels <- sprintf("%s  ·  %s", run_at_values, run_map$db)
    list(
      run_at = setNames(run_at_values, run_at_labels),
      db     = sort(unique(df$database_name)),
      schema = sort(unique(df$schema_name))
    )
  })

  observe({
    ch <- qa_filter_choices()
    updateSelectizeInput(session, "filterRunAtQ",  choices = ch$run_at)
    updateSelectizeInput(session, "filterDbQ",     choices = ch$db)
    updateSelectizeInput(session, "filterSchemaQ", choices = ch$schema)
  })

  # all/clear handlers
  observeEvent(input$filterRunAtAllQ,    updateSelectizeInput(session, "filterRunAtQ",  selected = qa_filter_choices()$run_at))
  observeEvent(input$filterRunAtClearQ,  updateSelectizeInput(session, "filterRunAtQ",  selected = character(0)))
  observeEvent(input$filterDbAllQ,       updateSelectizeInput(session, "filterDbQ",     selected = qa_filter_choices()$db))
  observeEvent(input$filterDbClearQ,     updateSelectizeInput(session, "filterDbQ",     selected = character(0)))
  observeEvent(input$filterSchemaAllQ,   updateSelectizeInput(session, "filterSchemaQ", selected = qa_filter_choices()$schema))
  observeEvent(input$filterSchemaClearQ, updateSelectizeInput(session, "filterSchemaQ", selected = character(0)))

  # ── DATA QA: Filtered + sorted history used by the DT render and delete ──
  qa_display_df <- reactive({
    df <- app_historyQ()
    if (is.null(df) || nrow(df) == 0) return(NULL)
    if (length(input$filterRunAtQ) > 0) {
      df <- df[format(df$run_at, "%Y-%m-%d %H:%M:%S") %in% input$filterRunAtQ, ,
               drop = FALSE]
    }
    if (length(input$filterDbQ) > 0)
      df <- df[df$database_name %in% input$filterDbQ, , drop = FALSE]
    if (length(input$filterSchemaQ) > 0)
      df <- df[df$schema_name %in% input$filterSchemaQ, , drop = FALSE]
    if (nrow(df) == 0) return(df)
    key <- data.frame(schema = tolower(df$schema_name), name = tolower(df$table_name),
                      idx = seq_len(nrow(df)), stringsAsFactors = FALSE)
    merged <- merge(key, LINEAGE_RANKS, by = c("schema","name"),
                    all.x = TRUE, sort = FALSE)
    merged <- merged[order(merged$idx), ]
    rk <- merged$rank; rk[is.na(rk)] <- .Machine$integer.max %/% 2L
    sp <- match(merged$schema, SCHEMA_ORDER); sp[is.na(sp)] <- 100L
    df[order(-as.numeric(df$run_at), sp, rk, merged$name), , drop = FALSE]
  })

  output$qaHistoryTable <- DT::renderDT({
    df <- qa_display_df()
    if (is.null(df) || nrow(df) == 0) {
      return(DT::datatable(
        data.frame(Note = "No history yet. Run Generate Chart, or relax filters."),
        selection = "none", rownames = FALSE, class = "compact",
        options = list(dom = "t", ordering = FALSE)))
    }
    display <- data.frame(
      `Run At`    = format(df$run_at, "%Y-%m-%d %H:%M:%S"),
      Database    = df$database_name,
      Schema      = df$schema_name,
      Table       = df$table_name,
      Tickers     = formatC(df$ticker_count, format = "d", big.mark = ","),
      check.names = FALSE,
      stringsAsFactors = FALSE
    )
    DT::datatable(
      display,
      selection = list(mode = "multiple", target = "row"),
      rownames  = FALSE,
      class     = "compact",
      extensions = "Buttons",
      options   = list(
        pageLength = 25, lengthMenu = c(10, 25, 50, 100, 500),
        dom = "Btip", searching = FALSE, ordering = FALSE,
        buttons = c("copy", "csv"),
        columnDefs = list(list(className = "dt-right", targets = 4))
      )
    )
  }, server = FALSE)

  # Select all / none via DT proxy
  qa_dt_proxy <- DT::dataTableProxy("qaHistoryTable")
  observeEvent(input$qaSelectAllQ, {
    disp <- qa_display_df()
    if (is.null(disp) || nrow(disp) == 0) return()
    DT::selectRows(qa_dt_proxy, seq_len(nrow(disp)))
  })
  observeEvent(input$qaSelectNoneQ, {
    DT::selectRows(qa_dt_proxy, integer(0))
  })
  observeEvent(input$qaSelectInvertQ, {
    disp <- qa_display_df()
    if (is.null(disp) || nrow(disp) == 0) {
      status_msgQ("Invert: no rows in view.")
      return()
    }
    current  <- input$qaHistoryTable_rows_selected
    inverted <- setdiff(seq_len(nrow(disp)), current)
    status_msgQ(sprintf("Invert: %d → %d rows selected (of %d).",
                        length(current), length(inverted), nrow(disp)))
    DT::selectRows(qa_dt_proxy, as.integer(inverted))
  }, ignoreInit = TRUE)

  # Delete selected rows from the parquet
  observeEvent(input$qaDeleteSelectedQ, {
    sel <- input$qaHistoryTable_rows_selected
    if (length(sel) == 0) {
      status_msgQ("Select rows first — click rows in the table.")
      return()
    }
    showModal(modalDialog(
      title = "Delete selected rows",
      sprintf("Delete %d rows from the parquet? This cannot be undone.", length(sel)),
      footer = tagList(
        modalButton("Cancel"),
        actionButton("qaDeleteConfirmQ", "Delete", class = "btn-primary")),
      easyClose = TRUE, size = "s"
    ))
  })

  observeEvent(input$qaDeleteConfirmQ, {
    removeModal()
    sel <- input$qaHistoryTable_rows_selected
    if (length(sel) == 0) return()
    disp <- qa_display_df()
    to_del <- disp[sel, , drop = FALSE]
    full <- app_historyQ()
    key_full <- paste(as.numeric(full$run_at), full$database_name,
                      full$schema_name, full$table_name, sep = "|")
    key_del  <- paste(as.numeric(to_del$run_at), to_del$database_name,
                      to_del$schema_name, to_del$table_name, sep = "|")
    remaining <- full[!(key_full %in% key_del), , drop = FALSE]
    if (nrow(remaining) == 0) {
      if (file.exists(QA_HISTORY_PATH)) file.remove(QA_HISTORY_PATH)
    } else {
      nanoparquet::write_parquet(remaining, QA_HISTORY_PATH)
    }
    app_historyQ(remaining)
    DT::selectRows(qa_dt_proxy, integer(0))
    status_msgQ(sprintf("Deleted %d rows. Parquet has %d rows now.",
                        nrow(to_del), nrow(remaining)))
  })

  # ── COVERAGE: Reactive values ──
  app_dataV <- reactiveVal(NULL)
  status_msgV <- reactiveVal("Ready")
  output$statusMessageV <- renderText({ status_msgV() })
  setup_env_switcher(input, session, "V")

  # ── COVERAGE: Connect — just smoke-test the query source ──
  observeEvent(input$connect_btnV, {
    if (input$db_passV == "") { status_msgV("Error: Password is not set."); return() }
    status_msgV("Connecting...")
    tryCatch({
      con <- get_con(input, "V")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      n <- dbGetQuery(con,
        "SELECT COUNT(DISTINCT ticker) AS n FROM cdm.ingest_combined")$n[1]
      status_msgV(sprintf("Connected — %s distinct tickers.", n))
    }, error = function(e) { status_msgV(paste("Error:", e$message)) })
  })

  # ── COVERAGE: Execute — load min/max date per ticker ──
  observeEvent(input$execute_V, {
    if (input$db_passV == "") { status_msgV("Error: Password is not set."); return() }
    status_msgV("Loading ticker coverage...")
    tryCatch({
      con <- get_con(input, "V")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      df <- dbGetQuery(con, "
        SELECT ticker,
               MIN(\"date\")::date AS first_date,
               MAX(\"date\")::date AS last_date,
               COUNT(*) AS n_obs
        FROM cdm.ingest_combined
        GROUP BY ticker
        ORDER BY MIN(\"date\")")
      df$first_date <- as.Date(df$first_date)
      df$last_date  <- as.Date(df$last_date)
      df$n_obs      <- as.numeric(df$n_obs)
      app_dataV(df)
      status_msgV(sprintf("Loaded %d tickers.", nrow(df)))
    }, error = function(e) { app_dataV(NULL); status_msgV(paste("Error:", e$message)) })
  })

  # ── COVERAGE: Container with dynamic height ──
  output$coveragePlotContainer <- renderUI({
    req(app_dataV())
    ph <- max(3000, nrow(app_dataV()) * 14)
    plotlyOutput("coveragePlot", height = paste0(ph, "px"))
  })

  # ── COVERAGE: Render ──
  output$coveragePlot <- renderPlotly({
    req(app_dataV())
    df <- app_dataV()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))

    # Sort: earliest start at bottom (longest history), most recent start at top
    df <- df[order(df$first_date), ]
    df$ticker_f <- factor(df$ticker, levels = df$ticker)
    today <- Sys.Date()
    ph <- max(3000, nrow(df) * 14)

    hover <- sprintf(
      "<b>%s</b><br>%s → %s<br>%s obs<extra></extra>",
      df$ticker, df$first_date, df$last_date, format(df$n_obs, big.mark = ","))

    plot_ly(height = ph) %>%
      add_segments(
        x = df$first_date, xend = df$last_date,
        y = df$ticker_f,   yend = df$ticker_f,
        line = list(color = "#fb7185", width = 4),
        hovertemplate = hover,
        hoverlabel = list(font = list(family = "JetBrains Mono, monospace", size = 12))
      ) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        xaxis = list(title = "Observation date", color = "#94a3b8",
                     gridcolor = "rgba(255,255,255,0.08)",
                     zerolinecolor = "rgba(255,255,255,0.08)",
                     fixedrange = TRUE),
        yaxis = list(title = "", color = "#94a3b8", automargin = TRUE,
                     tickfont = list(size = 8),
                     categoryorder = "array",
                     categoryarray = levels(df$ticker_f),
                     fixedrange = TRUE),
        shapes = list(list(
          type = "line", x0 = today, x1 = today, y0 = 0, y1 = 1, yref = "paper",
          line = list(color = "#f8fafc", width = 2, dash = "dot"))),
        annotations = list(list(
          x = today, y = 1.005, yref = "paper", xanchor = "right", yanchor = "bottom",
          text = sprintf("today (%s)", today), showarrow = FALSE,
          font = list(color = "#94a3b8", size = 11, family = "Inter"))),
        margin = list(l = 80, r = 40, t = 40, b = 60),
        showlegend = FALSE
      ) %>%
      config(scrollZoom = FALSE, displayModeBar = FALSE)
  })

  # ── CLUSTERS ──
  app_dataK   <- reactiveVal(NULL)
  status_msgK <- reactiveVal("Not connected.")
  output$statusMessageK <- renderText({ status_msgK() })
  setup_env_switcher(input, session, "K")

  observeEvent(input$connect_btnK, {
    if (input$db_passK == "") { status_msgK("Error: Password is not set."); return() }
    status_msgK("Connecting...")
    tryCatch({
      con <- get_con(input, "K")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM analysis.ticker_cluster_segments")$n[1]
      status_msgK(sprintf("Connected — %s tickers clustered.", n))
    }, error = function(e) { status_msgK(paste("Error:", e$message)) })
  })

  observeEvent(input$execute_K, {
    if (input$db_passK == "") { status_msgK("Error: Password is not set."); return() }
    status_msgK("Loading clusters...")
    tryCatch({
      con <- get_con(input, "K")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      df <- dbGetQuery(con, "
        SELECT s.ticker,
               v.id,
               s.months_count,
               s.growth_pct_per_month
        FROM analysis.ticker_cluster_segments s
        JOIN clustering.ticker_cluster_volatility_summary v
          ON v.cluster_id = s.cluster_id
        WHERE s.months_count > 0 AND s.growth_pct_per_month IS NOT NULL")
      app_dataK(df)
      status_msgK(sprintf("Loaded %d tickers across %d clusters.",
                          nrow(df), length(unique(df$id))))
    }, error = function(e) { app_dataK(NULL); status_msgK(paste("Error:", e$message)) })
  })

  output$clusterPlot <- renderPlotly({
    req(app_dataK())
    df <- app_dataK()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))

    df <- df[is.finite(df$months_count) & is.finite(df$growth_pct_per_month), ]
    # Draw lower-priority ids first so id=1 ends up on top of the stack.
    df <- df[order(-df$id), ]
    df$id_factor <- factor(df$id)
    palette20 <- c("#ef4444","#f97316","#eab308","#84cc16","#22c55e","#10b981",
                   "#14b8a6","#06b6d4","#0ea5e9","#3b82f6","#6366f1","#8b5cf6",
                   "#a855f7","#d946ef","#ec4899","#f43f5e","#fbbf24","#a3e635",
                   "#2dd4bf","#7c3aed")
    id_levels <- sort(unique(df$id))
    color_map <- setNames(palette20[((seq_along(id_levels) - 1) %% length(palette20)) + 1],
                          as.character(id_levels))

    # Clip y-axis using p0.5..p99.5 with a generous ceiling so high-growth
    # clusters (id=1, 2) stay visible while penny-stock crashes (-50%) don't
    # compress the bulk into y=0. Min ceiling 25 because the highest-growth
    # cluster (id=1) typically has 14-50 tickers with growth 12-25%/mo, which
    # the p99.5 of the bulk distribution (~8%) would clip out of view.
    y_lo <- min(-10, quantile(df$growth_pct_per_month, 0.005, na.rm = TRUE))
    y_hi <- max( 25, quantile(df$growth_pct_per_month, 0.995, na.rm = TRUE))
    x_hi <- quantile(df$months_count,         0.995, na.rm = TRUE)

    p <- ggplot(df, aes(x = months_count, y = growth_pct_per_month, color = id_factor, text = ticker)) +
      geom_point(size = 0.25, alpha = 0.7) +
      scale_color_manual(values = color_map) +
      coord_cartesian(xlim = c(0, x_hi), ylim = c(y_lo, y_hi), expand = FALSE) +
      labs(x = "Tenure (months)", y = "Growth rate (%/mo)", color = "id") +
      theme_minimal(base_size = 10) +
      theme(
        plot.background  = element_rect(fill = "#0b1220", color = NA),
        panel.background = element_rect(fill = "#0b1220", color = NA),
        panel.grid       = element_line(color = "#1e293b"),
        text             = element_text(color = "#cbd5e1"),
        strip.text       = element_text(color = "#f8fafc")
      )

    ggplotly(p, tooltip = "text") %>%
      layout(paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)") %>%
      config(displayModeBar = FALSE)
  })

  observeEvent(input$clusterShowAll, {
    plotlyProxy("clusterPlot", session) %>%
      plotlyProxyInvoke("restyle", list(visible = TRUE))
  })

  observeEvent(input$clusterHideAll, {
    plotlyProxy("clusterPlot", session) %>%
      plotlyProxyInvoke("restyle", list(visible = "legendonly"))
  })

  # ── TOP PICKS: Reactive values ──
  app_dataP <- reactiveVal(NULL)
  cluster_ic_metaP <- reactiveVal(NULL)
  status_msgP <- reactiveVal("Ready")
  output$statusMessageP <- renderText({ status_msgP() })
  setup_env_switcher(input, session, "P")

  # ── TOP PICKS: Connect ──
  observeEvent(input$connect_btnP, {
    if (input$db_passP == "") { status_msgP("Error: Password is not set."); return() }
    status_msgP("Connecting...")
    tryCatch({
      con <- get_con(input, "P")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals <- dbGetQuery(con,
        "SELECT DISTINCT id FROM serving.return_cluster_ticker_summary_current ORDER BY 1")
      updateSelectInput(session, "id_valP", choices = id_vals[[1]], selected = id_vals[[1]][1])
      status_msgP("Filters loaded!")
    }, error = function(e) { status_msgP(paste("Error:", e$message)) })
  })

  # ── TOP PICKS: Execute ──
  observeEvent(input$execute_P, {
    if (input$db_passP == "") { status_msgP("Error: Password is not set."); return() }
    if (input$id_valP == "") { status_msgP("Error: Select a cluster first."); return() }
    status_msgP("Running query...")

    query <- sprintf("
      WITH top_picks AS (
          SELECT ticker, agg_rank, coverage_cell_count, agg_directional_score
          FROM serving.return_cluster_ticker_summary_current
          WHERE id = %s AND agg_rank <= %d
      )
      SELECT
          tp.agg_rank,
          tp.ticker,
          tp.coverage_cell_count,
          tp.agg_directional_score,
          p.fut_lag,
          AVG(
              CASE
                  WHEN p.recommendation IN ('STRONG_PICK','BUY','OUTLIER_BUY')   THEN  c.wilson_lower - 0.5
                  WHEN p.recommendation IN ('AVOID','SIGNAL_TRAP','OUTLIER_AVOID') THEN -(c.wilson_lower - 0.5)
              END
          ) FILTER (
              WHERE c.wilson_lower IS NOT NULL
                AND p.recommendation IN
                    ('STRONG_PICK','BUY','AVOID','SIGNAL_TRAP','OUTLIER_BUY','OUTLIER_AVOID')
          ) AS signed_edge,
          AVG(c.wilson_lower) FILTER (
              WHERE c.wilson_lower IS NOT NULL
                AND p.recommendation IN
                    ('STRONG_PICK','BUY','AVOID','SIGNAL_TRAP','OUTLIER_BUY','OUTLIER_AVOID')
          ) AS avg_sign_agreement,
          COUNT(*) FILTER (
              WHERE c.wilson_lower IS NOT NULL
                AND p.recommendation IN
                    ('STRONG_PICK','BUY','AVOID','SIGNAL_TRAP','OUTLIER_BUY','OUTLIER_AVOID')
          ) AS n_directional_cells_with_cred,
          COUNT(*) FILTER (
              WHERE p.recommendation IN
                  ('STRONG_PICK','BUY','AVOID','SIGNAL_TRAP','OUTLIER_BUY','OUTLIER_AVOID')
          ) AS n_directional_cells_total,
          COUNT(*) AS n_total_cells
      FROM top_picks tp
      JOIN serving.return_cluster_ticker_pair_current p
        ON p.ticker = tp.ticker AND p.id = %s
      -- 2026-07-24 grain fix: cell_credibility no longer carries
      -- vol_bucket_num; join on the cell shape alone.
      LEFT JOIN validation.cell_credibility c
        ON c.id            = p.id
       AND c.past_lag      = p.past_lag
       AND c.fut_lag       = p.fut_lag
       AND c.bucket        = p.bucket
      GROUP BY tp.agg_rank, tp.ticker, tp.coverage_cell_count,
               tp.agg_directional_score, p.fut_lag
      ORDER BY tp.agg_rank, p.fut_lag;",
      input$id_valP, input$top_n_valP, input$id_valP)

    tryCatch({
      con <- get_con(input, "P")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      res <- dbGetQuery(con, query)
      res$agg_rank <- as.integer(res$agg_rank)
      res$fut_lag <- as.integer(res$fut_lag)
      res$coverage_cell_count <- as.integer(res$coverage_cell_count)
      res$signed_edge <- as.numeric(res$signed_edge)
      res$avg_sign_agreement <- as.numeric(res$avg_sign_agreement)
      res$n_directional_cells_with_cred  <- as.integer(res$n_directional_cells_with_cred)
      res$n_directional_cells_total      <- as.integer(res$n_directional_cells_total)
      res$n_directional_cells            <- res$n_directional_cells_with_cred
      res$n_total_cells <- as.integer(res$n_total_cells)
      app_dataP(res)

      ic_query <- sprintf("
        SELECT
          ROUND(mean_ic::numeric, 4) AS mean_ic,
          ROUND(median_ic::numeric, 4) AS median_ic,
          n_cohorts,
          n_positive,
          ROUND(mean_decile_spread::numeric, 4) AS mean_decile_spread
        FROM validation.walk_forward_id_ic
        WHERE id = %s AND fut_lag = 12;",
        input$id_valP)
      ic_df <- tryCatch(dbGetQuery(con, ic_query), error = function(e) NULL)
      cluster_ic_metaP(ic_df)

      status_msgP(paste("Loaded", nrow(res), "rows."))
    }, error = function(e) {
      app_dataP(NULL); cluster_ic_metaP(NULL)
      status_msgP(paste("Error:", e$message))
    })
  })

  output$clusterIcDisplayP <- renderText({
    df <- cluster_ic_metaP()
    if (is.null(df) || nrow(df) == 0 || is.na(df$mean_ic[1])) {
      return("Walk-forward IC: no data for this cluster (run walk_forward_ticker_ic.py)")
    }
    sprintf(
      "Walk-forward IC for cluster %s (fut_lag=12, %d cohorts): mean=%.4f  median=%.4f  positive=%d/%d  decile_spread=%.4f",
      input$id_valP,
      as.integer(df$n_cohorts[1]),
      as.numeric(df$mean_ic[1]),
      as.numeric(df$median_ic[1]),
      as.integer(df$n_positive[1]),
      as.integer(df$n_cohorts[1]),
      as.numeric(df$mean_decile_spread[1])
    )
  })

  # ── TOP PICKS: Render heatmap ──
  output$topPicksPlot <- renderPlotly({
    req(app_dataP())
    df <- app_dataP()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))

    df <- df[order(df$agg_rank, df$fut_lag), ]
    row_keys <- unique(df[, c("agg_rank", "ticker", "coverage_cell_count")])
    row_keys <- row_keys[order(row_keys$agg_rank), ]
    row_labels <- sprintf("#%d %s (n=%d)",
                          row_keys$agg_rank, row_keys$ticker, row_keys$coverage_cell_count)

    fut_lag_vals <- sort(unique(df$fut_lag))
    # Proportional spacing via sqrt(fut_lag) so adjacent small lags (1,2) stay
    # similar in width while larger lags (20,33) get visibly wider cells.
    # Linear axis with sqrt-positioned ticks; tick labels show real fut_lag.
    fut_lag_pos  <- sqrt(fut_lag_vals)

    z_mat   <- matrix(NA_real_, nrow = nrow(row_keys), ncol = length(fut_lag_vals),
                      dimnames = list(row_labels, as.character(fut_lag_vals)))
    sa_mat  <- matrix(NA_real_, nrow = nrow(row_keys), ncol = length(fut_lag_vals),
                      dimnames = list(row_labels, as.character(fut_lag_vals)))
    n_mat   <- matrix(NA_integer_, nrow = nrow(row_keys), ncol = length(fut_lag_vals),
                      dimnames = list(row_labels, as.character(fut_lag_vals)))
    for (i in seq_len(nrow(df))) {
      row_i <- which(row_keys$agg_rank == df$agg_rank[i] & row_keys$ticker == df$ticker[i])
      col_i <- which(fut_lag_vals == df$fut_lag[i])
      if (length(row_i) > 0 && length(col_i) > 0) {
        z_mat[row_i, col_i]  <- df$signed_edge[i]
        sa_mat[row_i, col_i] <- df$avg_sign_agreement[i]
        n_mat[row_i, col_i]  <- df$n_directional_cells[i]
      }
    }

    max_abs <- 0.3

    colorscale <- list(
      c(0.00, '#dc2626'),
      c(0.25, '#f97316'),
      c(0.50, '#facc15'),
      c(0.75, '#84cc16'),
      c(1.00, '#16a34a')
    )

    custom_arr <- array(NA_real_, dim = c(dim(z_mat), 2))
    custom_arr[,,1] <- sa_mat
    custom_arr[,,2] <- n_mat

    plot_ly(
      x = fut_lag_pos, y = row_labels, z = z_mat, type = "heatmap",
      colorscale = colorscale, zmin = -max_abs, zmax = max_abs,
      customdata = custom_arr,
      text = matrix(rep(as.character(fut_lag_vals), each = nrow(z_mat)), nrow = nrow(z_mat)),
      hovertemplate = paste0(
        "%{y}<br>",
        "fut_lag: %{text}<br>",
        "signed edge (wilson_lower - 0.5) x dir: %{z:.3f}<br>",
        "avg wilson_lower: %{customdata[0]:.3f}<br>",
        "n directional cells: %{customdata[1]}<extra></extra>"),
      colorbar = list(
        title = list(text = "Signed edge\n(wilson_lower\n- 0.5, × dir;\n0 = coin flip)",
                     font = list(color = "#f8fafc")),
        tickfont = list(color = "#94a3b8"))
    ) %>% layout(
      title = list(
        text = sprintf("Cluster %s — Top %d picks: signed reliability edge (green=reliable BUY, red=reliable AVOID, yellow=coin flip)",
                       input$id_valP, input$top_n_valP),
        font = list(color = "#f8fafc", family = "Inter", size = 14)),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
      xaxis = list(title = "Future lag (months, sqrt-spaced)", type = "linear",
                   tickvals = fut_lag_pos, ticktext = as.character(fut_lag_vals),
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "Ticker (by agg_rank)", type = "category",
                   autorange = "reversed",
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      margin = list(l = 160, r = 60, b = 60, t = 60)
    )
  })

  # ── RANK STABILITY: Reactive values ──
  app_dataRS_slot     <- reactiveVal(NULL)
  app_dataRS_heatmap  <- reactiveVal(NULL)
  app_dataRS_meta     <- reactiveVal(NULL)
  status_msgRS <- reactiveVal("Ready")
  output$statusMessageRS <- renderText({ status_msgRS() })
  setup_env_switcher(input, session, "RS")

  observeEvent(input$connect_btnRS, {
    if (input$db_passRS == "") { status_msgRS("Error: Password is not set."); return() }
    status_msgRS("Connecting...")
    tryCatch({
      con <- get_con(input, "RS")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals <- dbGetQuery(con,
        "SELECT DISTINCT id FROM validation.walk_forward_pctile_summary ORDER BY id")
      cl_choices <- c("All ids" = "ALL",
                      setNames(as.character(id_vals$id), as.character(id_vals$id)))
      updateSelectInput(session, "id_valRS", choices = cl_choices, selected = "ALL")
      cutoff_bounds <- dbGetQuery(con,
        "SELECT MIN(train_cutoff_date) AS min_d, MAX(train_cutoff_date) AS max_d
         FROM validation.walk_forward_ticker_rank")
      if (!is.na(cutoff_bounds$min_d[1]) && !is.na(cutoff_bounds$max_d[1])) {
        updateDateRangeInput(session, "cutoff_rangeRS",
                             start = cutoff_bounds$min_d[1],
                             end   = cutoff_bounds$max_d[1],
                             min   = cutoff_bounds$min_d[1],
                             max   = cutoff_bounds$max_d[1])
      }
      status_msgRS("Filters loaded!")
    }, error = function(e) { status_msgRS(paste("Error:", e$message)) })
  })

  # Auto-tier the display depth to the selected cluster's size: 5 / 10 / 20.
  observeEvent(input$id_valRS, {
    id_sel <- input$id_valRS
    if (is.null(id_sel) || id_sel == "" || id_sel == "ALL") {
      updateSliderInput(session, "top_n_valRS", value = 20); return()
    }
    con <- tryCatch(get_con(input, "RS"), error = function(e) NULL)
    # register cleanup BEFORE the early return so a half-open connection
    # can't leak when con is NULL-but-partially-created
    on.exit({ if (!is.null(con) && DBI::dbIsValid(con)) try(dbDisconnect(con), silent = TRUE) }, add = TRUE)
    if (is.null(con)) return()
    med <- tryCatch(dbGetQuery(con, sprintf(
      "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY r.cluster_size) AS med
       FROM validation.walk_forward_ticker_rank r
       JOIN validation.walk_forward_cluster_id_map m
         ON m.train_cutoff_date = r.train_cutoff_date AND m.cluster_id = r.cluster_id
       WHERE m.id = %s AND r.fut_lag = 12;", id_sel))$med[1],
      error = function(e) NA_real_)
    if (length(med) == 0 || is.na(med)) return()
    tier <- if (med >= 20) 20L else if (med >= 10) 10L else 5L
    updateSliderInput(session, "top_n_valRS", value = tier)
  }, ignoreInit = TRUE)

  observeEvent(input$execute_RS, {
    if (input$db_passRS == "") { status_msgRS("Error: Password is not set."); return() }
    if (is.null(input$id_valRS) || input$id_valRS == "") {
      status_msgRS("Error: Select a cluster (or 'All clusters') first."); return()
    }
    status_msgRS("Running queries...")
    date_lo <- format(input$cutoff_rangeRS[1], "%Y-%m-%d")
    date_hi <- format(input$cutoff_rangeRS[2], "%Y-%m-%d")
    slot_cluster_filter <- if (input$id_valRS == "ALL") "" else
      sprintf("AND m.id = %s", input$id_valRS)

    slot_query <- sprintf("
      SELECT
        r.rank_within_cluster,
        COUNT(*) AS n_obs,
        AVG(r.forward_return) AS mean_fwd,
        STDDEV(r.forward_return) AS sd_fwd
      FROM validation.walk_forward_ticker_rank r
      JOIN validation.walk_forward_cluster_id_map m
        ON m.train_cutoff_date = r.train_cutoff_date
       AND m.cluster_id = r.cluster_id
      WHERE r.train_cutoff_date BETWEEN '%s' AND '%s' %s
      GROUP BY r.rank_within_cluster
      ORDER BY r.rank_within_cluster;",
      date_lo, date_hi, slot_cluster_filter)

    summary_cluster_filter <- if (input$id_valRS == "ALL") "" else
      sprintf("AND id = %s", input$id_valRS)
    metric_col <- input$metric_valRS
    # combo_quadrants is computed in R (needs both hit_rate and median_return),
    # not a column in the table. Use a special SQL that returns both, then
    # combine in the render function.
    if (metric_col == "combo_quadrants") {
      # Align median per-cluster INSIDE the aggregation (long: +1, short: -1),
      # otherwise the weighted average of raw medians across long+short clusters
      # cancels out when ALL ids are selected.
      topn_query <- sprintf("
        SELECT
          pctile_bin AS rank_within_cluster,
          fut_lag,
          SUM(hit_rate * n_obs) / NULLIF(SUM(n_obs), 0) AS hit_rate,
          SUM(
            (CASE WHEN id <= 12 THEN median_return ELSE -median_return END)
            * n_obs
          ) / NULLIF(SUM(n_obs), 0) AS aligned_median,
          SUM(n_obs) AS n_tickers
        FROM validation.walk_forward_pctile_summary
        WHERE pctile_bin <= %d
          AND fut_lag <= 33
          %s
        GROUP BY pctile_bin, fut_lag
        ORDER BY pctile_bin, fut_lag;",
        input$top_n_valRS, summary_cluster_filter)
    } else if (metric_col == "combo_mean_quadrants") {
      # Same shape as combo_quadrants but uses mean_return instead of
      # median_return for the magnitude axis.
      topn_query <- sprintf("
        SELECT
          pctile_bin AS rank_within_cluster,
          fut_lag,
          SUM(hit_rate * n_obs) / NULLIF(SUM(n_obs), 0) AS hit_rate,
          SUM(
            (CASE WHEN id <= 12 THEN mean_return ELSE -mean_return END)
            * n_obs
          ) / NULLIF(SUM(n_obs), 0) AS aligned_median,
          SUM(n_obs) AS n_tickers
        FROM validation.walk_forward_pctile_summary
        WHERE pctile_bin <= %d
          AND fut_lag <= 33
          %s
        GROUP BY pctile_bin, fut_lag
        ORDER BY pctile_bin, fut_lag;",
        input$top_n_valRS, summary_cluster_filter)
    } else {
      metric_expr <- if (metric_col %in% c("hit_rate", "sharpe_like", "median_return")) {
        sprintf("AVG(%s)", metric_col)
      } else {
        sprintf("SUM(%s * n_obs) / NULLIF(SUM(n_obs), 0)", metric_col)
      }
      topn_query <- sprintf("
        SELECT
          pctile_bin AS rank_within_cluster,
          fut_lag,
          %s AS realized_return,
          SUM(n_obs) AS n_tickers
        FROM validation.walk_forward_pctile_summary
        WHERE pctile_bin <= %d
          AND fut_lag <= 33
          %s
        GROUP BY pctile_bin, fut_lag
        ORDER BY pctile_bin, fut_lag;",
        metric_expr, input$top_n_valRS, summary_cluster_filter)
    }

    tryCatch({
      con <- get_con(input, "RS")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      slot_df <- dbGetQuery(con, slot_query)
      slot_df$rank_within_cluster <- as.integer(slot_df$rank_within_cluster)
      slot_df$mean_fwd <- as.numeric(slot_df$mean_fwd)
      slot_df$sd_fwd   <- as.numeric(slot_df$sd_fwd)
      slot_df$n_obs    <- as.integer(slot_df$n_obs)
      app_dataRS_slot(slot_df)

      hm_df <- dbGetQuery(con, topn_query)
      if (nrow(hm_df) == 0) {
        app_dataRS_heatmap(data.frame())
      } else {
        hm_df$fut_lag           <- as.integer(hm_df$fut_lag)
        hm_df$rank_within_cluster <- as.integer(hm_df$rank_within_cluster)
        hm_df$n_tickers         <- as.integer(hm_df$n_tickers)
        if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) {
          # Median was already aligned per-cluster inside the SQL aggregation.
          hm_df$hit_rate <- as.numeric(hm_df$hit_rate)
          hm_df$aligned_median <- as.numeric(hm_df$aligned_median)
          hm_df$realized_return <- ifelse(
            hm_df$hit_rate > 0.5 & hm_df$aligned_median > 0, 0.875,
            ifelse(hm_df$hit_rate <= 0.5 & hm_df$aligned_median > 0, 0.625,
              ifelse(hm_df$hit_rate > 0.5 & hm_df$aligned_median <= 0, 0.375,
                0.125)))
        } else {
          hm_df$realized_return <- as.numeric(hm_df$realized_return)
        }
        app_dataRS_heatmap(hm_df)
      }

      n_cut_filter <- if (input$id_valRS == "ALL") "" else
        sprintf("AND m.id = %s", input$id_valRS)
      n_cut <- dbGetQuery(con, sprintf("
        SELECT COUNT(DISTINCT r.train_cutoff_date) AS n
        FROM validation.walk_forward_ticker_rank r
        JOIN validation.walk_forward_cluster_id_map m
          ON m.train_cutoff_date = r.train_cutoff_date
         AND m.cluster_id = r.cluster_id
        WHERE r.train_cutoff_date BETWEEN '%s' AND '%s' %s;",
        date_lo, date_hi, n_cut_filter))$n[1]
      app_dataRS_meta(list(n_cutoffs = n_cut, lo = date_lo, hi = date_hi,
                           cluster = input$id_valRS,
                           metric = input$metric_valRS))
      status_msgRS(sprintf("Loaded: %d slot rows, %d cutoffs.",
                           as.integer(nrow(slot_df)), as.integer(n_cut)))
    }, error = function(e) {
      app_dataRS_slot(NULL); app_dataRS_heatmap(NULL); app_dataRS_meta(NULL)
      status_msgRS(paste("Error:", e$message))
    })
  })

  output$cutoffCountRS <- renderText({
    meta <- app_dataRS_meta()
    if (is.null(meta)) return("")
    sprintf("Cluster filter: %s   |   %d distinct cutoffs in window [%s -> %s]",
            meta$cluster, as.integer(meta$n_cutoffs), meta$lo, meta$hi)
  })

  output$slotPerfPlotRS <- renderPlotly({
    req(app_dataRS_slot())
    df <- app_dataRS_slot()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    df$se <- df$sd_fwd / sqrt(pmax(df$n_obs, 1))
    plot_ly(df, x = ~rank_within_cluster, y = ~mean_fwd,
            type = "scatter", mode = "lines+markers",
            error_y = list(type = "data", array = ~se,
                           color = "rgba(148,163,184,0.5)"),
            marker = list(size = 8, color = "#38bdf8"),
            line = list(color = "#38bdf8", width = 2),
            hovertemplate = paste0(
              "rank slot: %{x}<br>",
              "mean fwd return: %{y:.4f}<br>",
              "n obs: %{customdata}<extra></extra>"),
            customdata = ~n_obs) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        xaxis = list(title = "rank_within_cluster (1 = top)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        yaxis = list(title = "Mean forward return (+/- SE)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)",
                     zeroline = TRUE, zerolinecolor = "rgba(255,255,255,0.25)"),
        margin = list(l = 70, r = 30, b = 50, t = 30))
  })

  output$stabilityHeatmapRS <- renderPlotly({
    req(app_dataRS_heatmap())
    df <- app_dataRS_heatmap()
    if (nrow(df) == 0) return(empty_plot("No tickers met threshold."))
    fut_lags <- sort(unique(df$fut_lag))
    ranks    <- sort(unique(df$rank_within_cluster))
    z_mat <- matrix(NA_real_, nrow = length(ranks), ncol = length(fut_lags),
                    dimnames = list(as.character(ranks), as.character(fut_lags)))
    n_mat <- matrix(NA_integer_, nrow = length(ranks), ncol = length(fut_lags),
                    dimnames = list(as.character(ranks), as.character(fut_lags)))
    for (i in seq_len(nrow(df))) {
      r <- which(ranks == df$rank_within_cluster[i])
      c <- which(fut_lags == df$fut_lag[i])
      if (length(r) && length(c)) {
        z_mat[r, c] <- df$realized_return[i]
        n_mat[r, c] <- df$n_tickers[i]
      }
    }
    metric <- isolate(app_dataRS_meta()$metric)
    if (is.null(metric)) metric <- "mean_return"
    if (metric == "hit_rate") {
      colorscale <- DIVERGING_COLORSCALE   # centered at coin flip 0.5
      max_abs <- 0.2
      z_center <- 0.5
    } else if (metric == "sharpe_like") {
      colorscale <- list(
        c(0.00, '#dc2626'),
        c(0.25, '#f97316'),
        c(0.50, '#facc15'),
        c(0.75, '#84cc16'),
        c(1.00, '#16a34a')
      )
      max_abs <- 1
      z_center <- 0
    } else if (metric %in% c("combo_quadrants", "combo_mean_quadrants")) {
      # Discrete bands: gray (neither), blue (hit only),
      # gold (median only = Bessembinder), green (both high).
      # Short clusters (id > 12) get a purple palette so green never appears
      # on declining clusters; longs keep the green palette.
      sel_id <- suppressWarnings(as.integer(input$id_valRS))
      is_short_selected <- !is.na(sel_id) && sel_id > 12
      colorscale <- if (is_short_selected) {
        list(
          c(0.00, '#888888'), c(0.25, '#888888'),
          c(0.25, '#c4b5fd'), c(0.50, '#c4b5fd'),
          c(0.50, '#a78bfa'), c(0.75, '#a78bfa'),
          c(0.75, '#7c3aed'), c(1.00, '#7c3aed')
        )
      } else {
        list(
          c(0.00, '#888888'), c(0.25, '#888888'),
          c(0.25, '#3b82f6'), c(0.50, '#3b82f6'),
          c(0.50, '#f59e0b'), c(0.75, '#f59e0b'),
          c(0.75, '#10b981'), c(1.00, '#10b981')
        )
      }
      max_abs <- 0.5
      z_center <- 0.5
    } else {
      colorscale <- list(
        c(0.00, '#dc2626'),
        c(0.25, '#f97316'),
        c(0.50, '#facc15'),
        c(0.75, '#84cc16'),
        c(1.00, '#16a34a')
      )
      max_abs <- 50
      p95 <- quantile(abs(z_mat - 0), 0.95, na.rm = TRUE)
      if (is.finite(p95) && p95 > 0) max_abs <- min(max_abs, max(p95, 5))
      z_center <- 0
    }
    metric_label <- switch(metric,
      "hit_rate" = "Hit rate\n(coin flip = 0.5)",
      "sharpe_like" = "Sharpe-like\n(mean/sd)",
      "median_return" = "Median\nreturn",
      "combo_quadrants" = "Quadrant\n0.875=both high\n0.625=median only\n(Bessembinder)\n0.375=hit only\n0.125=neither",
      "combo_mean_quadrants" = "Quadrant (mean)\n0.875=both high\n0.625=mean only\n(Bessembinder)\n0.375=hit only\n0.125=neither",
      "Mean\nrealized\nreturn")
    fut_lag_pos <- fut_lags   # linear spacing: cell width proportional to actual horizon
    text_mat <- matrix(rep(as.character(fut_lags), each = nrow(z_mat)), nrow = nrow(z_mat))
    plot_ly(
      x = fut_lag_pos, y = as.character(ranks), z = z_mat, type = "heatmap",
      colorscale = colorscale,
      xgap = 1, ygap = 1,
      zmin = z_center - max_abs, zmax = z_center + max_abs,
      customdata = n_mat,
      text = text_mat,
      hovertemplate = paste0(
        "rank: %{y}<br>",
        "fut_lag: %{text}<br>",
        metric_label, ": %{z:.4f}<br>",
        "n observations: %{customdata}<extra></extra>"),
      colorbar = list(
        title = list(text = metric_label, font = list(color = "#f8fafc")),
        tickfont = list(color = "#94a3b8"))
    ) %>% layout(
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
      xaxis = list(title = "fut_lag (months, to scale)", type = "linear",
                   tickvals = fut_lag_pos, ticktext = as.character(fut_lags),
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "vingtile (1 = top 5%, 20 = bottom 5%)",
                   type = "category",
                   autorange = "reversed",
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      margin = list(l = 90, r = 60, b = 60, t = 30))
  })

  # ── RANK STABILITY: Small-multiples grid of all ids ──
  app_dataRS_allIds <- reactiveVal(NULL)

  observeEvent(input$execute_all_RS, {
    if (input$db_passRS == "") { status_msgRS("Error: Password is not set."); return() }
    status_msgRS("Querying all ids...")
    tryCatch({
      con <- get_con(input, "RS")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      df <- dbGetQuery(con, "
        SELECT id, pctile_bin, fut_lag, mean_return, median_return, hit_rate, sharpe_like, n_obs
        FROM validation.walk_forward_pctile_summary
        WHERE fut_lag <= 33
        ORDER BY id, pctile_bin, fut_lag;")
      df$id <- as.integer(df$id)
      df$pctile_bin <- as.integer(df$pctile_bin)
      df$fut_lag <- as.integer(df$fut_lag)
      df$mean_return <- as.numeric(df$mean_return)
      df$median_return <- as.numeric(df$median_return)
      df$hit_rate <- as.numeric(df$hit_rate)
      df$sharpe_like <- as.numeric(df$sharpe_like)
      # n_obs arrives from Postgres as integer64 (bit64). Left as-is, it is the
      # weight in weighted.mean() during the adaptive rebin below, where bit64
      # arithmetic truncates the fractional metric to an integer BEFORE dividing
      # (hit_rate 0.75 -> 0), collapsing every long cell to hit<=0.5 and washing
      # all green into gold. Cast to double so the weighting is exact.
      df$n_obs <- as.numeric(df$n_obs)
      tiers <- dbGetQuery(con, "
        SELECT m.id AS id,
               CASE WHEN percentile_cont(0.5) WITHIN GROUP (ORDER BY r.cluster_size) >= 20 THEN 20
                    WHEN percentile_cont(0.5) WITHIN GROUP (ORDER BY r.cluster_size) >= 10 THEN 10
                    ELSE 5 END AS tier
        FROM validation.walk_forward_ticker_rank r
        JOIN validation.walk_forward_cluster_id_map m
          ON m.train_cutoff_date = r.train_cutoff_date AND m.cluster_id = r.cluster_id
        WHERE r.fut_lag = 12
        GROUP BY m.id;")
      tier_map <- setNames(as.integer(tiers$tier), as.character(as.integer(tiers$id)))
      df$tier <- tier_map[as.character(df$id)]
      df$tier[is.na(df$tier)] <- 20L
      df$active_metric <- input$metric_valRS
      app_dataRS_allIds(df)
      status_msgRS(sprintf("Loaded all ids: %d rows across %d distinct ids.",
                           as.integer(nrow(df)),
                           as.integer(length(unique(df$id)))))
    }, error = function(e) {
      app_dataRS_allIds(NULL)
      status_msgRS(paste("Error:", e$message))
    })
  })

  output$allIdsGridRS <- renderPlotly({
    req(app_dataRS_allIds())
    df_subset <- app_dataRS_allIds()
    if (nrow(df_subset) == 0) return(empty_plot("No data matched your filters."))

    # Adaptive bins: collapse the 20 vingtiles into each cluster's tier (5/10/20)
    # so small clusters render fewer rows. Rebin the raw metrics (weighted by
    # n_obs) BEFORE the signal is derived, so the quadrant recomputes on the
    # merged bin instead of averaging categorical codes.
    if (!is.null(df_subset$tier)) {
      df_subset$pctile_bin <- pmax(1L, as.integer(ceiling(df_subset$pctile_bin * df_subset$tier / 20)))
      df_subset <- df_subset %>%
        group_by(id, tier, pctile_bin, fut_lag, active_metric) %>%
        summarise(
          mean_return   = weighted.mean(mean_return,   n_obs, na.rm = TRUE),
          median_return = weighted.mean(median_return, n_obs, na.rm = TRUE),
          hit_rate      = weighted.mean(hit_rate,      n_obs, na.rm = TRUE),
          sharpe_like   = weighted.mean(sharpe_like,   n_obs, na.rm = TRUE),
          n_obs         = sum(n_obs, na.rm = TRUE),
          .groups = "drop") %>%
        as.data.frame()
    }

    metric_col <- df_subset$active_metric[1]
    if (is.null(metric_col) || is.na(metric_col)) metric_col <- "mean_return"

    if (metric_col == "hit_rate") {
      df_subset$signal <- df_subset$hit_rate - 0.5
      max_abs <- 0.2
      z_center <- 0
    } else if (metric_col == "sharpe_like") {
      df_subset$direction_sign <- ifelse(df_subset$id <= 12, 1, -1)
      df_subset$signal <- df_subset$direction_sign * df_subset$sharpe_like
      max_abs <- 1
      z_center <- 0
    } else if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) {
      # Quadrant categorical view: hit_rate crossed with aligned return.
      # combo_quadrants uses median_return; combo_mean_quadrants uses mean_return.
      df_subset$direction_sign <- ifelse(df_subset$id <= 12, 1, -1)
      df_subset$aligned_median <- if (metric_col == "combo_quadrants") {
        df_subset$direction_sign * df_subset$median_return
      } else {
        df_subset$direction_sign * df_subset$mean_return
      }
      df_subset$signal <- ifelse(
        df_subset$hit_rate > 0.5 & df_subset$aligned_median > 0, 0.875,
        ifelse(df_subset$hit_rate <= 0.5 & df_subset$aligned_median > 0, 0.625,
          ifelse(df_subset$hit_rate > 0.5 & df_subset$aligned_median <= 0, 0.375,
            0.125)))
      max_abs <- 1
      z_center <- 0.5
    } else {
      val_col <- if (metric_col == "median_return") "median_return" else "mean_return"
      df_subset$val <- df_subset[[val_col]]
      horizon_median <- aggregate(val ~ fut_lag, data = df_subset,
                                  FUN = function(x) median(x, na.rm = TRUE))
      names(horizon_median)[2] <- "horizon_median"
      df_subset <- merge(df_subset, horizon_median, by = "fut_lag", all.x = TRUE)
      df_subset$direction_sign <- ifelse(df_subset$id <= 12, 1, -1)
      df_subset$signal <- df_subset$direction_sign *
                         (df_subset$val - df_subset$horizon_median)
      max_abs <- 10
      p90 <- quantile(abs(df_subset$signal), 0.90, na.rm = TRUE)
      if (is.finite(p90) && p90 > 0) max_abs <- min(max_abs, max(p90, 3))
      z_center <- 0
    }
    colorscale <- if (metric_col == "hit_rate") {
      DIVERGING_COLORSCALE
    } else if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) list(
      # Four discrete bands: gray (neither), blue (hit only),
      # gold (median only = Bessembinder), green (both high)
      c(0.00, '#888888'), c(0.25, '#888888'),
      c(0.25, '#3b82f6'), c(0.50, '#3b82f6'),
      c(0.50, '#f59e0b'), c(0.75, '#f59e0b'),
      c(0.75, '#10b981'), c(1.00, '#10b981')
    ) else list(
      c(0.00, '#dc2626'),
      c(0.25, '#f97316'),
      c(0.50, '#facc15'),
      c(0.75, '#84cc16'),
      c(1.00, '#16a34a')
    )

    ids <- sort(unique(df_subset$id))
    n_ids <- length(ids)
    n_cols <- 4
    n_rows <- ceiling(n_ids / n_cols)

    # Consistent axes across all subplots: full vingtile range 1-20, fut_lag from data
    all_ranks <- 1:20
    all_fut_lags <- sort(unique(df_subset$fut_lag))
    all_fut_lag_pos <- sqrt(all_fut_lags)

    plots <- lapply(ids, function(this_id) {
      sub <- df_subset[df_subset$id == this_id, ]
      # Per-cluster row count: 5 / 10 / 20 by its tier (shadows the outer 1:20).
      this_tier <- if (!is.null(sub$tier) && length(sub$tier) && !is.na(sub$tier[1])) sub$tier[1] else 20L
      all_ranks <- 1:this_tier
      # Pad to full vingtile range so all subplots have same axis
      z_mat <- matrix(NA_real_, nrow = length(all_ranks), ncol = length(all_fut_lags),
                      dimnames = list(as.character(all_ranks), as.character(all_fut_lags)))
      for (i in seq_len(nrow(sub))) {
        r <- which(all_ranks == sub$pctile_bin[i])
        c <- which(all_fut_lags == sub$fut_lag[i])
        if (length(r) && length(c)) z_mat[r, c] <- sub$signal[i]
      }
      # Reverse rows so vingtile 1 is at top of plot (matches single-id view)
      z_mat_rev <- z_mat[rev(seq_len(nrow(z_mat))), , drop = FALSE]
      text_mat <- matrix(rep(as.character(all_fut_lags), each = nrow(z_mat_rev)),
                         nrow = nrow(z_mat_rev))
      # combo_quadrants uses categorical [0,1] range; others use centered [-max_abs, max_abs]
      zmin_val <- if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) 0 else -max_abs
      zmax_val <- if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) 1 else  max_abs
      # For combo_quadrants ONLY: short clusters (id > 12) get a purple
      # palette so green never appears on declining clusters. Same 4-tier
      # information (gray/light purple/medium purple/deep purple), distinct
      # palette from the long-cluster green family.
      this_colorscale <- if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants") && this_id > 12) {
        list(
          c(0.00, '#888888'), c(0.25, '#888888'),
          c(0.25, '#c4b5fd'), c(0.50, '#c4b5fd'),
          c(0.50, '#a78bfa'), c(0.75, '#a78bfa'),
          c(0.75, '#7c3aed'), c(1.00, '#7c3aed')
        )
      } else {
        colorscale
      }
      plot_ly(
        x = all_fut_lag_pos,
        y = rev(as.character(all_ranks)),
        z = z_mat_rev,
        type = "heatmap", colorscale = this_colorscale,
        zmin = zmin_val, zmax = zmax_val, showscale = FALSE,
        xgap = 1, ygap = 1,
        text = text_mat,
        hovertemplate = if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) paste0(
          "id ", this_id,
          " (", ifelse(this_id <= 12, "long", "short"), ")",
          "<br>vingtile: %{y}<br>fut_lag: %{text}<br>",
          "0.125=neither | 0.375=hit-only | 0.625=median-only (Bessembinder) | 0.875=both-high",
          "<br>value: %{z:.3f}<extra></extra>") else paste0(
          "id ", this_id,
          " (", ifelse(this_id <= 12, "long", "short"), ")",
          "<br>vingtile: %{y}<br>fut_lag: %{text}<br>",
          "model-aligned signal: %{z:+.2f}<extra></extra>")
      ) %>% layout(
        annotations = list(
          list(text = paste0("id ", this_id),
               xref = "paper", yref = "paper", x = 0.5, y = 1.05,
               showarrow = FALSE,
               font = list(color = "#f8fafc", size = 11, family = "Inter"))
        ),
        yaxis = list(type = "category",
                     categoryorder = "array",
                     categoryarray = rev(as.character(all_ranks)),
                     tickvals = c("1","5","10","15","20"),
                     showgrid = FALSE, zeroline = FALSE,
                     color = "#94a3b8", tickfont = list(size = 8)),
        xaxis = list(type = "linear",
                     tickvals = all_fut_lag_pos,
                     ticktext = as.character(all_fut_lags),
                     showgrid = FALSE, zeroline = FALSE,
                     color = "#94a3b8", tickfont = list(size = 8))
      )
    })

    subplot(plots, nrows = n_rows, shareX = FALSE, shareY = FALSE,
            margin = 0.025, titleY = TRUE) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        showlegend = FALSE,
        font = list(color = "#94a3b8", size = 9),
        margin = list(l = 40, r = 30, b = 40, t = 40))
  })

  # ── MODEL VALIDATION: Reactive values ──
  app_dataMV_ic     <- reactiveVal(NULL)
  app_dataMV_payoff <- reactiveVal(NULL)
  app_dataMV_tiers  <- reactiveVal(NULL)
  app_dataMV_forest <- reactiveVal(NULL)
  status_msgMV <- reactiveVal("Ready")
  output$statusMessageMV <- renderText({ status_msgMV() })
  setup_env_switcher(input, session, "MV")

  observeEvent(input$connect_btnMV, {
    if (input$db_passMV == "") { status_msgMV("Error: Password is not set."); return() }
    status_msgMV("Connecting...")
    tryCatch({
      con <- get_con(input, "MV")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals <- dbGetQuery(con,
        "SELECT DISTINCT id FROM validation.cell_credibility
         UNION SELECT DISTINCT id FROM validation.return_cluster_payoff_backtest
         ORDER BY 1")
      updateSelectInput(session, "id_valMV",
        choices = c("All ids" = "ALL",
                    setNames(as.character(id_vals$id), as.character(id_vals$id))),
        selected = "ALL")
      past_vals <- dbGetQuery(con,
        "SELECT DISTINCT past_lag FROM validation.cell_credibility ORDER BY 1")
      fut_vals <- dbGetQuery(con,
        "SELECT DISTINCT fut_lag FROM validation.cell_credibility ORDER BY 1")
      updateSelectInput(session, "past_lagMV",
                        choices = as.character(past_vals$past_lag), selected = "12")
      updateSelectInput(session, "fut_lagMV",
                        choices = as.character(fut_vals$fut_lag), selected = "12")
      status_msgMV("Filters loaded!")
    }, error = function(e) { status_msgMV(paste("Error:", e$message)) })
  })

  observeEvent(input$execute_MV, {
    if (input$db_passMV == "") { status_msgMV("Error: Password is not set."); return() }
    status_msgMV("Running queries...")
    id_filter <- if (is.null(input$id_valMV) || input$id_valMV %in% c("", "ALL")) "" else
      sprintf("AND id = %d", as.integer(input$id_valMV))
    tryCatch({
      con <- get_con(input, "MV")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)

      ic_df <- dbGetQuery(con, "
        SELECT id, fut_lag, weighted_ic, mean_ic, median_ic,
               n_positive, n_cohorts, mean_decile_spread
        FROM validation.walk_forward_id_ic
        ORDER BY id, fut_lag;")
      ic_df$id          <- as.integer(ic_df$id)
      ic_df$fut_lag     <- as.integer(ic_df$fut_lag)
      ic_df$weighted_ic <- as.numeric(ic_df$weighted_ic)
      ic_df$mean_decile_spread <- as.numeric(ic_df$mean_decile_spread)
      # COUNT(*) columns arrive as integer64 (see the RS n_obs note above);
      # cast to double before any arithmetic or sprintf.
      ic_df$n_positive <- as.numeric(ic_df$n_positive)
      ic_df$n_cohorts  <- as.numeric(ic_df$n_cohorts)
      app_dataMV_ic(ic_df)

      payoff_df <- dbGetQuery(con, sprintf("
        SELECT id, past_lag, fut_lag, n_holdout, win_pct, expectancy,
               avg_win, avg_loss, median_ret, p90_ret, max_win
        FROM validation.return_cluster_payoff_backtest
        WHERE 1=1 %s
        ORDER BY id, past_lag, fut_lag;", id_filter))
      payoff_df$id        <- as.integer(payoff_df$id)
      payoff_df$past_lag  <- as.integer(payoff_df$past_lag)
      payoff_df$fut_lag   <- as.integer(payoff_df$fut_lag)
      payoff_df <- coerce_numeric_cols(payoff_df, c(
        "n_holdout", "win_pct", "expectancy", "avg_win", "avg_loss",
        "median_ret", "p90_ret", "max_win"))
      app_dataMV_payoff(payoff_df)

      tier_df <- dbGetQuery(con, "
        SELECT id, tier, COUNT(*) AS n_cells
        FROM validation.cell_credibility
        GROUP BY id, tier
        ORDER BY id, tier;")
      tier_df$id      <- as.integer(tier_df$id)
      tier_df$n_cells <- as.numeric(tier_df$n_cells)           # integer64
      app_dataMV_tiers(tier_df)

      if (!input$id_valMV %in% c("", "ALL") &&
          isTruthy(input$past_lagMV) && isTruthy(input$fut_lagMV)) {
        forest_df <- dbGetQuery(con, sprintf("
          SELECT vol_bucket_num, bucket, n_cutoffs, wf_n_holdout, n_scored,
                 wf_agreement, wilson_lower, wilson_upper, tier, credibility_weight
          FROM validation.cell_credibility
          WHERE id = %d AND past_lag = %d AND fut_lag = %d
          ORDER BY vol_bucket_num, bucket;",
          as.integer(input$id_valMV), as.integer(input$past_lagMV),
          as.integer(input$fut_lagMV)))
        forest_df <- coerce_numeric_cols(forest_df, c(
          "n_cutoffs", "wf_n_holdout", "n_scored",
          "wf_agreement", "wilson_lower", "wilson_upper", "credibility_weight"))
        app_dataMV_forest(forest_df)
      } else {
        app_dataMV_forest(data.frame())
      }
      status_msgMV(sprintf("Loaded: %d IC rows, %d payoff rows, %d forest rows.",
                           nrow(ic_df), nrow(payoff_df),
                           nrow(app_dataMV_forest())))
    }, error = function(e) {
      app_dataMV_ic(NULL); app_dataMV_payoff(NULL)
      app_dataMV_tiers(NULL); app_dataMV_forest(NULL)
      status_msgMV(paste("Error:", e$message))
    })
  })

  output$icHeatmapMV <- renderPlotly({
    req(app_dataMV_ic())
    df <- app_dataMV_ic()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    fut_lags <- sort(unique(df$fut_lag))
    ids      <- sort(unique(df$id))
    z_mat <- matrix(NA_real_, nrow = length(ids), ncol = length(fut_lags),
                    dimnames = list(as.character(ids), as.character(fut_lags)))
    cd_mat <- matrix("", nrow = length(ids), ncol = length(fut_lags))
    for (i in seq_len(nrow(df))) {
      r <- which(ids == df$id[i])
      c <- which(fut_lags == df$fut_lag[i])
      if (length(r) && length(c)) {
        z_mat[r, c] <- df$weighted_ic[i]
        cd_mat[r, c] <- sprintf("%d/%d cohorts positive | decile spread %.3f",
                                as.integer(df$n_positive[i]),
                                as.integer(df$n_cohorts[i]),
                                df$mean_decile_spread[i])
      }
    }
    # symmetric zmin/zmax so IC = 0 sits at the palette midpoint
    colorscale <- DIVERGING_COLORSCALE
    max_abs <- max(abs(z_mat), na.rm = TRUE)
    if (!is.finite(max_abs) || max_abs == 0) max_abs <- 0.1
    text_mat <- matrix(rep(as.character(fut_lags), each = nrow(z_mat)),
                       nrow = nrow(z_mat))
    plot_ly(
      x = fut_lags, y = as.character(ids), z = z_mat, type = "heatmap",
      colorscale = colorscale,
      xgap = 1, ygap = 1,
      zmin = -max_abs, zmax = max_abs,
      customdata = cd_mat,
      text = text_mat,
      hovertemplate = paste0(
        "id: %{y}<br>",
        "fut_lag: %{text}<br>",
        "weighted IC: %{z:.3f}<br>",
        "%{customdata}<extra></extra>"),
      colorbar = list(
        title = list(text = "Weighted\nIC", font = list(color = "#f8fafc")),
        tickfont = list(color = "#94a3b8"))
    ) %>% layout(
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
      xaxis = list(title = "fut_lag (months, to scale)", type = "linear",
                   tickvals = fut_lags, ticktext = as.character(fut_lags),
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "id (1-12 long, 13-19 short)", type = "category",
                   autorange = "reversed",
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      margin = list(l = 90, r = 60, b = 60, t = 30))
  })

  output$payoffScatterMV <- renderPlotly({
    req(app_dataMV_payoff())
    df <- app_dataMV_payoff()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    df$hover <- sprintf(
      "id %d | past %d -> fut %d<br>n holdout: %d<br>avg win %.3f | avg loss %.3f<br>p90 %.3f",
      df$id, df$past_lag, df$fut_lag, as.integer(df$n_holdout),
      df$avg_win, df$avg_loss, df$p90_ret)
    plot_ly(df, x = ~win_pct, y = ~expectancy,
            color = ~factor(fut_lag),
            colors = c('#38bdf8', '#a855f7', '#f59e0b', '#10b981',
                       '#dc2626', '#c4b5fd', '#facc15'),
            size = ~n_holdout, sizes = c(6, 30),
            type = "scatter", mode = "markers",
            marker = list(sizemode = "area", opacity = 0.75),
            customdata = ~hover,
            hovertemplate = paste0(
              "win %: %{x:.1f}<br>",
              "expectancy: %{y:.3f}<br>",
              "%{customdata}<extra></extra>")) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        legend = list(title = list(text = "fut_lag"),
                      font = list(color = "#94a3b8")),
        shapes = list(
          list(type = "line", x0 = 50, x1 = 50, yref = "paper", y0 = 0, y1 = 1,
               line = list(dash = "dash", color = "rgba(255,255,255,0.25)")),
          list(type = "line", y0 = 0, y1 = 0, xref = "paper", x0 = 0, x1 = 1,
               line = list(dash = "dash", color = "rgba(255,255,255,0.25)"))),
        xaxis = list(title = "Win % (holdout, coin flip = 50)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        yaxis = list(title = "Expectancy (mean trade return)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        margin = list(l = 70, r = 30, b = 50, t = 30))
  })

  output$payoffHeatmapMV <- renderPlotly({
    req(app_dataMV_payoff())
    df <- app_dataMV_payoff()
    sel <- input$id_valMV
    if (is.null(sel) || sel %in% c("", "ALL")) return(empty_plot("Select a specific id"))
    df <- df[df$id == as.integer(sel), ]
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    fut_lags  <- sort(unique(df$fut_lag))
    past_lags <- sort(unique(df$past_lag))
    z_mat <- matrix(NA_real_, nrow = length(past_lags), ncol = length(fut_lags),
                    dimnames = list(as.character(past_lags), as.character(fut_lags)))
    cd_mat <- matrix("", nrow = length(past_lags), ncol = length(fut_lags))
    for (i in seq_len(nrow(df))) {
      r <- which(past_lags == df$past_lag[i])
      c <- which(fut_lags == df$fut_lag[i])
      if (length(r) && length(c)) {
        z_mat[r, c] <- df$expectancy[i]
        cd_mat[r, c] <- sprintf("win %.1f%% | n %d",
                                df$win_pct[i], as.integer(df$n_holdout[i]))
      }
    }
    colorscale <- DIVERGING_COLORSCALE
    max_abs <- max(abs(z_mat), na.rm = TRUE)
    if (!is.finite(max_abs) || max_abs == 0) max_abs <- 0.1
    text_mat <- matrix(rep(as.character(fut_lags), each = nrow(z_mat)),
                       nrow = nrow(z_mat))
    plot_ly(
      x = fut_lags, y = as.character(past_lags), z = z_mat, type = "heatmap",
      colorscale = colorscale,
      xgap = 1, ygap = 1,
      zmin = -max_abs, zmax = max_abs,
      customdata = cd_mat,
      text = text_mat,
      hovertemplate = paste0(
        "past_lag: %{y}<br>",
        "fut_lag: %{text}<br>",
        "expectancy: %{z:.3f}<br>",
        "%{customdata}<extra></extra>"),
      colorbar = list(
        title = list(text = "Expectancy", font = list(color = "#f8fafc")),
        tickfont = list(color = "#94a3b8"))
    ) %>% layout(
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
      xaxis = list(title = "fut_lag (months, to scale)", type = "linear",
                   tickvals = fut_lags, ticktext = as.character(fut_lags),
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "past_lag (months)", type = "category",
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      margin = list(l = 90, r = 60, b = 60, t = 30))
  })

  output$tierBarMV <- renderPlotly({
    req(app_dataMV_tiers())
    df <- app_dataMV_tiers()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    tier_colors <- TIER_COLORS
    df$tier <- factor(df$tier, levels = names(tier_colors))
    plot_ly(df, x = ~factor(id), y = ~n_cells, color = ~tier,
            colors = tier_colors, type = "bar",
            hovertemplate = "id %{x}<br>%{fullData.name}: %{y} cells<extra></extra>") %>%
      layout(
        barmode = "stack",
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        legend = list(font = list(color = "#94a3b8")),
        xaxis = list(title = "id (1-12 long, 13-19 short)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        yaxis = list(title = "Cells (vol x past x fut x z-bucket)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        margin = list(l = 70, r = 30, b = 50, t = 30))
  })

  output$forestPlotMV <- renderPlotly({
    req(app_dataMV_forest())
    df <- app_dataMV_forest()
    if (nrow(df) == 0) return(empty_plot("Select a specific id (+ past/fut lag), then Generate"))
    df$cell <- sprintf("vol %d | z %d",
                       as.integer(df$vol_bucket_num), as.integer(df$bucket))
    df$hover <- sprintf(
      "n scored: %d | cutoffs: %d | holdout: %d<br>credibility weight: %.3f",
      as.integer(df$n_scored), as.integer(df$n_cutoffs),
      as.integer(df$wf_n_holdout), df$credibility_weight)
    tier_colors <- TIER_COLORS
    df$tier <- factor(df$tier, levels = names(tier_colors))
    plot_ly(df, x = ~wf_agreement, y = ~cell, color = ~tier,
            colors = tier_colors,
            type = "scatter", mode = "markers",
            marker = list(size = 9),
            error_x = list(type = "data", symmetric = FALSE,
                           array = ~(wilson_upper - wf_agreement),
                           arrayminus = ~(wf_agreement - wilson_lower),
                           color = "rgba(148,163,184,0.6)"),
            customdata = ~hover,
            hovertemplate = paste0(
              "%{y}<br>agreement: %{x:.3f}<br>%{customdata}<extra></extra>")) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        legend = list(font = list(color = "#94a3b8")),
        shapes = list(list(type = "line", x0 = 0.5, x1 = 0.5, yref = "paper",
                           y0 = 0, y1 = 1,
                           line = list(dash = "dash",
                                       color = "rgba(255,255,255,0.4)"))),
        xaxis = list(title = "Walk-forward sign agreement (Wilson 95% CI)",
                     range = c(-0.05, 1.05),
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        yaxis = list(title = "", type = "category",
                     categoryorder = "array",
                     categoryarray = rev(sort(unique(df$cell))),
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        margin = list(l = 110, r = 30, b = 50, t = 30))
  })

  # ── BUY LIST: Reactive values ──
  app_dataBL <- reactiveVal(NULL)
  app_modeBL <- reactiveVal("current")   # "current", "ledger" (as-of replay), or "wf" (backtest replay)
  ledger_boundsBL <- reactiveVal(NULL)   # c(min_date, max_date) of prediction_ledger
  wf_minBL <- reactiveVal(NULL)          # earliest walk-forward train cutoff
  status_msgBL <- reactiveVal("Ready")
  output$statusMessageBL <- renderText({ status_msgBL() })
  setup_env_switcher(input, session, "BL")

  # Build the as-of date from the three dropdowns; clamp day so e.g.
  # Feb 31 resolves to Feb 28 instead of NA.
  asof_dateBL <- function() {
    y <- suppressWarnings(as.integer(input$asof_yearBL))
    m <- suppressWarnings(as.integer(input$asof_monthBL))
    d <- suppressWarnings(as.integer(input$asof_dayBL))
    if (is.na(y) || is.na(m) || is.na(d)) return(NULL)
    # as.Date ERRORS (not NA) on impossible dates like Feb 31, so wrap it
    safe_date <- function(s) tryCatch(as.Date(s), error = function(e) as.Date(NA))
    dt <- safe_date(sprintf("%04d-%02d-%02d", y, m, d))
    while (is.na(dt) && d > 28) {
      d <- d - 1
      dt <- safe_date(sprintf("%04d-%02d-%02d", y, m, d))
    }
    dt
  }

  set_asof_dropdowns <- function(session, date) {
    updateSelectInput(session, "asof_yearBL",  selected = format(date, "%Y"))
    updateSelectInput(session, "asof_monthBL", selected = as.integer(format(date, "%m")))
    updateSelectInput(session, "asof_dayBL",   selected = as.integer(format(date, "%d")))
  }

  # ── Delisting metadata display (raw.ticker_metadata) ──
  # Dot/text color = HOW the company left the market: green = good exit
  # (acquired / went private), red = bad (bankrupt / distressed / failed),
  # amber = neutral (SPAC wind-down, ticker rename), burnt-orange = delisted
  # but no category on file. raw.ticker_metadata holds ONLY delisted names
  # (all active = false), so alive tickers can never pick up a label.
  DELIST_CLASSES <- c(
    "delisted: acquired/private" = "#34d399",
    "delisted: bankrupt/failed"  = "#ef4444",
    "delisted: SPAC/renamed"     = "#eab308",
    "delisted: uncategorized"    = "#c2410c")
  delist_enrich <- function(df) {
    cat <- if ("delisting_category" %in% names(df))
      toupper(trimws(as.character(df$delisting_category)))
    else rep(NA_character_, nrow(df))
    lab <- c("M&A_OR_PRIVATE"      = "acquired / went private",
             "BANKRUPTCY"          = "bankruptcy",
             "DISTRESSED"          = "distressed exit",
             "SPAC"                = "SPAC wound down / merged",
             "TICKER_VARIANT"      = "ticker changed (renamed/relisted)",
             "SHELL_OR_FAILED_IPO" = "shell / failed IPO")
    cls <- c("M&A_OR_PRIVATE"      = "delisted: acquired/private",
             "BANKRUPTCY"          = "delisted: bankrupt/failed",
             "DISTRESSED"          = "delisted: bankrupt/failed",
             "SHELL_OR_FAILED_IPO" = "delisted: bankrupt/failed",
             "SPAC"                = "delisted: SPAC/renamed",
             "TICKER_VARIANT"      = "delisted: SPAC/renamed")
    known <- !is.na(cat) & cat %in% names(lab)
    df$del_label <- ifelse(known, unname(lab[cat]), "no category on file")
    df$del_class <- ifelse(known, unname(cls[cat]), "delisted: uncategorized")
    nm <- if ("company_name" %in% names(df)) as.character(df$company_name)
          else rep(NA_character_, nrow(df))
    dt <- if ("delisted_date" %in% names(df)) as.character(df$delisted_date)
          else rep(NA_character_, nrow(df))
    piece <- function(x) ifelse(is.na(x) | !nzchar(x), "", paste0(" · ", x))
    df$del_info <- paste0(df$del_label, piece(nm), piece(dt))
    # alive rows carry no exit story - blank them so table styling and hovers
    # can key on nzchar() without consulting the delisted flag twice
    if ("delisted" %in% names(df)) {
      blank <- !df$delisted
      df$del_label[blank] <- ""; df$del_class[blank] <- ""; df$del_info[blank] <- ""
    }
    df
  }

  # Which mode the SELECTED date will resolve to (live, pre-Generate), driving
  # the conditionalPanels: 'current' shows View, 'wf' shows the horizon.
  # Before Connect (no ledger bounds) default to 'current'.
  output$blCtlMode <- renderText({
    b <- ledger_boundsBL(); asof <- asof_dateBL()
    if (is.null(b) || is.null(asof) || length(asof) != 1 || is.na(asof))
      return("current")
    if (asof < b[1]) "wf" else if (asof < b[2]) "ledger" else "current"
  })
  outputOptions(output, "blCtlMode", suspendWhenHidden = FALSE)

  output$modeNoteBL <- renderUI({
    mode <- app_modeBL()
    df <- app_dataBL()
    is_picks <- mode == "wf" && !is.null(df) && nrow(df) > 0 &&
      "view_kind" %in% names(df) && identical(df$view_kind[1], "picks")
    h_style <- "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"
    if (is_picks) {
      tagList(
        h4("Model's top picks (reconstructed) - validation.walk_forward_top_picks",
           style = h_style),
        tags$div(class = "caveat-warning",
          tags$b(style = "color: #fbbf24;", "Read: "),
          "what the model would have recommended at the nearest quarterly ",
          "cutoff: its top 10 per cluster (positive evidence only), shown ",
          "ONLY for clusters that had EARNED TRUST BY THAT DATE - walk-",
          "forward IC >= 0.10 with positive spread over at least 8 settled ",
          "prior quarters. No future information decides eligibility, so ",
          "this is an honest as-of reconstruction; it is NOT the live BUY ",
          "gate (which did not exist then and pools evidence across all ",
          "years). Each bar = how the pick REALLY did vs SPY over the next ",
          "12 months, from actual adjusted prices: green beat SPY, red ",
          "lagged, grey = no tradable price window. A dot at the bar tip = ",
          "the company no longer trades; its color = HOW it left (green ",
          "acquired/private, red bankrupt/failed, amber SPAC/rename, ",
          "burnt-orange uncategorized). The chips above are this date's ",
          "scorecard: % positive = share of picks that beat SPY."))
    } else if (mode == "wf") {
      tagList(
        h4("Backtest replay - validation.walk_forward_ticker_rank", style = h_style),
        tags$div(class = "caveat-warning",
          tags$b(style = "color: #fbbf24;", "Read: "),
          "the PREDICTION as it stood at the nearest quarterly walk-forward ",
          "cutoff: every ranked ticker per cluster (ids 13+ = short side), ",
          "in the model's own ",
          "order (r1 at the top of each cluster block = its top pick). ",
          "Each bar = how that pick REALLY did vs SPY over the replay horizon ",
          "(sidebar, default 12 months), from actual adjusted prices (first ",
          "close after the cutoff to the last close within the horizon): green ",
          "beat SPY, red lagged, grey = no tradable price window. Ranking and ",
          "grading window switch together with the horizon. If the model works, ",
          "green concentrates near each block's r1. A dot at the bar tip = the ",
          "company no longer trades; its color = HOW it left: green acquired/",
          "went private, red bankrupt/failed, amber SPAC or ticker rename, ",
          "burnt-orange no category on file. Hover the dot (or see the table's ",
          "Delisted column) for reason, company and date. Backtest ranking ",
          "only - the live BUY gates did not exist then."))
    } else if (mode == "ledger") {
      tagList(
        h4("Ledger replay - monitoring.prediction_ledger", style = h_style),
        tags$div(class = "caveat-warning",
          tags$b(style = "color: #fbbf24;", "Read: "),
          "the BUY calls the live system actually logged on the selected date, ",
          "graded to the latest close. Each bar = one ticker's return since entry ",
          "relative to SPY. Green beat SPY, red lagged it. A dot = delisted since ",
          "entry; dot color = why (green acquired/private, red bankrupt/failed, ",
          "amber SPAC/rename, burnt-orange uncategorized - hover it or see the ",
          "table's Delisted column); its 'latest close' is the final traded ",
          "price."))
    } else {
      tagList(
        h4("Current BUY list - serving.return_cluster_ticker_global_action_current x validated payoff",
           style = h_style),
        tags$div(class = "caveat-warning",
          tags$b(style = "color: #fbbf24;", "Read: "),
          "tickers the model currently marks BUY. Order = win likelihood: each ",
          "ticker's bin = its slot in the latest WALK-FORWARD ranking (20 bins ",
          "of 5%; same ranking the bin win rates were measured on), and rows ",
          "sort by their bin's realized win rate vs SPY (most winners on top; ",
          "bars still show expectancy = mean holdout trade return). Tickers the ",
          "walk-forward has no scored evidence for show 'no evidence' and sink ",
          "below the graded BUYs. Use the View selector to switch between the ",
          "decision shortlist (rank bins winning >= 55% on >= 100 graded obs), ",
          "every BUY, and ALL ranked tickers per cluster - it switches ",
          "instantly, no re-Generate."))
    }
  })

  observeEvent(input$connect_btnBL, {
    if (input$db_passBL == "") { status_msgBL("Error: Password is not set."); return() }
    status_msgBL("Connecting...")
    tryCatch({
      con <- get_con(input, "BL")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      n <- dbGetQuery(con, "
        SELECT COUNT(*) AS n
        FROM serving.return_cluster_ticker_global_action_current
        WHERE global_action = 'BUY'")$n[1]
      bounds <- dbGetQuery(con, "
        SELECT MIN(prediction_date) AS min_d, MAX(prediction_date) AS max_d
        FROM monitoring.prediction_ledger")
      wf_min <- dbGetQuery(con, "
        SELECT MIN(r.train_cutoff_date) AS d
        FROM validation.walk_forward_ticker_rank r
        WHERE r.fut_lag = 12
          AND EXISTS (SELECT 1 FROM validation.walk_forward_cluster_id_map m
                      WHERE m.train_cutoff_date = r.train_cutoff_date)")$d[1]
      if (!is.na(bounds$min_d[1])) {
        ledger_boundsBL(c(bounds$min_d[1], bounds$max_d[1]))
        wf_minBL(wf_min)
        yr_lo <- as.integer(format(if (!is.na(wf_min)) wf_min else bounds$min_d[1], "%Y"))
        yr_hi <- as.integer(format(bounds$max_d[1], "%Y"))
        updateSelectInput(session, "asof_yearBL", choices = seq(yr_hi, yr_lo),
                          selected = yr_hi)
        set_asof_dropdowns(session, bounds$max_d[1])
      }
      status_msgBL(sprintf("Connected. %d tickers currently BUY. Ledger %s to %s; walk-forward back to %s.",
                           as.numeric(n), bounds$min_d[1], bounds$max_d[1], wf_min))
    }, error = function(e) { status_msgBL(paste("Error:", e$message)) })
  })

  observeEvent(input$todayBtnBL, {
    b <- ledger_boundsBL()
    if (is.null(b)) { status_msgBL("Connect first to load the date range."); return() }
    set_asof_dropdowns(session, b[2])
    status_msgBL(sprintf("As-of date set to latest snapshot (%s).", b[2]))
  })

  observeEvent(input$execute_BL, {
    if (input$db_passBL == "") { status_msgBL("Error: Password is not set."); return() }
    status_msgBL("Running query...")
    b <- ledger_boundsBL()
    asof <- asof_dateBL()
    has_date <- !is.null(b) && !is.null(asof) && length(asof) == 1 && !is.na(asof)
    is_wf     <- has_date && asof < b[1]
    is_ledger <- has_date && !is_wf && asof < b[2]

    if (is_wf && !identical(input$wf_view_modeBL, "ranking")) {
      # ── Backtest default: the model's trust-gated TOP PICKS ──
      # validation.walk_forward_top_picks holds the as-of reconstruction
      # (top 10 per cluster, cluster eligible only on evidence settled BY
      # that cutoff, outcomes pre-graded on real 12mo prices). If the table
      # is missing (first build lands with the next walk-forward) or has no
      # cutoff early enough, fall through to the full ranking replay below.
      handled <- tryCatch({
        con <- get_con(input, "BL")
        on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
        has_picks <- isTRUE(tryCatch(dbGetQuery(con, "
          SELECT EXISTS (SELECT 1 FROM information_schema.tables
                         WHERE table_schema = 'validation'
                           AND table_name = 'walk_forward_top_picks') AS ok")$ok[1],
          error = function(e) FALSE))
        if (!has_picks) {
          status_msgBL(paste("Top-picks table not built yet (first build lands",
                             "with the next walk-forward run). Showing the full",
                             "ranking replay instead."))
          FALSE
        } else {
          query <- sprintf("
            WITH sel AS (
                SELECT MAX(train_cutoff_date) AS d
                FROM validation.walk_forward_top_picks
                WHERE train_cutoff_date <= '%s'
            )
            , mkt AS (
                SELECT MAX(date) AS market_max FROM cdm.ingest_combined
                WHERE ticker = 'SPY'
            )
            SELECT p.train_cutoff_date, p.id, p.ticker,
                   p.pick_rank AS rank_within_cluster,
                   p.cluster_n AS cluster_size,
                   p.ticker_score,
                   p.real_excess_pct AS fwd_excess_pct,
                   p.trailing_ic,
                   ((SELECT MAX(i.date) FROM cdm.ingest_combined i
                     WHERE i.ticker = p.ticker)
                      >= mkt.market_max - INTERVAL '10 days') AS is_active,
                   mkt.market_max__METACOLS__
            FROM validation.walk_forward_top_picks p
            JOIN sel ON p.train_cutoff_date = sel.d
            CROSS JOIN mkt
            __METAJOIN__
            ORDER BY p.id, p.pick_rank;",
            format(asof, "%Y-%m-%d"))
          has_meta <- isTRUE(tryCatch(dbGetQuery(con, "
            SELECT EXISTS (SELECT 1 FROM information_schema.tables
                           WHERE table_schema = 'raw'
                             AND table_name = 'ticker_metadata') AS ok")$ok[1],
            error = function(e) FALSE))
          query <- gsub("__METACOLS__", if (has_meta)
            ", tm.name AS company_name, tm.delisting_category, tm.delisted_utc::date AS delisted_date"
            else "", query, fixed = TRUE)
          query <- gsub("__METAJOIN__", if (has_meta)
            "LEFT JOIN raw.ticker_metadata tm ON tm.ticker = p.ticker"
            else "", query, fixed = TRUE)
          df <- dbGetQuery(con, query)
          if (nrow(df) == 0) {
            earliest <- tryCatch(dbGetQuery(con, "
              SELECT MIN(train_cutoff_date) AS d
              FROM validation.walk_forward_top_picks")$d[1],
              error = function(e) NA)
            status_msgBL(sprintf(paste(
              "No trust-gated picks at any cutoff on or before %s (earliest",
              "qualifying cutoff: %s - before that no cluster had 8 settled",
              "quarters of passing evidence). Showing the full ranking",
              "replay instead."), asof, earliest))
            FALSE
          } else {
            for (col in c("id", "rank_within_cluster", "cluster_size"))
              df[[col]] <- as.integer(df[[col]])
            df <- coerce_numeric_cols(df, c("ticker_score", "fwd_excess_pct",
                                            "trailing_ic"))
            df$delisted <- is.na(df$is_active) | !df$is_active
            df <- delist_enrich(df)
            df$horizon <- 12L      # picks are graded at 12mo in the table
            df$view_kind <- "picks"
            app_modeBL("wf")
            app_dataBL(df)
            status_msgBL(sprintf(
              "Top picks: cutoff %s, %d picks across %d trust-gated clusters, graded on real 12-month prices.",
              df$train_cutoff_date[1], nrow(df), length(unique(df$id))))
            TRUE
          }
        }
      }, error = function(e) {
        app_dataBL(NULL); app_modeBL("current")
        status_msgBL(paste("Error:", e$message))
        TRUE
      })
      if (isTRUE(handled)) return()
      # fall through: full ranking replay below
    }

    if (is_wf) {
      # ── Backtest replay: every ranked ticker at the nearest cutoff ──
      # (rank depth is the client-side Rank filter, not a query knob)
      # Horizon selector: ranking AND grading window switch together, so the
      # question stays matched ("best over N months" graded over N months)
      hz <- suppressWarnings(as.integer(input$wf_horizon_valBL))
      if (!hz %in% c(4L, 7L, 12L, 20L, 33L)) hz <- 12L
      query <- sprintf("
        WITH sel AS (
            -- nearest USABLE cutoff: must have ranks at this horizon and
            -- id-map coverage (early cutoffs and gap quarters have neither)
            SELECT MAX(r.train_cutoff_date) AS d
            FROM validation.walk_forward_ticker_rank r
            WHERE r.train_cutoff_date <= '%s'
              AND r.fut_lag = %d
              AND EXISTS (SELECT 1 FROM validation.walk_forward_cluster_id_map m
                          WHERE m.train_cutoff_date = r.train_cutoff_date)
        )
        , mkt AS (
            -- SPY always has the latest bar; MAX(date) without the ticker
            -- prefix would full-scan (no bare date index)
            SELECT MAX(date) AS market_max FROM cdm.ingest_combined
            WHERE ticker = 'SPY'
        )
        , spy AS (
            SELECT
              (SELECT adj_close FROM cdm.ingest_combined WHERE ticker = 'SPY'
                 AND date > (SELECT d FROM sel) ORDER BY date LIMIT 1) AS spy_entry,
              (SELECT adj_close FROM cdm.ingest_combined WHERE ticker = 'SPY'
                 AND date > (SELECT d FROM sel)
                 AND date <= (SELECT d FROM sel) + INTERVAL '%d months'
                 ORDER BY date DESC LIMIT 1) AS spy_exit
        )
        -- fwd_excess_pct = REAL price return (div/split-adjusted): first bar
        -- after the cutoff -> last bar within 12 months, minus SPY over the
        -- same window. The model's forward_return column is a holdout LABEL
        -- statistic (smoothed basis, pre-cutoff anchors) - it graded BRO -30
        -- in a year it beat SPY by 9pts; never chart it as a trade outcome.
        SELECT r.train_cutoff_date, m.id, r.ticker, r.rank_within_cluster,
               r.cluster_size,
               ROUND(r.ticker_score::numeric, 4)   AS ticker_score,
               ROUND((100 * ((x.px / e.px) - (s.spy_exit / s.spy_entry)))::numeric, 1)
                                                   AS fwd_excess_pct,
               ((SELECT MAX(i.date) FROM cdm.ingest_combined i
                 WHERE i.ticker = r.ticker)
                  >= mkt.market_max - INTERVAL '10 days') AS is_active,
               mkt.market_max__METACOLS__
        FROM validation.walk_forward_ticker_rank r
        JOIN sel ON r.train_cutoff_date = sel.d
        JOIN validation.walk_forward_cluster_id_map m
          ON m.train_cutoff_date = r.train_cutoff_date
         AND m.cluster_id = r.cluster_id
        CROSS JOIN mkt
        CROSS JOIN spy s
        LEFT JOIN LATERAL (SELECT adj_close AS px FROM cdm.ingest_combined i
          WHERE i.ticker = r.ticker AND i.date > r.train_cutoff_date
          ORDER BY i.date LIMIT 1) e ON TRUE
        LEFT JOIN LATERAL (SELECT adj_close AS px FROM cdm.ingest_combined i
          WHERE i.ticker = r.ticker AND i.date > r.train_cutoff_date
            AND i.date <= r.train_cutoff_date + INTERVAL '%d months'
          ORDER BY i.date DESC LIMIT 1) x ON TRUE
        __METAJOIN__
        WHERE r.fut_lag = %d
        ORDER BY m.id, r.rank_within_cluster;",
        format(asof, "%Y-%m-%d"), hz, hz, hz, hz)
      tryCatch({
        con <- get_con(input, "BL")
        on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
        # delisting metadata is display-only enrichment; a DB without the
        # table (env dropdown on dev) degrades to plain 'delisted'. Injected
        # via gsub tokens, NOT sprintf placeholders (the stray-%% rule).
        has_meta <- isTRUE(tryCatch(dbGetQuery(con, "
          SELECT EXISTS (SELECT 1 FROM information_schema.tables
                         WHERE table_schema = 'raw'
                           AND table_name = 'ticker_metadata') AS ok")$ok[1],
          error = function(e) FALSE))
        query <- gsub("__METACOLS__", if (has_meta)
          ", tm.name AS company_name, tm.delisting_category, tm.delisted_utc::date AS delisted_date"
          else "", query, fixed = TRUE)
        query <- gsub("__METAJOIN__", if (has_meta)
          "LEFT JOIN raw.ticker_metadata tm ON tm.ticker = r.ticker"
          else "", query, fixed = TRUE)
        df <- dbGetQuery(con, query)
        if (nrow(df) == 0) {
          app_modeBL("wf"); app_dataBL(df)
          status_msgBL(sprintf("No walk-forward cutoff on or before %s.", asof))
          return()
        }
        for (col in c("id", "rank_within_cluster", "cluster_size"))
          df[[col]] <- as.integer(df[[col]])   # stay integer: used in sprintf("%d")
        df <- coerce_numeric_cols(df, c("ticker_score", "fwd_excess_pct"))
        df$delisted <- is.na(df$is_active) | !df$is_active
        df <- delist_enrich(df)
        df$horizon <- hz   # carried so chart/table labels match the grading window
        app_modeBL("wf")
        app_dataBL(df)
        status_msgBL(sprintf(
          "Backtest replay: cutoff %s, all ranked tickers per cluster, %d-month horizon, %d rows.",
          df$train_cutoff_date[1], hz, nrow(df)))
      }, error = function(e) {
        app_dataBL(NULL); app_modeBL("current")
        status_msgBL(paste("Error:", e$message))
      })
      return()
    }

    if (is_ledger) {
      # ── As-of replay: ledger snapshot graded to the latest price ──
      # NOTE: this string is a sprintf FORMAT - any literal % in it (even in
      # a SQL comment) must be %%, or sprintf dies with "too few arguments".
      # An unescaped % here ("-74%") was the 2026-07-22 crash-loop bug.
      tryCatch({
      query <- sprintf("
        WITH sel AS (
            SELECT MAX(prediction_date) AS d
            FROM monitoring.prediction_ledger
            WHERE prediction_date <= '%s'
        ),
        snap AS (
            SELECT l.*
            FROM monitoring.prediction_ledger l
            JOIN sel ON l.prediction_date = sel.d
            WHERE l.global_action = 'BUY'
        ),
        px AS (
            -- latest bar per ticker, bounded to bars since the snapshot's
            -- earliest entry (minus a cushion): the unbounded ROW_NUMBER
            -- version walked every ticker's FULL history (~2M cold heap
            -- fetches, 90s+) to keep one row each. Any last bar before
            -- entry_date means the ticker never traded post-entry anyway.
            SELECT DISTINCT ON (ticker) ticker, adj_close, date AS last_bar
            FROM cdm.ingest_combined
            WHERE ticker IN (SELECT ticker FROM snap)
              AND date >= (SELECT MIN(entry_date) FROM snap) - INTERVAL '10 days'
            ORDER BY ticker, date DESC
        ),
        -- price entries from the CURRENT series at entry_date, not the frozen
        -- ledger values: frozen prices sit on the adjustment basis of their
        -- logging day and go stale when a split re-bases history
        -- (2026-07 incident: CRWD 4:1 graded as -74 pct)
        entry_now AS (
            SELECT ic.ticker, ic.date AS entry_date, ic.adj_close AS entry_px_now
            FROM cdm.ingest_combined ic
            JOIN (SELECT DISTINCT ticker, entry_date FROM snap) k
              ON k.ticker = ic.ticker AND k.entry_date = ic.date
        ),
        spy_entry_now AS (
            SELECT ic.date AS entry_date, ic.adj_close AS spy_px_now
            FROM cdm.ingest_combined ic
            JOIN (SELECT DISTINCT entry_date FROM snap) k ON k.entry_date = ic.date
            WHERE ic.ticker = 'SPY'
        ),
        spy AS (
            SELECT adj_close, date AS market_max FROM cdm.ingest_combined
            WHERE ticker = 'SPY' ORDER BY date DESC LIMIT 1
        )
        SELECT s.prediction_date, s.ticker, s.buy_votes, s.buy_weight,
               s.best_agg_rank, s.entry_date,
               COALESCE(en.entry_px_now, s.entry_adj_close) AS entry_adj_close,
               px.adj_close AS latest_close,
               ROUND((((px.adj_close
                        / NULLIF(COALESCE(en.entry_px_now, s.entry_adj_close), 0)) - 1) * 100)::numeric, 1)
                 AS ret_since_pct,
               ROUND((((px.adj_close
                        / NULLIF(COALESCE(en.entry_px_now, s.entry_adj_close), 0))
                       - (spy.adj_close
                        / NULLIF(COALESCE(sen.spy_px_now, s.entry_spy_close), 0))) * 100)::numeric, 1)
                 AS excess_vs_spy_pct,
               (px.last_bar >= spy.market_max - INTERVAL '10 days') AS is_active__METACOLS__
        FROM snap s
        LEFT JOIN px ON px.ticker = s.ticker
        LEFT JOIN entry_now en
               ON en.ticker = s.ticker AND en.entry_date = s.entry_date
        LEFT JOIN spy_entry_now sen ON sen.entry_date = s.entry_date
        CROSS JOIN spy
        __METAJOIN__
        ORDER BY excess_vs_spy_pct DESC NULLS LAST;",
        format(asof, "%Y-%m-%d"))
        con <- get_con(input, "BL")
        on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
        # same display-only delisting enrichment as the wf branch (gsub
        # tokens, not sprintf - see the %% note above)
        has_meta <- isTRUE(tryCatch(dbGetQuery(con, "
          SELECT EXISTS (SELECT 1 FROM information_schema.tables
                         WHERE table_schema = 'raw'
                           AND table_name = 'ticker_metadata') AS ok")$ok[1],
          error = function(e) FALSE))
        query <- gsub("__METACOLS__", if (has_meta)
          ", tm.name AS company_name, tm.delisting_category, tm.delisted_utc::date AS delisted_date"
          else "", query, fixed = TRUE)
        query <- gsub("__METAJOIN__", if (has_meta)
          "LEFT JOIN raw.ticker_metadata tm ON tm.ticker = s.ticker"
          else "", query, fixed = TRUE)
        df <- dbGetQuery(con, query)
        df <- coerce_numeric_cols(df, c(
          "buy_votes", "best_agg_rank", "buy_weight", "entry_adj_close",
          "latest_close", "ret_since_pct", "excess_vs_spy_pct"))
        df$delisted <- is.na(df$is_active) | !df$is_active
        df <- delist_enrich(df)
        app_modeBL("ledger")
        app_dataBL(df)
        snap_d <- if (nrow(df) > 0) df$prediction_date[1] else asof
        status_msgBL(sprintf("Replayed snapshot %s: %d BUYs, graded to latest close.",
                             snap_d, nrow(df)))
      }, error = function(e) {
        app_dataBL(NULL); app_modeBL("current")
        status_msgBL(paste("Error:", e$message))
      })
      return()
    }

    # ── Current mode: live BUY list with payoff evidence ──
    # payoff evidence fixed to fut_lag <= 12 (matched short horizons)
    fut_cap <- 12L
    tryCatch({
      con <- get_con(input, "BL")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      # evidence_status ships with the Phase A dbt deploy; the query adapts so
      # the dashboard works BEFORE and AFTER the column lands on prod
      has_ev <- tryCatch(nrow(dbGetQuery(con,
        "SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'serving'
            AND table_name = 'return_cluster_ticker_global_action_current'
            AND column_name = 'evidence_status'")) > 0,
        error = function(e) FALSE)
      ev_buys <- if (has_ev) ", evidence_status, ticker_months_count" else ""
      ev_sel  <- if (has_ev) "b.evidence_status, b.ticker_months_count," else
                 "'mature'::text AS evidence_status, NULL::integer AS ticker_months_count,"
      # Label-drift fix 2026-07-23: evidence tables (IC, payoff, credibility,
      # rank bins) are keyed in EVIDENCE-space ids; serving now exposes the
      # membership-derived evidence_id. Join evidence through it when present,
      # fall back to the raw id before the rebuild lands. Injected via gsub
      # tokens, NOT sprintf placeholders (a stray % in sprintf killed the app
      # on 2026-07-22 - keep the sprintf surface minimal).
      has_eid <- tryCatch(nrow(dbGetQuery(con,
        "SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'serving'
            AND table_name = 'return_cluster_ticker_global_action_current'
            AND column_name = 'evidence_id'")) > 0,
        error = function(e) FALSE)
      eid_buys <- if (has_eid)
        ", evidence_id, cluster_untradeable_reason, cluster_failed_checks" else ""
      eid_sel  <- if (has_eid)
        "b.evidence_id, b.cluster_untradeable_reason, b.cluster_failed_checks," else
        paste0("NULL::integer AS evidence_id, NULL::text AS cluster_untradeable_reason, ",
               "NULL::integer AS cluster_failed_checks,")
      eid_key  <- if (has_eid) "COALESCE(b.evidence_id, b.id)" else "b.id"
      query <- sprintf("
      WITH buys AS (
          -- ALL actions loaded (not just BUY): the rankall view shows every
          -- ranked ticker per id; the BUY-only views subset in the renderer.
          SELECT ticker, id, archetype, global_action, buy_weight, buy_votes,
                 agg_rank__EIDBUYS__%s
          FROM serving.return_cluster_ticker_global_action_current
      ),
      wfbin AS (
          -- Rank-audit fix 2026-07-23: the displayed bin is the ticker's slot
          -- in the WALK-FORWARD ranking at the latest cutoff - the ranking
          -- the (id, pctile_bin) win-rate evidence was actually built on.
          -- (Previously the bin came from serving agg_rank, a different
          -- formula: pricing one ranking with the other's evidence.)
          -- Joined by TICKER, carrying the walk-forward's own cluster (eid):
          -- memberships drift between the WF build and today's labels, so
          -- forcing the current cluster to match would orphan most tickers
          -- (measured: only 57 of 361 BUYs matched). Bin AND win-rate both
          -- cite the cluster the ticker actually occupied in the WF build.
          -- ticker_score = 0 rows carry no rank evidence: excluded, so those
          -- tickers get NULL bin -> 'no evidence' downstream. NTILE tiebreak
          -- mirrors the rebuilt walk_forward_pctile_summary exactly.
          SELECT m.id AS eid, r.ticker,
                 NTILE(20) OVER (PARTITION BY m.id
                   ORDER BY r.ticker_score DESC, r.n_weighted DESC, r.ticker
                 )::int AS wf_bin
          FROM validation.walk_forward_ticker_rank r
          JOIN validation.walk_forward_cluster_id_map m
            ON m.train_cutoff_date = r.train_cutoff_date
           AND m.cluster_id       = r.cluster_id
          WHERE r.fut_lag = 12
            AND r.ticker_score IS NOT NULL AND r.ticker_score <> 0
            AND r.train_cutoff_date = (SELECT MAX(train_cutoff_date)
                  FROM validation.walk_forward_ticker_rank WHERE fut_lag = 12)
      ),
      cells AS (
          SELECT p.ticker, p.id, __EIDKEY__ AS eid, p.past_lag, p.fut_lag, p.bucket
          FROM serving.return_cluster_ticker_pair_current p
          JOIN buys b ON b.ticker = p.ticker AND b.id = p.id
          WHERE p.recommendation IN ('STRONG_PICK','BUY','OUTLIER_BUY')
            AND p.fut_lag <= %d
      ),
      payoff AS (
          SELECT c.ticker, c.id,
                 COUNT(*) AS n_buy_cells,
                 SUM(pb.expectancy * pb.n_holdout)
                   / NULLIF(SUM(pb.n_holdout), 0) AS wtd_expectancy,
                 SUM(pb.win_pct * pb.n_holdout)
                   / NULLIF(SUM(pb.n_holdout), 0) AS wtd_win_pct,
                 SUM(pb.n_holdout) AS total_holdout
          FROM cells c
          JOIN validation.return_cluster_payoff_backtest pb
            ON pb.id = c.eid AND pb.past_lag = c.past_lag AND pb.fut_lag = c.fut_lag
          GROUP BY c.ticker, c.id
      ),
      cred AS (
          SELECT c.ticker, c.id,
                 AVG(cc.credibility_weight) AS avg_cred_weight,
                 COUNT(*) FILTER (WHERE cc.tier IN ('high','medium')) AS n_trusted_cells
          FROM cells c
          -- 2026-07-24 grain fix: cell_credibility no longer carries
          -- vol_bucket_num; join on the cell shape alone.
          LEFT JOIN validation.cell_credibility cc
            ON cc.id = c.eid AND cc.past_lag = c.past_lag
           AND cc.fut_lag = c.fut_lag AND cc.bucket = c.bucket
          GROUP BY c.ticker, c.id
      )
      SELECT b.ticker, b.id, b.archetype, b.global_action, b.buy_weight, b.buy_votes, b.agg_rank,
             wb.wf_bin AS rank_bin,
             %s
             __EIDSEL__
             p.n_buy_cells,
             ROUND(p.wtd_expectancy::numeric, 3) AS wtd_expectancy,
             ROUND(p.wtd_win_pct::numeric, 1)    AS wtd_win_pct,
             p.total_holdout,
             ROUND(cr.avg_cred_weight::numeric, 3) AS avg_cred_weight,
             cr.n_trusted_cells,
             ROUND(ic.weighted_ic::numeric, 3) AS cluster_ic_12,
             ROUND(ps.hit_rate::numeric * 100, 1) AS bin_win_pct,
             ROUND(ps.mean_return::numeric, 1)    AS bin_mean_ret,
             ps.n_obs AS bin_n
      FROM buys b
      LEFT JOIN payoff p USING (ticker, id)
      LEFT JOIN cred cr USING (ticker, id)
      LEFT JOIN wfbin wb
        ON wb.ticker = b.ticker
      LEFT JOIN validation.walk_forward_id_ic ic
        ON ic.id = __EIDKEY__ AND ic.fut_lag = 12
      LEFT JOIN validation.walk_forward_pctile_summary ps
        ON ps.id = wb.eid AND ps.fut_lag = 12 AND ps.pctile_bin = wb.wf_bin
      ORDER BY p.wtd_expectancy DESC NULLS LAST, b.buy_weight DESC;",
        ev_buys, fut_cap, ev_sel)
      query <- gsub("__EIDBUYS__", eid_buys, query, fixed = TRUE)
      query <- gsub("__EIDSEL__",  eid_sel,  query, fixed = TRUE)
      query <- gsub("__EIDKEY__",  eid_key,  query, fixed = TRUE)
      df <- dbGetQuery(con, query)
      df$id       <- as.integer(df$id)
      df$agg_rank <- as.integer(df$agg_rank)
      # COUNT/SUM columns arrive as integer64 (see the RS n_obs note above)
      df <- coerce_numeric_cols(df, c(
        "buy_votes", "n_buy_cells", "total_holdout", "n_trusted_cells",
        "buy_weight", "wtd_expectancy", "wtd_win_pct",
        "avg_cred_weight", "cluster_ic_12",
        "rank_bin", "bin_win_pct", "bin_mean_ret", "bin_n",
        "evidence_id", "cluster_failed_checks"))
      app_modeBL("current")
      app_dataBL(df)
      status_msgBL(sprintf(
        "Loaded %d ranked tickers (%d BUY).",
        nrow(df), sum(df$global_action == "BUY", na.rm = TRUE)))
    }, error = function(e) {
      app_dataBL(NULL); app_modeBL("current")
      status_msgBL(paste("Error:", e$message))
    })
  })

  # Shared id/rank filter, applied identically by chart + table (live, no
  # re-Generate). id filter: wf replay + current mode (ledger rows carry no
  # id). rank range: keep ranks in [lo, hi] on rank_within_cluster (wf) /
  # agg_rank (current) - e.g. 5-20 = skip the head, take the mid-block.
  bl_apply_filters <- function(df) {
    idv <- input$flt_id_valBL
    if (!is.null(idv) && !idv %in% c("", "ALL") && "id" %in% names(df))
      df <- df[!is.na(df$id) & df$id == as.integer(idv), , drop = FALSE]
    rr <- input$flt_rank_rangeBL
    if (!is.null(rr) && length(rr) == 2) {
      col <- if ("rank_within_cluster" %in% names(df)) "rank_within_cluster"
             else if ("agg_rank" %in% names(df)) "agg_rank" else NULL
      if (!is.null(col))
        df <- df[!is.na(df[[col]]) & df[[col]] >= rr[1] & df[[col]] <= rr[2],
                 , drop = FALSE]
    }
    # replay depth preset: top X% of EACH cluster (per-row cluster_size), the
    # backtest analogue of the current-date Shortlist; ANDs with the slider
    dp <- input$wf_depth_valBL
    if (!is.null(dp) && dp %in% c("p5", "p10", "p20") &&
        all(c("rank_within_cluster", "cluster_size") %in% names(df))) {
      frac <- c(p5 = 0.05, p10 = 0.10, p20 = 0.20)[[dp]]
      df <- df[!is.na(df$rank_within_cluster) &
               df$rank_within_cluster <= pmax(1, ceiling(frac * df$cluster_size)),
               , drop = FALSE]
    }
    df
  }

  # Chart container sized to the number of bars the chart ACTUALLY draws.
  # The renderer reports the count after mode/view/filters/trim - sizing
  # from the raw loaded count made a 21-bar shortlist stretch over 2800px
  # (plotly fills whatever height it gets).
  chart_rowsBL <- reactiveVal(0)
  set_chart_rowsBL <- function(n) {
    if (!identical(chart_rowsBL(), n)) chart_rowsBL(n)
  }
  output$buyChartContainerBL <- renderUI({
    n <- chart_rowsBL()
    h <- max(340, min(10000, 16 * n + 160))
    plotlyOutput("buyScatterBL", height = paste0(h, "px"))
  })

  # Rank slider ALWAYS snaps to the full span of whatever the id filter
  # selects (id 9 in 2020 = 27 ranks, in 2022 = 415): max and both handles
  # reset to 1..max on every id switch or data load, so the top handle never
  # lingers at a stale ceiling. A hand-narrowed window (e.g. 5-20) lives only
  # until the next id switch / Generate. Deliberately NOT reactive on the
  # slider itself, or user narrowing would instantly snap back. Ledger rows
  # carry no rank column, so the slider is left alone there (the filter
  # no-ops anyway).
  observeEvent(list(input$flt_id_valBL, app_dataBL()), {
    df <- app_dataBL()
    if (is.null(df) || nrow(df) == 0) return()
    col <- if ("rank_within_cluster" %in% names(df)) "rank_within_cluster"
           else if ("agg_rank" %in% names(df)) "agg_rank" else return()
    idv <- input$flt_id_valBL
    if (!is.null(idv) && !idv %in% c("", "ALL") && "id" %in% names(df))
      df <- df[!is.na(df$id) & df$id == as.integer(idv), , drop = FALSE]
    if (nrow(df) == 0) return()
    mx <- suppressWarnings(max(df[[col]], na.rm = TRUE))
    if (!is.finite(mx) || mx < 1) return()
    updateSliderInput(session, "flt_rank_rangeBL", max = mx, value = c(1, mx))
  })

  # Cluster dropdown mirrors the LOADED data: only ids that actually have
  # rows, each labeled with its row count, so an all-delisted cluster (id 1
  # today) can't be selected into a confusing empty chart. A selection that
  # vanishes falls back to All. Ledger rows carry no id - leave it alone.
  observeEvent(app_dataBL(), {
    df <- app_dataBL()
    if (is.null(df) || nrow(df) == 0 || !"id" %in% names(df)) return()
    tab <- table(df$id)
    ids <- sort(as.integer(names(tab)))
    if (length(ids) == 0) return()
    labs <- sprintf("%d  (%d rows)", ids, as.integer(tab[as.character(ids)]))
    sel <- isolate(input$flt_id_valBL)
    if (is.null(sel) || !(sel %in% c("ALL", as.character(ids)))) sel <- "ALL"
    updateSelectInput(session, "flt_id_valBL",
                      choices = c("All" = "ALL", setNames(as.character(ids), labs)),
                      selected = sel)
  })

  # Selection stats: mean/median of the metric over ALL graded rows in the
  # current mode/view/id/rank selection (not just drawn bars), as stat chips
  # above the chart. The chart renderer stashes values here (same pattern as
  # chart_rowsBL); NULL = nothing graded, render no chips.
  sel_statsBL <- reactiveVal(NULL)
  set_sel_statsBL <- function(vals, label) {
    v <- vals[!is.na(vals)]
    if (length(v) == 0) { sel_statsBL(NULL); return(invisible(NULL)) }
    sel_statsBL(list(label = label, mean = mean(v), med = stats::median(v),
                     n = length(v), pos = 100 * mean(v > 0)))
  }
  output$selStatsBL <- renderUI({
    s <- sel_statsBL()
    if (is.null(s)) return(NULL)
    chip <- function(lab, val, col = "#f8fafc") {
      div(style = paste0("background:#1e293b;border:1px solid rgba(255,255,255,0.12);",
                         "border-radius:8px;padding:6px 16px;"),
          div(lab, style = paste0("color:#94a3b8;font-size:0.65rem;",
                                  "letter-spacing:0.05em;text-transform:uppercase;")),
          div(val, style = sprintf("color:%s;font-size:1.05rem;font-weight:600;", col)))
    }
    sign_col <- function(x) if (x >= 0) "#10b981" else "#dc2626"
    div(style = "display:flex;gap:10px;flex-wrap:wrap;margin-bottom:0.75rem;",
        chip(s$label, sprintf("%+.1f", s$mean), sign_col(s$mean)),
        chip("median", sprintf("%+.1f", s$med), sign_col(s$med)),
        chip("graded picks", sprintf("%d", s$n)),
        chip("% positive", sprintf("%.0f%%", s$pos)))
  })

  output$buyScatterBL <- renderPlotly({
    req(app_dataBL())
    sel_statsBL(NULL)   # cleared until a branch computes this selection
    df <- bl_apply_filters(app_dataBL())
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    if (app_modeBL() %in% c("wf", "ledger")) {
      # Replay modes: one bar per pick, sorted by realized excess vs SPY.
      # Green = beat SPY, red = lagged it. Reads top-to-bottom: best to worst.
      trunc_note <- NULL   # set when the horizon outruns the price data
      if (app_modeBL() == "wf") {
        hz <- if ("horizon" %in% names(df)) df$horizon[1] else 12L
        df$excess <- df$fwd_excess_pct
        # e.g. 33mo from the 2024-12-31 cutoff ends in 2027: rows are graded
        # entry -> last available bar, a partial in-progress grade
        if ("market_max" %in% names(df)) {
          co <- as.Date(df$train_cutoff_date[1])
          mm <- as.Date(df$market_max[1])
          full_end <- seq(co, by = sprintf("%d months", hz), length.out = 2)[2]
          if (!is.na(full_end) && !is.na(mm) && full_end > mm)
            trunc_note <- sprintf("~%d of %dmo elapsed",
              floor(as.numeric(difftime(mm, co, units = "days")) / 30.44), hz)
        }
        df$hover <- sprintf(
          "%s (id %d)<br>cutoff %s | rank %d of %d | score %.4f<br>realized %dmo excess vs SPY %+.1f%%",
          df$ticker, df$id, df$train_cutoff_date,
          df$rank_within_cluster, df$cluster_size,
          ifelse(is.na(df$ticker_score), 0, df$ticker_score),
          hz, ifelse(is.na(df$fwd_excess_pct), 0, df$fwd_excess_pct))
        if ("view_kind" %in% names(df) && identical(df$view_kind[1], "picks") &&
            "trailing_ic" %in% names(df))
          df$hover <- paste0(df$hover, sprintf(
            "<br>cluster trust IC by then %.3f",
            ifelse(is.na(df$trailing_ic), 0, df$trailing_ic)))
        x_title <- if (is.null(trunc_note))
          sprintf("Realized %dmo excess return vs SPY (%%)", hz)
        else sprintf("Excess return vs SPY so far (%%; %s)", trunc_note)
      } else {
        hz <- NA_integer_
        df$excess <- df$excess_vs_spy_pct
        df$hover <- sprintf(
          "%s<br>snapshot %s | entry %s @ %.2f -> latest %.2f<br>ret %.1f%% | vs SPY %+.1f%% | buy weight %.2f",
          df$ticker, df$prediction_date, df$entry_date, df$entry_adj_close,
          ifelse(is.na(df$latest_close), 0, df$latest_close),
          ifelse(is.na(df$ret_since_pct), 0, df$ret_since_pct),
          ifelse(is.na(df$excess_vs_spy_pct), 0, df$excess_vs_spy_pct),
          df$buy_weight)
        x_title <- "Return since entry, relative to SPY (%)"
      }
      # delisted rows carry the exit story on the bar hover too (the dot has
      # it as well, but bars are the bigger hover target)
      if (all(c("delisted", "del_info") %in% names(df)))
        df$hover <- paste0(df$hover,
          ifelse(df$delisted & !is.na(df$del_info) & nzchar(df$del_info),
                 paste0("<br>delisted · ", df$del_info), ""))
      # selection average BEFORE any head-trim: nets wins/losses over every
      # graded row in the current mode/id/rank selection
      set_sel_statsBL(
        df$excess,
        if (app_modeBL() != "wf") "avg excess vs SPY since entry (pp)"
        else if (is.null(trunc_note)) sprintf("avg %dmo excess vs SPY (pp)", hz)
        else sprintf("avg excess vs SPY so far (pp; %s)", trunc_note))
      # Replay shows the PREDICTION as it stood: always grouped by cluster
      # then model rank (r1 = top pick), bar = realized outcome. Sorting by
      # outcome read as a results chart (the model ranked LXP 508/548 yet it
      # topped the chart); best/worst hunting lives in the sortable table.
      rank_mode <- app_modeBL() == "wf" && "rank_within_cluster" %in% names(df)
      if (rank_mode) {
        df <- df[order(df$id, df$rank_within_cluster), ]   # display top->bottom
        df$ylab <- sprintf("id%d r%d | %s", df$id, df$rank_within_cluster, df$ticker)
        head_word <- if ("view_kind" %in% names(df) &&
                         identical(df$view_kind[1], "picks"))
          "Trust-gated top picks" else "Prediction order"
        chart_title <- list(
          text = sprintf("%s: %d ranked picks (r1 = model's top pick; bar = realized %dmo vs SPY)", head_word, nrow(df), hz),
          font = list(color = "#94a3b8", size = 12))
        if (nrow(df) > 180) {
          n_total <- nrow(df)
          df <- head(df, 180)
          chart_title <- list(
            text = sprintf("Prediction order: first 180 of %d ranked picks (narrow with the id filter)", n_total),
            font = list(color = "#94a3b8", size = 12))
        }
        # ids 13+ are SHORT-side clusters: a red bar there is the model being
        # right, so flag it or green reads as a win
        if (all(df$id > 12L))
          chart_title$text <- paste0(chart_title$text,
            " | SHORT-side cluster: model expected these to LAG SPY")
        if (!is.null(trunc_note))
          chart_title$text <- paste0(chart_title$text,
            " | horizon outruns data: ", trunc_note)
        cat_arr <- rev(df$ylab)   # categoryarray runs bottom->top
      } else {
        # plotly DROPS NA-x bars but not their marker colors, misaligning
        # every color after the first NA - ungraded rows stay table-only
        n_all <- nrow(df)
        df <- df[!is.na(df$excess), , drop = FALSE]
        if (nrow(df) == 0)
          return(empty_plot("No graded picks to chart - see the table."))
        df <- df[order(df$excess, decreasing = FALSE), ]
        df$ylab <- df$ticker
        chart_title <- list(
          text = sprintf("All %d graded picks (of %d rows; rest in table)", nrow(df), n_all),
          font = list(color = "#94a3b8", size = 12))
        if (nrow(df) > 180) {
          n_gr <- nrow(df)
          df <- rbind(head(df, 90), tail(df, 90))
          chart_title <- list(
            text = sprintf("Worst 90 and best 90 of %d graded picks (narrow with the id/rank filters; full list in table)", n_gr),
            font = list(color = "#94a3b8", size = 12))
        }
        cat_arr <- df$ylab
      }
      # rank mode keeps ungraded rows so the ranking stays complete: draw
      # them at 0, grey, instead of letting plotly drop them (color shift)
      df$xplot <- ifelse(is.na(df$excess), 0, df$excess)
      set_chart_rowsBL(nrow(df))
      p <- plot_ly(df, x = ~xplot, y = ~ylab, type = "bar", orientation = "h",
                   marker = list(color = ifelse(is.na(df$excess), '#64748b',
                                         ifelse(df$excess >= 0, '#10b981', '#dc2626'))),
                   customdata = ~hover, name = "excess vs SPY",
                   hovertemplate = "%{customdata}<extra></extra>")
      # dot at the bar tip = ticker no longer trades (no price bar within 10
      # days of the latest market date); dot color = HOW it left the market
      # (DELIST_CLASSES), hover = reason · company · date
      del <- df[df$delisted, , drop = FALSE]
      if (nrow(del) > 0) {
        if (!"del_class" %in% names(del)) del$del_class <- ""
        if (!"del_info"  %in% names(del)) del$del_info  <- ""
        del$del_class[is.na(del$del_class) | !nzchar(del$del_class)] <-
          "delisted: uncategorized"
        del$del_info[is.na(del$del_info) | !nzchar(del$del_info)] <-
          "no category on file"
        for (cl in intersect(names(DELIST_CLASSES), unique(del$del_class))) {
          d1 <- del[del$del_class == cl, , drop = FALSE]
          p <- p %>% add_markers(
            data = d1, x = ~xplot, y = ~ylab,
            marker = list(color = DELIST_CLASSES[[cl]], size = 8,
                          line = list(color = "#f8fafc", width = 1)),
            name = cl, customdata = ~del_info,
            hovertemplate = "%{y}: delisted<br>%{customdata}<extra></extra>")
        }
      }
      return(
        p %>%
          layout(
            paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
            legend = list(font = list(color = "#94a3b8")),
            showlegend = nrow(del) > 0,
            title = chart_title,
            shapes = c(
              list(list(type = "line", x0 = 0, x1 = 0, yref = "paper", y0 = 0, y1 = 1,
                        line = list(color = "rgba(255,255,255,0.4)"))),
              if (rank_mode) rank_sep_shapes(df$id) else list()),
            xaxis = list(title = x_title,
                         color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
            yaxis = list(title = "", type = "category",
                         categoryorder = "array", categoryarray = cat_arr,
                         # pin the range to the category span: stray phantom
                         # slots otherwise render as dead space above the top
                         range = c(-0.5, nrow(df) - 0.5),
                         tickfont = list(size = 9),
                         color = "#94a3b8", gridcolor = "rgba(255,255,255,0.05)"),
            margin = list(l = if (rank_mode) 120 else 70, r = 30, b = 50,
                          t = if (is.null(chart_title)) 10 else 40))
      )
    }
    # Current mode. Three altitudes, switchable live (no re-Generate):
    #   shortlist - top 10% by expectancy among names with win% > 50 (decision view)
    #   all       - every BUY (chart caps at 60 bars; table has everything)
    #   cluster   - one bar per id: the cohort's median expectancy
    view <- input$view_valBL
    if (is.null(view) || !view %in% c("shortlist", "all", "rankall")) view <- "all"

    if (view == "rankall") {
      # Every ranked ticker per id - BUY gate and evidence filters ignored.
      # Grouped id -> agg_rank with separator lines; bar color = action.
      df <- df[order(df$id, df$agg_rank), ]
      n_total <- nrow(df)
      set_sel_statsBL(df$wtd_expectancy, "avg holdout expectancy (pp/trade)")
      n_ids <- length(unique(df$id))
      # With All selected the ladders draw STACKED (id 2 block, then 3, ...),
      # separator lines between clusters (Kevin 2026-07-23: an empty chart on
      # All read as broken). Rank numbers still restart per cluster - use the
      # rank slider (e.g. 1-20) to see every cluster's head side by side.
      chart_text <- if (n_ids > 1) sprintf(
        "%d clusters stacked by id, r1 -> last each (green BUY / grey SKIP / red SELL)",
        n_ids)
      else sprintf(
        "Cluster id %d: all %d ranks, r1 -> last (green BUY / grey SKIP / red SELL)",
        df$id[1], n_total)
      # 600 covers the biggest single cluster end to end
      if (n_total > 600) {
        df <- head(df, 600)
        chart_text <- sprintf(
          "First 600 of %d rows - tighten the rank slider or pick a cluster",
          n_total)
      }
      # Sparse marking is DB-driven (evidence_status, Phase A): a sparse SKIP
      # means "cannot be evidenced yet", not "evidence rejected". Bar fill
      # stays action-colored; sparse rows get an amber OUTLINE + label tag,
      # so a sparse BUY reads green-but-flagged, not quarantined.
      df$sparse <- !is.na(df$evidence_status) & df$evidence_status != "mature"
      base_tag <- ifelse(df$global_action == "BUY", "", df$global_action)
      tag <- ifelse(df$sparse & base_tag == "", "SPARSE",
             ifelse(df$sparse, paste0(base_tag, "+SPARSE"), base_tag))
      df$ylab <- sprintf("id%d r%d | %s%s", df$id, df$agg_rank, df$ticker,
                         ifelse(tag == "", "", paste0(" [", tag, "]")))
      df$xval <- ifelse(is.na(df$wtd_expectancy), 0, df$wtd_expectancy)
      sr <- severity_ring(df)
      df$hover <- sprintf(
        "%s (id %d, %s)<br>action %s | evidence %s | tenure %s | rank %d | buy weight %.2f<br>holdout expectancy %s | win %s<br>checks failed %s%s",
        df$ticker, df$id, df$archetype, df$global_action,
        ifelse(is.na(df$evidence_status), "n/a", df$evidence_status),
        ifelse(is.na(df$ticker_months_count), "n/a",
               sprintf("%dmo", as.integer(df$ticker_months_count))),
        df$agg_rank,
        ifelse(is.na(df$buy_weight), 0, df$buy_weight),
        ifelse(is.na(df$wtd_expectancy), "n/a", sprintf("%.3f", df$wtd_expectancy)),
        ifelse(is.na(df$wtd_win_pct), "n/a", sprintf("%.1f%%", df$wtd_win_pct)),
        ifelse(is.na(sr$checks), "n/a", as.character(sr$checks)),
        ifelse(is.na(df$cluster_untradeable_reason) | df$cluster_untradeable_reason == "",
               "", paste0(" (", df$cluster_untradeable_reason, ")")))
      set_chart_rowsBL(nrow(df))
      act_col <- ifelse(df$global_action == "BUY", "#10b981",
                 ifelse(df$global_action == "SELL", "#dc2626", "#64748b"))
      ln_col <- sr$col
      ln_w   <- sr$w
      chart_text <- if (any(!is.na(sr$checks))) paste0(
        chart_text, " / ring = failed checks: green 0, yellow 1, orange 2, red 3+")
      else if (any(df$sparse)) paste0(
        chart_text, " / amber outline = sparse evidence (young cluster or ticker)")
      else chart_text
      return(
        plot_ly(df, x = ~xval, y = ~ylab, type = "bar", orientation = "h",
                marker = list(color = act_col,
                              line = list(color = ln_col, width = ln_w)),
                customdata = ~hover,
                hovertemplate = "%{customdata}<extra></extra>") %>%
          add_markers(x = ~xval, y = ~ylab,
                      marker = list(color = act_col, size = 5,
                                    line = list(color = ln_col, width = ln_w)),
                      customdata = ~hover, showlegend = FALSE,
                      hovertemplate = "%{customdata}<extra></extra>") %>%
          layout(
            paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
            showlegend = FALSE,
            title = list(text = chart_text, font = list(color = "#94a3b8", size = 12)),
            shapes = c(
              list(list(type = "line", x0 = 0, x1 = 0, yref = "paper", y0 = 0, y1 = 1,
                        line = list(color = "rgba(255,255,255,0.4)"))),
              rank_sep_shapes(df$id)),
            xaxis = list(title = "Payoff-weighted expectancy (%, 0 = no BUY-cell evidence)",
                         color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
            yaxis = list(title = "", type = "category",
                         categoryorder = "array", categoryarray = rev(df$ylab),
                         # pin the range to the category span: stray phantom
                         # slots otherwise render as dead space above the top
                         range = c(-0.5, nrow(df) - 0.5),
                         tickfont = list(size = 9),
                         color = "#94a3b8", gridcolor = "rgba(255,255,255,0.05)"),
            margin = list(l = 150, r = 30, b = 50, t = 40))
      )
    }

    # BUY-gated views: subset to gated rows passing the evidence filters.
    # The "all" view ALSO keeps the active members of young (sparse) clusters
    # - SKIPs drawn at 0 with the amber ring - so the acquisition-watch pool
    # stays visible inside the existing view instead of its own dropdown entry.
    if ("global_action" %in% names(df)) {
      keep <- df$global_action == "BUY"
      if (view == "all") {
        # category toggles: regular = gated BUYs, sparse = young-id watchlist
        cats <- input$show_catBL
        if (is.null(cats)) cats <- c("regular", "sparse")
        young_row <- !is.na(df$evidence_status) &
                     df$evidence_status %in% c("sparse_cluster", "sparse_both") &
                     df$id <= 12   # ids 13+ = short-side, not buy-watch material
        keep <- (("regular" %in% cats) & df$global_action == "BUY") |
                (("sparse" %in% cats) & young_row)
      }
      df <- df[keep, , drop = FALSE]
    }
    if (nrow(df) == 0) return(empty_plot(
      "Nothing to chart - check the Regular/Sparse toggles and filters."))

    if (view == "shortlist") {
      # decision view = rank bins that actually win: the cluster's ranks cut
      # into 20 bins of 5 pct, kept only when the bin's realized walk-forward
      # win rate is >= 55 pct on >= 100 graded observations (Kevin 2026-07-22:
      # likelihood of winning outranks payoff size; a 60 bar would delete
      # whole clusters - id 6's best bin wins 59)
      df <- df[!is.na(df$bin_win_pct) & df$bin_win_pct >= 55 &
               !is.na(df$bin_n) & df$bin_n >= 100, ]
      if (nrow(df) == 0) return(empty_plot("No BUYs sit in rank bins with win >= 55% (n >= 100)."))
      df <- df[order(-df$bin_win_pct, df$agg_rank), ]
      set_sel_statsBL(df$wtd_expectancy, "avg holdout expectancy (pp/trade)")
      chart_text <- sprintf("Shortlist: %d BUYs in rank bins winning >= 55%% of holdout trades", nrow(df))
    } else {
      # BUYs need payoff evidence to chart (NA-x bars would silently drop and
      # shift every marker color); young-cluster non-BUYs stay regardless,
      # as a watchlist block below the BUY ladder showing their (thin-sample)
      # cell expectancy at real length - grey fill + amber ring marks them.
      is_buy <- df$global_action == "BUY"
      buys  <- df[is_buy & !is.na(df$wtd_expectancy), , drop = FALSE]
      young <- df[!is_buy, , drop = FALSE]
      n_total <- sum(is_buy)
      if (nrow(buys) == 0 && nrow(young) == 0)
        return(empty_plot("No BUYs with payoff evidence to chart - see the table for all BUYs."))
      # top-400 cut keeps the best-WIN-RATE bins, not the biggest payoffs;
      # unmeasured bins (n < 100) rank below any measured one
      bw_cut <- ifelse(!is.na(buys$bin_win_pct) & !is.na(buys$bin_n) & buys$bin_n >= 100,
                       buys$bin_win_pct, -Inf)
      buys <- buys[order(-bw_cut, buys$agg_rank), ]
      n_ev <- nrow(buys)
      set_sel_statsBL(buys$wtd_expectancy, "avg holdout expectancy (pp/trade)")
      if (n_ev > 400) buys <- head(buys, 400)
      young <- young[order(young$id, young$agg_rank), ]
      df <- rbind(buys, young)
      chart_text <- sprintf(
        "%d of %d BUYs charted, ordered by rank-bin win rate%s%s",
        nrow(buys), n_total,
        if (n_ev > 400) " - top 400, narrow with filters" else "",
        if (nrow(young) > 0) sprintf(
          " + %d young-cluster names (amber ring = thin evidence, mostly SKIP)",
          nrow(young)) else "")
    }
    set_chart_rowsBL(nrow(df))
    # sparse rows STAY in the BUY views (marked, not hidden): amber outline +
    # label tag; evidence_status comes from the DB (Phase A). The "all" view's
    # own title already explains the amber ring - no suffix there.
    df$sparse <- !is.na(df$evidence_status) & df$evidence_status != "mature"
    # severity ring stored AS COLUMNS so it survives the re-sort below
    sr <- severity_ring(df)
    df$ring_checks <- sr$checks; df$ring_col <- sr$col; df$ring_w <- sr$w
    if (view != "all" && any(df$sparse) && all(is.na(df$ring_checks)))
      chart_text <- paste0(chart_text, " / amber outline = sparse evidence")
    df$hover <- sprintf(
      "%s (id %d, %s)<br>evidence %s | tenure %s<br>rank bin %s | bin win %s<br>expectancy %.3f | win %.1f%%<br>cells %d | holdout %d | cred %.3f<br>buy weight %.2f | cluster IC(12) %.3f<br>checks failed %s%s",
      df$ticker, df$id, df$archetype,
      ifelse(is.na(df$evidence_status), "n/a", df$evidence_status),
      ifelse(is.na(df$ticker_months_count), "n/a",
             sprintf("%dmo", as.integer(df$ticker_months_count))),
      ifelse(is.na(df$rank_bin), "no evidence",
             sprintf("%d/20 (top %d%%)", as.integer(df$rank_bin),
                     as.integer(df$rank_bin) * 5L)),
      ifelse(is.na(df$bin_win_pct), "unmeasured",
             sprintf("%.0f%% of %d obs%s", df$bin_win_pct, as.integer(df$bin_n),
                     ifelse(!is.na(df$bin_n) & df$bin_n < 100, " (thin)", ""))),
      df$wtd_expectancy, df$wtd_win_pct,
      as.integer(df$n_buy_cells), as.integer(df$total_holdout),
      ifelse(is.na(df$avg_cred_weight), 0, df$avg_cred_weight),
      df$buy_weight, ifelse(is.na(df$cluster_ic_12), 0, df$cluster_ic_12),
      ifelse(is.na(df$ring_checks), "n/a", as.character(df$ring_checks)),
      ifelse(is.na(df$cluster_untradeable_reason) | df$cluster_untradeable_reason == "",
             "", paste0(" (", df$cluster_untradeable_reason, ")")))
    # ONE global sort, ascending (top of chart = best). Primary key = the
    # rank bin's realized win rate (Kevin 2026-07-22: likelihood of winning
    # outranks payoff size; bars still SHOW expectancy, order carries win).
    # Ties inside a bin break by model rank (r1 above r23). Rows without a
    # measured bin (n < 100: the young/sparse ids) sink below every graded
    # BUY as a block, ordered by their thin-sample expectancy as before.
    bw_ok <- !is.na(df$bin_win_pct) & !is.na(df$bin_n) & df$bin_n >= 100
    exp_s <- ifelse(is.na(df$wtd_expectancy), -999,
                    pmin(pmax(df$wtd_expectancy, -99), 99))
    sort_key <- ifelse(bw_ok,
                       df$bin_win_pct + (1000 - df$agg_rank) / 1e6,
                       exp_s / 1000 - 1)
    df <- df[order(sort_key), ]
    # same id/rank label format as the rankall view
    df$ylab <- sprintf("id%d r%d | %s%s", df$id, df$agg_rank, df$ticker,
                       ifelse(df$sparse & df$global_action != "BUY", " [SKIP+SPARSE]",
                       ifelse(df$sparse, " [SPARSE]", "")))
    # the marker dot keeps a zero-length bar visible as an amber-ringed point
    df$xplot <- ifelse(is.na(df$wtd_expectancy), 0, df$wtd_expectancy)
    # one outlier (e.g. WHR at -11) otherwise stretches the axis and crushes
    # every other bar into a sliver: clip to the 2-98 pctile span (zero kept
    # in view); clipped bars keep their true value in the hover
    qs <- stats::quantile(df$xplot, c(0.02, 0.98), na.rm = TRUE, names = FALSE)
    pad <- max(0.15, 0.1 * (qs[2] - qs[1]))
    x_lo <- min(qs[1] - pad, -pad); x_hi <- max(qs[2] + pad, pad)
    n_clip <- sum(df$xplot < x_lo | df$xplot > x_hi)
    x_title <- paste0("Payoff-weighted expectancy (mean holdout trade return, %)",
                      if (n_clip > 0) sprintf(" - axis clipped, %d bar%s extend beyond (hover for value)",
                                              n_clip, if (n_clip == 1) "" else "s") else "")
    fill_col <- ifelse(df$global_action != "BUY" | is.na(df$wtd_expectancy), '#64748b',
                ifelse(df$wtd_expectancy >= 0, '#10b981', '#dc2626'))
    ring_col <- df$ring_col
    ring_w   <- df$ring_w
    if (any(!is.na(df$ring_checks))) chart_text <- paste0(
      chart_text, " / ring = failed checks: green 0, yellow 1, orange 2, red 3+")
    plot_ly(df, x = ~xplot, y = ~ylab, type = "bar", orientation = "h",
            marker = list(color = fill_col,
                          line = list(color = ring_col, width = ring_w)),
            customdata = ~hover,
            hovertemplate = "%{customdata}<extra></extra>") %>%
      add_markers(x = ~xplot, y = ~ylab,
                  marker = list(color = fill_col, size = 5,
                                line = list(color = ring_col, width = ring_w)),
                  customdata = ~hover, showlegend = FALSE,
                  hovertemplate = "%{customdata}<extra></extra>") %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        showlegend = FALSE,
        title = list(text = chart_text,
                     font = list(color = "#94a3b8", size = 12)),
        shapes = list(
          list(type = "line", x0 = 0, x1 = 0, yref = "paper", y0 = 0, y1 = 1,
               line = list(color = "rgba(255,255,255,0.4)"))),
        xaxis = list(title = x_title, range = c(x_lo, x_hi),
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        yaxis = list(title = "", type = "category",
                     categoryorder = "array", categoryarray = df$ylab,
                     tickfont = list(size = 9),
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.05)"),
        margin = list(l = 130, r = 30, b = 50, t = 40))
  })

  output$buyTableBL <- DT::renderDT({
    df <- app_dataBL()
    if (is.null(df) || nrow(df) == 0) {
      return(DT::datatable(
        data.frame(Note = "Connect and Generate Chart to load the BUY list."),
        selection = "none", rownames = FALSE, class = "compact",
        options = list(dom = "t", ordering = FALSE)))
    }
    df <- bl_apply_filters(df)   # same id/rank filters as the chart
    if (nrow(df) == 0) {
      return(DT::datatable(
        data.frame(Note = "No rows match the id/rank filters."),
        selection = "none", rownames = FALSE, class = "compact",
        options = list(dom = "t", ordering = FALSE)))
    }
    if (app_modeBL() == "wf") {
      hz <- if ("horizon" %in% names(df)) df$horizon[1] else 12L
      di <- if ("del_info"  %in% names(df)) as.character(df$del_info)  else rep("", nrow(df))
      dc <- if ("del_class" %in% names(df)) as.character(df$del_class) else rep("", nrow(df))
      display <- data.frame(
        Ticker            = df$ticker,
        Status            = ifelse(df$delisted, "delisted", ""),
        Delisted          = ifelse(df$delisted,
                                   ifelse(nzchar(di), di, "no category on file"), ""),
        Id                = df$id,
        Cutoff            = as.character(df$train_cutoff_date),
        Rank              = df$rank_within_cluster,
        `Cluster size`    = df$cluster_size,
        Score             = df$ticker_score,
        excess            = df$fwd_excess_pct,
        del_class         = dc,
        check.names = FALSE,
        stringsAsFactors = FALSE
      )
      hz_end <- seq(as.Date(df$train_cutoff_date[1]),
                    by = sprintf("%d months", hz), length.out = 2)[2]
      hz_cut <- "market_max" %in% names(df) &&
        !is.na(hz_end) && hz_end > as.Date(df$market_max[1])
      names(display)[names(display) == "excess"] <-
        if (hz_cut) sprintf("excess vs SPY %% so far (of %dmo)", hz)
        else sprintf("%dmo excess vs SPY %%", hz)
      return(DT::datatable(
        display,
        selection = "none",
        rownames  = FALSE,
        class     = "compact",
        extensions = "Buttons",
        options   = list(
          pageLength = 25, lengthMenu = c(10, 25, 50, 100),
          dom = "Bftip",
          buttons = c("copy", "csv"),
          order = list(list(8, "desc")),
          columnDefs = list(
            list(className = "dt-right", targets = 5:8),
            list(visible = FALSE, targets = 9))
        )
      ) %>% DT::formatStyle(c("Ticker", "Status", "Delisted"),
                            valueColumns = "del_class",
                            color = DT::styleEqual(names(DELIST_CLASSES),
                                                   unname(DELIST_CLASSES))))
    }
    if (app_modeBL() == "ledger") {
      di <- if ("del_info"  %in% names(df)) as.character(df$del_info)  else rep("", nrow(df))
      dc <- if ("del_class" %in% names(df)) as.character(df$del_class) else rep("", nrow(df))
      display <- data.frame(
        Ticker          = df$ticker,
        Status          = ifelse(df$delisted, "delisted", ""),
        Delisted        = ifelse(df$delisted,
                                 ifelse(nzchar(di), di, "no category on file"), ""),
        Snapshot        = as.character(df$prediction_date),
        `Entry date`    = as.character(df$entry_date),
        `Entry close`   = df$entry_adj_close,
        `Latest close`  = df$latest_close,
        `Ret %`         = df$ret_since_pct,
        `vs SPY %`      = df$excess_vs_spy_pct,
        `Buy weight`    = df$buy_weight,
        `Buy votes`     = as.integer(df$buy_votes),
        `Agg rank`      = as.integer(df$best_agg_rank),
        del_class       = dc,
        check.names = FALSE,
        stringsAsFactors = FALSE
      )
      return(DT::datatable(
        display,
        selection = "none",
        rownames  = FALSE,
        class     = "compact",
        extensions = "Buttons",
        options   = list(
          pageLength = 25, lengthMenu = c(10, 25, 50, 100, 500),
          dom = "Bftip",
          buttons = c("copy", "csv"),
          order = list(list(8, "desc")),
          columnDefs = list(
            list(className = "dt-right", targets = 5:11),
            list(visible = FALSE, targets = 12))
        )
      ) %>% DT::formatStyle(c("Ticker", "Status", "Delisted"),
                            valueColumns = "del_class",
                            color = DT::styleEqual(names(DELIST_CLASSES),
                                                   unname(DELIST_CLASSES))))
    }
    view <- input$view_valBL
    if (is.null(view) || !view %in% c("shortlist", "all", "rankall")) view <- "all"

    if (view == "rankall") {
      df <- df[order(df$id, df$agg_rank), ]
      display <- data.frame(
        Ticker       = df$ticker,
        Id           = df$id,
        Archetype    = df$archetype,
        Action       = df$global_action,
        Evidence     = df$evidence_status,
        `Tenure mo`  = as.integer(df$ticker_months_count),
        `Rank`       = df$agg_rank,
        `Buy weight` = df$buy_weight,
        `Expectancy` = df$wtd_expectancy,
        `Win %`      = df$wtd_win_pct,
        `Holdout n`  = as.integer(df$total_holdout),
        check.names = FALSE,
        stringsAsFactors = FALSE
      )
      return(DT::datatable(
        display,
        selection = "none", rownames = FALSE, class = "compact",
        extensions = "Buttons",
        options = list(
          pageLength = 25, lengthMenu = c(10, 25, 50, 100, 500, 2000),
          dom = "Bftip", buttons = c("copy", "csv"),
          columnDefs = list(list(className = "dt-right", targets = 5:10))
        )
      ) %>% DT::formatStyle("Action",
              color = DT::styleEqual(c("BUY", "SELL", "SKIP"),
                                     c("#10b981", "#dc2626", "#94a3b8"))) %>%
        DT::formatStyle("Evidence",
              color = DT::styleEqual(
                c("sparse_cluster", "sparse_ticker", "sparse_both"),
                c("#f59e0b", "#f59e0b", "#d97706"))))
    }

    # BUY-gated views: same subset as the chart ("all" also keeps the active
    # long-side young-cluster names, gated by the same Regular/Sparse toggles)
    if ("global_action" %in% names(df)) {
      keep <- df$global_action == "BUY"
      if (view == "all") {
        cats <- input$show_catBL
        if (is.null(cats)) cats <- c("regular", "sparse")
        young_row <- !is.na(df$evidence_status) &
                     df$evidence_status %in% c("sparse_cluster", "sparse_both") &
                     df$id <= 12
        keep <- (("regular" %in% cats) & df$global_action == "BUY") |
                (("sparse" %in% cats) & young_row)
      }
      df <- df[keep, , drop = FALSE]
    }
    if (nrow(df) == 0) {
      return(DT::datatable(
        data.frame(Note = "No BUY tickers right now."),
        selection = "none", rownames = FALSE, class = "compact",
        options = list(dom = "t", ordering = FALSE)))
    }

    if (view == "shortlist") {
      # keep in lockstep with the chart's shortlist rule: rank bins whose
      # realized win rate is >= 55 pct on >= 100 graded observations
      df <- df[!is.na(df$bin_win_pct) & df$bin_win_pct >= 55 &
               !is.na(df$bin_n) & df$bin_n >= 100, ]
      if (nrow(df) > 0) df <- df[order(-df$bin_win_pct, df$agg_rank), ]
    }
    # bin win% blanks out when the bin has < 100 graded obs (young ids):
    # a noise-level number reads as evidence in a table cell
    bin_win_shown <- ifelse(!is.na(df$bin_n) & df$bin_n >= 100, df$bin_win_pct, NA)
    sr_tbl <- severity_ring(df)
    display <- data.frame(
      Ticker        = df$ticker,
      Id            = df$id,
      Archetype     = df$archetype,
      Action        = df$global_action,
      Evidence      = df$evidence_status,
      `Tenure mo`   = as.integer(df$ticker_months_count),
      `Expectancy`  = df$wtd_expectancy,
      `Win %`       = df$wtd_win_pct,
      `Bin`         = as.integer(df$rank_bin),
      `Bin win %`   = bin_win_shown,
      `Buy cells`   = as.integer(df$n_buy_cells),
      `Holdout n`   = as.integer(df$total_holdout),
      `Cred weight` = df$avg_cred_weight,
      `Trusted cells` = as.integer(df$n_trusted_cells),
      `Buy weight`  = df$buy_weight,
      `Agg rank`    = df$agg_rank,
      `Cluster IC(12)` = df$cluster_ic_12,
      `Checks`      = as.integer(sr_tbl$checks),
      `Reason`      = ifelse(is.na(df$cluster_untradeable_reason), "",
                             df$cluster_untradeable_reason),
      check.names = FALSE,
      stringsAsFactors = FALSE
    )
    DT::datatable(
      display,
      selection = "none",
      rownames  = FALSE,
      class     = "compact",
      extensions = "Buttons",
      options   = list(
        pageLength = 25, lengthMenu = c(10, 25, 50, 100, 500),
        dom = "Bftip",
        buttons = c("copy", "csv"),
        order = list(list(9, "desc"), list(15, "asc")),
        columnDefs = list(list(className = "dt-right", targets = c(5:16, 17)))
      )
    ) %>% DT::formatStyle("Action",
            color = DT::styleEqual(c("BUY", "SELL", "SKIP"),
                                   c("#10b981", "#dc2626", "#94a3b8"))) %>%
      DT::formatStyle("Evidence",
            color = DT::styleEqual(
              c("sparse_cluster", "sparse_ticker", "sparse_both"),
              c("#f59e0b", "#f59e0b", "#d97706")))
  }, server = FALSE)
}

# Run the application
runApp(list(ui = ui, server = server), host="0.0.0.0", port=3838)
