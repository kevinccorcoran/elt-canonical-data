library(shiny)
library(DBI)
library(RPostgres)
library(plotly)
library(jsonlite)
library(nanoparquet)
library(DT)
library(ggplot2)

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
        h4("Transition range — inference.return_cluster_cell_score_extended",
           style = "color: #f8fafc; margin-bottom: 0.5rem; font-weight: 600;"),
        uiOutput("transitionHeader"),
        div(style = "flex: 1; min-height: 0;", plotlyOutput("transitionPlot", height = "100%"))
      ))
    )
  ),

  # ── Tab 2: Heatmap ──
  tabPanel("Heatmap",
    sidebarLayout(
      make_sidebar("H", "Database Connection (Heatmap)", tagList(
        selectInput("id_valH", "ID", choices = c("Connect first..." = ""), selected = ""),
        selectInput("bucket_valH", "Past Z-Bucket", choices = c("All" = "ALL"), selected = "ALL"),
        selectInput("metric_valH", "Fill Metric",
          choices = c("Combined Score"  = "combined_score",
                      "Net Score"       = "net_score",
                      "Positive Score"  = "positive_score",
                      "Negative Score"  = "negative_score",
                      "Return /mo"      = "future_confidence_score",
                      "Improv /mo"      = "future_improvement_score",
                      "Risk /mo"        = "future_risk_score",
                      "Tail Risk /mo"   = "future_tail_risk_score"),
          selected = "combined_score"),
        checkboxInput("viable_onlyH", "Viable combinations only", value = TRUE)
      )),
      mainPanel(div(class = "main-card",
        h4("Past × Future Lag Heatmap", style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        plotlyOutput("heatmapPlot", height = "700px")
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
        h4("Per-ticker scatter — analysis.ticker_cluster_segments × metrics.ticker_cluster_volatility_summary",
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
        h4("Top picks across horizons — inference.return_cluster_ticker_summary_current x return_cluster_ticker_pair_current",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #38bdf8; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; line-height: 1.4;
                   margin-bottom: 0.75rem;",
          "Rows: top-N tickers by agg_rank in the selected cluster (rank 1 at top). ",
          "Columns: fut_lag (1 to 54 months). ",
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
                                "Combo: hit + median (quadrants)" = "combo_quadrants"),
                    selected = "mean_return"),
        dateRangeInput("cutoff_rangeRS", "Cutoff range",
                       start = NULL, end = NULL,
                       startview = "year", separator = " to ")
      )),
      mainPanel(div(class = "main-card",
        h4("Rank stability across walk-forward cohorts - inference.walk_forward_ticker_rank",
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
        h5("Slot performance - mean realized forward return per rank slot",
           style = "color: #f8fafc; font-weight: 600;"),
        plotlyOutput("slotPerfPlotRS", height = "380px"),
        tags$br(),
        div(style = "display: flex; align-items: center; gap: 0.75rem;",
            h5("Vingtile (5% bin) vs fut_lag heatmap - mean realized return at each (vingtile, horizon) across 84 cohorts. Rank normalized to within-cluster vingtile so different cluster sizes compare fairly.",
               style = "color: #f8fafc; font-weight: 600; margin: 0; flex: 1;"),
            checkboxInput("show_vingtile_heatmap", "Show", value = TRUE)
        ),
        conditionalPanel(
          condition = "input.show_vingtile_heatmap",
          plotlyOutput("stabilityHeatmapRS", height = "800px")
        ),
        tags$br(),
        actionButton("execute_all_RS", "Generate small-multiples for ALL ids",
                     class = "btn-primary", style = "margin-bottom: 1rem;"),
        h5("All 19 ids - green = model was right (sign-adjusted: longs up = green, shorts down = green). For combo: gray=neither, blue=hit only, gold=median-only (Bessembinder), green=both high.",
           style = "color: #f8fafc; font-weight: 600;"),
        plotlyOutput("allIdsGridRS", height = "1200px")
      ))
    )
  )
)

# ─── Helper: create a DB connection ───
get_con <- function(input, suffix) {
  env   <- input[[paste0("db_env", suffix)]]
  db_string <- if (env == "Production") "prod" else if (env == "Staging") "staging" else "dev"
  con <- dbConnect(RPostgres::Postgres(),
    dbname   = db_string,
    host     = input[[paste0("db_host", suffix)]],
    port     = as.integer(input[[paste0("db_port", suffix)]]),
    user     = input[[paste0("db_user", suffix)]],
    password = input[[paste0("db_pass", suffix)]],
    sslmode  = "prefer",
    connect_timeout = 10
  )
  # Hard cap so a slow or stuck query can never freeze the single-threaded app
  DBI::dbExecute(con, "SET statement_timeout = '60s'")
  con
}

# ─── Helper: wire up env-switcher for a given suffix ───
setup_env_switcher <- function(input, session, suffix) {
  observeEvent(input[[paste0("db_env", suffix)]], {
    env <- input[[paste0("db_env", suffix)]]
    if (env == "Production") {
      updateTextInput(session, paste0("db_host", suffix), value = "dbaas-db-4718169-do-user-32264340-0.l.db.ondigitalocean.com")
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

  # Keep the session alive across browser idle periods. Without this, Shiny
  # blanks the page to white when its websocket times out and the user has
  # to manually refresh. allowReconnect("force") makes the client auto-
  # reconnect to the same session silently.
  session$allowReconnect("force")

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
      id_vals         <- dbGetQuery(con, "SELECT DISTINCT id FROM inference.return_cluster_lag_viability ORDER BY 1")
      past_fib_vals   <- dbGetQuery(con, "SELECT DISTINCT fibonacci_lag_value FROM inference.return_cluster_lag_viability ORDER BY 1")
      future_fib_vals <- dbGetQuery(con, "SELECT DISTINCT future_fibonacci_lag_value FROM inference.return_cluster_lag_viability ORDER BY 1")
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
            SELECT 1 FROM inference.return_cluster_ticker_pair_current tpc
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
      FROM inference.return_cluster_cell_score_extended cs
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
    }, error = function(e) { status_msgT(paste("Error:", e$message)) })
  })

  # ── TRANSITION: Render ──
  output$transitionPlot <- renderPlotly({
    req(app_dataT())
    df <- app_dataT()
    if (nrow(df) == 0) return(plot_ly() %>% layout(title = list(text = "No data found", font = list(color="#f8fafc")), paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))

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

  # ── HEATMAP: Reactive values ──
  app_dataH <- reactiveVal(NULL)
  status_msgH <- reactiveVal("Ready")
  output$statusMessageH <- renderText({ status_msgH() })
  setup_env_switcher(input, session, "H")

  # ── HEATMAP: Connect ──
  observeEvent(input$connect_btnH, {
    if (input$db_passH == "") { status_msgH("Error: Password is not set."); return() }
    status_msgH("Connecting...")
    tryCatch({
      con <- get_con(input, "H")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals <- dbGetQuery(con,
        "SELECT DISTINCT id FROM inference.return_cluster_cell_score_extended ORDER BY 1")
      bucket_vals <- dbGetQuery(con,
        "SELECT DISTINCT past_excess_return_z_bucket, past_excess_return_z_bucket_num
         FROM inference.return_cluster_cell_score_extended
         ORDER BY past_excess_return_z_bucket_num")
      bucket_choices <- c("All" = "ALL", setNames(bucket_vals[[1]], bucket_vals[[1]]))
      updateSelectInput(session, "id_valH", choices = id_vals[[1]], selected = id_vals[[1]][1])
      updateSelectInput(session, "bucket_valH", choices = bucket_choices, selected = "ALL")
      status_msgH("Filters loaded!")
    }, error = function(e) { status_msgH(paste("Error:", e$message)) })
  })

  # ── HEATMAP: Execute ──
  observeEvent(input$execute_H, {
    if (input$db_passH == "") { status_msgH("Error: Password is not set."); return() }
    if (input$id_valH == "") { status_msgH("Error: Select an ID first."); return() }
    status_msgH("Running query...")

    bucket_clause <- if (input$bucket_valH == "ALL") "" else sprintf(
      "AND past_excess_return_z_bucket = '%s'", gsub("'", "''", input$bucket_valH))
    viable_clause <- if (isTRUE(input$viable_onlyH)) "AND is_viable" else ""

    query <- sprintf("
      SELECT past_fibonacci_lag_value,
             future_fibonacci_lag_value,
             past_excess_return_z_bucket,
             future_confidence_score,
             future_improvement_score,
             future_risk_score,
             future_tail_risk_score,
             positive_score,
             negative_score,
             net_score,
             combined_score,
             recommendation,
             signal,
             is_viable
      FROM inference.return_cluster_cell_score_extended
      WHERE id = %s %s %s
      ORDER BY past_fibonacci_lag_value, future_fibonacci_lag_value;",
      input$id_valH, bucket_clause, viable_clause)

    tryCatch({
      con <- get_con(input, "H")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      res <- dbGetQuery(con, query)
      num_cols <- c("past_fibonacci_lag_value","future_fibonacci_lag_value",
                    "future_confidence_score","future_improvement_score",
                    "future_risk_score","future_tail_risk_score",
                    "positive_score","negative_score","net_score","combined_score")
      for (col in num_cols) res[[col]] <- as.numeric(res[[col]])
      app_dataH(res)
      status_msgH(paste("Loaded", nrow(res), "rows."))
    }, error = function(e) { status_msgH(paste("Error:", e$message)) })
  })

  # ── HEATMAP: Render ──
  output$heatmapPlot <- renderPlotly({
    req(app_dataH())
    df <- app_dataH()
    if (nrow(df) == 0) return(plot_ly() %>% layout(
      title = list(text = "No data found", font = list(color="#f8fafc")),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))

    metric <- input$metric_valH
    metric_labels <- c(
      "future_confidence_score"  = "Return /mo",
      "future_improvement_score" = "Improv /mo",
      "future_risk_score"        = "Risk /mo",
      "future_tail_risk_score"   = "Tail Risk /mo"
    )

    # Aggregate by (past_lag, future_lag) — mean over buckets when "All" is selected
    agg <- aggregate(df[[metric]],
      by = list(past = df$past_fibonacci_lag_value,
                future = df$future_fibonacci_lag_value),
      FUN = mean, na.rm = TRUE)
    names(agg)[3] <- "value"

    x_vals <- sort(unique(agg$past))
    y_vals <- sort(unique(agg$future))
    z_mat <- matrix(NA_real_, nrow = length(y_vals), ncol = length(x_vals),
                    dimnames = list(as.character(y_vals), as.character(x_vals)))
    for (i in seq_len(nrow(agg))) {
      z_mat[as.character(agg$future[i]), as.character(agg$past[i])] <- agg$value[i]
    }

    is_diverging <- metric %in% c("future_confidence_score","future_improvement_score")
    if (is_diverging) {
      max_abs <- max(abs(z_mat), na.rm = TRUE)
      if (!is.finite(max_abs) || max_abs == 0) max_abs <- 1
      zmin <- -max_abs; zmax <- max_abs
      colorscale <- list(c(0, '#dc2626'), c(0.5, '#1e293b'), c(1, '#34d399'))
    } else {
      zmin <- min(z_mat, na.rm = TRUE); zmax <- max(z_mat, na.rm = TRUE)
      if (!is.finite(zmin)) zmin <- 0
      if (!is.finite(zmax) || zmax == zmin) zmax <- zmin + 1
      colorscale <- list(c(0, '#1e293b'), c(0.5, '#fbbf24'), c(1, '#f87171'))
    }

    bucket_label <- if (input$bucket_valH == "ALL") "" else sprintf(" · bucket %s", input$bucket_valH)

    plot_ly(
      x = as.character(x_vals), y = as.character(y_vals), z = z_mat, type = "heatmap",
      colorscale = colorscale, zmin = zmin, zmax = zmax,
      hovertemplate = sprintf(
        "Past lag: %%{x}<br>Future lag: %%{y}<br>%s: %%{z:.4f}<extra></extra>",
        metric_labels[[metric]]),
      colorbar = list(title = list(text = metric_labels[[metric]],
                                   font = list(color = "#f8fafc")),
                      tickfont = list(color = "#94a3b8"))
    ) %>% layout(
      title = list(
        text = sprintf("ID %s — Past × Future Lag (%s%s)",
                       input$id_valH, metric_labels[[metric]], bucket_label),
        font = list(color = "#f8fafc", family = "Inter", size = 18)),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
      xaxis = list(title = "Past Lag (months)", type = "category",
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "Future Lag (months)", type = "category",
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      margin = list(l = 80, r = 60, b = 60, t = 60)
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

      placeholders <- paste(sprintf("'%s'", gsub("'", "''", selected)), collapse = ",")
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
      options   = list(
        pageLength = 25, lengthMenu = c(10, 25, 50, 100, 500),
        dom = "tip", searching = FALSE, ordering = FALSE,
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
    }, error = function(e) { status_msgV(paste("Error:", e$message)) })
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
    if (nrow(df) == 0) return(plot_ly() %>% layout(
      title = list(text = "No data", font = list(color="#f8fafc")),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))

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
        JOIN metrics.ticker_cluster_volatility_summary v
          ON v.cluster_id = s.cluster_id
        WHERE s.months_count > 0 AND s.growth_pct_per_month IS NOT NULL")
      app_dataK(df)
      status_msgK(sprintf("Loaded %d tickers across %d clusters.",
                          nrow(df), length(unique(df$id))))
    }, error = function(e) { status_msgK(paste("Error:", e$message)) })
  })

  output$clusterPlot <- renderPlotly({
    req(app_dataK())
    df <- app_dataK()
    if (nrow(df) == 0) return(plot_ly() %>% layout(
      title = list(text = "No data", font = list(color = "#f8fafc")),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))

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
        "SELECT DISTINCT id FROM inference.return_cluster_ticker_summary_current ORDER BY 1")
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
          FROM inference.return_cluster_ticker_summary_current
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
      JOIN inference.return_cluster_ticker_pair_current p
        ON p.ticker = tp.ticker AND p.id = %s
      LEFT JOIN analysis.ticker_cluster_segments s
        ON s.ticker = p.ticker
      LEFT JOIN inference.cell_credibility c
        ON c.vol_bucket_num = s.monthly_growth_vol_z_bucket_num
       AND c.id            = p.id
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
        FROM inference.walk_forward_id_ic
        WHERE id = %s AND fut_lag = 12;",
        input$id_valP)
      ic_df <- tryCatch(dbGetQuery(con, ic_query), error = function(e) NULL)
      cluster_ic_metaP(ic_df)

      status_msgP(paste("Loaded", nrow(res), "rows."))
    }, error = function(e) { status_msgP(paste("Error:", e$message)) })
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
    if (nrow(df) == 0) return(plot_ly() %>% layout(
      title = list(text = "No data found", font = list(color = "#f8fafc")),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))

    df <- df[order(df$agg_rank, df$fut_lag), ]
    row_keys <- unique(df[, c("agg_rank", "ticker", "coverage_cell_count")])
    row_keys <- row_keys[order(row_keys$agg_rank), ]
    row_labels <- sprintf("#%d %s (n=%d)",
                          row_keys$agg_rank, row_keys$ticker, row_keys$coverage_cell_count)

    fut_lag_vals <- sort(unique(df$fut_lag))
    # Proportional spacing via sqrt(fut_lag) so adjacent small lags (1,2) stay
    # similar in width while larger lags (33,54,88) get visibly wider cells.
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
        "SELECT DISTINCT id FROM inference.walk_forward_pctile_summary ORDER BY id")
      cl_choices <- c("All ids" = "ALL",
                      setNames(as.character(id_vals$id), as.character(id_vals$id)))
      updateSelectInput(session, "id_valRS", choices = cl_choices, selected = "ALL")
      cutoff_bounds <- dbGetQuery(con,
        "SELECT MIN(train_cutoff_date) AS min_d, MAX(train_cutoff_date) AS max_d
         FROM inference.walk_forward_ticker_rank")
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
      FROM inference.walk_forward_ticker_rank r
      JOIN inference.walk_forward_cluster_id_map m
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
        FROM inference.walk_forward_pctile_summary
        WHERE pctile_bin <= %d
          AND fut_lag <= 88
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
        FROM inference.walk_forward_pctile_summary
        WHERE pctile_bin <= %d
          AND fut_lag <= 88
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
        if (metric_col == "combo_quadrants") {
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
        FROM inference.walk_forward_ticker_rank r
        JOIN inference.walk_forward_cluster_id_map m
          ON m.train_cutoff_date = r.train_cutoff_date
         AND m.cluster_id = r.cluster_id
        WHERE r.train_cutoff_date BETWEEN '%s' AND '%s' %s;",
        date_lo, date_hi, n_cut_filter))$n[1]
      app_dataRS_meta(list(n_cutoffs = n_cut, lo = date_lo, hi = date_hi,
                           cluster = input$id_valRS,
                           metric = input$metric_valRS))
      status_msgRS(sprintf("Loaded: %d slot rows, %d cutoffs.",
                           as.integer(nrow(slot_df)), as.integer(n_cut)))
    }, error = function(e) { status_msgRS(paste("Error:", e$message)) })
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
    if (nrow(df) == 0) return(plot_ly() %>% layout(
      title = list(text = "No data", font = list(color = "#f8fafc")),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))
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
    if (nrow(df) == 0) return(plot_ly() %>% layout(
      title = list(text = "No tickers met threshold", font = list(color = "#f8fafc")),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))
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
      # purple → yellow → teal centered at coin flip 0.5
      colorscale <- list(
        c(0.00, '#762a83'),
        c(0.25, '#c2a5cf'),
        c(0.50, '#f7f7b6'),
        c(0.75, '#5ab4ac'),
        c(1.00, '#01665e')
      )
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
    } else if (metric == "combo_quadrants") {
      # Discrete bands: gray (neither), blue (hit only),
      # gold (median only = Bessembinder), green (both high).
      colorscale <- list(
        c(0.00, '#888888'), c(0.25, '#888888'),
        c(0.25, '#3b82f6'), c(0.50, '#3b82f6'),
        c(0.50, '#f59e0b'), c(0.75, '#f59e0b'),
        c(0.75, '#10b981'), c(1.00, '#10b981')
      )
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
        FROM inference.walk_forward_pctile_summary
        WHERE fut_lag <= 88
        ORDER BY id, pctile_bin, fut_lag;")
      df$id <- as.integer(df$id)
      df$pctile_bin <- as.integer(df$pctile_bin)
      df$fut_lag <- as.integer(df$fut_lag)
      df$mean_return <- as.numeric(df$mean_return)
      df$median_return <- as.numeric(df$median_return)
      df$hit_rate <- as.numeric(df$hit_rate)
      df$sharpe_like <- as.numeric(df$sharpe_like)
      df$active_metric <- input$metric_valRS
      app_dataRS_allIds(df)
      status_msgRS(sprintf("Loaded all ids: %d rows across %d distinct ids.",
                           as.integer(nrow(df)),
                           as.integer(length(unique(df$id)))))
    }, error = function(e) { status_msgRS(paste("Error:", e$message)) })
  })

  output$allIdsGridRS <- renderPlotly({
    req(app_dataRS_allIds())
    df_subset <- app_dataRS_allIds()
    if (nrow(df_subset) == 0) return(plot_ly() %>% layout(
      title = list(text = "No data", font = list(color = "#f8fafc")),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))

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
    } else if (metric_col == "combo_quadrants") {
      # Quadrant categorical view: hit_rate (already direction-aligned in
      # pctile_summary) crossed with aligned median return. Bessembinder cells
      # show up as MEDIAN ONLY (low hit but positive median = rare big wins).
      df_subset$direction_sign <- ifelse(df_subset$id <= 12, 1, -1)
      df_subset$aligned_median <- df_subset$direction_sign * df_subset$median_return
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
    colorscale <- if (metric_col == "hit_rate") list(
      c(0.00, '#762a83'),
      c(0.25, '#c2a5cf'),
      c(0.50, '#f7f7b6'),
      c(0.75, '#5ab4ac'),
      c(1.00, '#01665e')
    ) else if (metric_col == "combo_quadrants") list(
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
      zmin_val <- if (metric_col == "combo_quadrants") 0 else -max_abs
      zmax_val <- if (metric_col == "combo_quadrants") 1 else  max_abs
      plot_ly(
        x = all_fut_lag_pos,
        y = rev(as.character(all_ranks)),
        z = z_mat_rev,
        type = "heatmap", colorscale = colorscale,
        zmin = zmin_val, zmax = zmax_val, showscale = FALSE,
        xgap = 1, ygap = 1,
        text = text_mat,
        hovertemplate = if (metric_col == "combo_quadrants") paste0(
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
}

# Run the application
runApp(list(ui = ui, server = server), host="0.0.0.0", port=3838)
