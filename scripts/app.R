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
    actionButton(paste0("connect_btn", suffix), "Connect & Load Filters", class = "btn-primary"),
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
        h4("Ticker history coverage (cdm.ingest_combined)",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        uiOutput("coveragePlotContainer")
      ))
    )
  ),

  # ── Tab: Clusters ──
  tabPanel("Clusters",
    sidebarLayout(
      make_sidebar("K", "Database Connection (Clusters)", tagList(
        tags$div(
          style = "padding: 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #64748b; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; font-family: 'Inter'; line-height: 1.5;",
          tags$div(style = "color: #f8fafc; font-weight: 600; margin-bottom: 0.4rem;", "What this shows"),
          "Cluster overview fused with walk-forward credibility + current recommendations. Click a row to drill into ",
          "trustworthy cells and active BUY/SELL tickers. Color-coded trust rank: green=TRUST, yellow=MAYBE, grey=THIN, red=SKIP."
        ),
        tags$hr(style = "border-color: rgba(255,255,255,0.1);"),
        h4("Filters", style = "color: #f8fafc;"),
        sliderInput("fK_trust_max", "Trust rank ≤ (1=best)", 1, 4, 4, 1),
        sliderInput("fK_min_hq",    "Min pct_high_quality",      0, 100, 0, 5),
        checkboxInput("fK_has_buy",  "Has BUY signal",  FALSE),
        checkboxInput("fK_has_sell", "Has SELL signal", FALSE)
      )),
      mainPanel(div(class = "main-card",
        h4("Cluster overview", style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        DT::DTOutput("clusterOverviewTable"),
        uiOutput("clusterDrilldownUI"),
        tags$hr(style = "border-color: rgba(255,255,255,0.1); margin-top: 2rem;"),
        h4("Growth vs volatility scatter (raw clusters)",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        plotlyOutput("clusterPlot", height = "600px")
      ))
    )
  )
)

# ─── Helper: create a DB connection ───
get_con <- function(input, suffix) {
  env   <- input[[paste0("db_env", suffix)]]
  db_string <- if (env == "Production") "prod" else if (env == "Staging") "staging" else "dev"
  dbConnect(RPostgres::Postgres(),
    dbname   = db_string,
    host     = input[[paste0("db_host", suffix)]],
    port     = as.integer(input[[paste0("db_port", suffix)]]),
    user     = input[[paste0("db_user", suffix)]],
    password = input[[paste0("db_pass", suffix)]],
    sslmode  = "prefer"
  )
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
      FROM inference.return_cluster_cell_score cs
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
        "SELECT DISTINCT id FROM inference.return_cluster_cell_score ORDER BY 1")
      bucket_vals <- dbGetQuery(con,
        "SELECT DISTINCT past_excess_return_z_bucket, past_excess_return_z_bucket_num
         FROM inference.return_cluster_cell_score
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
      FROM inference.return_cluster_cell_score
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
        SELECT ticker,
               monthly_growth_vol_z_bucket_num AS bucket,
               cluster_id,
               (LN(months_count) - AVG(LN(months_count)) OVER w)
                 / NULLIF(STDDEV_POP(LN(months_count)) OVER w, 0) AS z_logmo,
               (growth_pct_per_month - AVG(growth_pct_per_month) OVER w)
                 / NULLIF(STDDEV_POP(growth_pct_per_month) OVER w, 0) AS z_growth
        FROM analysis.ticker_cluster_segments
        WHERE months_count > 0 AND growth_pct_per_month IS NOT NULL
        WINDOW w AS (PARTITION BY monthly_growth_vol_z_bucket_num)")
      app_dataK(df)
      status_msgK(sprintf("Loaded %d tickers across %d buckets.",
                          nrow(df), length(unique(df$bucket))))
    }, error = function(e) { status_msgK(paste("Error:", e$message)) })
  })

  # ── CLUSTERS: cluster summary fused with walk-forward credibility ──
  cluster_summaryK <- reactiveVal(NULL)
  trust_palette    <- c(`1` = "#10b981", `2` = "#fbbf24", `3` = "#64748b", `4` = "#ef4444")

  observeEvent(input$execute_K, {
    if (input$db_passK == "") return()
    tryCatch({
      con <- get_con(input, "K")
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      summary_df <- dbGetQuery(con, "
        WITH credibility_rollup AS (
          SELECT id,
                 COUNT(*)                            AS n_cells,
                 ROUND(AVG(wf_agreement)::numeric, 3) AS avg_wf_agreement,
                 ROUND(100.0 * COUNT(*) FILTER (WHERE tier IN ('high','medium'))
                              / NULLIF(COUNT(*), 0), 1) AS pct_high_quality
          FROM inference.cell_credibility
          GROUP BY id
        ),
        action_rollup AS (
          SELECT id,
                 COUNT(DISTINCT ticker) FILTER (WHERE global_action = 'BUY')  AS n_buy,
                 COUNT(DISTINCT ticker) FILTER (WHERE global_action = 'SELL') AS n_sell,
                 COUNT(DISTINCT ticker) FILTER (WHERE global_action = 'SKIP') AS n_skip
          FROM inference.return_cluster_ticker_global_action_current
          GROUP BY id
        )
        SELECT v.id,
               v.monthly_growth_vol_z_bucket                   AS vol_bucket,
               v.cluster_id,
               v.ticker_count,
               v.avg_weighted_growth                           AS avg_growth_pct_per_month,
               v.min_months_count || '-' || v.max_months_count AS months_range,
               CASE WHEN COALESCE(c.pct_high_quality,0) >= 60 AND COALESCE(c.n_cells,0) >= 20 THEN 1
                    WHEN COALESCE(c.pct_high_quality,0) >= 40 THEN 2
                    WHEN COALESCE(c.n_cells,0) < 20 THEN 3
                    ELSE 4 END                                  AS cluster_trust_rank,
               COALESCE(c.pct_high_quality, 0) AS pct_high_quality,
               COALESCE(c.avg_wf_agreement, 0) AS avg_wf_agreement,
               COALESCE(c.n_cells, 0)          AS n_cells,
               COALESCE(a.n_buy,  0)           AS n_buy,
               COALESCE(a.n_sell, 0)           AS n_sell,
               COALESCE(a.n_skip, 0)           AS n_skip,
               array_to_string(v.tickers[1:5], ', ') AS top_5_tickers
        FROM metrics.ticker_cluster_volatility_summary v
        LEFT JOIN credibility_rollup c USING (id)
        LEFT JOIN action_rollup      a USING (id)
        ORDER BY cluster_trust_rank ASC, v.id ASC")
      cluster_summaryK(summary_df)
    }, error = function(e) { status_msgK(paste("Cluster summary error:", e$message)) })
  })

  filtered_clustersK <- reactive({
    req(cluster_summaryK())
    df <- cluster_summaryK()
    df[df$cluster_trust_rank <= input$fK_trust_max &
       df$pct_high_quality   >= input$fK_min_hq &
       (!input$fK_has_buy  | df$n_buy  > 0) &
       (!input$fK_has_sell | df$n_sell > 0), ]
  })

  output$clusterOverviewTable <- DT::renderDT({
    d <- filtered_clustersK()
    if (is.null(d) || nrow(d) == 0) return(NULL)
    DT::datatable(d, selection = "single", rownames = FALSE,
      options = list(pageLength = 14, dom = "tip",
                     order = list(list(which(names(d) == "cluster_trust_rank") - 1, "asc")))) |>
      DT::formatStyle("cluster_trust_rank", target = "row",
        backgroundColor = DT::styleEqual(names(trust_palette), trust_palette),
        color = "#0b1220", fontWeight = "600") |>
      DT::formatRound("avg_growth_pct_per_month", 2) |>
      DT::formatRound(c("avg_wf_agreement"), 3)
  })

  selected_clusterK <- reactive({
    i <- input$clusterOverviewTable_rows_selected
    req(i, length(i) > 0)
    filtered_clustersK()$id[i]
  })

  cells_in_clusterK <- reactive({
    req(selected_clusterK(), input$db_passK != "")
    con <- get_con(input, "K")
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    dbGetQuery(con,
      sprintf("SELECT past_lag, fut_lag, bucket, tier, wf_agreement, wf_n_holdout
               FROM inference.cell_credibility
               WHERE id = %d
               ORDER BY wf_agreement DESC NULLS LAST, wf_n_holdout DESC
               LIMIT 50", as.integer(selected_clusterK())))
  })

  tickers_in_clusterK <- reactive({
    req(selected_clusterK(), input$db_passK != "")
    con <- get_con(input, "K")
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    dbGetQuery(con,
      sprintf("SELECT ticker, global_action, action, is_active
               FROM inference.return_cluster_ticker_global_action_current
               WHERE id = %d AND global_action IN ('BUY','SELL')
               ORDER BY CASE global_action WHEN 'BUY' THEN 0 ELSE 1 END, ticker",
              as.integer(selected_clusterK())))
  })

  output$clusterDrilldownUI <- renderUI({
    req(selected_clusterK())
    tagList(
      tags$hr(style = "border-color: rgba(255,255,255,0.1); margin-top: 1.5rem;"),
      h4(paste("Cluster", selected_clusterK(), "detail"),
         style = "color: #f8fafc; margin-bottom: 1rem;"),
      fluidRow(
        column(6,
          h5("Top 50 cells (sorted by walk-forward agreement)", style = "color: #cbd5e1;"),
          DT::DTOutput("clusterCellsTable")),
        column(6,
          h5("Active BUY / SELL tickers in this cluster", style = "color: #cbd5e1;"),
          DT::DTOutput("clusterTickersTable"))
      )
    )
  })

  output$clusterCellsTable <- DT::renderDT({
    d <- cells_in_clusterK()
    if (is.null(d) || nrow(d) == 0) return(NULL)
    tier_colors <- c(high = "#10b981", medium = "#fbbf24", thin = "#64748b", bad = "#ef4444")
    DT::datatable(d, rownames = FALSE,
                  options = list(pageLength = 10, dom = "tip")) |>
      DT::formatStyle("tier",
        backgroundColor = DT::styleEqual(names(tier_colors), tier_colors),
        color = "#0b1220", fontWeight = "600") |>
      DT::formatRound("wf_agreement", 3)
  })

  output$clusterTickersTable <- DT::renderDT({
    d <- tickers_in_clusterK()
    if (is.null(d) || nrow(d) == 0) return(NULL)
    DT::datatable(d, rownames = FALSE,
                  options = list(pageLength = 10, dom = "tip")) |>
      DT::formatStyle("global_action",
        backgroundColor = DT::styleEqual(c("BUY","SELL"), c("#10b981","#ef4444")),
        color = "#0b1220", fontWeight = "600")
  })

  output$clusterPlot <- renderPlotly({
    req(app_dataK())
    df <- app_dataK()
    if (nrow(df) == 0) return(plot_ly() %>% layout(
      title = list(text = "No data", font = list(color = "#f8fafc")),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)"))

    df <- df[is.finite(df$z_logmo) & is.finite(df$z_growth), ]
    df$cluster <- factor(df$cluster_id)
    df$bucket  <- factor(df$bucket)

    p <- ggplot(df, aes(x = z_logmo, y = z_growth, color = cluster, text = ticker)) +
      geom_point(size = 1.1, alpha = 0.6) +
      facet_wrap(~ bucket, labeller = labeller(bucket = function(x) paste("bucket", x))) +
      scale_color_manual(values = c("0"="#f59e0b","1"="#22d3ee","2"="#a855f7",
                                    "3"="#ef4444","4"="#10b981")) +
      labs(x = "z(log months)", y = "z(growth)", color = "cluster") +
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
}

# Run the application 
runApp(list(ui = ui, server = server), host="0.0.0.0", port=3838)
