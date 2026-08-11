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

# ─── Personal portfolio positions (DCA tracker; same host-mounted dir) ───
# One row per buy PLAN, not per fill. Persisted as parquet so it survives
# restarts, exactly like QA history. sold_* are reserved for the Phase-2 sell.
PORTFOLIO_PATH <- file.path(QA_HISTORY_DIR, "portfolio_positions.parquet")

portfolio_empty <- function() data.frame(
  id = character(0), ticker = character(0), amount_usd = numeric(0),
  cadence = character(0), day1 = integer(0), day2 = integer(0),
  start_date = character(0), end_date = character(0),
  sold_date = character(0), sold_fraction = numeric(0),
  mode = character(0), adopted_at = character(0),
  created_at = character(0), stringsAsFactors = FALSE)

# Older parquets predate mode/adopted_at; default every legacy row to a plain
# manual DCA plan. Does not rewrite the file - the next save persists the columns.
migrate_portfolio <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(portfolio_empty())
  if (!"mode" %in% names(df)) df$mode <- "manual"
  df$mode[is.na(df$mode) | !nzchar(df$mode)] <- "manual"
  if (!"adopted_at" %in% names(df)) df$adopted_at <- ""
  df
}

load_portfolio <- function() {
  if (!file.exists(PORTFOLIO_PATH)) return(portfolio_empty())
  tryCatch(migrate_portfolio(as.data.frame(nanoparquet::read_parquet(PORTFOLIO_PATH))),
           error = function(e) portfolio_empty())
}

save_portfolio <- function(df) {
  if (!dir.exists(QA_HISTORY_DIR)) dir.create(QA_HISTORY_DIR, recursive = TRUE)
  if (is.null(df) || nrow(df) == 0) {
    if (file.exists(PORTFOLIO_PATH)) file.remove(PORTFOLIO_PATH)
  } else nanoparquet::write_parquet(df, PORTFOLIO_PATH)
  invisible(df)
}

# Qualstream orange-+ rule, shared by the board's mark AND the portfolio's
# default seed so the two can never diverge: a current BUY graded >= QS_MIN by
# qualstream, capped at the top QS_CAP by grade.
QS_MIN <- 68L
QS_CAP <- 25L

# Dismissed set: tickers the user explicitly removed, kept so the qualstream
# auto-seed does not resurrect them on the next Generate. Stored as a one-column
# parquet next to the positions; empty file is deleted (mirrors save_portfolio).
PORTFOLIO_DISMISS_PATH <- file.path(QA_HISTORY_DIR, "portfolio_dismissed.parquet")
load_dismissed <- function() {
  if (!file.exists(PORTFOLIO_DISMISS_PATH)) return(character(0))
  tryCatch(unique(toupper(as.character(
    nanoparquet::read_parquet(PORTFOLIO_DISMISS_PATH)$ticker))),
    error = function(e) character(0))
}
save_dismissed <- function(tickers) {
  tickers <- unique(toupper(tickers[nzchar(tickers)]))
  if (!dir.exists(QA_HISTORY_DIR)) dir.create(QA_HISTORY_DIR, recursive = TRUE)
  if (!length(tickers)) {
    if (file.exists(PORTFOLIO_DISMISS_PATH)) file.remove(PORTFOLIO_DISMISS_PATH)
  } else nanoparquet::write_parquet(
    data.frame(ticker = tickers, stringsAsFactors = FALSE), PORTFOLIO_DISMISS_PATH)
  invisible(tickers)
}

# ─── DB-backed portfolio store (portfolio.* schema) ───
# When the connected DB has the portfolio schema (tools/create_portfolio_schema.sql),
# it is the authoritative store - one shared copy across the local + server
# dashboards, and joinable to the model tables. Otherwise the app falls back to
# the parquet files above. All take an already-open connection; none call get_con.
pf_db_ready <- function(con) {
  tryCatch(isTRUE(DBI::dbGetQuery(con,
    "SELECT to_regclass('portfolio.positions') IS NOT NULL AS ok")$ok[1]),
    error = function(e) FALSE)
}

# empty text -> NA so date/timestamp columns receive NULL, not ''
.pf_nz <- function(x) { x <- as.character(x); x[is.na(x) | !nzchar(x)] <- NA; x }

db_load_positions <- function(con) {
  df <- tryCatch(DBI::dbGetQuery(con, "
    SELECT id, ticker, amount_usd, cadence, day1, day2,
           to_char(start_date,'YYYY-MM-DD')              AS start_date,
           COALESCE(to_char(end_date,'YYYY-MM-DD'),'')   AS end_date,
           COALESCE(to_char(sold_date,'YYYY-MM-DD'),'')  AS sold_date,
           sold_fraction, mode,
           COALESCE(to_char(adopted_at,'YYYY-MM-DD HH24:MI:SS'),'') AS adopted_at,
           to_char(created_at,'YYYY-MM-DD HH24:MI:SS')   AS created_at
    FROM portfolio.positions ORDER BY created_at, ticker"),
    error = function(e) NULL)
  if (is.null(df) || !nrow(df)) return(portfolio_empty())
  df$amount_usd    <- as.numeric(df$amount_usd)
  df$day1 <- as.integer(df$day1); df$day2 <- as.integer(df$day2)
  df$sold_fraction <- as.numeric(df$sold_fraction)
  migrate_portfolio(df)
}

# Whole-state replace in one transaction: mirrors the parquet "df IS the state"
# semantics, so the existing observers (which compute a full merged df) need no
# rework. The table is tiny and single-writer, so truncate+insert is fine.
db_save_positions <- function(con, df) {
  DBI::dbWithTransaction(con, {
    DBI::dbExecute(con, "DELETE FROM portfolio.positions")
    if (!is.null(df) && nrow(df) > 0)
      DBI::dbExecute(con, "
        INSERT INTO portfolio.positions
          (id,ticker,amount_usd,cadence,day1,day2,start_date,end_date,
           sold_date,sold_fraction,mode,adopted_at,created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
        params = list(
          as.character(df$id), toupper(as.character(df$ticker)),
          as.numeric(df$amount_usd), as.character(df$cadence),
          as.integer(df$day1), as.integer(df$day2),
          .pf_nz(df$start_date), .pf_nz(df$end_date), .pf_nz(df$sold_date),
          as.numeric(df$sold_fraction), as.character(df$mode),
          .pf_nz(df$adopted_at), .pf_nz(df$created_at)))
  })
  invisible(df)
}

db_load_dismissed <- function(con) {
  tryCatch(unique(toupper(as.character(
    DBI::dbGetQuery(con, "SELECT ticker FROM portfolio.dismissed")$ticker))),
    error = function(e) character(0))
}

db_save_dismissed <- function(con, tickers) {
  tickers <- unique(toupper(tickers[nzchar(tickers)]))
  DBI::dbWithTransaction(con, {
    DBI::dbExecute(con, "DELETE FROM portfolio.dismissed")
    if (length(tickers))
      DBI::dbExecute(con, "INSERT INTO portfolio.dismissed (ticker) VALUES ($1)",
                     params = list(tickers))
  })
  invisible(tickers)
}

# One snapshot per (ticker, as_of, hz): re-Generating the same day refreshes the
# row, new days accumulate -> the Buy->Hold->Sell trail per position.
db_upsert_state_history <- function(con, rows) {
  if (is.null(rows) || !nrow(rows)) return(invisible())
  DBI::dbExecute(con, "
    INSERT INTO portfolio.state_history (ticker, as_of, hz, state, why, grade)
    VALUES ($1,$2::date,$3,$4,$5,$6)
    ON CONFLICT (ticker, as_of, hz) DO UPDATE
      SET state = EXCLUDED.state, why = EXCLUDED.why,
          grade = EXCLUDED.grade, captured_at = now()",
    params = list(toupper(as.character(rows$ticker)), as.character(rows$as_of),
                  as.integer(rows$hz), as.character(rows$state),
                  .pf_nz(rows$why), suppressWarnings(as.numeric(rows$grade))))
  invisible()
}

# every month between start and stop on day-of-month `dom` (clamped to 28 so the
# day exists in every month; weekends/holidays are resolved to the next trading
# bar by the fill lookup in LC_PORTFOLIO_SQL, so only the calendar date matters).
schedule_monthly <- function(start, stop_d, dom) {
  start <- as.Date(start)
  if (is.na(dom) || dom < 1) dom <- as.integer(format(start, "%d"))
  dom <- min(max(as.integer(dom), 1L), 28L)
  months <- seq(as.Date(format(start, "%Y-%m-01")),
                as.Date(format(stop_d, "%Y-%m-01")), by = "month")
  d <- as.Date(sprintf("%s-%02d", format(months, "%Y-%m"), dom))
  # Always buy on the start date (the first contribution), then on day-of-month
  # after. Without the start buy, a plan begun mid-month shows nothing until the
  # dom rolls around next month - so a freshly added position priced empty.
  sort(unique(c(start, d)))
}

# expand saved position rows into (ticker, date, amount) buy events from
# start_date to `today`, honoring cadence + optional end_date. Returns char dates.
expand_schedule <- function(positions, today = Sys.Date()) {
  empty <- data.frame(ticker = character(0), d = character(0),
                      amount = numeric(0), stringsAsFactors = FALSE)
  if (is.null(positions) || nrow(positions) == 0) return(empty)
  out <- list()
  for (i in seq_len(nrow(positions))) {
    p <- positions[i, ]
    start <- suppressWarnings(as.Date(as.character(p$start_date)))
    if (is.na(start)) next
    end_raw <- as.character(p$end_date)
    stop_d <- if (!is.na(p$end_date) && nzchar(end_raw))
                min(as.Date(end_raw), today) else today
    if (start > stop_d) next
    amt <- as.numeric(p$amount_usd); tk <- toupper(trimws(as.character(p$ticker)))
    if (is.na(amt) || amt <= 0 || !nzchar(tk)) next
    dates <- switch(as.character(p$cadence),
      "once"        = start,
      "monthly"     = schedule_monthly(start, stop_d, p$day1),
      "semimonthly" = sort(unique(c(schedule_monthly(start, stop_d, p$day1),
                                    schedule_monthly(start, stop_d, p$day2)))),
      start)
    dates <- dates[dates >= start & dates <= stop_d]
    if (length(dates)) out[[length(out) + 1]] <- data.frame(
      ticker = tk, d = format(dates, "%Y-%m-%d"), amount = amt,
      stringsAsFactors = FALSE)
  }
  if (!length(out)) return(empty)
  do.call(rbind, out)
}

# Wind-down windows per sell reason. n_bars = trading-bar window for the runup
# distribution AND the ladder's fill horizon; w_days = the calendar deadline.
LC_WINDDOWN <- list(
  gate    = list(n_bars = 20L, w_days = 28L),   # gate-flip SELL: 4 weeks
  matured = list(n_bars = 40L, w_days = 56L))   # matured / washed-out: 8 weeks
LC_SIGNAL_MAX_AGE <- 10L   # calendar days a BUY run stays actionable

# expand_schedule, but model-linked rows only fire a buy when the model still
# said BUY around that date: the ticker must have a BUY row on the most recent
# ledger run on/before the buy date, and that run within LC_SIGNAL_MAX_AGE days.
# This single filter yields pause (name off the buy list), resume (BUY returns),
# buy-off during a wind-down, and a fresh accumulation leg after re-entry.
# Manual rows are unchanged. Model rows need `led`; without it they contribute
# nothing and their tickers land in attr(out, "model_skipped").
# model_start picks the tracking basis for MODEL rows only:
#   "epoch"  - strategy view: track from the regime epoch regardless of stored
#              start, so a holding shows the model's real multi-week record.
#   "stored" - personal view: from the stored (add) date, clamped to the epoch,
#              so a holding shows only the user's own cash flows.
gated_expand_schedule <- function(positions, led = NULL, today = Sys.Date(),
                                  epoch = as.Date(LEDGER_EPOCH),
                                  model_start = c("epoch", "stored")) {
  model_start <- match.arg(model_start)
  empty <- data.frame(ticker = character(0), d = character(0),
                      amount = numeric(0), stringsAsFactors = FALSE)
  if (is.null(positions) || nrow(positions) == 0) {
    attr(empty, "model_skipped") <- character(0); return(empty)
  }
  mode <- if ("mode" %in% names(positions)) positions$mode else rep("manual", nrow(positions))
  mode[is.na(mode) | !nzchar(mode)] <- "manual"
  man <- positions[mode != "model", , drop = FALSE]
  mod <- positions[mode == "model", , drop = FALSE]
  out <- expand_schedule(man, today)
  skipped <- character(0)
  if (nrow(mod)) {
    if (is.null(led) || !nrow(led)) {
      skipped <- unique(toupper(trimws(as.character(mod$ticker))))
    } else {
      run_dates <- sort(unique(as.Date(led$d)))
      buy_keys  <- paste(toupper(led$ticker),
                         format(as.Date(led$d)))[led$global_action == "BUY"]
      parts <- list()
      for (i in seq_len(nrow(mod))) {
        r <- mod[i, , drop = FALSE]
        if (model_start == "epoch") {
          r$start_date <- format(epoch)
        } else {
          st <- suppressWarnings(as.Date(as.character(r$start_date)))
          r$start_date <- format(max(c(st, epoch), na.rm = TRUE))
        }
        cand <- expand_schedule(r, today)
        if (!nrow(cand)) next
        cd   <- as.Date(cand$d)
        idx  <- findInterval(cd, run_dates)                 # latest run <= D (0 if none)
        keep <- idx >= 1 &
                as.numeric(cd - run_dates[pmax(idx, 1L)]) <= LC_SIGNAL_MAX_AGE &
                paste(toupper(cand$ticker), format(run_dates[pmax(idx, 1L)])) %in% buy_keys
        if (any(keep)) parts[[length(parts) + 1]] <- cand[keep, , drop = FALSE]
      }
      if (length(parts)) out <- rbind(out, do.call(rbind, parts))
    }
  }
  attr(out, "model_skipped") <- skipped
  out
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

/* Portfolio + transition tables: readable body text. No !important on the color
   so the formatStyle-colored State/Current cells keep their inline green/amber/red. */
#lcPosTable table.dataTable tbody td,
#lcTransitionsTable table.dataTable tbody td { color: #e2e8f0; }
#lcPosTable table.dataTable thead th,
#lcTransitionsTable table.dataTable thead th { color: #94a3b8 !important; }
#lcPosTable table.dataTable tbody tr:hover td,
#lcTransitionsTable table.dataTable tbody tr:hover td { background: rgba(56,189,248,0.08) !important; }
#lcPosTable table.dataTable tbody tr.selected td { background: rgba(56,189,248,0.22) !important; }
/* row-select checkboxes: accent-tinted, centered, compact select column */
#lcPosTable td.pf-sel, #lcPosTable th.pf-sel { width: 30px; text-align: center; padding-left: 4px; padding-right: 4px; }
#lcPosTable input.pfrow, #lcPosTable input.pfall { accent-color: #38bdf8; cursor: pointer; width: 15px; height: 15px; vertical-align: middle; }

/* Global loading progress bar: an estimated-progress overlay shown whenever
   Shiny is busy (Connect, Generate, any heavy render), on every tab. Real query
   progress isn't reported, so the fill trickles toward ~90% over the expected
   load time, then snaps to 100% on idle. Toggled by the shiny:busy/idle handler. */
#global-spinner {
  position: fixed; inset: 0; z-index: 99999;
  display: none; flex-direction: column; align-items: center; justify-content: center;
  gap: 14px; background: rgba(2, 6, 23, 0.55);
}
#global-spinner.gs-on { display: flex; }
#gs-track {
  width: 320px; max-width: 60vw; height: 8px;
  background: rgba(148, 163, 184, 0.2); border-radius: 999px; overflow: hidden;
  box-shadow: 0 0 0 1px rgba(148, 163, 184, 0.15);
}
#gs-fill {
  height: 100%; width: 0%;
  background: linear-gradient(90deg, #38bdf8, #22d3ee);
  border-radius: 999px; transition: width 0.35s ease-out;
}
#gs-label {
  color: #cbd5e1; font-size: 0.8rem; font-family: 'Inter', sans-serif; letter-spacing: 0.02em;
}
"

# ─── Single source of truth for DB endpoints ───
# Every environment's host/port/user/dbname/sslmode lives here and NOWHERE else
# (previously duplicated across widget defaults, the get_con dbname map, the
# sslmode host-regex, and the env-switcher). Env vars override each value so the
# same code serves local dev and the prod server without editing this table;
# `pass_env` names the env var(s) the password field is seeded from (first
# non-empty wins). Production reads the bespoke PROD_DB_* overrides if present,
# else falls back to the generic DB_* vars that the rest of the stack and the
# prod server actually set -- so Host/User/Password autopopulate on the server
# too (it sets only DB_*; that gap left the server's Host + Password blank).
getenv_any <- function(names, default = "") {
  for (n in names) { v <- Sys.getenv(n, ""); if (nzchar(v)) return(v) }
  default
}
DB_ENVIRONMENTS <- list(
  Production = list(host = getenv_any(c("PROD_DB_HOST", "DB_HOST")),
                    port = getenv_any(c("PROD_DB_PORT", "DB_PORT"), "25060"),
                    user = getenv_any(c("PROD_DB_USER", "DB_USER"), "doadmin"),
                    dbname = getenv_any(c("PROD_DB_NAME", "DB_DATABASE"), "prod"),
                    sslmode = "require",
                    pass_env = c("PROD_DB_PASSWORD", "DB_PASSWORD")),
  Staging    = list(host = Sys.getenv("LOCAL_DB_HOST", "host.docker.internal"),
                    port = Sys.getenv("LOCAL_DB_PORT", "5432"),
                    user = Sys.getenv("LOCAL_DB_USER", "postgres"),
                    dbname = "staging", sslmode = "prefer", pass_env = "DB_PASSWORD"),
  Dev        = list(host = Sys.getenv("LOCAL_DB_HOST", "host.docker.internal"),
                    port = Sys.getenv("LOCAL_DB_PORT", "5432"),
                    user = Sys.getenv("LOCAL_DB_USER", "postgres"),
                    dbname = "dev", sslmode = "prefer", pass_env = "DB_PASSWORD"))
DB_ENV_DEFAULT <- "Production"

# ─── Helper: the ONE shared connection bar (rendered once in the navbar header) ───
# Replaces the old per-tab connection forms: pick env, type the password once,
# click Connect once, and every tab reads these global inputs.
connection_bar <- function() {
  d <- DB_ENVIRONMENTS[[DB_ENV_DEFAULT]]
  div(class = "conn-bar",
      style = paste("display:flex; flex-wrap:wrap; align-items:flex-end; gap:0.6rem;",
                    "padding:0.6rem 0.9rem; margin:0 0 0.5rem; background:rgba(255,255,255,0.03);",
                    "border:1px solid #1e293b; border-radius:6px;"),
    div(style = "min-width:130px;",
        selectInput("db_env", "Environment",
                    choices = names(DB_ENVIRONMENTS), selected = DB_ENV_DEFAULT, width = "100%")),
    div(style = "min-width:210px; flex:1 1 210px;",
        textInput("db_host", "Host", value = d$host, width = "100%")),
    div(style = "width:90px;",  numericInput("db_port", "Port", value = as.integer(d$port), min = 1, max = 65535, width = "100%")),
    div(style = "width:120px;", textInput("db_user", "User", value = d$user, width = "100%")),
    div(style = "min-width:150px;", passwordInput("db_pass", "Password", value = "", width = "100%")),
    div(actionButton("connect_btn", "Connect", class = "btn-primary")),
    div(style = "flex:1 1 200px; color:#94a3b8; font-size:0.8rem; padding-bottom:0.4rem;",
        textOutput("statusMessageConn", inline = TRUE)))
}

# ─── Helper: sidebar panel for a given tab suffix ───
# Connection widgets now live in the shared connection_bar(); the sidebar holds
# only this tab's filters, its Generate button, optional post-Generate refiners,
# and its own load-status line.
make_sidebar <- function(suffix, title, filter_widgets, post_widgets = NULL) {
  sidebarPanel(
    h4(title),
    h5("Filters"),
    div(id = paste0("filter_panel", suffix), filter_widgets),
    hr(),
    actionButton(paste0("execute_", suffix), "Generate Chart", class = "btn-primary w-100", style = "margin-top: 1rem;"),
    # post_widgets render UNDER Generate: controls that refine already-loaded
    # data (cluster id filter, rank range) belong after the button, not mixed
    # in with the pre-Generate query params (Kevin 2026-07-25).
    if (!is.null(post_widgets)) tagList(
      hr(),
      div(id = paste0("post_panel", suffix), post_widgets)
    ),
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

# ─── Forecast tab SQL ───
# Backtest growth curve: the 8-strategy x 12-vintage $10/mo DCA backtest of the
# trust-gated top-picks basket, averaged to ONE cumulative return per horizon
# (12/18/24/30/36 mo) plus the same money's SPY leg. ~10s on prod. The (0,0)
# anchor and the ledger projection are added in R, not here.
FORECAST_CURVE_SQL <- "
WITH vintages AS (
    SELECT unnest(ARRAY[
        DATE '2013-07-01', DATE '2014-07-01', DATE '2015-07-01', DATE '2016-07-01',
        DATE '2017-07-01', DATE '2018-07-01', DATE '2019-07-01', DATE '2020-07-01',
        DATE '2021-07-01', DATE '2022-07-01', DATE '2023-07-01', DATE '2024-07-01'
    ]) AS start_d),
mkt AS (SELECT MAX(date) AS max_d FROM cdm.ingest_combined WHERE ticker='SPY'),
horizons AS (SELECT unnest(ARRAY[12,18,24,30,36]) AS h),
vcut AS (SELECT v.start_d, (SELECT MAX(t.train_cutoff_date) FROM validation.walk_forward_top_picks t WHERE t.train_cutoff_date <= v.start_d) AS c FROM vintages v),
picks AS (SELECT vc.start_d, vc.c, t.ticker FROM vcut vc JOIN validation.walk_forward_top_picks t ON t.train_cutoff_date=vc.c),
mons AS (SELECT generate_series(0,11) AS m),
spy_hist AS (SELECT date, adj_close, adj_close/lag(adj_close) OVER (ORDER BY date) - 1 AS ret,
               CASE WHEN adj_close >= AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS above200
             FROM cdm.ingest_combined WHERE ticker='SPY'
               AND date >= (SELECT MIN(start_d) FROM vintages) - INTERVAL '400 days'
               AND date <= (SELECT MAX(start_d) FROM vintages) + INTERVAL '13 months'),
spy_drops AS (SELECT start_d, date FROM (
      SELECT v.start_d, s.date, ROW_NUMBER() OVER (PARTITION BY v.start_d ORDER BY s.ret ASC) AS rn
      FROM vintages v JOIN spy_hist s ON s.ret IS NOT NULL AND s.date BETWEEN v.start_d AND (v.start_d + INTERVAL '12 months')) z WHERE rn <= 10),
month_regime AS (SELECT md.start_d, md.d, (SELECT s.above200 FROM spy_hist s WHERE s.date >= md.d ORDER BY s.date LIMIT 1) AS above
      FROM (SELECT DISTINCT v.start_d, (v.start_d + (mo.m||' months')::interval)::date AS d FROM vintages v CROSS JOIN mons mo) md),
sched AS (
    SELECT 'A) MONTHLY'::text AS s, p.start_d, p.ticker, (p.start_d + (mo.m||' months')::interval)::date AS d, 10.0 AS amt FROM picks p CROSS JOIN mons mo
    UNION ALL SELECT 'B) SPY DIP', p.start_d, p.ticker, (sd.date + 10)::date, 10.0 FROM picks p JOIN spy_drops sd ON sd.start_d=p.start_d
    UNION ALL SELECT 'C) LUMP', p.start_d, p.ticker, p.start_d, 120.0 FROM picks p
    UNION ALL SELECT 'D) FRONT 6MO', p.start_d, p.ticker, (p.start_d + (mo.m||' months')::interval)::date, 20.0 FROM picks p CROSS JOIN mons mo WHERE mo.m < 6
    UNION ALL SELECT 'E) QUARTERLY', p.start_d, p.ticker, (p.start_d + (mo.m||' months')::interval)::date, 30.0 FROM picks p CROSS JOIN mons mo WHERE mo.m % 3 = 0
    UNION ALL SELECT 'F) MON+200DMA', p.start_d, p.ticker, (p.start_d + (mo.m||' months')::interval)::date, 10.0
       FROM picks p CROSS JOIN mons mo
       WHERE EXISTS (SELECT 1 FROM month_regime mr WHERE mr.start_d=p.start_d AND mr.d=(p.start_d + (mo.m||' months')::interval)::date AND mr.above=1)),
buy_prices AS (SELECT bn.ticker, bn.d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=bn.ticker AND i.date>=bn.d ORDER BY i.date LIMIT 1) AS px FROM (SELECT DISTINCT ticker, d FROM sched) bn),
spy_buy AS (SELECT x.d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker='SPY' AND i.date>=x.d ORDER BY i.date LIMIT 1) AS spy_px FROM (SELECT DISTINCT d FROM sched) x),
priced AS (SELECT b.s, b.start_d, b.ticker, b.amt, bp.px AS buy_px, sb.spy_px FROM sched b JOIN buy_prices bp ON bp.ticker=b.ticker AND bp.d=b.d JOIN spy_buy sb ON sb.d=b.d),
acc AS (SELECT s, start_d, ticker,
      SUM(CASE WHEN buy_px>0 THEN amt/buy_px ELSE 0 END) AS sh, SUM(CASE WHEN buy_px>0 THEN amt ELSE 0 END) AS inv,
      SUM(CASE WHEN buy_px>0 AND spy_px>0 THEN amt/spy_px ELSE 0 END) AS spy_sh FROM priced GROUP BY s, start_d, ticker),
ax AS (SELECT a.*, hz.h, (a.start_d + (hz.h||' months')::interval)::date AS exit_d,
              ((a.start_d + (hz.h||' months')::interval)::date <= (SELECT max_d FROM mkt)) AS valid FROM acc a CROSS JOIN horizons hz),
final_prices AS (SELECT fn.ticker, fn.exit_d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=fn.ticker AND i.date<=fn.exit_d ORDER BY i.date DESC LIMIT 1) AS px FROM (SELECT DISTINCT ticker, exit_d FROM ax WHERE valid) fn),
spy_finals AS (SELECT y.exit_d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker='SPY' AND i.date<=y.exit_d ORDER BY i.date DESC LIMIT 1) AS px FROM (SELECT DISTINCT exit_d FROM ax WHERE valid) y),
per AS (
    SELECT ax.s, ax.start_d, ax.h,
      100*(SUM(ax.sh*fp.px)/NULLIF(SUM(ax.inv),0)-1)     AS port_ret,
      100*(SUM(ax.spy_sh*sf.px)/NULLIF(SUM(ax.inv),0)-1) AS spy_ret
    FROM ax JOIN final_prices fp ON fp.ticker=ax.ticker AND fp.exit_d=ax.exit_d
            JOIN spy_finals sf ON sf.exit_d=ax.exit_d
    WHERE ax.valid GROUP BY ax.s, ax.start_d, ax.h)
SELECT p.h AS horizon_months,
       ROUND(AVG(p.port_ret)::numeric,2)             AS all_strategies_ret_pct,
       ROUND(AVG(p.spy_ret)::numeric,2)              AS spy_ret_pct,
       ROUND(AVG(p.port_ret - p.spy_ret)::numeric,2) AS beat_pct,
       COUNT(*)                                      AS n_cells
FROM per p GROUP BY p.h ORDER BY p.h;"

# LEDGER REGIME EPOCH. monitoring.prediction_ledger rows before this date were
# produced by a BUY gate that was replaced on 2026-07-02 (elt-inference-models
# b650cb2) because it emitted ZERO BUYs and its in-sample screen was measured
# ANTI-correlated with realized IC (CORR -0.72). The record either side of that
# boundary is two different systems: BUYs run 63 -> 19 -> 0 through Jul 1, then
# 0 -> 572 overnight, while SELLs stay flat (~170-186) because only the long gate
# changed that night. Jul 2's batch was hand-run at 00:30 UTC, 54 minutes BEFORE
# the commit landed, so Jul 3 07:46 is the first scheduled run on committed code
# and is the epoch. Nothing that averages runs or grades forward performance may
# span this date. Smaller boundaries are NOT floored (they would leave 14 usable
# days): Jul 5 IC tuning settles, Jul 18 the SELL gate becomes performance-based,
# Jul 23 evidence_status/buy_weight_mature begin.
# Deliberately NOT applied to: the 30-day majority window (already clears it and
# always will) and the as-of replay (time travel: it must show what was actually
# said on the date the user picks).
LEDGER_EPOCH <- "2026-07-03"

# Live ledger basket = the earliest recorded BUY snapshot at/after the regime
# epoch (2026-07-03; pre-epoch picks came from the retired gate), equal-weighted,
# graded to the latest bar off the CURRENT series so a post-entry split can't
# re-base it (same split-safe rule the Buy List ledger replay uses).
# Expect entry_d to read EARLIER than the epoch (2026-06-23 at the time of
# writing) and the per-name entry prices to be non-uniform. That is the ledger's
# entry_date, i.e. the last price bar available when the call was logged, and
# prediction_ledger admits names whose last bar is up to 10 days old. The Jun 16
# anchor hid this because all 63 of its names happened to price at Jun 15; the
# Jul 3 anchor spreads Jun 23 - Jul 2. Pre-existing and out of scope here (Kevin:
# the price dating is by design) - but it means the basket banks a few days of
# drift before the call, so read the first days of this chart loosely.
FORECAST_LEDGER_SQL <- gsub("__EPOCH__", LEDGER_EPOCH, "
WITH first_snap AS (
    SELECT ticker, entry_date
    FROM monitoring.prediction_ledger
    WHERE prediction_date = (SELECT MIN(prediction_date)
                             FROM monitoring.prediction_ledger
                             WHERE prediction_date >= '__EPOCH__')
      AND global_action = 'BUY'
),
entry_now AS (
    SELECT ic.ticker, ic.adj_close AS entry_px
    FROM cdm.ingest_combined ic
    JOIN first_snap k ON k.ticker = ic.ticker AND k.entry_date = ic.date
),
now_px AS (
    SELECT DISTINCT ON (ticker) ticker, adj_close AS px_now
    FROM cdm.ingest_combined
    WHERE ticker IN (SELECT ticker FROM first_snap)
      AND date >= (SELECT MIN(entry_date) FROM first_snap) - INTERVAL '10 days'
    ORDER BY ticker, date DESC
),
spy AS (
    SELECT
      (SELECT adj_close FROM cdm.ingest_combined WHERE ticker='SPY'
         AND date = (SELECT MIN(entry_date) FROM first_snap)) AS spy_entry,
      (SELECT adj_close FROM cdm.ingest_combined WHERE ticker='SPY'
         ORDER BY date DESC LIMIT 1) AS spy_now
)
SELECT
  COUNT(*) AS n_names,
  ROUND((AVG((n.px_now/NULLIF(e.entry_px,0))-1)*100)::numeric, 2) AS basket_ret_pct,
  ROUND(((SELECT spy_now FROM spy)/(SELECT spy_entry FROM spy)-1)::numeric*100, 2) AS spy_ret_pct,
  (SELECT MIN(entry_date) FROM first_snap)::text AS entry_d,
  ROUND((( (SELECT MAX(date) FROM cdm.ingest_combined WHERE ticker='SPY')
          - (SELECT MIN(entry_date) FROM first_snap) )/30.0)::numeric, 2) AS months_held
FROM first_snap f
JOIN entry_now e ON e.ticker = f.ticker
JOIN now_px n ON n.ticker = f.ticker;", fixed = TRUE)

# Live all-strategy portfolio, REAL calendar monthly series. Buys the CURRENT
# picks (latest cutoff) starting at __ANCHOR__ via the 6-strategy DCA, values the
# accumulated shares at each month-end, averages the strategies, and returns the
# same money's SPY leg beside it. __ANCHOR__ is replaced (gsub) with the lookback
# start date in R. ~7s on prod. Latest cutoff (2024-12-31) is <= any anchor we
# use, so buying its picks in the past is knowledge the model already had.
FORECAST_LIVE_SQL <- "
WITH params AS (SELECT DATE '__ANCHOR__' AS start_d),
cutoffp AS (SELECT MAX(train_cutoff_date) AS cutoff FROM validation.walk_forward_top_picks, params p WHERE train_cutoff_date <= p.start_d),
maxd AS (SELECT MAX(date) AS md FROM cdm.ingest_combined WHERE ticker='SPY'),
picks AS (SELECT t.ticker FROM validation.walk_forward_top_picks t, cutoffp c WHERE t.train_cutoff_date = c.cutoff __IDFILTER__),
mons AS (SELECT generate_series(0,11) AS m),
spy_hist AS (SELECT date, adj_close, adj_close/lag(adj_close) OVER (ORDER BY date) - 1 AS ret,
               CASE WHEN adj_close >= AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS above200
             FROM cdm.ingest_combined WHERE ticker='SPY'
               AND date >= (SELECT start_d FROM params) - INTERVAL '400 days'
               AND date <= (SELECT start_d FROM params) + INTERVAL '13 months'),
spy_drops AS (SELECT date FROM (
      SELECT s.date, ROW_NUMBER() OVER (ORDER BY s.ret ASC) AS rn
      FROM spy_hist s, params p WHERE s.ret IS NOT NULL AND s.date BETWEEN p.start_d AND (p.start_d + INTERVAL '12 months')) z WHERE rn <= 10),
month_regime AS (SELECT md.d, (SELECT s.above200 FROM spy_hist s WHERE s.date >= md.d ORDER BY s.date LIMIT 1) AS above
      FROM (SELECT (p.start_d + (mo.m||' months')::interval)::date AS d FROM params p CROSS JOIN mons mo) md),
sched AS (
    SELECT 'A) MONTHLY'::text AS s, pk.ticker, (p.start_d + (mo.m||' months')::interval)::date AS d, 10.0 AS amt FROM picks pk CROSS JOIN mons mo, params p
    UNION ALL SELECT 'B) SPY DIP', pk.ticker, (sd.date + 10)::date, 10.0 FROM picks pk CROSS JOIN spy_drops sd
    UNION ALL SELECT 'C) LUMP', pk.ticker, p.start_d, 120.0 FROM picks pk, params p
    UNION ALL SELECT 'D) FRONT 6MO', pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 20.0 FROM picks pk CROSS JOIN mons mo, params p WHERE mo.m < 6
    UNION ALL SELECT 'E) QUARTERLY', pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 30.0 FROM picks pk CROSS JOIN mons mo, params p WHERE mo.m % 3 = 0
    UNION ALL SELECT 'F) MON+200DMA', pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 10.0
       FROM picks pk CROSS JOIN mons mo, params p
       WHERE EXISTS (SELECT 1 FROM month_regime mr WHERE mr.d=(p.start_d + (mo.m||' months')::interval)::date AND mr.above=1)),
buy_prices AS (SELECT bn.ticker, bn.d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=bn.ticker AND i.date>=bn.d ORDER BY i.date LIMIT 1) AS px FROM (SELECT DISTINCT ticker, d FROM sched) bn),
priced_buys AS (SELECT sc.s, sc.ticker, sc.d AS buy_d, sc.amt, sc.amt/NULLIF(bp.px,0) AS shares FROM sched sc JOIN buy_prices bp ON bp.ticker=sc.ticker AND bp.d=sc.d WHERE bp.px>0),
vdates AS (SELECT generate_series(p.start_d, LEAST((p.start_d + INTERVAL '__HOLD__ months'), (SELECT md FROM maxd)), INTERVAL '1 month')::date AS d FROM params p),
val_prices AS (SELECT k.ticker, k.vdate, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=k.ticker AND i.date<=k.vdate ORDER BY i.date DESC LIMIT 1) AS px
      FROM (SELECT DISTINCT pb.ticker, vd.d AS vdate FROM priced_buys pb CROSS JOIN vdates vd) k),
holdings AS (SELECT pb.s, pb.ticker, vd.d AS vdate, SUM(pb.shares) AS shares, SUM(pb.amt) AS invested
      FROM priced_buys pb CROSS JOIN vdates vd WHERE pb.buy_d <= vd.d GROUP BY pb.s, pb.ticker, vd.d),
valued AS (SELECT h.s, h.vdate, SUM(h.shares*vp.px) AS value, SUM(h.invested) AS invested
      FROM holdings h JOIN val_prices vp ON vp.ticker=h.ticker AND vp.vdate=h.vdate GROUP BY h.s, h.vdate),
per_strat AS (SELECT s, vdate, 100*(value/NULLIF(invested,0)-1) AS ret FROM valued),
spy_base AS (SELECT adj_close AS px0 FROM cdm.ingest_combined, params p WHERE ticker='SPY' AND date>=p.start_d ORDER BY date LIMIT 1),
spy_val AS (SELECT vd.d AS vdate, (SELECT adj_close FROM cdm.ingest_combined WHERE ticker='SPY' AND date<=vd.d ORDER BY date DESC LIMIT 1) AS px FROM vdates vd)
SELECT ps.vdate::text AS vdate,
       ROUND(AVG(ps.ret)::numeric,2) AS portfolio_pct,
       ROUND(((sv.px/(SELECT px0 FROM spy_base))-1)::numeric*100,2) AS spy_pct
FROM per_strat ps JOIN spy_val sv ON sv.vdate=ps.vdate
GROUP BY ps.vdate, sv.px ORDER BY ps.vdate;"

# Per-id decomposition: the SAME 6-strategy DCA as the portfolio line, but grouped
# by cluster id (all ids, no filter) -> one monthly return path per id. Drawn as
# faint grey dashed lines (one per id, id labelled at the right end) when EVERY id
# is selected. Same anchor/hold placeholders as the live query. ~0.7s.
FORECAST_PERID_SQL <- "
WITH params AS (SELECT DATE '__ANCHOR__' AS start_d),
cutoffp AS (SELECT MAX(train_cutoff_date) AS cutoff FROM validation.walk_forward_top_picks, params p WHERE train_cutoff_date <= p.start_d),
maxd AS (SELECT MAX(date) AS md FROM cdm.ingest_combined WHERE ticker='SPY'),
picks AS (SELECT t.ticker, t.id FROM validation.walk_forward_top_picks t, cutoffp c WHERE t.train_cutoff_date = c.cutoff),
mons AS (SELECT generate_series(0,11) AS m),
spy_hist AS (SELECT date, adj_close, adj_close/lag(adj_close) OVER (ORDER BY date) - 1 AS ret,
               CASE WHEN adj_close >= AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS above200
             FROM cdm.ingest_combined WHERE ticker='SPY'
               AND date >= (SELECT start_d FROM params) - INTERVAL '400 days'
               AND date <= (SELECT start_d FROM params) + INTERVAL '13 months'),
spy_drops AS (SELECT date FROM (
      SELECT s.date, ROW_NUMBER() OVER (ORDER BY s.ret ASC) AS rn
      FROM spy_hist s, params p WHERE s.ret IS NOT NULL AND s.date BETWEEN p.start_d AND (p.start_d + INTERVAL '12 months')) z WHERE rn <= 10),
month_regime AS (SELECT md.d, (SELECT s.above200 FROM spy_hist s WHERE s.date >= md.d ORDER BY s.date LIMIT 1) AS above
      FROM (SELECT (p.start_d + (mo.m||' months')::interval)::date AS d FROM params p CROSS JOIN mons mo) md),
sched AS (
    SELECT 'A) MONTHLY'::text AS s, pk.id, pk.ticker, (p.start_d + (mo.m||' months')::interval)::date AS d, 10.0 AS amt FROM picks pk CROSS JOIN mons mo, params p
    UNION ALL SELECT 'B) SPY DIP', pk.id, pk.ticker, (sd.date + 10)::date, 10.0 FROM picks pk CROSS JOIN spy_drops sd
    UNION ALL SELECT 'C) LUMP', pk.id, pk.ticker, p.start_d, 120.0 FROM picks pk, params p
    UNION ALL SELECT 'D) FRONT 6MO', pk.id, pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 20.0 FROM picks pk CROSS JOIN mons mo, params p WHERE mo.m < 6
    UNION ALL SELECT 'E) QUARTERLY', pk.id, pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 30.0 FROM picks pk CROSS JOIN mons mo, params p WHERE mo.m % 3 = 0
    UNION ALL SELECT 'F) MON+200DMA', pk.id, pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 10.0
       FROM picks pk CROSS JOIN mons mo, params p
       WHERE EXISTS (SELECT 1 FROM month_regime mr WHERE mr.d=(p.start_d + (mo.m||' months')::interval)::date AND mr.above=1)),
buy_prices AS (SELECT bn.ticker, bn.d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=bn.ticker AND i.date>=bn.d ORDER BY i.date LIMIT 1) AS px FROM (SELECT DISTINCT ticker, d FROM sched) bn),
priced_buys AS (SELECT sc.s, sc.id, sc.ticker, sc.d AS buy_d, sc.amt, sc.amt/NULLIF(bp.px,0) AS shares FROM sched sc JOIN buy_prices bp ON bp.ticker=sc.ticker AND bp.d=sc.d WHERE bp.px>0),
vdates AS (SELECT generate_series(p.start_d, LEAST((p.start_d + INTERVAL '__HOLD__ months'), (SELECT md FROM maxd)), INTERVAL '1 month')::date AS d FROM params p),
val_prices AS (SELECT k.ticker, k.vdate, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=k.ticker AND i.date<=k.vdate ORDER BY i.date DESC LIMIT 1) AS px
      FROM (SELECT DISTINCT pb.ticker, vd.d AS vdate FROM priced_buys pb CROSS JOIN vdates vd) k),
holdings AS (SELECT pb.s, pb.id, pb.ticker, vd.d AS vdate, SUM(pb.shares) AS shares, SUM(pb.amt) AS invested
      FROM priced_buys pb CROSS JOIN vdates vd WHERE pb.buy_d <= vd.d GROUP BY pb.s, pb.id, pb.ticker, vd.d),
valued AS (SELECT h.s, h.id, h.vdate, SUM(h.shares*vp.px) AS value, SUM(h.invested) AS invested
      FROM holdings h JOIN val_prices vp ON vp.ticker=h.ticker AND vp.vdate=h.vdate GROUP BY h.s, h.id, h.vdate),
per_strat AS (SELECT s, id, vdate, 100*(value/NULLIF(invested,0)-1) AS ret FROM valued)
SELECT ps.vdate::text AS vdate, ps.id::int AS id, ROUND(AVG(ps.ret)::numeric,2) AS ret_pct
FROM per_strat ps GROUP BY ps.id, ps.vdate ORDER BY ps.id, ps.vdate;"

# Live ledger vs Benchmark on the ledger's OWN clock: daily cumulative return of the
# epoch BUY basket (equal-weight, split-safe current-series entry) and the same-day
# SPY, both rebased to 0 at the basket's entry date. Fast (~0.1s). This is the fair
# out-of-sample view - the ledger is weeks old and cannot share the 2-year calendar
# axis with a portfolio that has compounded since 2024. Anchored at LEDGER_EPOCH,
# not the ledger's first row: the pre-epoch snapshots were picked by the retired
# gate, so charting them would grade a system that no longer exists.
FORECAST_LEDGER_SERIES_SQL <- gsub("__EPOCH__", LEDGER_EPOCH, "
WITH first_snap AS (
    SELECT ticker, entry_date FROM monitoring.prediction_ledger
    WHERE prediction_date = (SELECT MIN(prediction_date)
                             FROM monitoring.prediction_ledger
                             WHERE prediction_date >= '__EPOCH__')
      AND global_action = 'BUY'
),
entry_now AS (SELECT ic.ticker, ic.adj_close AS entry_px
    FROM cdm.ingest_combined ic JOIN first_snap k ON k.ticker=ic.ticker AND k.entry_date=ic.date),
dates AS (SELECT DISTINCT date AS d FROM cdm.ingest_combined
    WHERE date >= (SELECT MIN(entry_date) FROM first_snap)
      AND date <= (SELECT MAX(date) FROM cdm.ingest_combined WHERE ticker='SPY') AND ticker='SPY'),
px AS (SELECT en.ticker, d.d, en.entry_px,
        (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=en.ticker AND i.date<=d.d ORDER BY i.date DESC LIMIT 1) AS px
       FROM entry_now en CROSS JOIN dates d),
spy AS (SELECT d.d,
        (SELECT adj_close FROM cdm.ingest_combined WHERE ticker='SPY' AND date<=d.d ORDER BY date DESC LIMIT 1) AS px,
        (SELECT adj_close FROM cdm.ingest_combined WHERE ticker='SPY' AND date=(SELECT MIN(entry_date) FROM first_snap)) AS px0
       FROM dates d)
SELECT p.d::text AS d,
       ROUND((AVG(p.px/NULLIF(p.entry_px,0)-1)*100)::numeric,2) AS ledger_pct,
       ROUND(((s.px/s.px0)-1)::numeric*100,2) AS spy_pct
FROM px p JOIN spy s ON s.d=p.d
GROUP BY p.d, s.px, s.px0 ORDER BY p.d;", fixed = TRUE)

# Lifecycle qualstream comparison: equal-weight return of the CURRENT BUYs that
# qualstream graded (latest non-vetoed scorecard) vs the >= 68 subset vs SPY,
# from the current 4-month window start (__ANCHOR__). DESCRIPTIVE, not a
# walk-forward test: qualstream's grades are a single as-of snapshot applied
# across the whole window, and it only covers the curated top-picks (a fraction
# of the gate's BUYs). Same 68 cut and 150d freshness as the board's orange +.
LC_QS_COMPARE_SQL <- "
WITH params AS (SELECT DATE '__ANCHOR__' AS start_d),
scored AS (
    -- latest NON-VETOED scorecard per ticker: veto filtered BEFORE DISTINCT ON,
    -- the same rule the board's orange + uses, so chart and board populations
    -- never diverge once multiple grade runs exist
    SELECT DISTINCT ON (ticker) ticker, overall
    FROM qual.ticker_scorecards
    WHERE NOT veto
      AND rubric_version = 'buy_decision_v1' AND as_of >= CURRENT_DATE - 150
    ORDER BY ticker, as_of DESC, graded_at DESC
),
graded_buys AS (
    -- basket = qualstream's graded standing-rec buys (the board's buy list = the
    -- orange + universe), NOT the current-day gate snapshot. The snapshot dropped
    -- names reading SKIP today that are still board buys (monthly-majority standing
    -- recs), so the green line undercounted the + (10 shown vs 15 on the board).
    SELECT s.ticker, (s.overall >= 68) AS passed FROM scored s
),
maxd AS (SELECT MAX(date) AS d FROM cdm.ingest_combined WHERE ticker = 'SPY'),
-- one indexed range scan of the basket tickers' prices over the window, reused
-- for the entry price, the per-day return, and (via SPY) the date grid. The old
-- shape did a correlated as-of subquery per ticker per day (617ms); this is a
-- plain join on SPY's trading calendar (~40ms), numerically identical.
slice AS (
    SELECT i.ticker, i.date AS d, i.adj_close AS px
    FROM cdm.ingest_combined i
    JOIN graded_buys g ON g.ticker = i.ticker
    WHERE i.date >= (SELECT start_d FROM params) AND i.date <= (SELECT d FROM maxd)
),
entry AS (SELECT DISTINCT ON (ticker) ticker, px AS entry_px FROM slice ORDER BY ticker, d),
basket AS (
    SELECT s.d, g.passed, s.px / NULLIF(e.entry_px, 0) - 1 AS ret
    FROM slice s
    JOIN graded_buys g ON g.ticker = s.ticker
    JOIN entry e ON e.ticker = s.ticker
    WHERE e.entry_px > 0
),
spy AS (
    SELECT i.date AS d, i.adj_close AS px,
      (SELECT adj_close FROM cdm.ingest_combined WHERE ticker = 'SPY'
       AND date >= (SELECT start_d FROM params) ORDER BY date LIMIT 1) AS px0
    FROM cdm.ingest_combined i
    WHERE i.ticker = 'SPY' AND i.date >= (SELECT start_d FROM params) AND i.date <= (SELECT d FROM maxd)
)
SELECT sp.d::text AS d,
       ROUND((AVG(b.ret) * 100)::numeric, 2) AS graded_pct,
       ROUND((AVG(b.ret) FILTER (WHERE b.passed) * 100)::numeric, 2) AS passed_pct,
       ROUND((((sp.px / sp.px0) - 1) * 100)::numeric, 2) AS spy_pct
FROM spy sp
LEFT JOIN basket b ON b.d = sp.d
GROUP BY sp.d, sp.px, sp.px0
ORDER BY sp.d;"

# Personal DCA portfolio value + return vs a SAME-CASH-FLOW SPY benchmark.
# __BUYS__ is a VALUES list of (ticker, buy_date, dollars) expanded in R from the
# saved positions. Each buy fills at the first adj_close on/after its date; the
# SAME dollars also buy SPY (spy_shares) so the benchmark runs the identical DCA
# (this is the FORECAST_CURVE_SQL spy_sh trick, not a lump SPY index). Holdings
# are marked to market monthly (+ today) with the as-of close; return =
# value/invested - 1. Reuses the FORECAST_LIVE_SQL share-accumulation shape.
LC_PORTFOLIO_SQL <- "
WITH buys(ticker, d, amt) AS (VALUES __BUYS__),
sells(ticker, d, frac, trig_d) AS (__SELLS__),
maxd AS (SELECT MAX(date) AS d FROM cdm.ingest_combined WHERE ticker = 'SPY'),
fills AS (
    SELECT b.ticker, b.d::date AS buy_d, b.amt::numeric AS amt,
      (SELECT i.adj_close FROM cdm.ingest_combined i
       WHERE i.ticker = b.ticker AND i.date >= b.d::date ORDER BY i.date LIMIT 1) AS px,
      (SELECT i.adj_close FROM cdm.ingest_combined i
       WHERE i.ticker = 'SPY' AND i.date >= b.d::date ORDER BY i.date LIMIT 1) AS spy_px
    FROM buys b
),
lots AS (
    SELECT ticker, buy_d, amt, amt / NULLIF(px, 0) AS shares,
           amt / NULLIF(spy_px, 0) AS spy_shares
    FROM fills WHERE px > 0 AND spy_px > 0
),
-- ladder legs: every episode fully exits (backstops + supersede), so shares
-- held at trigger k = cum bought by k minus cum bought by k-1 (telescoping LAG)
trig_leg AS (
    SELECT c.ticker, c.trig_d,
           c.cum_sh  - COALESCE(LAG(c.cum_sh)  OVER (PARTITION BY c.ticker ORDER BY c.trig_d), 0) AS leg_sh,
           c.cum_spy - COALESCE(LAG(c.cum_spy) OVER (PARTITION BY c.ticker ORDER BY c.trig_d), 0) AS leg_spy
    FROM (
        SELECT sv.ticker, sv.trig_d::date AS trig_d,
               COALESCE(SUM(l.shares), 0) AS cum_sh, COALESCE(SUM(l.spy_shares), 0) AS cum_spy
        FROM (SELECT DISTINCT ticker, trig_d FROM sells) sv
        LEFT JOIN lots l ON l.ticker = sv.ticker AND l.buy_d <= sv.trig_d::date
        GROUP BY sv.ticker, sv.trig_d::date
    ) c
),
sell_ev AS (
    SELECT s.ticker, s.d::date AS sell_d,
           tl.leg_sh  * s.frac::numeric AS sh_sold,
           tl.leg_spy * s.frac::numeric AS spy_sold,
           (SELECT i.adj_close FROM cdm.ingest_combined i
            WHERE i.ticker = s.ticker AND i.date <= s.d::date ORDER BY i.date DESC LIMIT 1) AS px,
           (SELECT i.adj_close FROM cdm.ingest_combined i
            WHERE i.ticker = 'SPY' AND i.date <= s.d::date ORDER BY i.date DESC LIMIT 1) AS spy_px
    FROM sells s JOIN trig_leg tl
      ON tl.ticker = s.ticker AND tl.trig_d = s.trig_d::date
),
vdates AS (
    SELECT d FROM generate_series(
        (SELECT MIN(buy_d) FROM lots),
        (SELECT d FROM maxd), INTERVAL '1 month') AS g(d)
    UNION SELECT (SELECT d FROM maxd)
    UNION SELECT sell_d FROM sell_ev
),
holdings AS (
    SELECT v.d AS vdate, l.ticker,
           SUM(l.shares) AS shares, SUM(l.amt) AS invested,
           SUM(l.spy_shares) AS spy_shares
    FROM vdates v JOIN lots l ON l.buy_d <= v.d
    GROUP BY v.d, l.ticker
),
sold_cum AS (
    SELECT v.d AS vdate, e.ticker,
           SUM(e.sh_sold) AS sh_sold, SUM(e.spy_sold) AS spy_sold,
           SUM(e.sh_sold * e.px) AS cash, SUM(e.spy_sold * e.spy_px) AS spy_cash
    FROM vdates v JOIN sell_ev e ON e.sell_d <= v.d
    GROUP BY v.d, e.ticker
),
valued AS (
    SELECT h.vdate, SUM(h.invested) AS invested,
           SUM(GREATEST(h.shares - COALESCE(sc.sh_sold, 0), 0) *
             (SELECT i.adj_close FROM cdm.ingest_combined i
              WHERE i.ticker = h.ticker AND i.date <= h.vdate
              ORDER BY i.date DESC LIMIT 1)) AS mkt,
           SUM(COALESCE(sc.cash, 0)) AS cash,
           SUM(GREATEST(h.spy_shares - COALESCE(sc.spy_sold, 0), 0)) AS spy_sh,
           SUM(COALESCE(sc.spy_cash, 0)) AS spy_cash
    FROM holdings h
    LEFT JOIN sold_cum sc ON sc.vdate = h.vdate AND sc.ticker = h.ticker
    GROUP BY h.vdate
)
SELECT vdate::date::text AS d,
       ROUND(invested::numeric, 2) AS invested,
       ROUND((mkt + cash)::numeric, 2) AS value,
       ROUND((100 * ((mkt + cash) / NULLIF(invested, 0) - 1))::numeric, 2) AS ret_pct,
       ROUND((spy_sh * (SELECT adj_close FROM cdm.ingest_combined
         WHERE ticker = 'SPY' AND date <= vdate ORDER BY date DESC LIMIT 1)
         + spy_cash)::numeric, 2) AS spy_value,
       ROUND((100 * ((spy_sh * (SELECT adj_close FROM cdm.ingest_combined
         WHERE ticker = 'SPY' AND date <= vdate ORDER BY date DESC LIMIT 1)
         + spy_cash) / NULLIF(invested, 0) - 1))::numeric, 2) AS spy_ret_pct
FROM valued ORDER BY vdate;"

# Per-TICKER return series for the holdings chart: one weekly cumulative-return
# line per ticker (100 * value/invested - 1) plus a single same-cash SPY line
# labelled '__SPY__'. __BUYS__ = VALUES (id, ticker, 'YYYY-MM-DD', amt), one per
# gated buy. Accumulation view (no sell wind-down); each ticker's
# line starts at 0% on its first buy. Weekly vdates keep it light but smooth; a
# brand-new position yields a single point (rendered as a marker in R).
LC_PORTFOLIO_TICKER_SQL <- "
WITH buys(id, ticker, d, amt) AS (VALUES __BUYS__),
maxd AS (SELECT MAX(date) AS d FROM cdm.ingest_combined WHERE ticker = 'SPY'),
fills AS (
    SELECT b.ticker, b.d::date AS buy_d, b.amt::numeric AS amt,
      (SELECT i.adj_close FROM cdm.ingest_combined i
       WHERE i.ticker = b.ticker AND i.date >= b.d::date ORDER BY i.date LIMIT 1) AS px,
      (SELECT i.adj_close FROM cdm.ingest_combined i
       WHERE i.ticker = 'SPY' AND i.date >= b.d::date ORDER BY i.date LIMIT 1) AS spy_px
    FROM buys b
),
lots AS (
    SELECT ticker, buy_d, amt, amt / NULLIF(px, 0) AS shares,
           amt / NULLIF(spy_px, 0) AS spy_shares
    FROM fills WHERE px > 0 AND spy_px > 0
),
vdates AS (
    SELECT g::date AS d FROM generate_series(
        (SELECT MIN(buy_d) FROM lots), (SELECT d FROM maxd), INTERVAL '1 week') AS g
    UNION SELECT (SELECT d FROM maxd)
    UNION SELECT buy_d FROM lots   -- each line starts at 0% on its own first buy
),
tk AS (
    SELECT v.d AS vdate, l.ticker, SUM(l.shares) AS shares, SUM(l.amt) AS invested,
           SUM(l.spy_shares) AS spy_shares
    FROM vdates v JOIN lots l ON l.buy_d <= v.d
    GROUP BY v.d, l.ticker
),
tk_val AS (
    SELECT vdate, ticker, invested,
           shares * (SELECT i.adj_close FROM cdm.ingest_combined i
             WHERE i.ticker = tk.ticker AND i.date <= tk.vdate
             ORDER BY i.date DESC LIMIT 1) AS value,
           spy_shares * (SELECT adj_close FROM cdm.ingest_combined
             WHERE ticker = 'SPY' AND date <= tk.vdate
             ORDER BY date DESC LIMIT 1) AS spy_value
    FROM tk
),
spy AS (
    SELECT v.d AS vdate, SUM(l.amt) AS invested, SUM(l.spy_shares) AS spy_sh
    FROM vdates v JOIN lots l ON l.buy_d <= v.d
    GROUP BY v.d
)
-- per-ticker rows carry ret_pct (chart) AND invested/value/spy_value (the table
-- reads the latest row per ticker); the '__SPY__' row is the chart benchmark.
SELECT vdate::text AS d, ticker,
       ROUND((100 * (value / NULLIF(invested, 0) - 1))::numeric, 2) AS ret_pct,
       ROUND(invested::numeric, 2)  AS invested,
       ROUND(value::numeric, 2)     AS value,
       ROUND(spy_value::numeric, 2) AS spy_value
FROM tk_val
UNION ALL
SELECT vdate::text AS d, '__SPY__' AS ticker,
       ROUND((100 * ((spy_sh * (SELECT adj_close FROM cdm.ingest_combined
                WHERE ticker = 'SPY' AND date <= spy.vdate ORDER BY date DESC LIMIT 1))
              / NULLIF(invested, 0) - 1))::numeric, 2) AS ret_pct,
       NULL::numeric AS invested, NULL::numeric AS value, NULL::numeric AS spy_value
FROM spy
ORDER BY ticker, d;"

# Dynamic per-stock sell ladder. __TRIGGERS__ = VALUES ('TICK','YYYY-MM-DD',20)
# (ticker, trigger date, n_bars in {20,40}). Delisted episodes never reach here.
# One output row per tranche (stage 1..4): 25% at trigger, then 25% at the first
# close >= safe/good/best (p40/p60/p80 of the stock's own 3y max-runup over
# n_bars windows), with deadline + stop-loss backstops. has_hist=FALSE (<60
# windows) falls back to time-thirds. kind in trigger/level/sched/stop/deadline/pending.
LC_SELL_LADDER_SQL <- "
WITH trig(ticker, trig_d, n_bars) AS (VALUES __TRIGGERS__),
trig2 AS (
    SELECT ticker, trig_d::date AS trig_d, n_bars::int AS n_bars,
           (n_bars::int * 7 / 5) AS w_days,
           LEAD(trig_d::date) OVER (PARTITION BY ticker ORDER BY trig_d::date) AS next_trig_d
    FROM trig
),
px AS (
    SELECT i.ticker, i.date, i.adj_close,
           MAX(i.adj_close) OVER (PARTITION BY i.ticker ORDER BY i.date
             ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) / NULLIF(i.adj_close,0) - 1 AS runup20,
           MAX(i.adj_close) OVER (PARTITION BY i.ticker ORDER BY i.date
             ROWS BETWEEN 1 FOLLOWING AND 40 FOLLOWING) / NULLIF(i.adj_close,0) - 1 AS runup40,
           LEAD(i.date, 20) OVER (PARTITION BY i.ticker ORDER BY i.date) AS lead20_d,
           LEAD(i.date, 40) OVER (PARTITION BY i.ticker ORDER BY i.date) AS lead40_d
    FROM cdm.ingest_combined i
    WHERE i.ticker IN (SELECT DISTINCT ticker FROM trig2)
      AND i.date >= (SELECT MIN(trig_d) FROM trig2) - INTERVAL '3 years'
),
samples AS (
    SELECT t.ticker, t.trig_d,
           CASE WHEN t.n_bars = 20 THEN p.runup20 ELSE p.runup40 END AS runup
    FROM trig2 t JOIN px p ON p.ticker = t.ticker
    WHERE p.date >= t.trig_d - INTERVAL '3 years'
      AND CASE WHEN t.n_bars = 20 THEN p.lead20_d ELSE p.lead40_d END <= t.trig_d
),
lv AS (
    SELECT ticker, trig_d, COUNT(*) AS n_win,
           percentile_cont(0.40) WITHIN GROUP (ORDER BY runup) AS p40,
           percentile_cont(0.60) WITHIN GROUP (ORDER BY runup) AS p60,
           percentile_cont(0.80) WITHIN GROUP (ORDER BY runup) AS p80
    FROM samples GROUP BY ticker, trig_d
),
base AS (
    SELECT t.*, l.n_win, l.p40, l.p60, l.p80,
           COALESCE(l.n_win, 0) >= 60 AS has_hist,
           (SELECT i.adj_close FROM cdm.ingest_combined i
            WHERE i.ticker = t.ticker AND i.date <= t.trig_d
            ORDER BY i.date DESC LIMIT 1) AS p0_px,
           (SELECT MAX(i.date) FROM cdm.ingest_combined i WHERE i.ticker = t.ticker) AS max_d,
           LEAST(t.trig_d + (t.n_bars * 7 / 5), COALESCE(t.next_trig_d, DATE '9999-01-01')) AS win_end
    FROM trig2 t LEFT JOIN lv l ON l.ticker = t.ticker AND l.trig_d = t.trig_d
),
stops AS (
    SELECT b.*, sd.stop_detect_d, se.stop_exec_d
    FROM base b
    LEFT JOIN LATERAL (
        SELECT MIN(i.date) AS stop_detect_d FROM cdm.ingest_combined i
        WHERE i.ticker = b.ticker AND i.date > b.trig_d AND i.date <= b.win_end
          AND i.adj_close <= b.p0_px * 0.90) sd ON TRUE
    LEFT JOIN LATERAL (
        SELECT MIN(i.date) AS stop_exec_d FROM cdm.ingest_combined i
        WHERE i.ticker = b.ticker AND i.date > sd.stop_detect_d AND i.date <= b.win_end) se ON TRUE
    WHERE b.p0_px IS NOT NULL
),
tspec AS (
    SELECT s.*, st.stage,
           CASE WHEN s.has_hist THEN s.p0_px * (1 + CASE st.stage
                WHEN 2 THEN s.p40 WHEN 3 THEN s.p60 ELSE s.p80 END) END AS level_px,
           CASE WHEN NOT s.has_hist THEN s.trig_d + (s.w_days * (st.stage - 1) / 3) END AS sched_d
    FROM stops s CROSS JOIN (VALUES (2), (3), (4)) st(stage)
),
tfill AS (
    SELECT ts.*,
           CASE WHEN ts.level_px IS NOT NULL THEN
             (SELECT MIN(i.date) FROM cdm.ingest_combined i
              WHERE i.ticker = ts.ticker AND i.date > ts.trig_d
                AND i.date <= ts.win_end AND i.adj_close >= ts.level_px)
           WHEN ts.max_d >= LEAST(ts.sched_d, ts.win_end) THEN
             (SELECT MAX(i.date) FROM cdm.ingest_combined i
              WHERE i.ticker = ts.ticker AND i.date > ts.trig_d
                AND i.date <= LEAST(ts.sched_d, ts.win_end))
           END AS raw_fill_d
    FROM tspec ts
),
resolved AS (
    SELECT ticker, trig_d, n_bars, w_days, win_end, n_win, has_hist,
           p0_px, p40, p60, p80, stop_detect_d, stop_exec_d, max_d,
           1 AS stage, 0.25 AS frac, 'trigger' AS kind, trig_d AS sell_d,
           NULL::numeric AS level_px, NULL::date AS sched_d
    FROM stops
    UNION ALL
    SELECT tf.ticker, tf.trig_d, tf.n_bars, tf.w_days, tf.win_end, tf.n_win, tf.has_hist,
           tf.p0_px, tf.p40, tf.p60, tf.p80, tf.stop_detect_d, tf.stop_exec_d, tf.max_d,
           tf.stage, 0.25,
           CASE
             WHEN tf.raw_fill_d IS NOT NULL
                  AND (tf.stop_exec_d IS NULL OR tf.raw_fill_d < tf.stop_exec_d)
               THEN CASE WHEN tf.level_px IS NOT NULL THEN 'level' ELSE 'sched' END
             WHEN tf.stop_exec_d IS NOT NULL THEN 'stop'
             WHEN tf.max_d >= tf.win_end THEN 'deadline'
             ELSE 'pending' END,
           CASE
             WHEN tf.raw_fill_d IS NOT NULL
                  AND (tf.stop_exec_d IS NULL OR tf.raw_fill_d < tf.stop_exec_d)
               THEN tf.raw_fill_d
             WHEN tf.stop_exec_d IS NOT NULL THEN tf.stop_exec_d
             WHEN tf.max_d >= tf.win_end THEN
               (SELECT MAX(i.date) FROM cdm.ingest_combined i
                WHERE i.ticker = tf.ticker AND i.date <= tf.win_end)
             END,
           tf.level_px, tf.sched_d
    FROM tfill tf
)
SELECT r.ticker, r.trig_d::text AS trig_d, r.n_bars, r.w_days,
       r.win_end::text AS deadline_d, COALESCE(r.n_win, 0) AS n_win, r.has_hist,
       ROUND(r.p0_px::numeric, 4)                 AS p0,
       ROUND((r.p0_px * (1 + r.p40))::numeric, 4) AS level_safe,
       ROUND((r.p0_px * (1 + r.p60))::numeric, 4) AS level_good,
       ROUND((r.p0_px * (1 + r.p80))::numeric, 4) AS level_best,
       r.stage, r.frac, r.kind,
       r.sell_d::text AS sell_d, r.sched_d::text AS sched_d,
       ROUND((SELECT i.adj_close FROM cdm.ingest_combined i
              WHERE i.ticker = r.ticker AND i.date <= r.sell_d
              ORDER BY i.date DESC LIMIT 1)::numeric, 4) AS fill_px,
       r.stop_detect_d::text AS stop_detect_d, r.stop_exec_d::text AS stop_exec_d
FROM resolved r
ORDER BY r.ticker, r.trig_d, r.stage;"

# Run LC_PORTFOLIO_SQL for the saved positions on an open connection, honoring
# signal-gated model buys and (optional) ladder sell events. Returns a tidy
# series (d/invested/value/ret_pct/spy_value/spy_ret_pct) or NULL. Injection-safe:
# tickers ^[A-Za-z.-]+$, dates ISO, dollars/fractions numeric.
compute_portfolio_series <- function(con, positions, led = NULL,
                                     sell_events = NULL, today = Sys.Date(),
                                     model_start = "epoch") {
  sched <- gated_expand_schedule(positions, led, today, model_start = model_start)
  skipped <- attr(sched, "model_skipped")
  if (nrow(sched) == 0) return(NULL)
  ok <- grepl("^[A-Za-z.-]+$", sched$ticker) &
        grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", sched$d) & is.finite(sched$amount)
  sched <- sched[ok, , drop = FALSE]
  if (nrow(sched) == 0) return(NULL)
  vals <- paste(sprintf("('%s','%s',%s)", toupper(sched$ticker), sched$d,
                        format(sched$amount, scientific = FALSE, trim = TRUE)),
                collapse = ", ")
  sell_sql <- "SELECT NULL::text, NULL::text, NULL::numeric, NULL::text WHERE FALSE"
  if (!is.null(sell_events) && nrow(sell_events)) {
    se <- sell_events[toupper(sell_events$ticker) %in% toupper(sched$ticker), , drop = FALSE]
    if (nrow(se)) {
      se$frac <- suppressWarnings(as.numeric(se$frac))
      ok2 <- grepl("^[A-Za-z.-]+$", se$ticker) &
             grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", se$sell_d) &
             grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", se$trig_d) &
             is.finite(se$frac) & se$frac > 0 & se$frac <= 1
      se <- se[ok2, , drop = FALSE]
      if (nrow(se))
        sell_sql <- paste0("VALUES ", paste(sprintf("('%s','%s',%s,'%s')",
          toupper(se$ticker), se$sell_d,
          format(se$frac, scientific = FALSE, trim = TRUE), se$trig_d), collapse = ", "))
    }
  }
  q <- gsub("__BUYS__", vals, LC_PORTFOLIO_SQL, fixed = TRUE)
  q <- gsub("__SELLS__", sell_sql, q, fixed = TRUE)
  df <- coerce_numeric_cols(dbGetQuery(con, q),
    c("invested", "value", "ret_pct", "spy_value", "spy_ret_pct"))
  if (is.null(df) || nrow(df) == 0) return(NULL)
  df$d <- as.Date(df$d)
  attr(df, "model_skipped") <- skipped
  df
}

# Turn the board's current sell/closed model tickers into ladder triggers.
# Uses derivedLC's already-computed state/reason/exit (one episode per name in
# the current ledger window). reason in gate/matured/delisted -> window sizing.
sell_triggers_from_dv <- function(dv, led, meta, model_tickers, today = Sys.Date()) {
  if (is.null(dv) || is.null(dv$state_now)) return(NULL)
  st <- dv$state_now
  cand <- intersect(toupper(model_tickers), names(st)[st %in% c("sell", "closed")])
  if (!length(cand)) return(NULL)
  dl <- NULL
  if (!is.null(meta) && nrow(meta))
    dl <- setNames(suppressWarnings(as.Date(meta$delisted_date)), toupper(meta$ticker))
  g1 <- function(x, t) if (!is.null(x) && t %in% names(x)) x[[t]] else NA
  rows <- list()
  for (t in cand) {
    rsn  <- tolower(paste(g1(dv$reason_of, t), g1(dv$why_now, t)))
    exd  <- suppressWarnings(as.Date(g1(dv$exit_of, t)))
    matd <- suppressWarnings(as.Date(g1(dv$mat_of, t)))
    is_del <- grepl("delist", rsn) || (!is.null(dl) && t %in% names(dl) && !is.na(dl[[t]]))
    if (is_del) {
      reason <- "delisted"
      trig <- if (!is.null(dl) && t %in% names(dl) && !is.na(dl[[t]])) dl[[t]] else exd
    } else if (grepl("matur", rsn)) {
      reason <- "matured"; trig <- if (!is.na(matd)) matd else exd
    } else { reason <- "gate"; trig <- exd }
    if (is.na(trig)) trig <- today
    if (trig > today) trig <- today
    wd <- switch(reason, gate = LC_WINDDOWN$gate, matured = LC_WINDDOWN$matured, NULL)
    rows[[length(rows) + 1]] <- data.frame(
      ticker = t, trig_d = trig, reason = reason,
      n_bars = if (is.null(wd)) NA_integer_ else wd$n_bars,
      w_days = if (is.null(wd)) NA_integer_ else wd$w_days, stringsAsFactors = FALSE)
  }
  if (!length(rows)) return(NULL)
  out <- do.call(rbind, rows)
  out[!duplicated(paste(out$ticker, out$trig_d)), , drop = FALSE]
}

# Run the ladder query for laddered triggers (delisted -> 100% local event).
# Returns list(events, meta) or NULL. Capped at 25 triggers/run.
build_sell_events <- function(con, triggers) {
  if (is.null(triggers) || !nrow(triggers)) return(NULL)
  triggers <- triggers[order(triggers$ticker, triggers$trig_d), , drop = FALSE]
  if (nrow(triggers) > 25L) triggers <- triggers[seq_len(25L), , drop = FALSE]
  mcols <- c("ticker", "trig_d", "reason", "p0", "level_safe", "level_good",
             "level_best", "n_win", "has_hist", "deadline_d", "stop_detect_d", "no_price")
  events <- list(); meta <- list()
  del <- triggers[triggers$reason == "delisted", , drop = FALSE]
  lad <- triggers[triggers$reason != "delisted", , drop = FALSE]
  if (nrow(del)) {
    events[[length(events) + 1]] <- data.frame(
      ticker = del$ticker, sell_d = format(del$trig_d), frac = 1.0,
      trig_d = format(del$trig_d), stage = 1L, kind = "delisted",
      fill_px = NA_real_, stringsAsFactors = FALSE)
    meta[[length(meta) + 1]] <- data.frame(
      ticker = del$ticker, trig_d = format(del$trig_d), reason = "delisted",
      p0 = NA_real_, level_safe = NA_real_, level_good = NA_real_, level_best = NA_real_,
      n_win = 0L, has_hist = FALSE, deadline_d = format(del$trig_d),
      stop_detect_d = NA_character_, no_price = FALSE, stringsAsFactors = FALSE)[, mcols]
  }
  if (nrow(lad)) {
    ok <- grepl("^[A-Za-z.-]+$", lad$ticker) &
          grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", format(lad$trig_d)) &
          lad$n_bars %in% c(20L, 40L)
    lad <- lad[ok, , drop = FALSE]
    if (nrow(lad)) {
      vals <- paste(sprintf("('%s','%s',%d)", toupper(lad$ticker),
                            format(lad$trig_d), as.integer(lad$n_bars)), collapse = ", ")
      res <- tryCatch(coerce_numeric_cols(
        dbGetQuery(con, gsub("__TRIGGERS__", vals, LC_SELL_LADDER_SQL, fixed = TRUE)),
        c("p0", "level_safe", "level_good", "level_best", "frac", "fill_px", "n_win")),
        error = function(e) NULL)
      if (!is.null(res) && nrow(res)) {
        ev <- res[res$kind != "pending" & !is.na(res$sell_d),
                  c("ticker", "sell_d", "frac", "trig_d", "stage", "kind", "fill_px")]
        if (nrow(ev)) events[[length(events) + 1]] <- ev
        m <- res[!duplicated(paste(res$ticker, res$trig_d)),
                 c("ticker", "trig_d", "p0", "level_safe", "level_good", "level_best",
                   "n_win", "has_hist", "deadline_d", "stop_detect_d")]
        m$reason <- lad$reason[match(paste(m$ticker, m$trig_d),
                                     paste(toupper(lad$ticker), format(lad$trig_d)))]
        m$no_price <- is.na(m$p0)
        meta[[length(meta) + 1]] <- m[, mcols]
      }
    }
  }
  ev_df   <- if (length(events)) do.call(rbind, events) else NULL
  meta_df <- if (length(meta))   do.call(rbind, meta)   else NULL
  if (is.null(ev_df) && is.null(meta_df)) return(NULL)
  list(events = ev_df, meta = meta_df)
}

# One-line plan/status string for a positions-table row.
plan_label_for_row <- function(tk, mode, dv, ladder, today = Sys.Date()) {
  tk <- toupper(trimws(as.character(tk)))
  if (!identical(as.character(mode), "model")) return("manual DCA")
  if (is.null(dv)) return("model-linked - Generate to activate")
  st <- if (!is.null(dv$state_now) && tk %in% names(dv$state_now)) dv$state_now[[tk]] else NA
  tryCatch({
    if (!is.null(ladder) && !is.null(ladder$meta) && tk %in% ladder$meta$ticker) {
      m  <- ladder$meta[ladder$meta$ticker == tk, , drop = FALSE][1, ]
      ev <- if (!is.null(ladder$events)) ladder$events[ladder$events$ticker == tk, , drop = FALSE] else NULL
      sold <- if (!is.null(ev) && nrow(ev)) sum(!is.na(ev$sell_d) & as.Date(ev$sell_d) <= today) else 0L
      if (identical(m$reason, "delisted")) sprintf("delisted - exited %s", m$trig_d)
      else if (!is.null(ev) && any(ev$kind == "stop", na.rm = TRUE))
        sprintf("stopped out %s", max(ev$sell_d[ev$kind == "stop"], na.rm = TRUE))
      else if (sold >= 4L || identical(st, "closed")) sprintf("exited (%d/4)", min(sold, 4L))
      else {
        idx <- max(1L, min(3L, sold)); nm <- c("safe", "good", "best")[idx]
        px  <- c(m$level_safe, m$level_good, m$level_best)[idx]
        hn  <- if (isTRUE(!m$has_hist)) " (hist-light)" else ""
        sprintf("SELL %d/4 - next %s%s @ $%.2f - by %s", min(sold, 4L), nm, hn,
                ifelse(is.na(px), 0, px), m$deadline_d)
      }
    } else if (identical(st, "buy")) "buying"
    else if (isTRUE(st %in% c("sell", "closed"))) "exited"
    else if (identical(st, "hold")) "paused (off buy list)"
    else "awaiting first signal"
  }, error = function(e) "-")
}

# Risk-adjustment series: the EQUAL-WEIGHT buy-and-hold basket value (=$1 -> avg
# of price/entry over the picks) and SPY value, monthly, over the hold window.
# Equal-weight-hold (not DCA) so month-over-month ratios are pure time-weighted
# returns with no cash-flow distortion -> clean beta/alpha/vol regression in R.
FORECAST_RISK_SQL <- "
WITH params AS (SELECT DATE '__ANCHOR__' AS start_d),
cutoffp AS (SELECT MAX(train_cutoff_date) AS cutoff FROM validation.walk_forward_top_picks, params p WHERE train_cutoff_date <= p.start_d),
maxd AS (SELECT MAX(date) AS md FROM cdm.ingest_combined WHERE ticker='SPY'),
enddate AS (SELECT LEAST((SELECT start_d FROM params) + INTERVAL '__HOLD__ months', (SELECT md FROM maxd)) AS ed),
picks AS (SELECT t.ticker FROM validation.walk_forward_top_picks t, cutoffp c WHERE t.train_cutoff_date = c.cutoff __IDFILTER__),
entry AS (SELECT pk.ticker, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=pk.ticker AND i.date>=(SELECT start_d FROM params) ORDER BY i.date LIMIT 1) AS e_px FROM picks pk),
vdates AS (SELECT generate_series((SELECT start_d FROM params), (SELECT ed FROM enddate), INTERVAL '1 month')::date AS d),
bv AS (SELECT v.d, e.ticker, e.e_px, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=e.ticker AND i.date<=v.d ORDER BY i.date DESC LIMIT 1) AS px FROM vdates v CROSS JOIN entry e WHERE e.e_px>0),
spy_e AS (SELECT adj_close AS px0 FROM cdm.ingest_combined, params p WHERE ticker='SPY' AND date>=p.start_d ORDER BY date LIMIT 1)
SELECT v.d::text AS d,
   AVG(bv.px/NULLIF(bv.e_px,0))::double precision AS basket_val,
   ((SELECT adj_close FROM cdm.ingest_combined WHERE ticker='SPY' AND date<=v.d ORDER BY date DESC LIMIT 1)/(SELECT px0 FROM spy_e))::double precision AS spy_val
FROM vdates v JOIN bv ON bv.d=v.d GROUP BY v.d ORDER BY v.d;"

# ─── Define UI ───
ui <- navbarPage(
  title = "Analysis Dashboard",
  id = "mainNav",   # active-tab input; gates the Lifecycle auto-refresh timer
  # the ONE shared connection, rendered on every tab above its content, plus the
  # global loading overlay (toggled by the shiny:busy/idle handler in head)
  header = tagList(
    connection_bar(),
    tags$div(id = "global-spinner",
      tags$div(id = "gs-track", tags$div(id = "gs-fill")),
      tags$div(id = "gs-label", "Loading..."))
  ),
  tags$head(
    tags$style(HTML(custom_css)),
    # Auto-heal Shiny's grey disconnect overlay. Forced reconnect stays OFF
    # (see server(), 2026-07-23: it replays sessions into the busy event loop
    # and segfaults), so a dropped websocket otherwise sits grey until a manual
    # reload. Instead: on a real drop, reload the tab after a short backoff, and
    # reset the counter once we reconnect. Caps at MAX quick tries so a server
    # that is genuinely down is not hammered (overlay is left for the user).
    tags$script(HTML("
      (function(){
        var KEY = 'wf_reconnectTries', MAX = 5;
        $(document).on('shiny:connected', function(){
          try { sessionStorage.removeItem(KEY); } catch (e) {}
        });
        $(document).on('shiny:disconnected', function(){
          var n = 0;
          try { n = parseInt(sessionStorage.getItem(KEY) || '0', 10); } catch (e) {}
          if (n >= MAX) return;               // likely down; stop reloading, leave overlay
          try { sessionStorage.setItem(KEY, n + 1); } catch (e) {}
          var delay = Math.min(1500 * Math.pow(1.6, n), 10000);
          // Reload only once the server actually answers 200. Reloading into a
          // still-booting / segfault-restarting process adds reconnect churn,
          // which is itself a segfault trigger - so poll readiness first.
          setTimeout(function ready(){
            fetch('/', { method: 'GET', cache: 'no-store' })
              .then(function(r){ if (r && r.ok) { location.reload(); } else { setTimeout(ready, 2000); } })
              .catch(function(){ setTimeout(ready, 2000); });
          }, delay);
        });
      })();
    ")),
    # Global estimated-progress bar: on any tab, whenever Shiny is busy (Connect,
    # Generate, heavy render). Real query progress isn't reported, so the fill
    # trickles asymptotically toward 90% over the expected load time and snaps to
    # 100% on idle. A 150ms delay skips the flash on trivial updates.
    tags$script(HTML("
      (function(){
        var startT = null, trickle = null, pct = 0, active = false;
        function ov(){ return document.getElementById('global-spinner'); }
        function paint(){
          var f = document.getElementById('gs-fill'); if(f) f.style.width = pct + '%';
          var l = document.getElementById('gs-label'); if(l) l.textContent = 'Loading... ' + Math.round(pct) + '%';
        }
        function start(){
          active = true; pct = 8; paint();
          var o = ov(); if(o) o.classList.add('gs-on');
          clearInterval(trickle);
          trickle = setInterval(function(){
            if(!active) return;
            pct += Math.max((90 - pct) * 0.06, 0.4);   // shrinking step -> approaches ~90%
            if(pct > 90) pct = 90;
            paint();
          }, 400);
        }
        function done(){
          active = false; clearInterval(trickle);
          pct = 100; paint();
          setTimeout(function(){ var o = ov(); if(o) o.classList.remove('gs-on'); pct = 0; paint(); }, 260);
        }
        $(document).on('shiny:busy', function(){ clearTimeout(startT); startT = setTimeout(start, 150); });
        $(document).on('shiny:idle', function(){
          clearTimeout(startT);
          var o = ov();
          if(active || (o && o.classList.contains('gs-on'))) done();
        });
      })();
    "))
  ),

  # ── Tab 1: Transition Range ──
  tabPanel("Transition Range",
    sidebarLayout(
      make_sidebar("T", "Transition", tagList(
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
              tags$div(HTML("<b>signal_score</b>: tiered from +3 (strongest) down to &minus;3 (weakest)")),
              tags$div(HTML("<b>combined_score</b> = signal_score + net_score")),
              tags$div(HTML("<b>recommendation</b> = combined_score tier"))
            )
          )
        )
      )),
      mainPanel(div(class = "main-card", style = "height: calc(100vh - 4rem); display: flex; flex-direction: column;",
        h4("Transition range",
           style = "color: #f8fafc; margin-bottom: 0.5rem; font-weight: 600;"),
        uiOutput("transitionHeader"),
        div(style = "flex: 1; min-height: 0;", plotlyOutput("transitionPlot", height = "100%"))
      ))
    )
  ),

  # ── Tab 3: Data QA ──
  tabPanel("Data QA",
    sidebarLayout(
      make_sidebar("Q", "Data QA", tagList(
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
          h4("Series counts per table (parquet history)",
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
  tabPanel("Coverage",
    sidebarLayout(
      make_sidebar("V", "Coverage", tagList(
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
        h4("Series history coverage",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        uiOutput("coveragePlotContainer")
      ))
    )
  ),

  # ── Tab: Clusters ──
  tabPanel("Clusters",
    sidebarLayout(
      make_sidebar("K", "Clusters", tagList()),
      mainPanel(div(class = "main-card",
        h4("Per-series scatter",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        tags$div(style = "margin-bottom: 0.35rem;",
          actionButton("clusterShowAll", "Show all", class = "btn-primary",
                       style = "margin-right: 0.5rem;"),
          actionButton("clusterHideAll", "Hide all", class = "btn-primary")
        ),
        checkboxInput("clusterNumbers", "Show cluster numbers (one per cluster; hidden clusters drop their number)", value = TRUE),
        plotlyOutput("clusterPlot", height = "600px")
      ))
    )
  ),

  # ── Tab: Top Picks ──
  tabPanel("Shortlist",
    sidebarLayout(
      make_sidebar("P", "Top Picks", tagList(
        selectInput("id_valP", "Cluster ID", choices = c("Connect first..." = ""), selected = ""),
        sliderInput("top_n_valP", "Top N tickers", min = 5, max = 400, value = 30, step = 5)
      )),
      mainPanel(div(class = "main-card",
        h4("Top-ranked across horizons",
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
      make_sidebar("RS", "Rank Stability", tagList(
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
        h4("Rank stability across walk-forward cohorts",
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
      make_sidebar("MV", "Model Validation", tagList(
        selectInput("id_valMV", "Cluster ID",
                    choices = c("Connect first..." = ""), selected = ""),
        selectInput("past_lagMV", "Past lag (Wilson forest)",
                    choices = c("Connect first..." = ""), selected = ""),
        selectInput("fut_lagMV", "Future lag (Wilson forest)",
                    choices = c("Connect first..." = ""), selected = "")
      )),
      mainPanel(div(class = "main-card",
        h4("Walk-forward IC",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        plotlyOutput("icHeatmapMV", height = "600px"),
        tags$br(),
        h4("Holdout payoff backtest",
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
        h4("Cell credibility",
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
  tabPanel("Predictions",
    sidebarLayout(
      make_sidebar("BL", "Buy List", tagList(
        radioButtons("date_modeBL", "As-of",
          choiceNames = list(
            tagList("Today (live)",
              tags$span("The model as of right now.",
                        style = "display:block; color:#64748b; font-size:0.68rem; font-weight:400; line-height:1.3; margin-top:0.1rem;")),
            tagList("Recorded day",
              tags$span("A recorded daily snapshot (since 2026-06-16).",
                        style = "display:block; color:#64748b; font-size:0.68rem; font-weight:400; line-height:1.3; margin-top:0.1rem;")),
            tagList("Backtest cutoff",
              tags$span("A quarterly walk-forward state - identical for the whole quarter-bundle.",
                        style = "display:block; color:#64748b; font-size:0.68rem; font-weight:400; line-height:1.3; margin-top:0.1rem;"))),
          choiceValues = c("today", "ledger", "backtest"),
          selected = "today"),
        conditionalPanel(
          # day-level precision is only offered where days actually differ: the
          # recorded BUY list is the one daily view. Picks/ladder snap to the
          # last walk-forward cutoff, so for them the picker is replaced by an
          # anchor note (ledger_anchorBL) instead of dead granularity.
          condition = "input.date_modeBL == 'ledger' && input.bl_viewBL == 'buys'",
          # bounds arrive from Connect via updateDateInput; until then the
          # widget defaults to today and asof_dateBL() returns NULL (no bounds)
          dateInput("ledger_dayBL", NULL, value = NULL, format = "yyyy-mm-dd")),
        conditionalPanel(
          condition = paste0("input.date_modeBL == 'ledger' && ",
                             "typeof input.bl_viewBL !== 'undefined' && ",
                             "input.bl_viewBL != 'buys'"),
          uiOutput("ledger_anchorBL")),
        conditionalPanel(
          condition = "input.date_modeBL == 'backtest'",
          selectInput("wf_cutoffBL", "Quarterly cutoff",
                      choices = c("Connect first..." = ""))),
        uiOutput("wf_resolvedBL"),
        tags$p(paste("Same three views at every date. The BUY list exists only",
                     "where a gate was recorded: live today and daily snapshots",
                     "since 2026-06-16; backtest dates show picks and ladder."),
               style = "color: #64748b; font-size: 0.7rem; margin-bottom: 0.75rem;"),
        # Unified 3-slot View (same menu at every date). Rendered server-side
        # so the BUY slot can grey out for pre-ledger dates: no recorded gate
        # exists before 2026-06-16 and reconstructing it would be lookahead
        # (see walk_forward_top_picks header for why that replay was rejected).
        uiOutput("blViewUI"),
        # BUY-view refinements (current date only): the decision shortlist is
        # a refinement of the BUY list, not a separate universe - so it lives
        # INSIDE the BUY view as a default-on toggle. Regular/Sparse toggles
        # only matter when the shortlist is off (sparse rows carry no bin).
        conditionalPanel(
          condition = "output.blCtlMode == 'current' && input.bl_viewBL == 'buys'",
          checkboxInput("buys_shortlistBL",
                        "Shortlist only - rank bins winning >= 55% (display filter, not the buy gate)",
                        value = TRUE),
          conditionalPanel(
            condition = "!input.buys_shortlistBL",
            checkboxGroupInput("show_catBL", NULL,
                               choices = c("Regular entries (BUYs)"      = "regular",
                                           "Sparse entries (young ids)"  = "sparse"),
                               selected = c("regular", "sparse")))),
        conditionalPanel(
          condition = "output.blCtlMode != 'current' && input.bl_viewBL == 'ladder'",
          # horizon/depth only steer the full-ladder replay; picks are a
          # top ~5% (tie-inclusive) slice graded at 12 months in the table.
          # Horizon is past-only by design: it selects a GRADING window, and
          # current-date rows have no realized outcomes to grade.
          selectInput("wf_horizon_valBL", "Replay horizon (months)",
                      choices = c("4", "7", "12", "20", "33"), selected = "12"),
          # replay's shortlist: the slider can't express "top X% of EACH
          # cluster" when clusters of different sizes are loaded together
          selectInput("wf_depth_valBL", "Per-cluster depth",
                      choices = c("All ranks"               = "all",
                                  "Top 5% of each cluster"  = "p5",
                                  "Top 10% of each cluster" = "p10",
                                  "Top 20% of each cluster" = "p20"),
                      selected = "all")),
        tags$div(
          style = paste0("margin: 0.25rem 0 0.75rem; padding: 0.5rem 0.75rem; ",
                         "background: rgba(255,255,255,0.03); ",
                         "border-left: 2px solid #64748b; border-radius: 4px; ",
                         "color: #94a3b8; font-size: 0.72rem; line-height: 1.5;"),
          tags$details(
            tags$summary("Glossary - the words on this tab"),
            tags$div(
              tags$div(HTML("<b>Walk-forward</b>: the backtest style here - train only on data before a cutoff, predict, then grade on what really happened after. No peeking ahead.")),
              tags$div(HTML("<b>Rank bin / slot</b>: each cluster's ranked list cut into 20 slots of 5% (slot 1 = top 5%). A ticker inherits its slot's history, not its own.")),
              tags$div(HTML("<b>Bin win rate</b>: of past walk-forward picks landing in this slot, the share that beat the benchmark. 50% = coin flip.")),
              tags$div(HTML("<b>Graded / holdout obs</b>: past predictions old enough to score against real later prices; 'n' counts them. Under 100 = 'no evidence'.")),
              tags$div(HTML("<b>Expectancy</b>: average return of past holdout trades, in pp per trade. Context only - the buy logic never reads it.")),
              tags$div(HTML("<b>pp</b>: percentage points - the gap between two percentages (55% is 5 pp above 50%).")),
              tags$div(HTML("<b>Serving rank vs pick rank</b>: serving rank = the model's position within the whole cluster today; pick rank = position inside the shown top-5% slice (r1 = best).")),
              tags$div(HTML("<b>Credibility</b>: a 0-1 weight each evidence cell earns from sample size and consistency; the serving BUY votes are credibility-weighted.")))))
      ),
      # rendered UNDER Generate Chart via make_sidebar's post_widgets slot:
      # both are post-load refinements (reactive on the loaded data, no
      # re-Generate needed), so they follow the button, not precede it.
      tagList(
        checkboxGroupInput("idsBL", "Cluster id filter (all on; uncheck to narrow)",
                           choices = NULL, inline = TRUE),
        div(style = "margin-top:-0.3rem; margin-bottom:0.5rem;",
          actionButton("idsAllBL", "Select all", style = "padding:2px 10px; font-size:0.72rem; margin-right:0.35rem;"),
          actionButton("idsNoneBL", "Deselect all", style = "padding:2px 10px; font-size:0.72rem;"),
          tags$span("Generate to load ids.", style = "color:#64748b; font-size:0.7rem; margin-left:0.4rem;")),
        sliderInput("flt_rank_rangeBL", "Rank range (within cluster)",
                    min = 1, max = 600, value = c(1, 600), step = 1)
      )),
      mainPanel(div(class = "main-card",
        uiOutput("modeNoteBL"),
        uiOutput("selStatsBL"),
        uiOutput("buyChartContainerBL"),
        tags$br(),
        DT::DTOutput("buyTableBL")
      ))
    )
  ),

  # ── Tab: Forecast (backtest growth curve vs live ledger) ──
  tabPanel("Forecast",
    sidebarLayout(
      make_sidebar("FC", "Forecast", tagList(
        tags$label("As-of date (stand here in the past)", style = "color: #94a3b8; font-weight: 600;"),
        tags$style(HTML(".shiny-split-layout > div { overflow: visible; }")),
        splitLayout(cellWidths = c("30%", "38%", "32%"),
          selectInput("asof_dayFC", NULL, choices = 1:31, selected = 1),
          selectInput("asof_monthFC", NULL, choices = setNames(1:12, month.abb), selected = 1),
          selectInput("asof_yearFC", NULL, choices = c("Connect first..." = ""), selected = "")),
        actionButton("asofRecentFC", "12 months ago", class = "btn-primary",
                     style = "margin-bottom: 0.5rem; padding:3px 10px; font-size:0.72rem;"),
        selectInput("cutoffFC", "Jump to a backtest cutoff",
                    choices = c("Connect first..." = "")),
        uiOutput("cutoffResolvedFC"),
        selectInput("holdFC", "Hold length (model horizons)",
                    choices = c("4 months" = "4", "7 months" = "7", "12 months" = "12",
                                "20 months" = "20", "33 months" = "33", "To today" = "240"),
                    selected = "12"),
        tags$span(paste("How long you hold before measuring. These are the model's Fibonacci forecast",
                        "horizons (same set as the Predictions tab's replay), capped at 33mo - holding to a",
                        "horizon realizes that prediction. 'To today' holds past it, where the edge decays."),
                  style = "color:#64748b; font-size:0.7rem; display:block; margin-bottom:0.6rem;"),
        checkboxGroupInput("idsFC", "Cluster ids (all on; uncheck to narrow)",
                           choices = NULL, inline = TRUE),
        div(style = "margin-top:-0.3rem; margin-bottom:0.5rem;",
          actionButton("idsAllFC", "Select all", style = "padding:2px 10px; font-size:0.72rem; margin-right:0.35rem;"),
          actionButton("idsNoneFC", "Deselect all", style = "padding:2px 10px; font-size:0.72rem;"),
          tags$span("Connect to load ids.", style = "color:#64748b; font-size:0.7rem; margin-left:0.4rem;")),
        tags$p(paste("Stand at any past date: it takes the selections known then (all-strategy",
                     "phased entry) and tracks them to today in REAL prices vs Benchmark, with the backtest",
                     "as the expectation and the live log in its own panel below. Solid =",
                     "realized, dotted = forward. First Generate ~6s, then under a second."),
               style = "color: #64748b; font-size: 0.72rem; margin-bottom: 0.5rem;")
      )),
      mainPanel(div(class = "main-card",
        uiOutput("forecastNoteFC"),
        uiOutput("forecastRiskFC"),
        plotlyOutput("forecastPlotFC", height = "560px"),
        tags$br(),
        plotlyOutput("forecastLedgerFC", height = "300px"),
        tags$br(),
        uiOutput("forecastTableFC")
      ))
    )
  ),

  # ── Tab: Lifecycle (buy/hold/sell transition grid of points: before -> now) ──
  tabPanel("Lifecycle",
    sidebarLayout(
      make_sidebar("LC", "Lifecycle", tagList(
        selectInput("holdLC", "Hold length (model horizons)",
                    choices = c("1 month" = "1", "2 months" = "2", "4 months" = "4",
                                "7 months" = "7", "12 months" = "12",
                                "20 months" = "20", "33 months" = "33"),
                    selected = "12"),
        tags$span(paste("Pick horizon AND hold duration: the board replays the model's",
                        "hz-month strategy. Entries come from the model's own record:",
                        "quarterly walk-forward top slots (proven bins only, win rate",
                        ">= 55% on >= 100 graded picks) plus the daily live ledger.",
                        "A held name flips to sell when the recorded gate flips to SELL,",
                        "when this horizon fully elapses (matured), or when the series",
                        "is delisted."),
                  style = "color:#64748b; font-size:0.7rem; display:block; margin-bottom:0.6rem;"),
        tags$p(paste("A decision board of the model's calls, by company name.",
                     "sell = exit now (gate flipped, matured lately, or delisted).",
                     "buy = the period's standing recs: proven rank slots backed by",
                     "the majority of the last month's recorded runs (a rolling",
                     "monthly evaluation, so one off-day cannot demote a durable",
                     "name), ranked by persistence over the trailing 4-month review",
                     "window (chip: win rate + BUY on n of N recorded runs).",
                     "hold = the period's dropped recs, still inside their hold",
                     "window; the chip shows entry date and % of horizon elapsed.",
                     "closed = matured more than a month ago, collapsed to a count",
                     "(full detail in the table). Unproven gate names live in the",
                     "table only, flagged."),
               style = "color: #64748b; font-size: 0.72rem; margin-bottom: 0.5rem;"),
        radioButtons("lcBoardLayout", "Board layout",
                     choices = c("Columns (board)" = "columns", "Stacked" = "stacked"),
                     selected = "columns", inline = TRUE),
        checkboxInput("lcBuyQSonly", "Show qualstream + picks only (all columns)", value = TRUE),
        checkboxInput("lcBuyStats", "Buy: also show win% · runs", value = FALSE),
        checkboxInput("lcAutoRefresh", "Auto-refresh board (every 20s)", value = TRUE),
        uiOutput("idFilterLC")
      )),
      mainPanel(div(class = "main-card",
        # Decision board leads the panel: the at-a-glance buy/hold/sell surface is
        # the primary artifact, so it sits at the top; benchmark + portfolio follow.
        uiOutput("boardLC"),
        tags$hr(style = "border-color:#1e293b; margin:0.9rem 0 1.1rem;"),
        # Benchmark comparison: does the qualstream + add return?
        plotlyOutput("qsCompareLC", height = "380px"),
        uiOutput("qsCompareNoteLC"),
        tags$hr(style = "border-color:#1e293b; margin:0.9rem 0 1rem;"),
        # --- My portfolio: strategy follower (model-linked DCA + ladder sells) ---
        tags$details(open = NA,
          style = "border:1px solid #1e293b; border-radius:8px; padding:0.6rem 0.9rem; margin-bottom:1rem;",
          tags$summary("My portfolio - strategy follower",
            uiOutput("lcPortSummary", inline = TRUE)),
          div(
            div(style = "color:#64748b; font-size:0.72rem; margin-bottom:0.6rem;",
                paste("Starts with every current BUY that passes qualstream (the board's",
                      "orange +), auto-added as a $100/monthly plan on Generate. By default",
                      "model holdings show the strategy's track record since the regime epoch;",
                      "flip the radio by the chart to 'My money since I added' to see only your",
                      "own cash flows. Remove any",
                      "you don't want and it stays removed; Clear all resets to the current",
                      "qualstream defaults. Add your own manual plans below.",
                      "Model-linked buys pause when a name leaves the buy list and resume",
                      "if it returns; a sell state winds the position down on a per-stock ladder",
                      "(safe/good/best levels from the stock's own history). Simulated fills at",
                      "daily closes; the SPY benchmark mirrors every buy and sell; sale proceeds",
                      "sit in cash on both legs.")),
            uiOutput("lcAdoptRow"),
            div(style = "color:#94a3b8; font-size:0.72rem; margin:0.5rem 0 0.15rem; font-weight:600;",
                "Or add a manual plan"),
            fluidRow(
              column(2, textInput("lcPosTicker", "Ticker", "")),
              column(2, numericInput("lcPosAmt", "$ per buy", value = 100, min = 1)),
              column(3, selectInput("lcPosCadence", "Cadence",
                       choices = c("Monthly" = "monthly", "One-off" = "once"),
                       selected = "monthly")),
              column(2, numericInput("lcPosDay", "Day of month", value = 1, min = 1, max = 28)),
              column(3, dateInput("lcPosStart", "Start date", value = Sys.Date() - 365))
            ),
            div(style = "margin:0.1rem 0 0.6rem;",
              actionButton("lcPosAdd", "Add manual", class = "btn-primary", style = "margin-right:0.4rem;"),
              actionButton("lcPosRemove", "Remove selected", style = "margin-right:0.4rem;"),
              actionButton("lcPosClear", "Clear all")),
            div(style = "color:#64748b; font-size:0.7rem; margin:0.1rem 0 0.35rem;",
                paste("id = cluster. Check rows (or the header box for all), then Remove selected.",
                      "Double-click a $/buy cell to change that row's amount.",
                      "Invested / Value / P&L / Return are that row's own accumulated buys;",
                      "vs SPY is the same dollars run into SPY (pp = percentage-point edge).",
                      "The chart below draws each holding's cumulative return against the same-cash SPY line.")),
            DT::DTOutput("lcPosTable"),
            uiOutput("lcChartFilter"),
            plotlyOutput("lcPortfolioChart", height = "340px"),
            uiOutput("lcPortfolioNote"),
            div(style = "color:#94a3b8; font-size:0.72rem; margin:0.8rem 0 0.15rem; font-weight:600;",
                "Transition history - each position's Buy → Hold → Sell path over time"),
            DT::DTOutput("lcTransitionsTable")
          )
        )
        # Decision board moved to the top of this panel; the benchmark chart sits
        # just below it. "Same companies with detail" table removed from view
        # (still computed; re-add hr + caption + DT::DTOutput("tableLC") to restore).
      ))
    )
  )
)

# ─── Helper: create a DB connection ───
# One shared connection: reads the global connection_bar() inputs (no per-tab
# suffix). dbname + sslmode come from DB_ENVIRONMENTS keyed by the selected
# environment; host/port/user still come from the fields so a manual override
# survives.
get_con <- function(input) {
  env <- input$db_env
  cfg <- DB_ENVIRONMENTS[[env]]
  if (is.null(cfg)) stop(sprintf("Unknown environment '%s'.", env))

  host_val <- input$db_host
  port_val <- suppressWarnings(as.integer(input$db_port))
  if (is.na(port_val)) stop("Port must be a number (e.g. 25060 for prod, 5432 for local).")
  # sslmode from config; upgrade prefer->require if the host is non-loopback so a
  # remote host typed under a local preset still gets TLS.
  ssl_mode <- cfg$sslmode
  if (ssl_mode == "prefer" &&
      !grepl("^(host\\.docker\\.internal|localhost|127\\.0\\.0\\.1)$", host_val)) {
    ssl_mode <- "require"
  }

  # In-container DNS hiccups surface as "could not translate host name ...
  # Temporary failure in name resolution" (EAI_AGAIN). They are transient, so
  # retry a few times before surfacing a raw error to the user. Non-transient
  # failures (bad password, refused) are raised immediately.
  transient <- "name resolution|translate host name|EAI_AGAIN|could not connect|server closed the connection|connection timed out"
  # The connect runs on the single-threaded event loop and burns NO CPU while it
  # blocks, so the watchdog's CPU-progress guard can't tell a stuck connect from a
  # hang. Bound the total connect budget hard: 2 attempts x 5s connect_timeout +
  # one short pause = ~10.3s worst case, well under the watchdog window, so a slow
  # SSL path can't stack into a self-inflicted kill (esp. with the portfolio
  # reactives each opening a connection per Generate).
  attempts  <- 2
  con <- NULL
  for (i in seq_len(attempts)) {
    con <- tryCatch(
      dbConnect(RPostgres::Postgres(),
        dbname   = cfg$dbname,
        host     = host_val,
        port     = port_val,
        user     = input$db_user,
        password = input$db_pass,
        sslmode  = ssl_mode,
        connect_timeout = 5
      ),
      error = function(e) {
        if (i < attempts && grepl(transient, e$message, ignore.case = TRUE)) {
          Sys.sleep(0.3)   # brief; connect_timeout already bounds the real wait
          return(NULL)
        }
        stop(e)            # non-transient, or out of retries
      }
    )
    if (!is.null(con)) break
  }
  # Hard cap BELOW the watchdog window so a stuck query self-aborts before the
  # process watchdog could ever fire. Wrapped so a failed SET (SSL drop right
  # after connect) disconnects instead of leaking the open socket.
  tryCatch(DBI::dbExecute(con, "SET statement_timeout = '25s'"),
           error = function(e) { try(dbDisconnect(con), silent = TRUE); stop(e) })
  con
}

# ─── Helper: wire up the ONE global env-switcher ───
# Fills the shared host/port/user/password fields from DB_ENVIRONMENTS whenever
# the environment changes (and once on load). Called a single time in server().
setup_env_switcher <- function(input, session) {
  observeEvent(input$db_env, {
    cfg <- DB_ENVIRONMENTS[[input$db_env]]
    if (is.null(cfg)) return()
    updateTextInput(session, "db_host", value = cfg$host)
    updateNumericInput(session, "db_port", value = suppressWarnings(as.integer(cfg$port)))
    updateTextInput(session, "db_user", value = cfg$user)
    updateTextInput(session, "db_pass", value = getenv_any(cfg$pass_env, ""))
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

  # ── Shared connection (one form in the navbar header for the whole app) ──
  # The env switcher and a single validating connect observer live here; every
  # tab's own Connect logic also listens to input$connect_btn, so one click sets
  # up all tabs. Enter the password once.
  setup_env_switcher(input, session)
  status_msgConn <- reactiveVal("Ready. Pick an environment and click Connect (top right).")
  output$statusMessageConn <- renderText({ status_msgConn() })
  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgConn("Enter the password, then Connect."); return() }
    status_msgConn(sprintf("Connecting to %s ...", input$db_env))
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      cfg <- DB_ENVIRONMENTS[[input$db_env]]
      status_msgConn(sprintf("Connected to %s (%s@%s/%s). Now Generate on any tab.",
                             input$db_env, input$db_user, input$db_host, cfg$dbname))
    }, error = function(e) status_msgConn(paste("Connection failed:", e$message)))
    # ignoreInit: do NOT fire on page load (before the password auto-populates),
    # otherwise the empty-password guard leaves a misleading status on screen.
  }, priority = 100, ignoreInit = TRUE)

  # ── TRANSITION: Reactive values ──
  app_dataT <- reactiveVal(NULL)
  status_msgT <- reactiveVal("Ready")
  output$statusMessageT <- renderText({ status_msgT() })

  # ── TRANSITION: Connect ──
  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgT("Error: Password is not set."); return() }
    status_msgT("Connecting...")
    tryCatch({
      con <- get_con(input)
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
    if (input$db_pass == "") { status_msgT("Error: Password is not set."); return() }
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
      con <- get_con(input)
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

  qa_schemasQ <- reactiveVal(NULL)

  # ── DATA QA: Connect — load list of schemas that have tables with a ticker column ──
  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgQ("Error: Password is not set."); return() }
    status_msgQ("Loading schemas...")
    tryCatch({
      con <- get_con(input)
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
    if (input$db_pass == "") { status_msgQ("Error: Password is not set."); return() }

    # First click: load schemas and show checkboxes, then stop and wait for user
    if (is.null(qa_schemasQ())) {
      status_msgQ("Loading schemas...")
      schemas <- tryCatch({
        con <- get_con(input)
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
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)

      placeholders <- paste(as.character(DBI::dbQuoteString(con, selected)), collapse = ",")
      tables <- dbGetQuery(con, sprintf("
        SELECT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name = 'ticker'
          AND table_schema IN (%s)
        ORDER BY table_schema, table_name", placeholders))

      if (nrow(tables) == 0) {
        status_msgQ("No tables with a series column.")
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

  # ── COVERAGE: Connect — just smoke-test the query source ──
  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgV("Error: Password is not set."); return() }
    status_msgV("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      n <- dbGetQuery(con,
        "SELECT COUNT(DISTINCT ticker) AS n FROM cdm.ingest_combined")$n[1]
      status_msgV(sprintf("Connected — %s distinct tickers.", n))
    }, error = function(e) { status_msgV(paste("Error:", e$message)) })
  })

  # ── COVERAGE: Execute — load min/max date per ticker ──
  observeEvent(input$execute_V, {
    if (input$db_pass == "") { status_msgV("Error: Password is not set."); return() }
    status_msgV("Loading coverage...")
    tryCatch({
      con <- get_con(input)
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

  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgK("Error: Password is not set."); return() }
    status_msgK("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM analysis.ticker_cluster_segments")$n[1]
      status_msgK(sprintf("Connected — %s tickers clustered.", n))
    }, error = function(e) { status_msgK(paste("Error:", e$message)) })
  })

  observeEvent(input$execute_K, {
    if (input$db_pass == "") { status_msgK("Error: Password is not set."); return() }
    status_msgK("Loading clusters...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      df <- dbGetQuery(con, "
        SELECT s.ticker,
               v.id::int AS id,   -- cast bigint->int: integer64 breaks the per-id for-loop (subscript out of bounds)
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
    palette20 <- c("#ef4444","#f97316","#eab308","#84cc16","#22c55e","#10b981",
                   "#14b8a6","#06b6d4","#0ea5e9","#3b82f6","#6366f1","#8b5cf6",
                   "#a855f7","#d946ef","#ec4899","#f43f5e","#fbbf24","#a3e635",
                   "#2dd4bf","#7c3aed")
    id_levels <- sort(unique(df$id))
    color_map <- setNames(palette20[((seq_along(id_levels) - 1) %% length(palette20)) + 1],
                          as.character(id_levels))
    # Clip y-axis (p0.5..p99.5, min ceiling 25) so high-growth clusters stay
    # visible while penny-stock crashes don't compress the bulk into y=0.
    y_lo <- min(-10, quantile(df$growth_pct_per_month, 0.005, na.rm = TRUE))
    y_hi <- max( 25, quantile(df$growth_pct_per_month, 0.995, na.rm = TRUE))
    x_hi <- quantile(df$months_count,         0.995, na.rm = TRUE)
    show_nums <- is.null(input$clusterNumbers) || isTRUE(input$clusterNumbers)

    # One markers trace + one centroid-text trace PER cluster, sharing a
    # legendgroup so toggling a cluster (legend click / Show-Hide all) also
    # drops its number. The number trace is only added when the checkbox is on.
    fig <- plot_ly()
    for (k in id_levels) {
      sub <- df[df$id == k, ]
      col <- color_map[[as.character(k)]]
      fig <- add_trace(fig, x = sub$months_count, y = sub$growth_pct_per_month,
        type = "scatter", mode = "markers", name = as.character(k), legendgroup = as.character(k),
        marker = list(color = col, size = 3, opacity = 0.6),
        text = sub$ticker, hovertemplate = paste0("%{text}<br>id ", k, "<extra></extra>"))
      if (show_nums) {
        fig <- add_trace(fig, x = median(sub$months_count, na.rm = TRUE),
          y = median(sub$growth_pct_per_month, na.rm = TRUE),
          type = "scatter", mode = "text", legendgroup = as.character(k), showlegend = FALSE,
          text = as.character(k), hoverinfo = "skip",
          textfont = list(color = "#f8fafc", size = 16))
      }
    }
    dark_layout(fig,
      xaxis = list(title = "Tenure (months)", range = c(0, x_hi), color = "#cbd5e1",
                   gridcolor = "#1e293b", zeroline = FALSE),
      yaxis = list(title = "Growth rate (%/mo)", range = c(y_lo, y_hi), color = "#cbd5e1",
                   gridcolor = "#1e293b", zeroline = FALSE),
      legend = list(font = list(color = "#e2e8f0"), title = list(text = "id"),
                    groupclick = "togglegroup"),
      margin = list(l = 55, r = 20, t = 15, b = 45)) %>%
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

  # ── TOP PICKS: Connect ──
  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgP("Error: Password is not set."); return() }
    status_msgP("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals <- dbGetQuery(con,
        "SELECT DISTINCT id FROM serving.return_cluster_ticker_summary_current ORDER BY 1")
      updateSelectInput(session, "id_valP", choices = id_vals[[1]], selected = id_vals[[1]][1])
      status_msgP("Filters loaded!")
    }, error = function(e) { status_msgP(paste("Error:", e$message)) })
  })

  # ── TOP PICKS: Execute ──
  observeEvent(input$execute_P, {
    if (input$db_pass == "") { status_msgP("Error: Password is not set."); return() }
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
      con <- get_con(input)
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
      yaxis = list(title = "Series (by agg_rank)", type = "category",
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

  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgRS("Error: Password is not set."); return() }
    status_msgRS("Connecting...")
    tryCatch({
      con <- get_con(input)
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
    con <- tryCatch(get_con(input), error = function(e) NULL)
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
    if (input$db_pass == "") { status_msgRS("Error: Password is not set."); return() }
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
      con <- get_con(input)
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
    if (input$db_pass == "") { status_msgRS("Error: Password is not set."); return() }
    status_msgRS("Querying all ids...")
    tryCatch({
      con <- get_con(input)
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

  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgMV("Error: Password is not set."); return() }
    status_msgMV("Connecting...")
    tryCatch({
      con <- get_con(input)
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
    if (input$db_pass == "") { status_msgMV("Error: Password is not set."); return() }
    status_msgMV("Running queries...")
    id_filter <- if (is.null(input$id_valMV) || input$id_valMV %in% c("", "ALL")) "" else
      sprintf("AND id = %d", as.integer(input$id_valMV))
    tryCatch({
      con <- get_con(input)
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
        yaxis = list(title = "Expectancy (mean period return)",
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
  wf_cutoffsBL <- reactiveVal(NULL)      # all distinct backtest cutoffs (desc) for the bundle picker
  status_msgBL <- reactiveVal("Ready")
  output$statusMessageBL <- renderText({ status_msgBL() })

  # As-of date from the three-mode control. Modes map 1:1 onto the resolver's
  # existing ranges: today -> latest snapshot (>= b[2] => current), ledger ->
  # a recorded day clamped into [b[1], b[2]-1], backtest -> the chosen cutoff
  # itself (< b[1] => wf). NULL before Connect or while an input is unset,
  # exactly like the old dropdown builder. switch() evaluates only the taken
  # branch, so reactive deps stay scoped to the active mode - and the today
  # branch reading ledger_boundsBL() is what fires the first post-Connect
  # auto-generate (do not "optimize" it to Sys.Date()).
  asof_dateBL <- function() {
    mode <- input$date_modeBL
    if (is.null(mode)) return(NULL)
    b <- ledger_boundsBL()
    switch(mode,
      today = if (is.null(b)) NULL else as.Date(b[2]),
      ledger = {
        d <- input$ledger_dayBL
        if (is.null(b) || is.null(d) || length(d) != 1 || is.na(d)) NULL
        else min(max(as.Date(d), as.Date(b[1])), as.Date(b[2]) - 1)
      },
      backtest = {
        v <- input$wf_cutoffBL
        if (is.null(v) || !nzchar(v)) NULL else as.Date(v)
      },
      NULL)
  }

  # A backtest date bundles: every date in [cutoff, next_cutoff) resolves to the
  # SAME cutoff (MAX cutoff <= date), so the picker offers one entry PER cutoff
  # instead of raw days that mostly no-op. Quarter label from the cutoff month.
  bl_qtr <- function(d) paste0("Q", (as.integer(format(d, "%m")) + 2) %/% 3, " ", format(d, "%Y"))
  bl_cutoff_choices <- function(cuts, all_later = TRUE) {
    if (length(cuts) == 0) return(character(0))
    cuts <- sort(cuts, decreasing = TRUE)
    labs <- vapply(seq_along(cuts), function(i) {
      base <- sprintf("%s  (%s)", format(cuts[i], "%Y-%m-%d"), bl_qtr(cuts[i]))
      if (i > 1) base
      else if (all_later) paste0(base, "  - and all later dates")
      else paste0(base, "  (newest)")
    }, character(1))
    setNames(as.character(cuts), labs)
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
  # the conditionalPanels and the View menu. Before Connect (no ledger bounds)
  # default to 'current'.
  bl_ctl_modeR <- reactive({
    b <- ledger_boundsBL(); asof <- asof_dateBL()
    if (is.null(b) || is.null(asof) || length(asof) != 1 || is.na(asof))
      return("current")
    if (asof < b[1]) "wf" else if (asof < b[2]) "ledger" else "current"
  })
  output$blCtlMode <- renderText(bl_ctl_modeR())
  outputOptions(output, "blCtlMode", suspendWhenHidden = FALSE)

  # Unified View selector: the SAME three slots at every date (top picks /
  # BUY list / full ladder). The BUY slot only exists where a gate record
  # does - live today, ledger snapshots since 2026-06-16; for earlier dates
  # it renders as a greyed explanation instead of an option, because no gate
  # existed then and reconstructing one is lookahead (the rejected replay).
  # Selection is sticky across date flips; a choice that vanishes (buys on a
  # pre-ledger date) falls back to picks.
  output$blViewUI <- renderUI({
    mode <- bl_ctl_modeR()
    hstyle <- paste0("display:block; color:#64748b; font-size:0.68rem; ",
                     "font-weight:400; line-height:1.3; margin-top:0.1rem;")
    lab <- function(title, help)
      tagList(title, tags$span(help, style = hstyle))
    picks_lab <- lab("Model's top picks (trust-gated)",
                     "What the validated rule would hold as of this date.")
    ladder_lab <- lab("Full ladder - every ranked ticker",
                      "Every ranked name, long and short side - no gate applied.")
    buys_lab <- if (mode == "ledger")
      lab("BUY list (recorded snapshot)",
          "The buy list the system actually recorded on this date.")
    else
      lab("Live signals (production gate)",
          "The model's buy list for today.")
    if (mode == "wf") {
      cn <- list(picks_lab, ladder_lab)
      cv <- c("picks", "ladder")
    } else {
      cn <- list(picks_lab, buys_lab, ladder_lab)
      cv <- c("picks", "buys", "ladder")
    }
    sel <- isolate(input$bl_viewBL)
    if (is.null(sel) || !sel %in% cv) sel <- "picks"
    tagList(
      radioButtons("bl_viewBL", "View",
                   choiceNames = cn, choiceValues = cv, selected = sel),
      if (mode == "wf") tags$p(
        paste("Signals: unavailable before 2026-06-16 - no recorded gate",
              "exists and reconstructing it would use future information."),
        style = paste0("color: #64748b; font-size: 0.7rem; ",
                       "margin-top: -0.5rem; margin-bottom: 0.75rem;"))
    )
  })

  # Past-date view switches change the QUERY (picks vs ladder vs snapshot), so
  # they re-Generate; current-date switches are renderer-side and instant. Two
  # guards keep this from double-firing: skip when the value is unchanged, and
  # skip when the MODE just changed (a date flip rebuilt the radio - the date
  # observer already bumped the generator, and coercion handles a vanished
  # option in the query itself).
  last_viewBL     <- reactiveVal(NULL)
  last_viewModeBL <- reactiveVal(NULL)
  observeEvent(input$bl_viewBL, {
    v <- input$bl_viewBL; m <- bl_ctl_modeR()
    prev_v <- last_viewBL(); prev_m <- last_viewModeBL()
    last_viewBL(v); last_viewModeBL(m)
    if (is.null(prev_v)) return()              # first render, no reload
    if (!identical(m, prev_m)) return()        # date/mode flip: date observer handles it
    if (identical(v, prev_v)) return()         # no real change
    if (m %in% c("wf", "ledger") &&
        !is.null(input$db_pass) && nzchar(input$db_pass) &&
        !is.null(ledger_boundsBL())) bump_genBL()
  })

  # Resolve the unified View input against a mode: anything unset or invalid
  # is picks; buys on a pre-ledger date coerces to picks (no gate to show).
  bl_view_resolved <- function(mode) {
    v <- input$bl_viewBL
    if (is.null(v) || !v %in% c("picks", "buys", "ladder")) v <- "picks"
    if (mode == "wf" && v == "buys") v <- "picks"
    v
  }

  output$modeNoteBL <- renderUI({
    mode <- app_modeBL()
    df <- app_dataBL()
    is_picks <- mode == "wf" && !is.null(df) && nrow(df) > 0 &&
      "view_kind" %in% names(df) && identical(df$view_kind[1], "picks")
    h_style <- "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"
    if (is_picks) {
      tagList(
        h4(sprintf("Model's top-ranked (backtest reconstruction - cutoff %s)",
                   format(as.Date(df$train_cutoff_date[1]), "%Y-%m-%d")),
           style = h_style),
        tags$div(class = "caveat-warning",
          tags$b(style = "color: #fbbf24;", "Read: "),
          "what the model would have recommended at the nearest quarterly ",
          "cutoff: its top ~5% of each cluster (tie-inclusive, positive ",
          "evidence only), shown ",
          "ONLY for clusters that had EARNED TRUST BY THAT DATE - walk-",
          "forward IC >= 0.10 with positive spread over at least 8 settled ",
          "prior quarters. No future information decides eligibility, so ",
          "this is an honest as-of reconstruction; it is NOT the live BUY ",
          "gate (which did not exist then and pools evidence across all ",
          "years). Each bar = how the pick REALLY did vs Benchmark over the next ",
          "12 months, from actual adjusted prices: green beat the Benchmark, red ",
          "lagged, grey = no tradable price window. Cluster identity is ",
          "membership-chained across quarters (labels can swap between ",
          "physical clusters; trust follows the members, not the label). ",
          "A dot at the bar tip = ",
          "the company no longer trades; its color = HOW it left (green ",
          "acquired/private, red bankrupt/failed, amber SPAC/rename, ",
          "burnt-orange uncategorized). The chips above are this date's ",
          "scorecard: % positive = share of picks that beat the Benchmark."))
    } else if (mode == "wf") {
      tagList(
        h4(if (!is.null(df) && nrow(df) > 0 && "train_cutoff_date" %in% names(df))
             sprintf("Backtest replay - cutoff %s",
                     format(as.Date(df$train_cutoff_date[1]), "%Y-%m-%d"))
           else "Backtest replay",
           style = h_style),
        tags$div(class = "caveat-warning",
          tags$b(style = "color: #fbbf24;", "Read: "),
          "the PREDICTION as it stood at the nearest quarterly walk-forward ",
          "cutoff: every ranked ticker per cluster (ids 13+ = short side), ",
          "in the model's own ",
          "order (r1 at the top of each cluster block = its top pick). ",
          "Each bar = how that pick REALLY did vs Benchmark over the replay horizon ",
          "(sidebar, default 12 months), from actual adjusted prices (first ",
          "close after the cutoff to the last close within the horizon): green ",
          "beat the Benchmark, red lagged, grey = no tradable price window. Ranking and ",
          "grading window switch together with the horizon. If the model works, ",
          "green concentrates near each block's r1. A dot at the bar tip = the ",
          "company no longer trades; its color = HOW it left: green acquired/",
          "went private, red bankrupt/failed, amber SPAC or ticker rename, ",
          "burnt-orange no category on file. Hover the dot (or see the table's ",
          "Delisted column) for reason, company and date. Backtest ranking ",
          "only - the live BUY gates did not exist then."))
    } else if (mode == "ledger") {
      tagList(
        h4(if (!is.null(df) && nrow(df) > 0 && "prediction_date" %in% names(df))
             sprintf("Live-log replay - snapshot %s",
                     format(as.Date(df$prediction_date[1]), "%Y-%m-%d"))
           else "Live-log replay",
           style = h_style),
        tags$div(class = "caveat-warning",
          tags$b(style = "color: #fbbf24;", "Read: "),
          "the BUY calls the live system actually logged on the selected date, ",
          "graded to the latest close. Each bar = one ticker's return since entry ",
          "relative to the Benchmark. Green beat it, red lagged it. A dot = delisted since ",
          "entry; dot color = why (green acquired/private, red bankrupt/failed, ",
          "amber SPAC/rename, burnt-orange uncategorized - hover it or see the ",
          "table's Delisted column); its 'latest close' is the final traded ",
          "price."))
    } else {
      # Current date: view-aware note (picks-today / live BUY list / ladder)
      uview <- bl_view_resolved("current")
      if (uview == "picks") {
        tagList(
          h4("Model's top-ranked (live, as of today)", style = h_style),
          tags$div(class = "caveat-warning",
            tags$b(style = "color: #fbbf24;", "Read: "),
            "What you see: the top ~5% of each long cluster the trust gate ",
            "currently trades, in the model's own rank order - the picks the ",
            "validated rule would hold today. No outcome exists yet; that ",
            "arrives through the ledger.", tags$br(),
            "Each bar: how tickers ranked in this same slot did in past ",
            "walk-forward tests - the slot's win rate vs the benchmark, drawn ",
            "as distance from 50% (coin flip). The label at the bar end is the ",
            "actual win rate; 'no evidence' = under 100 graded past picks.",
            tags$br(),
            "Green = the serving system also says BUY today. Grey = in the top ",
            "slice, but the ticker's own vote margin holds off. '!' = a BUY ",
            "whose slot historically lost (hover for detail).", tags$br(),
            "The BUY call itself comes from in-sample cell votes plus one ",
            "cluster-level walk-forward health gate - it never reads these win ",
            "rates. The chart cross-examines the picks against evidence the ",
            "buy logic does not see."))
      } else if (uview == "ladder") {
        tagList(
          h4("Full ladder - every ranked series (live)", style = h_style),
          tags$div(class = "caveat-warning",
            tags$b(style = "color: #fbbf24;", "Read: "),
            "every ticker the model ranks right now, grouped by cluster in rank ",
            "order (ids 13+ = short side), colored by live action (green BUY / ",
            "grey SKIP / red SELL). The diagnostic ladder - no BUY gate or ",
            "evidence filter applied. Use the rank slider to compare cluster ",
            "heads side by side."))
      } else {
        tagList(
          h4("Live signals - what the model marks BUY today",
             style = h_style),
          tags$div(class = "caveat-warning",
            tags$b(style = "color: #fbbf24;", "Read: "),
            "Every ticker the serving system marks BUY right now. That call ",
            "comes from in-sample cell votes plus a cluster-level walk-forward ",
            "health gate - not from anything drawn on this chart.", tags$br(),
            "Order: rows sort by the past win rate of each ticker's rank slot ",
            "(the cluster's ranking cut into 20 slots of 5%) - how often past ",
            "picks in that slot beat the benchmark, best on top. Bars show ",
            "expectancy: the average past holdout trade return in pp (green ",
            "positive, red negative) - display-only evidence, never a buy ",
            "input.", tags$br(),
            "'no evidence' rows have no graded history for their slot and sink ",
            "below the graded BUYs.", tags$br(),
            "The Shortlist toggle (slots winning >= 55% on >= 100 graded ",
            "picks) is a display filter for reading this page - the production ",
            "gate does not use it. Untick it for every BUY plus the ",
            "young-cluster watchlist. Switches instantly, no re-Generate."))
      }
    }
  })

  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgBL("Error: Password is not set."); return() }
    status_msgBL("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      n <- dbGetQuery(con, "
        SELECT COUNT(*) AS n
        FROM serving.return_cluster_ticker_global_action_current
        WHERE global_action = 'BUY'")$n[1]
      bounds <- dbGetQuery(con, "
        SELECT MIN(prediction_date) AS min_d, MAX(prediction_date) AS max_d
        FROM monitoring.prediction_ledger")
      wf_cuts <- as.Date(dbGetQuery(con, "
        SELECT DISTINCT train_cutoff_date AS d
        FROM validation.walk_forward_top_picks
        ORDER BY train_cutoff_date DESC")$d)
      wf_min <- if (length(wf_cuts)) min(wf_cuts) else as.Date(NA)
      if (!is.na(bounds$min_d[1])) {
        ledger_boundsBL(c(bounds$min_d[1], bounds$max_d[1]))
        wf_minBL(wf_min)
        wf_cutoffsBL(wf_cuts)
        # Backtest mode must stay in the wf era: a cutoff on/after ledger_min
        # would resolve downstream as "ledger" and replay the ledger under a
        # backtest label (first possible collision: a 2026-06-30 cutoff vs the
        # 2026-06-16 ledger start).
        bl_cuts <- wf_cuts[wf_cuts < as.Date(bounds$min_d[1])]
        if (length(bl_cuts)) {
          updateSelectInput(session, "wf_cutoffBL",
                            choices = bl_cutoff_choices(bl_cuts, all_later = FALSE),
                            selected = as.character(max(bl_cuts)))
        } else {
          updateSelectInput(session, "wf_cutoffBL",
                            choices = c("No backtest cutoffs yet" = ""))
        }
        led_min <- as.Date(bounds$min_d[1])
        led_max <- max(as.Date(bounds$max_d[1]) - 1, led_min)
        updateDateInput(session, "ledger_dayBL",
                        value = led_max, min = led_min, max = led_max)
        # date_modeBL stays at its UI default 'today'. First load fires via
        # ledger_boundsBL(...) set above: asof_dateBL()'s today branch reads
        # the bounds, so the date-change observer sees NULL -> b[2] and bumps
        # the generator exactly once - no explicit bump_genBL() here.
      }
      status_msgBL(sprintf("Connected. %d tickers currently BUY. Ledger %s to %s; walk-forward back to %s.",
                           as.numeric(n), bounds$min_d[1], bounds$max_d[1], wf_min))
    }, error = function(e) { status_msgBL(paste("Error:", e$message)) })
  })

  # One-line readout under the mode control: what the selection resolves to.
  # The pre-first-cutoff branch is gone (the backtest picker only offers real
  # cutoffs) and so is the "(no newer cutoff yet)" note (the newest choice is
  # tagged "(newest)" in the dropdown itself).
  # In Recorded-day mode the picks/ladder views cannot honor day precision:
  # they snap to the newest walk-forward cutoff <= the day, which is the SAME
  # cutoff for every recorded day. This block replaces the daily picker there.
  output$ledger_anchorBL <- renderUI({
    cuts <- wf_cutoffsBL()
    anchor <- if (length(cuts) > 0) format(max(as.Date(cuts)), "%Y-%m-%d")
              else "the last walk-forward cutoff"
    div(style = paste0("background:rgba(255,255,255,0.03); border:1px solid ",
                       "rgba(255,255,255,0.08); border-radius:6px; padding:0.5rem ",
                       "0.65rem; margin-bottom:0.6rem;"),
      tags$p(sprintf(paste("This view is anchored to cutoff %s and is identical",
                           "for every recorded day."), anchor),
             style = "color:#94a3b8; font-size:0.72rem; margin:0 0 0.25rem;"),
      tags$p(paste("Use 'Backtest cutoff' to move the anchor, or the BUY list",
                   "view for day-by-day changes."),
             style = "color:#64748b; font-size:0.7rem; margin:0;"))
  })

  output$wf_resolvedBL <- renderUI({
    mode <- input$date_modeBL
    d <- asof_dateBL()
    if (is.null(mode) || is.null(d) || length(d) != 1 || is.na(d)) return(NULL)
    sty <- "color:#64748b; font-size:0.7rem; margin:-0.4rem 0 0.75rem;"
    # ledger mode reads differently per view: only the BUY list is daily
    ledger_msg <- if (!is.null(input$bl_viewBL) && input$bl_viewBL != "buys") {
      cuts <- wf_cutoffsBL()
      anchor <- if (length(cuts) > 0) format(max(as.Date(cuts)), "%Y-%m-%d") else "?"
      sprintf("Anchored to cutoff %s - same for every recorded day.", anchor)
    } else {
      sprintf(paste("Snapshot for %s (weekend/holiday dates resolve",
                    "to the latest snapshot on or before)."),
              format(d, "%Y-%m-%d"))
    }
    msg <- switch(mode,
      today  = "Live data - the model as of right now.",
      ledger = ledger_msg,
      backtest = sprintf(paste("Backtest state at cutoff %s - the same for",
                               "every date this bundle covers."),
                         format(d, "%Y-%m-%d")),
      NULL)
    if (is.null(msg)) return(NULL)
    tags$p(msg, style = sty)
  })

  # Buy List refresh trigger. Bumped by the Generate button AND by any as-of
  # date change (once connected), so selecting a different past date reloads
  # the chart, table, and cluster-id filter automatically - no manual Generate.
  # NULL-seeded so the handler (ignoreNULL default) does not fire on page load.
  gen_triggerBL <- reactiveVal(NULL)
  bump_genBL <- function() {
    cur <- isolate(gen_triggerBL())
    gen_triggerBL(if (is.null(cur)) 1L else cur + 1L)
  }
  observeEvent(input$execute_BL, { bump_genBL() })
  # observeEvent fires on eventExpr INVALIDATION, not value change: a clamp
  # landing on the same date (e.g. a mode round-trip that resolves back to the
  # same day, or an updateDateInput echo) would double-generate. Dedupe
  # consecutive identical dates; distinct-mode dates can never collide
  # (today = b[2], ledger <= b[2]-1, backtest < b[1]), so a today -> backtest
  # -> today round trip still regenerates.
  last_asof_genBL <- reactiveVal(NULL)
  observeEvent(asof_dateBL(), {
    a <- asof_dateBL()
    if (identical(a, last_asof_genBL())) return()
    last_asof_genBL(a)
    if (!is.null(input$db_pass) && nzchar(input$db_pass) &&
        !is.null(ledger_boundsBL())) bump_genBL()
  }, ignoreInit = TRUE)

  observeEvent(gen_triggerBL(), {
    if (input$db_pass == "") { status_msgBL("Error: Password is not set."); return() }
    status_msgBL("Running query...")
    b <- ledger_boundsBL()
    asof <- asof_dateBL()
    has_date <- !is.null(b) && !is.null(asof) && length(asof) == 1 && !is.na(asof)
    is_wf     <- has_date && asof < b[1]
    is_ledger <- has_date && !is_wf && asof < b[2]
    ctl_mode  <- if (is_wf) "wf" else if (is_ledger) "ledger" else "current"
    uview     <- bl_view_resolved(ctl_mode)

    if ((is_wf || (is_ledger && uview != "buys")) && uview == "picks") {
      # ── Trust-gated TOP PICKS at the nearest quarterly cutoff <= asof ──
      # (backtest dates, and recent ledger dates asking for picks: the same
      # validated reconstruction. Current-date picks are computed live in the
      # renderer from serving, not here, since serving holds only today.)
      # validation.walk_forward_top_picks holds the as-of reconstruction
      # (top 10 per cluster, cluster eligible only on evidence settled BY
      # that cutoff, outcomes pre-graded on real 12mo prices). If the table
      # is missing (first build lands with the next walk-forward) or has no
      # cutoff early enough, fall through to the full ranking replay below.
      handled <- tryCatch({
        con <- get_con(input)
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

    if (is_wf || (is_ledger && uview != "buys")) {
      # ── Full ladder replay: every ranked ticker at the nearest cutoff ──
      # (rank depth is the client-side Rank filter, not a query knob)
      # Horizon selector: ranking AND grading window switch together, so the
      # question stays matched ("best over N months" graded over N months)
      # horizon control is hidden unless the ladder view is active (it grades a
      # window; picks/fallthrough leave it NULL) - default to 12 then
      hz <- suppressWarnings(as.integer(input$wf_horizon_valBL))
      if (length(hz) != 1 || is.na(hz) || !hz %in% c(4L, 7L, 12L, 20L, 33L)) hz <- 12L
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
        -- in a year it beat the Benchmark by 9pts; never chart it as a trade outcome.
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
        con <- get_con(input)
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

    if (is_ledger && uview == "buys") {
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
        con <- get_con(input)
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
      con <- get_con(input)
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

  # ids present in the currently-loaded data (checkbox choices); empty OR every
  # box checked = "all ids", no filter (mirrors the Forecast idsFC semantics).
  avail_idsBL <- reactiveVal(NULL)
  bl_ids_all_on <- function() {
    ids_sel <- suppressWarnings(as.integer(input$idsBL))
    av <- avail_idsBL()
    length(ids_sel) == 0 || (!is.null(av) && setequal(ids_sel, av))
  }

  # Shared id/rank filter, applied identically by chart + table (live, no
  # re-Generate). id filter: wf replay + current mode (ledger rows carry no
  # id). rank range: keep ranks in [lo, hi] on rank_within_cluster (wf) /
  # agg_rank (current) - e.g. 5-20 = skip the head, take the mid-block.
  bl_apply_filters <- function(df) {
    ids_sel <- suppressWarnings(as.integer(input$idsBL))
    if (!bl_ids_all_on() && "id" %in% names(df))
      df <- df[!is.na(df$id) & df$id %in% ids_sel, , drop = FALSE]
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
  observeEvent(list(input$idsBL, app_dataBL()), {
    df <- app_dataBL()
    if (is.null(df) || nrow(df) == 0) return()
    col <- if ("rank_within_cluster" %in% names(df)) "rank_within_cluster"
           else if ("agg_rank" %in% names(df)) "agg_rank" else return()
    ids_sel <- suppressWarnings(as.integer(input$idsBL))
    if (!bl_ids_all_on() && "id" %in% names(df))
      df <- df[!is.na(df$id) & df$id %in% ids_sel, , drop = FALSE]
    if (nrow(df) == 0) return()
    mx <- suppressWarnings(max(df[[col]], na.rm = TRUE))
    if (!is.finite(mx) || mx < 1) return()
    updateSliderInput(session, "flt_rank_rangeBL", max = mx, value = c(1, mx))
  })

  # Cluster checkboxes mirror the LOADED data: only ids that actually have
  # rows, so an all-delisted cluster (id 1 today) can't be checked into a
  # confusing empty chart. Preserve any still-valid current selection; a
  # selection that vanishes (or first load) falls back to all-on. Ledger rows
  # carry no id - leave the boxes alone there.
  observeEvent(app_dataBL(), {
    df <- app_dataBL()
    if (is.null(df) || nrow(df) == 0 || !"id" %in% names(df)) return()
    ids <- sort(as.integer(names(table(df$id))))
    if (length(ids) == 0) return()
    avail_idsBL(ids)
    prev <- suppressWarnings(as.integer(isolate(input$idsBL)))
    keep <- prev[!is.na(prev) & prev %in% ids]
    sel  <- if (length(keep) == 0) ids else keep     # default / vanished => all on
    updateCheckboxGroupInput(session, "idsBL", choices = ids, selected = sel, inline = TRUE)
  })

  # Select all / Deselect all, mirroring the Forecast tab's id checkboxes.
  observeEvent(input$idsAllBL, {
    req(avail_idsBL())
    updateCheckboxGroupInput(session, "idsBL", choices = avail_idsBL(),
                             selected = avail_idsBL(), inline = TRUE)
  })
  observeEvent(input$idsNoneBL, {
    req(avail_idsBL())
    updateCheckboxGroupInput(session, "idsBL", choices = avail_idsBL(),
                             selected = character(0), inline = TRUE)
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
      # Replay modes: one bar per pick, sorted by realized excess vs Benchmark.
      # Green = beat the Benchmark, red = lagged it. Reads top-to-bottom: best to worst.
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
          "%s (id %d)<br>cutoff %s | rank %d of %d | score %.4f<br>realized %dmo excess vs Benchmark %+.1f%%",
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
          sprintf("Realized %dmo excess return vs Benchmark (%%)", hz)
        else sprintf("Excess return vs Benchmark so far (%%; %s)", trunc_note)
      } else {
        hz <- NA_integer_
        df$excess <- df$excess_vs_spy_pct
        df$hover <- sprintf(
          "%s<br>snapshot %s | start %s @ %.2f -> latest %.2f<br>ret %.1f%% | vs Benchmark %+.1f%% | signal weight %.2f",
          df$ticker, df$prediction_date, df$entry_date, df$entry_adj_close,
          ifelse(is.na(df$latest_close), 0, df$latest_close),
          ifelse(is.na(df$ret_since_pct), 0, df$ret_since_pct),
          ifelse(is.na(df$excess_vs_spy_pct), 0, df$excess_vs_spy_pct),
          df$buy_weight)
        x_title <- "Return since start, relative to Benchmark (%)"
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
        if (app_modeBL() != "wf") "avg excess vs Benchmark since entry (pp)"
        else if (is.null(trunc_note)) sprintf("avg %dmo excess vs Benchmark (pp)", hz)
        else sprintf("avg excess vs Benchmark so far (pp; %s)", trunc_note))
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
          text = sprintf("%s: %d ranked picks (r1 = model's top pick; bar = realized %dmo vs Benchmark)", head_word, nrow(df), hz),
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
            " | SHORT-side cluster: model expected these to LAG the Benchmark")
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
                   customdata = ~hover, name = "excess vs Benchmark",
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
    # Current mode: the unified View (picks / buys / ladder), switchable live
    # (no re-Generate) - the live query loaded every ranked ticker, so each
    # view is a renderer-side subset.
    uview <- bl_view_resolved("current")
    if (uview == "picks") {
      # Live model top picks: within each LONG cluster the live gate currently
      # trades (a cluster yields BUYs iff cluster_is_tradeable), the top ~5% by
      # the serving rank - the today analogue of the validated backtest rule.
      # No realized outcome exists yet, so the bar is the ticker's WALK-FORWARD
      # rank-bin win rate (the same priced evidence the shortlist uses),
      # centered at 50 = coin flip. Green = the gate also marks it BUY today;
      # grey = in the top slice but the per-ticker gate holds off (the honest
      # rule-vs-gate disagreement).
      d <- df[!is.na(df$id) & df$id <= 12, , drop = FALSE]
      tradeable <- unique(d$id[d$global_action == "BUY"])
      d <- d[d$id %in% tradeable, , drop = FALSE]
      if (nrow(d) == 0) return(empty_plot(
        "No long cluster is trust-gated right now - nothing the rule would pick."))
      d <- do.call(rbind, lapply(split(d, d$id), function(g) {
        g <- g[order(g$agg_rank), ]
        head(g, max(1L, ceiling(0.05 * nrow(g))))
      }))
      d <- d[order(d$id, d$agg_rank), ]
      d$pick_rank <- as.integer(stats::ave(d$agg_rank, d$id, FUN = seq_along))
      set_sel_statsBL(d$bin_win_pct, "avg rank-bin win rate (%)")
      set_chart_rowsBL(nrow(d))
      is_buy   <- d$global_action == "BUY"
      measured <- !is.na(d$bin_win_pct) & !is.na(d$bin_n) & d$bin_n >= 100
      d$edge <- ifelse(measured, d$bin_win_pct - 50, 0)
      # Disagreement badge: serving says BUY but the pick's own MEASURED
      # walk-forward slot lost (win rate under 50 or negative mean return).
      # The buy logic never reads these numbers - flag the tension, not hide it.
      flagged <- is_buy & measured &
        (d$bin_win_pct < 50 | (!is.na(d$bin_mean_ret) & d$bin_mean_ret < 0))
      d$ylab <- sprintf("id%d r%d | %s%s", d$id, d$pick_rank, d$ticker,
                        ifelse(flagged, " !", ""))
      # Bar-end labels: print the ACTUAL win rate (the axis only shows the
      # distance from 50), and give zero-length bars words - "no evidence"
      # must not read as "exactly coin flip".
      d$bar_lab <- ifelse(measured, sprintf("%.1f%%", d$bin_win_pct),
                          "no evidence")
      lab_col <- ifelse(flagged, "#dc2626",
                 ifelse(measured, "#94a3b8", "#f59e0b"))
      ord <- function(n) {               # 1 -> "1st", 2 -> "2nd", 11 -> "11th"
        s <- rep("th", length(n))
        i <- !(n %% 100 %in% 11:13) & (n %% 10) %in% 1:3
        s[i] <- c("st", "nd", "rd")[n[i] %% 10]
        paste0(n, s)
      }
      d$hover <- sprintf(paste0(
        "%s - cluster %d, %s shown pick (serving rank %d)",
        "<br>Live serving call: %s - decided by cell votes, not by this chart.",
        "<br>%s",
        "<br>Avg past trade return: %s.%s"),
        d$ticker, d$id, ord(d$pick_rank), d$agg_rank, d$global_action,
        ifelse(measured,
               sprintf("Rank slot %d of 20: beat the benchmark %.1f%% of %d graded past picks.",
                       as.integer(d$rank_bin), d$bin_win_pct, as.integer(d$bin_n)),
        ifelse(is.na(d$rank_bin),
               "Not scored in the latest walk-forward ranking - no evidence.",
               sprintf("Rank slot %d of 20: only %s graded past picks - no evidence (needs 100).",
                       as.integer(d$rank_bin),
                       ifelse(is.na(d$bin_n), "0", as.character(as.integer(d$bin_n)))))),
        ifelse(is.na(d$wtd_expectancy), "n/a",
               sprintf("%+.3f pp per trade (display-only evidence)", d$wtd_expectancy)),
        ifelse(flagged, paste0(
          "<br>! BUY from serving votes; this pick's own walk-forward bin lost ",
          "historically - the buy logic never reads this number."), ""))
      bar_col <- ifelse(is_buy, "#10b981", "#64748b")
      chart_text <- sprintf(paste0(
        "Top ~5%% of each trust-gated cluster: %d picks / %d clusters ",
        "(label = slot's past win rate; green = live BUY, grey = vote holds ",
        "off, ! = BUY whose slot lost money)"),
        nrow(d), length(unique(d$id)))
      return(
        plot_ly(d, x = ~edge, y = ~ylab, type = "bar", orientation = "h",
                marker = list(color = bar_col), customdata = ~hover,
                text = ~bar_lab, textposition = "outside", cliponaxis = FALSE,
                textfont = list(color = lab_col, size = 9),
                hovertemplate = "%{customdata}<extra></extra>") %>%
          layout(
            paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
            showlegend = FALSE,
            title = list(text = chart_text, font = list(color = "#94a3b8", size = 12)),
            shapes = c(
              list(list(type = "line", x0 = 0, x1 = 0, yref = "paper", y0 = 0, y1 = 1,
                        line = list(color = "rgba(255,255,255,0.4)"))),
              rank_sep_shapes(d$id)),
            xaxis = list(title = "Past win rate of the pick's rank slot, as points above/below 50% (coin flip)",
                         color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
            yaxis = list(title = "", type = "category",
                         categoryorder = "array", categoryarray = rev(d$ylab),
                         range = c(-0.5, nrow(d) - 0.5),
                         tickfont = list(size = 9),
                         color = "#94a3b8", gridcolor = "rgba(255,255,255,0.05)"),
            margin = list(l = 130, r = 55, b = 50, t = 40))
      )
    }
    # BUY and ladder reuse the existing renderer branches:
    #   buys + shortlist toggle on  -> "shortlist"; off -> "all"
    #   ladder                      -> "rankall"
    view <- if (uview == "ladder") "rankall"
            else if (isTRUE(input$buys_shortlistBL)) "shortlist" else "all"

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
            xaxis = list(title = "Payoff-weighted expectancy (%, 0 = no signal-cell evidence)",
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
      if (view == "all") "No BUYs match the Regular/Sparse toggles and filters."
      else "No signal series right now."))

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
        if (hz_cut) sprintf("excess vs Benchmark %% so far (of %dmo)", hz)
        else sprintf("%dmo excess vs Benchmark %%", hz)
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
        `vs Benchmark %`      = df$excess_vs_spy_pct,
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
    uview <- bl_view_resolved("current")
    if (uview == "picks") {
      # Same top-~5%-per-trust-gated-cluster subset as the chart, as a table:
      # the names the validated rule would pick TODAY, with each one's live
      # gate action so the rule-vs-gate agreement is visible per row.
      d <- df[!is.na(df$id) & df$id <= 12, , drop = FALSE]
      tradeable <- unique(d$id[d$global_action == "BUY"])
      d <- d[d$id %in% tradeable, , drop = FALSE]
      if (nrow(d) == 0) return(DT::datatable(
        data.frame(Note = "No long cluster is trust-gated right now."),
        selection = "none", rownames = FALSE, class = "compact",
        options = list(dom = "t", ordering = FALSE)))
      d <- do.call(rbind, lapply(split(d, d$id), function(g) {
        g <- g[order(g$agg_rank), ]
        head(g, max(1L, ceiling(0.05 * nrow(g))))
      }))
      d <- d[order(d$id, d$agg_rank), ]
      d$pick_rank <- as.integer(stats::ave(d$agg_rank, d$id, FUN = seq_along))
      bin_win_shown <- ifelse(!is.na(d$bin_n) & d$bin_n >= 100, d$bin_win_pct, NA)
      display <- data.frame(
        Ticker          = d$ticker,
        Id              = d$id,
        `Pick rank`     = d$pick_rank,
        `Live action`   = d$global_action,
        `Serving rank`  = d$agg_rank,
        `Bin`           = as.integer(d$rank_bin),
        `Bin win %`     = bin_win_shown,
        `Expectancy`    = d$wtd_expectancy,
        `Cluster IC(12)` = d$cluster_ic_12,
        check.names = FALSE, stringsAsFactors = FALSE
      )
      return(DT::datatable(
        display, selection = "none", rownames = FALSE, class = "compact",
        extensions = "Buttons",
        options = list(
          pageLength = 25, lengthMenu = c(10, 25, 50, 100),
          dom = "Bftip", buttons = c("copy", "csv"),
          columnDefs = list(list(className = "dt-right", targets = 4:8))
        )
      ) %>% DT::formatStyle("Live action",
              color = DT::styleEqual(c("BUY", "SELL", "SKIP"),
                                     c("#10b981", "#dc2626", "#94a3b8"))))
    }
    view <- if (uview == "ladder") "rankall"
            else if (isTRUE(input$buys_shortlistBL)) "shortlist" else "all"

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
        data.frame(Note = "No signal series right now."),
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

  # ── FORECAST: backtest growth curve + live ledger + projection ──
  app_dataFC   <- reactiveVal(NULL)   # list(curve, live, ledger, ledseries, anchor, id_lbl)
  avail_idsFC  <- reactiveVal(NULL)   # ids present in the current cutoff (checkbox choices)
  status_msgFC <- reactiveVal("Ready")
  # caches for the input-INDEPENDENT queries: backtest curve + ledger are the same
  # every Generate, so fetch once per session. Only the live query re-runs.
  fc_curve     <- reactiveVal(NULL)
  fc_ledger    <- reactiveVal(NULL)
  fc_ledseries <- reactiveVal(NULL)
  output$statusMessageFC <- renderText({ status_msgFC() })

  # The valid cluster ids depend on which cutoff the History-start resolves to
  # (id 4 exists at the Dec-2024 cutoff but not at Jun-2024), so the checkboxes
  # must track the anchor. Load the full (cutoff, id) map once on Connect, then
  # repopulate the boxes locally whenever History-start changes.
  cut_ids_mapFC <- reactiveVal(NULL)
  # assemble the as-of date from the day/month/year dropdowns (mirror the Buy List)
  asof_dateFC <- function() {
    y <- suppressWarnings(as.integer(input$asof_yearFC))
    m <- suppressWarnings(as.integer(input$asof_monthFC))
    d <- suppressWarnings(as.integer(input$asof_dayFC))
    if (is.na(y) || is.na(m) || is.na(d)) return(NULL)
    safe_date <- function(s) tryCatch(as.Date(s), error = function(e) as.Date(NA))
    dt <- safe_date(sprintf("%04d-%02d-%02d", y, m, d))
    while (is.na(dt) && d > 28) { d <- d - 1; dt <- safe_date(sprintf("%04d-%02d-%02d", y, m, d)) }
    dt
  }
  set_asof_FC <- function(date) {
    updateSelectInput(session, "asof_yearFC",  selected = format(date, "%Y"))
    updateSelectInput(session, "asof_monthFC", selected = as.integer(format(date, "%m")))
    updateSelectInput(session, "asof_dayFC",   selected = as.integer(format(date, "%d")))
  }
  fc_ids_for <- function(anchor) {
    m <- cut_ids_mapFC(); if (is.null(m) || nrow(m) == 0) return(integer(0))
    elig <- m$cutoff[m$cutoff <= anchor]; if (length(elig) == 0) return(integer(0))
    sort(as.integer(unique(m$id[m$cutoff == max(elig)])))
  }
  fc_refresh_ids <- function() {
    a <- asof_dateFC(); if (is.null(a) || is.na(a)) return()
    ids <- fc_ids_for(as.Date(a))
    avail_idsFC(ids)
    updateCheckboxGroupInput(session, "idsFC", choices = ids, selected = ids, inline = TRUE)
  }

  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgFC("Error: Password is not set."); return() }
    status_msgFC("Connecting...")
    fc_curve(NULL); fc_ledger(NULL); fc_ledseries(NULL)  # invalidate caches on (re)connect
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM monitoring.prediction_ledger")$n[1]
      cim <- dbGetQuery(con, "SELECT DISTINCT train_cutoff_date AS cutoff, id FROM validation.walk_forward_top_picks ORDER BY 1, 2")
      cim$cutoff <- as.Date(cim$cutoff); cim$id <- as.integer(cim$id)
      cut_ids_mapFC(cim)
      updateSelectInput(session, "cutoffFC",
                        choices = c("Pick a cutoff..." = "",
                                    bl_cutoff_choices(sort(unique(cim$cutoff), decreasing = TRUE))))
      yr_lo <- as.integer(format(min(cim$cutoff), "%Y")); yr_hi <- as.integer(format(Sys.Date(), "%Y"))
      updateSelectInput(session, "asof_yearFC", choices = seq(yr_hi, yr_lo))
      def <- Sys.Date() - 365                              # default: ~12 months ago
      set_asof_FC(def)
      ids0 <- fc_ids_for(def); avail_idsFC(ids0)           # refresh ids now, don't wait for the flush
      updateCheckboxGroupInput(session, "idsFC", choices = ids0, selected = ids0, inline = TRUE)
      status_msgFC(sprintf("Connected - %s ledger rows; ids as of %s: %s.",
                           as.numeric(n), format(def, "%b %Y"), paste(ids0, collapse = ", ")))
    }, error = function(e) { status_msgFC(paste("Error:", e$message)) })
  })

  observeEvent(input$idsAllFC, {
    req(avail_idsFC())
    updateCheckboxGroupInput(session, "idsFC", choices = avail_idsFC(),
                             selected = avail_idsFC(), inline = TRUE)
  })
  observeEvent(input$idsNoneFC, {
    req(avail_idsFC())
    updateCheckboxGroupInput(session, "idsFC", choices = avail_idsFC(),
                             selected = character(0), inline = TRUE)
  })
  # As-of date changed -> repopulate the id boxes for that cutoff's clusters
  observeEvent(asof_dateFC(), {
    if (!is.null(cut_ids_mapFC())) fc_refresh_ids()
  }, ignoreInit = TRUE)
  observeEvent(input$asofRecentFC, { set_asof_FC(Sys.Date() - 365) })

  # Bundle picker: jump straight to a cutoff (one-way -> date dropdowns; the
  # asof-change observer above then repopulates the id boxes for that cutoff).
  observeEvent(input$cutoffFC, {
    v <- input$cutoffFC
    if (is.null(v) || !nzchar(v)) return()
    set_asof_FC(as.Date(v))
  }, ignoreInit = TRUE)

  output$cutoffResolvedFC <- renderUI({
    a <- asof_dateFC(); m <- cut_ids_mapFC()
    if (is.null(a) || is.na(a) || is.null(m) || nrow(m) == 0) return(NULL)
    cuts <- sort(unique(m$cutoff), decreasing = TRUE)
    elig <- cuts[cuts <= a]
    sty <- "color:#64748b; font-size:0.7rem; margin:-0.2rem 0 0.6rem;"
    if (length(elig) == 0)
      return(tags$p(sprintf("Before the first cutoff (%s).", format(min(cuts), "%Y-%m-%d")), style = sty))
    rc <- max(elig)
    note <- if (rc == max(cuts) && a > max(cuts)) " (no newer cutoff yet)" else ""
    tags$p(sprintf(paste("Entry = your date (%s). Selections = the model's picks at the",
                         "nearest quarterly cutoff %s%s - unchanged within a quarter, but you",
                         "still enter at your date's real prices."),
                   format(a, "%Y-%m-%d"), format(rc, "%Y-%m-%d"), note), style = sty)
  })

  observeEvent(input$execute_FC, {
    if (input$db_pass == "") { status_msgFC("Error: Password is not set."); return() }
    anchor <- asof_dateFC()
    if (is.null(anchor) || is.na(anchor)) { status_msgFC("Pick an as-of date first."); return() }
    status_msgFC(sprintf("Loading selected set + Benchmark from %s to today...", format(anchor, "%b %Y")))
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      DBI::dbExecute(con, "SET statement_timeout = '60s'")  # heavy forecast query; still < watchdog window
      ids_sel <- as.integer(input$idsFC)
      # empty OR every available id checked (the default) => no filter, "all ids"
      all_on  <- length(ids_sel) == 0 || (!is.null(avail_idsFC()) && setequal(ids_sel, avail_idsFC()))
      idfilter <- if (all_on) "" else sprintf("AND t.id IN (%s)", paste(sort(ids_sel), collapse = ","))
      id_lbl   <- if (all_on) "all ids" else paste("ids", paste(sort(ids_sel), collapse = ","))
      # curve + ledger are input-independent -> compute once per session, then cache
      # (curve is always all-ids: a per-cutoff id can't be traced across the 12 vintages).
      if (is.null(fc_curve())) fc_curve(coerce_numeric_cols(dbGetQuery(con, FORECAST_CURVE_SQL),
                  c("horizon_months","all_strategies_ret_pct","spy_ret_pct","beat_pct","n_cells")))
      curve <- fc_curve()
      if (is.null(fc_ledger())) {
        lg0 <- dbGetQuery(con, FORECAST_LEDGER_SQL)
        for (cc in setdiff(names(lg0), "entry_d")) lg0[[cc]] <- as.numeric(lg0[[cc]])
        fc_ledger(lg0) }
      ledger <- fc_ledger()
      if (is.null(fc_ledseries())) {
        ls0 <- dbGetQuery(con, FORECAST_LEDGER_SERIES_SQL)
        ls0$ledger_pct <- as.numeric(ls0$ledger_pct); ls0$spy_pct <- as.numeric(ls0$spy_pct)
        fc_ledseries(ls0) }
      ledseries <- fc_ledseries()
      # live portfolio depends on anchor + id filter + hold length -> always re-run (~0.7s)
      hold_v   <- if (is.null(input$holdFC)) "36" else input$holdFC
      H        <- as.integer(hold_v); to_today <- (hold_v == "240")
      market_max <- as.Date(dbGetQuery(con, "SELECT MAX(date) AS d FROM cdm.ingest_combined WHERE ticker='SPY'")$d[1])
      # An as-of date past the latest price bar has no forward window to hold, so
      # every downstream query comes back empty. Say THAT, don't blame the id filter.
      if (!is.na(market_max) && anchor > market_max) { app_dataFC(NULL)
        status_msgFC(sprintf("As-of date %s is past the latest price (%s) - no forward data to hold. Pick an earlier date (try '12 months ago').",
          format(anchor, "%b %d, %Y"), format(market_max, "%b %d, %Y"))); return() }
      live   <- dbGetQuery(con, gsub("__ANCHOR__", format(anchor, "%Y-%m-%d"),
                  gsub("__HOLD__", as.character(H),
                    gsub("__IDFILTER__", idfilter, FORECAST_LIVE_SQL, fixed = TRUE), fixed = TRUE), fixed = TRUE))
      if (nrow(live) == 0) { app_dataFC(NULL)
        status_msgFC(sprintf("No selections for %s in the cutoff at %s - clear the filter or choose other ids.",
          id_lbl, format(anchor, "%b %Y"))); return() }
      live$portfolio_pct <- as.numeric(live$portfolio_pct); live$spy_pct <- as.numeric(live$spy_pct)
      # Grey benchmark = the SAME 6-strategy DCA backtest for THIS window but over
      # ALL picks (no id filter), so the grey line tracks the selected period's
      # market instead of the all-years average. With no filter it equals the
      # portfolio, so reuse it; only pay for a second query when ids are narrowed.
      if (all_on) {
        bench <- data.frame(vdate = live$vdate, bench_pct = live$portfolio_pct, stringsAsFactors = FALSE)
      } else {
        b0 <- dbGetQuery(con, gsub("__ANCHOR__", format(anchor, "%Y-%m-%d"),
                gsub("__HOLD__", as.character(H),
                  gsub("__IDFILTER__", "", FORECAST_LIVE_SQL, fixed = TRUE), fixed = TRUE), fixed = TRUE))
        bench <- data.frame(vdate = b0$vdate, bench_pct = as.numeric(b0$portfolio_pct), stringsAsFactors = FALSE)
      }
      # Per-id decomposition (only when every id is selected): one backtest path per
      # cluster id, drawn as faint grey dashed lines labelled by id in the plot.
      perid <- NULL
      if (all_on) {
        p0 <- tryCatch(dbGetQuery(con, gsub("__ANCHOR__", format(anchor, "%Y-%m-%d"),
                gsub("__HOLD__", as.character(H), FORECAST_PERID_SQL, fixed = TRUE), fixed = TRUE)),
              error = function(e) NULL)
        if (!is.null(p0) && nrow(p0) > 0) { p0$ret_pct <- as.numeric(p0$ret_pct); p0$id <- as.integer(p0$id); perid <- p0 }
      }
      # risk-adjustment: equal-weight held basket monthly series -> beta / alpha / vol / info-ratio
      risk <- tryCatch({
        rs0 <- dbGetQuery(con, gsub("__ANCHOR__", format(anchor, "%Y-%m-%d"),
                 gsub("__HOLD__", as.character(H),
                   gsub("__IDFILTER__", idfilter, FORECAST_RISK_SQL, fixed = TRUE), fixed = TRUE), fixed = TRUE))
        bvv <- as.numeric(rs0$basket_val); svv <- as.numeric(rs0$spy_val)
        if (length(bvv) >= 5) {
          rb <- bvv[-1]/bvv[-length(bvv)] - 1; rsp <- svv[-1]/svv[-length(svv)] - 1
          vsr <- var(rsp); beta <- if (isTRUE(vsr > 0)) cov(rb, rsp)/vsr else NA_real_
          d_ <- rb - rsp
          list(beta = beta, alpha_ann = (mean(rb) - beta*mean(rsp))*12*100,
               ir = if (isTRUE(sd(d_) > 0)) mean(d_)/sd(d_)*sqrt(12) else NA_real_,
               vol_pf = sd(rb)*sqrt(12)*100, vol_spy = sd(rsp)*sqrt(12)*100, n = length(rb))
        } else NULL
      }, error = function(e) NULL)
      app_dataFC(list(curve = curve, live = live, bench = bench, perid = perid, all_on = all_on, ledger = ledger,
                      ledseries = ledseries, anchor = anchor,
                      id_lbl = id_lbl, hold_months = H, to_today = to_today, market_max = market_max, risk = risk))
      win_lbl <- if (to_today) sprintf("%d mo to today", nrow(live) - 1) else sprintf("%dmo hold", H)
      status_msgFC(sprintf("Loaded - %s from %s, %s; selected set %+.1f%% vs Benchmark %+.1f%%.",
        win_lbl, format(anchor, "%b %Y"), id_lbl, tail(live$portfolio_pct, 1), tail(live$spy_pct, 1)))
    }, error = function(e) { app_dataFC(NULL); status_msgFC(paste("Error:", e$message)) })
  })

  output$forecastNoteFC <- renderUI({
    d <- app_dataFC(); if (is.null(d)) return(NULL)
    live <- d$live; cv <- d$curve; lg <- d$ledger
    pend <- tail(live$portfolio_pct, 1); spend <- tail(live$spy_pct, 1)
    beat_live <- pend - spend
    beat12 <- cv$beat_pct[cv$horizon_months == 12][1]
    H <- if (is.null(d$hold_months)) 36 else d$hold_months
    win <- if (isTRUE(d$to_today)) sprintf("%d mo, held to today", nrow(live) - 1) else sprintf("%dmo hold", H)
    tags$div(style = "color:#cbd5e1; font-size:0.82rem; margin-bottom:0.6rem; line-height:1.5;",
      HTML(sprintf(paste0(
        "<b>As of %s</b> (%s): the selections returned <b>%+.1f%%</b> vs Benchmark <b>%+.1f%%</b> = <b>%+.1fpp</b>. ",
        "Typically +%.1fpp at 12mo (avg of all starts, not this window). &nbsp;|&nbsp; <b>Live log</b> (live out-of-sample, since %s): ",
        "<b>%+.1f%%</b> vs Benchmark <b>%+.1f%%</b> = <b>%+.1fpp</b>. A fixed hold makes as-of dates comparable; ",
        "'to today' lets old dates run 8-13 years (dropped series get frozen and drag them)."),
        format(d$anchor, "%b %Y"), win, pend, spend, beat_live, beat12,
        lg$entry_d[1], lg$basket_ret_pct[1], lg$spy_ret_pct[1], lg$basket_ret_pct[1] - lg$spy_ret_pct[1])))
  })

  # Risk-adjusted stats: does the beat survive accounting for how much market
  # risk (beta) and volatility the picks took? Computed on the equal-weight held
  # basket over the hold window (see FORECAST_RISK_SQL).
  output$forecastRiskFC <- renderUI({
    d <- app_dataFC(); if (is.null(d) || is.null(d$risk)) return(NULL)
    r <- d$risk
    a_col <- if (isTRUE(r$alpha_ann > 0)) "#34d399" else "#f87171"
    b_note <- if (isTRUE(r$beta < 1)) "less market risk than benchmark" else "more market risk than benchmark"
    card <- function(label, val, col = "#e2e8f0", sub = "") tags$div(
      style = "flex:1; min-width:118px; padding:8px 14px; background:rgba(148,163,184,0.06); border-radius:8px;",
      tags$div(label, style = "color:#94a3b8; font-size:0.64rem; text-transform:uppercase; letter-spacing:0.04em;"),
      tags$div(val, style = paste0("color:", col, "; font-size:1.15rem; font-weight:700; font-variant-numeric:tabular-nums;")),
      if (nzchar(sub)) tags$div(sub, style = "color:#64748b; font-size:0.62rem;"))
    verdict <- if (isTRUE(r$alpha_ann > 0) && isTRUE(r$beta <= 1.05))
        "Positive alpha with beta near/below 1: the beat is NOT just market risk - it's real risk-adjusted edge over this window (still in-sample; not proof of future edge)."
      else if (isTRUE(r$alpha_ann > 0))
        "Positive alpha, but beta > 1: part of the raw beat is extra market risk; the alpha is the skill portion left after stripping beta."
      else "Alpha <= 0: no risk-adjusted edge over this window - the raw beat (if any) was just market risk."
    tags$div(
      tags$div(style = "display:flex; gap:8px; flex-wrap:wrap; margin-bottom:0.4rem;",
        card("Alpha (ann.)", sprintf("%+.1f%%", r$alpha_ann), a_col, "beat beyond beta"),
        card("Beta vs benchmark", sprintf("%.2f", r$beta), "#e2e8f0", b_note),
        card("Info ratio", sprintf("%.2f", r$ir), if (isTRUE(r$ir > 0.5)) "#34d399" else "#e2e8f0", "excess / tracking risk"),
        card("Vol selections / benchmark", sprintf("%.0f%% / %.0f%%", r$vol_pf, r$vol_spy), "#e2e8f0", sprintf("%d monthly obs", r$n))),
      tags$p(verdict, style = "color:#cbd5e1; font-size:0.75rem; margin-bottom:0.7rem;"))
  })

  output$forecastPlotFC <- renderPlotly({
    req(app_dataFC())
    d <- app_dataFC(); live <- d$live; cv <- d$curve; lg <- d$ledger; anchor <- d$anchor
    if (is.null(live) || nrow(live) == 0) return(empty_plot("No data."))
    pf_lbl <- if (is.null(d$id_lbl) || d$id_lbl == "all ids") "current selections" else d$id_lbl
    live$vdate <- as.Date(live$vdate)
    realized_end <- max(live$vdate)                          # last date with real prices in window
    H  <- if (is.null(d$hold_months)) 36 else d$hold_months
    to_today <- isTRUE(d$to_today)
    mx <- if (is.null(d$market_max)) realized_end else as.Date(d$market_max)
    add_m <- function(dt, m) { p <- as.POSIXlt(dt); p$mon <- p$mon + m; as.Date(p) }
    exit_date  <- if (to_today) mx else add_m(anchor, H)     # intended sell date
    project    <- exit_date > realized_end                   # hold extends past available data
    show_today <- realized_end >= mx                          # window reached the present
    right_edge <- add_m(max(exit_date, realized_end), 2)

    # curve (all-years avg) is kept only to slope the forward projection and feed
    # the 'typically' note; the grey LINE itself is now the all-picks backtest for
    # THIS window (below), so it tracks the selected period's market.
    hmon_full <- c(0, cv$horizon_months); yb_all <- c(0, cv$all_strategies_ret_pct); yb_spy <- c(0, cv$spy_ret_pct)
    mfun_all <- function(m) approx(hmon_full, yb_all, xout = m, rule = 2)$y
    mfun_spy <- function(m) approx(hmon_full, yb_spy, xout = m, rule = 2)$y
    pend  <- tail(live$portfolio_pct, 1); spend <- tail(live$spy_pct, 1)

    fig <- plot_ly()
    perid_ann <- list()
    # Grey backtest lines. Filtered (a narrowed subset): ONE all-selections line.
    # All ids on: ONE faint grey line PER id (the per-cluster backtest), each
    # labelled with its id at the right end, so you can see which cluster drove
    # the result. (With all ids on the single all-picks line would just overlap
    # the green line, so we decompose instead.)
    if (!isTRUE(d$all_on) && !is.null(d$bench) && nrow(d$bench) > 0) {
      bench <- d$bench; bench$vdate <- as.Date(bench$vdate)
      fig <- add_trace(fig, x = bench$vdate, y = bench$bench_pct, type = "scatter", mode = "lines",
        name = "All selections (this window)", line = list(color = "#94a3b8", width = 2, dash = "dot"),
        hovertemplate = "all selections<br>%{x|%b %Y}: %{y:.1f}%<extra></extra>")
    } else if (isTRUE(d$all_on) && !is.null(d$perid) && nrow(d$perid) > 0) {
      pid <- d$perid; pid$vdate <- as.Date(pid$vdate); leg <- TRUE
      for (k in sort(unique(pid$id))) {
        sub <- pid[pid$id == k, ]; sub <- sub[order(sub$vdate), ]
        if (nrow(sub) == 0) next
        fig <- add_trace(fig, x = sub$vdate, y = sub$ret_pct, type = "scatter", mode = "lines",
          name = "per-cluster backtest", legendgroup = "perid", showlegend = leg,
          line = list(color = "rgba(148,163,184,0.5)", width = 1, dash = "dot"),
          hovertemplate = paste0("cluster ", k, "<br>%{x|%b %Y}: %{y:.1f}%<extra></extra>"))
        leg <- FALSE
        last <- sub[nrow(sub), ]
        perid_ann[[length(perid_ann) + 1]] <- list(x = format(last$vdate, "%Y-%m-%d"), y = last$ret_pct,
          text = as.character(k), showarrow = FALSE, xanchor = "left", xshift = 5,
          font = list(color = "rgba(148,163,184,0.95)", size = 10))
      }
    }
    fig <- add_trace(fig, x = live$vdate, y = live$spy_pct, type = "scatter", mode = "lines",
      name = "Benchmark", legendgroup = "spy", line = list(color = "#3b82f6", width = 3),
      hovertemplate = "Benchmark<br>%{x|%b %Y}: %{y:.1f}%<extra></extra>")
    fig <- add_trace(fig, x = live$vdate, y = live$portfolio_pct, type = "scatter", mode = "lines",
      name = sprintf("Selected set (%s)", pf_lbl), legendgroup = "pf", line = list(color = "#10b981", width = 3.5),
      hovertemplate = "Selected set<br>%{x|%b %Y}: %{y:.1f}%<extra></extra>")

    shapes <- list(); ann <- list()
    if (show_today) {
      tdy <- format(realized_end, "%Y-%m-%d")
      shapes[[length(shapes) + 1]] <- list(type = "line", x0 = tdy, x1 = tdy, yref = "paper", y0 = 0, y1 = 1,
        line = list(color = "rgba(148,163,184,0.5)", width = 1, dash = "dot"))
      ann[[length(ann) + 1]] <- list(x = tdy, y = 1, yref = "paper", text = "today", showarrow = FALSE,
        font = list(color = "#94a3b8", size = 11), yanchor = "bottom")
    }
    if (project) {
      # hold extends past available prices -> dotted projection realized_end -> exit along backtest slope
      mo_r <- as.numeric(realized_end - anchor) / 30.44
      p_proj <- pend  + (mfun_all(H) - mfun_all(mo_r)); s_proj <- spend + (mfun_spy(H) - mfun_spy(mo_r))
      fig <- add_trace(fig, x = c(realized_end, exit_date), y = c(spend, s_proj), type = "scatter", mode = "lines",
        legendgroup = "spy", showlegend = FALSE, line = list(color = "#3b82f6", width = 2, dash = "dot"),
        hovertemplate = "Benchmark projected<br>%{y:.1f}%<extra></extra>")
      fig <- add_trace(fig, x = c(realized_end, exit_date), y = c(pend, p_proj), type = "scatter", mode = "lines",
        legendgroup = "pf", showlegend = FALSE, line = list(color = "#10b981", width = 2.5, dash = "dot"),
        hovertemplate = "Selected set projected<br>%{y:.1f}%<extra></extra>")
      shapes[[length(shapes) + 1]] <- list(type = "rect", x0 = format(realized_end, "%Y-%m-%d"),
        x1 = format(exit_date, "%Y-%m-%d"), yref = "paper", y0 = 0, y1 = 1,
        fillcolor = "rgba(148,163,184,0.06)", line = list(width = 0))
      # future (projected) endpoint labels: kept ('nice to see') but dimmed
      fx <- format(exit_date, "%Y-%m-%d")
      ann[[length(ann) + 1]] <- list(x = fx, y = p_proj, text = sprintf("%+.0f%%", p_proj),
        showarrow = FALSE, xanchor = "left", xshift = 6, font = list(color = "rgba(16,185,129,0.5)", size = 11))
      ann[[length(ann) + 1]] <- list(x = fx, y = s_proj, text = sprintf("%+.0f%%", s_proj),
        showarrow = FALSE, xanchor = "left", xshift = 6, font = list(color = "rgba(59,130,246,0.5)", size = 11))
    }
    # PRESENT value labels: bold, at the last realized point - right where the solid
    # line ends and any dashed projection begins. Placed just LEFT of 'today' when
    # projecting (so they read before the dashed line), else just right of the end.
    px  <- format(realized_end, "%Y-%m-%d")
    pxa <- if (project) "right" else "left"; pxs <- if (project) -8 else 6
    ann[[length(ann) + 1]] <- list(x = px, y = pend, text = sprintf("%+.0f%%", pend),
      showarrow = FALSE, xanchor = pxa, xshift = pxs, font = list(color = "#10b981", size = 13))
    ann[[length(ann) + 1]] <- list(x = px, y = spend, text = sprintf("%+.0f%%", spend),
      showarrow = FALSE, xanchor = pxa, xshift = pxs, font = list(color = "#3b82f6", size = 13))

    ttl <- if (to_today) "Actual selected set vs Benchmark vs backtest (held to today)"
           else sprintf("Actual selected set vs Benchmark vs backtest (%dmo hold)", H)
    dark_layout(fig,
      title = list(text = ttl, font = list(color = "#f8fafc", size = 15), x = 0.5),
      xaxis = list(title = "", color = "#cbd5e1", type = "date",
                   range = c(format(anchor, "%Y-%m-%d"), format(right_edge, "%Y-%m-%d")),
                   gridcolor = "rgba(148,163,184,0.10)", zeroline = FALSE),
      yaxis = list(title = "Cumulative return (%)", color = "#cbd5e1",
                   gridcolor = "rgba(148,163,184,0.10)", zeroline = TRUE,
                   zerolinecolor = "rgba(148,163,184,0.30)", ticksuffix = "%"),
      legend = list(font = list(color = "#e2e8f0"), orientation = "h", x = 0, y = -0.14),
      hovermode = "closest", shapes = shapes, annotations = c(ann, perid_ann),
      margin = list(l = 60, r = 80, t = 50, b = 50))
  })

  # Ledger on its OWN clock (fair): both rebased to 0 at the June-2026 inception.
  output$forecastLedgerFC <- renderPlotly({
    req(app_dataFC())
    d <- app_dataFC(); ls <- d$ledseries; cv <- d$curve
    if (is.null(ls) || nrow(ls) == 0) return(empty_plot("No log data yet."))
    ls$d <- as.Date(ls$d)
    start <- min(ls$d); today <- max(ls$d)
    lend <- tail(ls$ledger_pct, 1); send <- tail(ls$spy_pct, 1)
    # Out-of-sample panel: show ONLY the realized ledger track. No typical-pace
    # projection - it borrowed the backtest portfolio's slope (not the ledger's),
    # and reading a down 6-week ledger as "heading up" was misleading.
    fig <- plot_ly()
    fig <- add_trace(fig, x = ls$d, y = ls$spy_pct, type = "scatter", mode = "lines", name = "Benchmark",
      legendgroup = "s", line = list(color = "#3b82f6", width = 2.5),
      hovertemplate = "Benchmark<br>%{x|%b %d}: %{y:.1f}%<extra></extra>")
    fig <- add_trace(fig, x = ls$d, y = ls$ledger_pct, type = "scatter", mode = "lines", name = "Live log (signal basket)",
      legendgroup = "l", line = list(color = "#f59e0b", width = 3),
      hovertemplate = "Live log<br>%{x|%b %d}: %{y:.1f}%<extra></extra>")
    tdy <- format(today, "%Y-%m-%d")
    ann <- list(
      list(x = tdy, y = 1, yref = "paper", text = "today", showarrow = FALSE,
           font = list(color = "#94a3b8", size = 10), yanchor = "bottom"),
      list(x = tdy, y = lend, text = sprintf("%+.1f%%", lend), showarrow = FALSE,
           xanchor = "left", xshift = 6, font = list(color = "#f59e0b", size = 12)),
      list(x = tdy, y = send, text = sprintf("%+.1f%%", send), showarrow = FALSE,
           xanchor = "left", xshift = 6, font = list(color = "#3b82f6", size = 12)))
    dark_layout(fig,
      title = list(text = "Live log vs Benchmark, own clock (out-of-sample, since Jun 2026)",
                   font = list(color = "#f8fafc", size = 13), x = 0.5),
      xaxis = list(title = "", color = "#cbd5e1", type = "date",
                   gridcolor = "rgba(148,163,184,0.10)", zeroline = FALSE),
      yaxis = list(title = "Return since Jun 2026 (%)", color = "#cbd5e1",
                   gridcolor = "rgba(148,163,184,0.10)", zeroline = TRUE,
                   zerolinecolor = "rgba(148,163,184,0.30)", ticksuffix = "%"),
      legend = list(font = list(color = "#e2e8f0"), orientation = "h", x = 0, y = -0.2),
      hovermode = "x unified",
      shapes = list(list(type = "line", x0 = tdy, x1 = tdy, yref = "paper", y0 = 0, y1 = 1,
                         line = list(color = "rgba(148,163,184,0.5)", width = 1, dash = "dot"))),
      annotations = ann,
      margin = list(l = 60, r = 52, t = 44, b = 40))
  })

  output$forecastTableFC <- renderUI({
    req(app_dataFC())
    d <- app_dataFC(); live <- d$live; cv <- d$curve; lg <- d$ledger
    pend <- tail(live$portfolio_pct, 1); spend <- tail(live$spy_pct, 1)
    beat12 <- cv$beat_pct[cv$horizon_months == 12][1]
    exp12  <- cv$all_strategies_ret_pct[cv$horizon_months == 12][1]
    H <- if (is.null(d$hold_months)) 36 else d$hold_months
    since_anchor <- if (isTRUE(d$to_today)) sprintf("%s, to today", format(d$anchor, "%b %Y"))
                    else sprintf("%s, %dmo hold", format(d$anchor, "%b %Y"), H)

    th  <- "padding:6px 16px; text-align:right; color:#94a3b8; font-weight:600; font-size:0.7rem; text-transform:uppercase; letter-spacing:0.03em; border-bottom:1px solid rgba(148,163,184,0.22);"
    thl <- sub("text-align:right", "text-align:left", th, fixed = TRUE)
    tdl <- "padding:5px 16px; text-align:left; font-variant-numeric:tabular-nums;"
    tdr <- "padding:5px 16px; text-align:right; font-variant-numeric:tabular-nums;"
    row <- function(nm, win, ret, vs, col) tags$tr(
      tags$td(nm,  style = paste0(tdl, "color:", col, "; font-weight:600;")),
      tags$td(win, style = paste0(tdl, "color:#94a3b8;")),
      tags$td(ret, style = paste0(tdr, "color:#e2e8f0;")),
      tags$td(vs,  style = paste0(tdr, "color:", col, ";")))
    tags$div(style = "overflow-x:auto;",
      tags$table(style = "border-collapse:collapse; width:100%; max-width:640px;",
        tags$thead(tags$tr(
          tags$th("Series", style = thl), tags$th("Window", style = thl),
          tags$th("Return", style = th), tags$th("vs Benchmark", style = th))),
        tags$tbody(
          row("Selected set (current selections)", since_anchor, sprintf("%+.1f%%", pend),
              sprintf("%+.1fpp", pend - spend), "#34d399"),
          row("Benchmark", since_anchor, sprintf("%+.1f%%", spend), "—", "#93c5fd"),
          row("Live log (live signal)", sprintf("since %s", lg$entry_d[1]),
              sprintf("%+.1f%%", lg$basket_ret_pct[1]),
              sprintf("%+.1fpp", lg$basket_ret_pct[1] - lg$spy_ret_pct[1]), "#fbbf24"),
          row("Typical (avg of all starts, 12mo)", "avg 2012-2024", sprintf("%+.1f%%", exp12),
              sprintf("%+.1fpp", beat12), "#94a3b8"))),
      tags$p(paste("Selected set (green) = your selections (latest cutoff) run through phased entry in",
                   "real prices; narrows with the id filter. Grey = the same backtest over ALL selections for",
                   "the selected window, shown only when you've filtered (otherwise it equals the",
                   "selection). Bold % marks today's level where the solid line ends; dotted lines carry",
                   "it forward along the typical all-years slope."),
             style = "color:#64748b; font-size:0.7rem; margin-top:0.5rem;"))
  })

  # ── LIFECYCLE: buy / hold / sell state matrix over ledger snapshots ──
  # Rows = every series ever signaled BUY in monitoring.prediction_ledger;
  # columns = the recorded daily snapshots. Cell state machine (per ticker,
  # walked in date order over the FULL record):
  #   ledger BUY with no open position -> "buy" (entry starts the maturity clock)
  #   open position, no trigger        -> "hold" (covers continuing-BUY and SKIP
  #                                        wash-outs: the ledger only records
  #                                        BUY/SELL, absence = washed out)
  #   triggers -> "sell", first match wins: ledger SELL row ("gate flipped"),
  #     delisted on/before the date ("delisted"), horizon elapsed ("matured")
  #   sold stays "sell" (sticky, like the sketch) until a fresh BUY re-entry.
  # Raw pulls are cached in app_dataLC; the state machine re-derives live on
  # horizon/snapshot-count changes without re-querying.
  app_dataLC   <- reactiveVal(NULL)
  status_msgLC <- reactiveVal("Ready")
  # the board's cluster-id selection, preserved across data refreshes (same
  # pattern as lc_chart_ids_sel, so an auto-refresh re-render cannot reset the
  # user's narrowing): NULL = never touched -> all on; character(0) = a real
  # deselect-all -> empty board. Reset on (re)connect / env switch only.
  lc_board_ids_sel <- reactiveVal(NULL)
  lcAutoTick   <- reactiveVal(0)
  observeEvent(input$idsLC, {
    lc_board_ids_sel(if (is.null(input$idsLC)) character(0)
                     else as.character(input$idsLC))
  }, ignoreNULL = FALSE, ignoreInit = TRUE)
  output$statusMessageLC <- renderText({ status_msgLC() })

  # ── Personal portfolio: strategy follower (model DCA + ladder sells) ──────
  lc_pos <- reactiveVal(load_portfolio())
  lc_dismissed <- reactiveVal(load_dismissed())
  lc_backend <- reactiveVal("parquet")   # flips to "db" once a portfolio-schema DB is seen

  # Route a positions/dismissed save to the DB (authoritative, shared) when the
  # connected DB has the portfolio schema, else to the local parquet files. The
  # observers below compute a full merged df and call these, unchanged otherwise.
  persist_positions <- function(df) {
    if (!identical(lc_backend(), "db")) { save_portfolio(df); return(invisible(TRUE)) }
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) {
      showNotification("DB unreachable - change not saved.", type = "error"); return(invisible(FALSE)) }
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    tryCatch({ db_save_positions(con, df); invisible(TRUE) },
      error = function(e) {
        showNotification(paste("Save failed:", e$message), type = "error"); invisible(FALSE) })
  }
  persist_dismissed <- function(v) {
    if (!identical(lc_backend(), "db")) { save_dismissed(v); return(invisible(TRUE)) }
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) return(invisible(FALSE))
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    tryCatch({ db_save_dismissed(con, v); invisible(TRUE) }, error = function(e) invisible(FALSE))
  }

  # Default the table to the qualstream orange-+ buys: on every Generate, each
  # current BUY graded >= 68 (dv$qs_buys, the exact board + set) is auto-added as
  # a model-linked $100/monthly plan unless it is already tracked or was
  # explicitly removed (dismissed). Idempotent: once seeded, qs_buys is a subset
  # of tracked+dismissed, so `toAdd` is empty and no further write fires (the
  # self-trigger on lc_pos() settles after one no-op pass).
  observe({
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    if (is.null(dv) || is.null(dv$qs_buys) || !length(dv$qs_buys)) return()
    cur <- lc_pos(); dis <- lc_dismissed()
    toAdd <- setdiff(dv$qs_buys, union(toupper(cur$ticker), dis))
    if (!length(toAdd)) return()
    now <- format(Sys.time())
    rows <- do.call(rbind, lapply(seq_along(toAdd), function(i) {
      tk <- toAdd[i]
      data.frame(
        id = paste0(tk, "-seed", i, "-", format(Sys.time(), "%Y%m%d%H%M%S")),
        ticker = tk, amount_usd = 100, cadence = "monthly",
        # Stored start = the day it was actually added (the personal truth used
        # by the "since I added" basis); the epoch basis ignores it and tracks
        # from the regime epoch inside gated_expand_schedule.
        day1 = as.integer(format(Sys.Date(), "%d")),
        day2 = NA_integer_, start_date = format(Sys.Date()),
        end_date = "", sold_date = "", sold_fraction = NA_real_,
        mode = "model", adopted_at = now, created_at = now,
        stringsAsFactors = FALSE)
    }))
    merged <- rbind(cur, rows)
    persist_positions(merged); lc_pos(merged)
  })

  # Transition log: on each Generate, snapshot every tracked ticker's current
  # state (buy/hold/sell/closed) to portfolio.state_history, upserted per
  # (ticker, as_of, hz). Same-day re-Generate refreshes; new days accumulate ->
  # the Buy->Hold->Sell trail per position. DB-backend only (needs the schema).
  observe({
    if (!identical(lc_backend(), "db")) return()
    dv <- tryCatch(derivedLC(), error = function(e) NULL); if (is.null(dv)) return()
    p <- lc_pos(); if (is.null(p) || !nrow(p)) return()
    ticks <- toupper(unique(p$ticker))
    st <- dv$state_now; wy <- dv$why_now; gr <- dv$qs_grade
    getv <- function(m, t, d) if (!is.null(m) && t %in% names(m)) unname(m[[t]]) else d
    rows <- data.frame(
      ticker = ticks, as_of = format(Sys.Date()), hz = dv$hz,
      state = vapply(ticks, function(t) getv(st, t, NA_character_), character(1)),
      why   = vapply(ticks, function(t) getv(wy, t, NA_character_), character(1)),
      grade = vapply(ticks, function(t) as.numeric(getv(gr, t, NA_real_)), numeric(1)),
      stringsAsFactors = FALSE)
    rows <- rows[!is.na(rows$state) & nzchar(rows$state), , drop = FALSE]  # only what the board knows
    if (!nrow(rows)) return()
    con <- tryCatch(get_con(input), error = function(e) NULL); if (is.null(con)) return()
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    tryCatch(db_upsert_state_history(con, rows), error = function(e) NULL)
  })

  observeEvent(input$lcPosAdd, {
    tk <- input$lcPosTicker; if (is.null(tk)) tk <- ""
    tk <- toupper(trimws(tk))
    amt <- suppressWarnings(as.numeric(input$lcPosAmt))
    if (!nzchar(tk) || !grepl("^[A-Z.-]{1,10}$", tk)) {
      showNotification("Enter a valid ticker (1-10 letters, dot, dash).", type = "warning"); return() }
    if (is.na(amt) || amt <= 0) {
      showNotification("Enter a positive dollar amount.", type = "warning"); return() }
    cur <- lc_pos()
    # one ticker = one row = one chart line: the per-ticker series and the
    # row-perf ticker->id map both collapse duplicates, so block them here.
    if (tk %in% toupper(cur$ticker)) {
      showNotification(sprintf("%s is already tracked - edit its $/buy or remove it first.", tk),
                       type = "warning"); return() }
    day <- suppressWarnings(as.integer(input$lcPosDay)); if (is.na(day)) day <- 1L
    day <- min(max(day, 1L), 28L)
    sd <- suppressWarnings(as.Date(input$lcPosStart)); if (is.na(sd)) sd <- Sys.Date()
    newrow <- data.frame(
      id = paste0(tk, "-", format(Sys.time(), "%Y%m%d%H%M%OS2")),
      ticker = tk, amount_usd = amt, cadence = input$lcPosCadence,
      day1 = day, day2 = NA_integer_, start_date = format(sd), end_date = "",
      sold_date = "", sold_fraction = NA_real_, mode = "manual", adopted_at = "",
      created_at = format(Sys.time()), stringsAsFactors = FALSE)
    merged <- rbind(cur, newrow)
    persist_positions(merged); lc_pos(merged)
    if (tk %in% lc_dismissed()) {            # an explicit add overrides a removal
      nd <- setdiff(lc_dismissed(), tk); persist_dismissed(nd); lc_dismissed(nd) }
    updateTextInput(session, "lcPosTicker", value = "")
  })

  # Adopt-from-model controls: only the board's current BUY tickers not yet tracked
  output$lcAdoptRow <- renderUI({
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    if (is.null(dv) || is.null(dv$state_now))
      return(div(style = "color:#64748b; font-size:0.72rem;",
        "Generate the board to adopt the model's BUY names. Manual plans work without it."))
    buys    <- names(dv$state_now)[dv$state_now == "buy"]
    choices <- sort(setdiff(buys, toupper(lc_pos()$ticker)))
    if (!length(choices))
      return(div(style = "color:#64748b; font-size:0.72rem;",
        "No new model BUYs to adopt (all current buys already tracked)."))
    fluidRow(
      column(3, selectInput("lcAdoptTicker", "Adopt model BUY", choices = choices)),
      column(2, numericInput("lcAdoptAmt", "$ per buy", value = 100, min = 1)),
      column(3, selectInput("lcAdoptCadence", "Cadence",
               choices = c("Monthly" = "monthly", "One-off" = "once"), selected = "monthly")),
      column(2, numericInput("lcAdoptDay", "Day", value = 1, min = 1, max = 28)),
      column(2, div(style = "margin-top:1.6rem;",
               actionButton("lcAdoptBtn", "Adopt", class = "btn-primary"))))
  })

  observeEvent(input$lcAdoptBtn, {
    tk <- toupper(trimws(as.character(input$lcAdoptTicker)))
    if (!nzchar(tk)) return()
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    st <- if (!is.null(dv) && !is.null(dv$state_now) && tk %in% names(dv$state_now))
            dv$state_now[[tk]] else NA
    if (!identical(st, "buy")) {
      showNotification("That ticker is no longer a model BUY.", type = "warning"); return() }
    if (tk %in% toupper(lc_pos()$ticker)) {
      showNotification(sprintf("%s is already tracked.", tk), type = "warning"); return() }
    amt <- suppressWarnings(as.numeric(input$lcAdoptAmt))
    if (is.na(amt) || amt <= 0) {
      showNotification("Enter a positive dollar amount.", type = "warning"); return() }
    day <- suppressWarnings(as.integer(input$lcAdoptDay)); if (is.na(day)) day <- 1L
    day <- min(max(day, 1L), 28L)
    newrow <- data.frame(
      id = paste0(tk, "-", format(Sys.time(), "%Y%m%d%H%M%OS2")),
      ticker = tk, amount_usd = amt, cadence = input$lcAdoptCadence,
      day1 = day, day2 = NA_integer_, start_date = format(Sys.Date()), end_date = "",
      sold_date = "", sold_fraction = NA_real_, mode = "model",
      adopted_at = format(Sys.time()), created_at = format(Sys.time()),
      stringsAsFactors = FALSE)
    merged <- rbind(lc_pos(), newrow)
    persist_positions(merged); lc_pos(merged)
    if (tk %in% lc_dismissed()) {            # adopting overrides a prior removal
      nd <- setdiff(lc_dismissed(), tk); persist_dismissed(nd); lc_dismissed(nd) }
  })

  observeEvent(input$lcPosRemove, {
    sel <- input$lcPosChecked; cur <- lc_pos()
    if (is.null(cur) || !nrow(cur)) return()
    sel <- suppressWarnings(as.integer(sel))
    sel <- sel[!is.na(sel) & sel >= 1 & sel <= nrow(cur)]
    if (!length(sel)) { showNotification("Check one or more rows to remove.", type = "message"); return() }
    removed <- toupper(cur$ticker[sel])
    merged <- cur[setdiff(seq_len(nrow(cur)), sel), , drop = FALSE]
    persist_positions(merged); lc_pos(merged)
    # remember the removal so the qualstream auto-seed won't re-add it next Generate
    nd <- union(lc_dismissed(), removed); persist_dismissed(nd); lc_dismissed(nd)
  })
  # Clear all = reset to defaults: wipe positions AND dismissals, so the next
  # Generate re-seeds the current qualstream orange-+ buys from scratch.
  observeEvent(input$lcPosClear, {
    persist_positions(portfolio_empty()); lc_pos(portfolio_empty())
    persist_dismissed(character(0)); lc_dismissed(character(0))
  })

  # Tracking basis for MODEL rows (radio next to the chart filter): "epoch" =
  # the strategy's record since the regime epoch (default); "stored" = only the
  # user's own cash flows since each row was added. Threaded into every
  # gated_expand_schedule call so table, chart, note and sells stay consistent.
  lc_track_basis <- reactive({
    if (identical(input$lcTrackBasis, "stored")) "stored" else "epoch"
  })

  # sell triggers for tracked model rows: pure R, cheap; NULL unless a model
  # position is currently in a sell/closed board state with buys behind it.
  lc_sell_triggers <- reactive({
    p <- lc_pos(); if (is.null(p) || !nrow(p)) return(NULL)
    mt <- if ("mode" %in% names(p)) toupper(p$ticker[p$mode == "model"]) else character(0)
    mt <- mt[nzchar(mt)]; if (!length(mt)) return(NULL)
    d <- app_dataLC(); if (is.null(d)) return(NULL)
    dv <- tryCatch(derivedLC(), error = function(e) NULL); if (is.null(dv)) return(NULL)
    tr <- sell_triggers_from_dv(dv, d$led, d$meta, mt)
    if (is.null(tr) || !nrow(tr)) return(NULL)
    sched <- gated_expand_schedule(p, d$led, model_start = lc_track_basis())
    keep <- vapply(seq_len(nrow(tr)), function(i)
      any(sched$ticker == tr$ticker[i] & as.Date(sched$d) <= tr$trig_d[i]), logical(1))
    tr <- tr[keep, , drop = FALSE]
    if (nrow(tr)) tr else NULL
  })

  # ladder query is the expensive step (~0.4s/ticker): cache on the trigger
  # signature so holdLC toggles / re-Generates with the same triggers are free.
  lc_ladder_env <- new.env(parent = emptyenv())   # plain memo cache (not reactive)
  lc_sell_events <- reactive({
    tr <- lc_sell_triggers(); if (is.null(tr)) return(NULL)
    key <- paste(sprintf("%s|%s|%s|%s", tr$ticker, tr$trig_d, tr$reason, tr$n_bars),
                 collapse = ";")
    if (!is.null(lc_ladder_env$key) && identical(lc_ladder_env$key, key))
      return(lc_ladder_env$val)
    if (is.null(input$db_pass) || input$db_pass == "") return(NULL)
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) return(NULL)
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    val <- tryCatch(build_sell_events(con, tr), error = function(e) NULL)
    lc_ladder_env$key <- key; lc_ladder_env$val <- val
    val
  })

  lc_port_series <- reactive({
    p <- lc_pos()
    if (is.null(p) || nrow(p) == 0) return(NULL)
    if (is.null(input$db_pass) || input$db_pass == "") return(NULL)
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) return(NULL)
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    d  <- app_dataLC()
    ev <- lc_sell_events()
    sold <- if (!is.null(ev) && !is.null(ev$events))
              ev$events[!is.na(ev$events$sell_d), , drop = FALSE] else NULL
    tryCatch(compute_portfolio_series(con, p, led = if (!is.null(d)) d$led else NULL,
                                      sell_events = sold,
                                      model_start = lc_track_basis()),
             error = function(e) NULL)
  })

  # Per-row performance as of today, DERIVED from lc_ticker_series (one shared
  # query feeds both the table and the chart - no second connection). Take the
  # latest priced row per ticker and map ticker -> position id via lc_pos. Returns
  # (id, since, invested, value, spy_value) or NULL; `since` = the first actual
  # fill date under the active basis (the series starts at each ticker's first buy).
  lc_row_perf <- reactive({
    p <- lc_pos(); if (is.null(p) || !nrow(p)) return(NULL)
    ser <- tryCatch(lc_ticker_series(), error = function(e) NULL)
    if (is.null(ser) || !nrow(ser)) return(NULL)
    ser <- ser[ser$ticker != "__SPY__" & !is.na(ser$invested), , drop = FALSE]
    if (!nrow(ser)) return(NULL)
    ser$d <- as.Date(ser$d)
    ser <- ser[order(ser$ticker, ser$d), , drop = FALSE]
    last  <- ser[!duplicated(ser$ticker, fromLast = TRUE), , drop = FALSE]  # latest per ticker
    first <- ser[!duplicated(ser$ticker), , drop = FALSE]                   # first fill per ticker
    tick2id <- setNames(as.character(p$id), toupper(as.character(p$ticker)))
    data.frame(id = unname(tick2id[last$ticker]),
               since = format(first$d[match(last$ticker, first$ticker)]),
               invested = as.numeric(last$invested), value = as.numeric(last$value),
               spy_value = as.numeric(last$spy_value), stringsAsFactors = FALSE)
  })

  # Per-ticker cumulative-return series for the holdings chart. Same gated buy
  # expansion as lc_row_perf, run through LC_PORTFOLIO_TICKER_SQL: tidy
  # (d, ticker, ret_pct) with a '__SPY__' benchmark line.
  lc_ticker_series <- reactive({
    p <- lc_pos(); if (is.null(p) || !nrow(p)) return(NULL)
    if (is.null(input$db_pass) || input$db_pass == "") return(NULL)
    d <- app_dataLC(); led <- if (!is.null(d)) d$led else NULL
    basis <- lc_track_basis()
    parts <- lapply(seq_len(nrow(p)), function(i) {
      s <- tryCatch(gated_expand_schedule(p[i, , drop = FALSE], led, model_start = basis),
                    error = function(e) NULL)
      if (is.null(s) || !nrow(s)) return(NULL)
      data.frame(id = p$id[i], ticker = toupper(s$ticker), d = s$d,
                 amt = s$amount, stringsAsFactors = FALSE)
    })
    buys <- do.call(rbind, parts)
    if (is.null(buys) || !nrow(buys)) return(NULL)
    ok <- grepl("^[A-Za-z.-]+$", buys$ticker) &
          grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", buys$d) & is.finite(buys$amt)
    buys <- buys[ok, , drop = FALSE]; if (!nrow(buys)) return(NULL)
    vals <- sprintf("('%s','%s','%s',%s)",
                    gsub("'", "''", buys$id, fixed = TRUE), buys$ticker, buys$d,
                    format(buys$amt, scientific = FALSE, trim = TRUE))
    sql <- gsub("__BUYS__", paste(vals, collapse = ","), LC_PORTFOLIO_TICKER_SQL, fixed = TRUE)
    con <- tryCatch(get_con(input), error = function(e) NULL); if (is.null(con)) return(NULL)
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    tryCatch(dbGetQuery(con, sql), error = function(e) NULL)
  })

  output$lcPosTable <- DT::renderDT({
    p <- lc_pos()
    if (is.null(p) || nrow(p) == 0)
      return(DT::datatable(
        data.frame(Note = "No positions yet - Generate to auto-load the qualstream buys, or adopt a model BUY / add a manual plan above."),
        rownames = FALSE, selection = "none", options = list(dom = "t", ordering = FALSE)))
    dv  <- tryCatch(derivedLC(), error = function(e) NULL)
    d   <- app_dataLC()
    perf <- tryCatch(lc_row_perf(), error = function(e) NULL)
    id_of <- if (!is.null(d) && !is.null(d$gate$id))
      setNames(suppressWarnings(as.integer(d$gate$id)), toupper(d$gate$ticker)) else integer(0)
    states <- vapply(seq_len(nrow(p)), function(i) {
      if (!identical(p$mode[i], "model")) return("-")
      tk <- toupper(p$ticker[i])
      s <- if (!is.null(dv) && tk %in% names(dv$state_now)) dv$state_now[[tk]] else ""
      if (is.null(s) || is.na(s)) "" else s
    }, character(1))
    pcol <- function(col) if (is.null(perf) || !nrow(perf)) rep(NA_real_, nrow(p)) else
      as.numeric(perf[[col]])[match(p$id, perf$id)]
    invested <- pcol("invested"); value <- pcol("value"); spyv <- pcol("spy_value")
    ret  <- 100 * (value / invested - 1)
    spyr <- 100 * (spyv  / invested - 1)
    pnl  <- value - invested
    vspp <- ret - spyr
    usd  <- function(x) ifelse(is.na(x), "-", paste0("$", formatC(round(x), format = "d", big.mark = ",")))
    susd <- function(x) ifelse(is.na(x), "-", sprintf("%s$%s", ifelse(x < 0, "-", "+"),
                        formatC(abs(round(x)), format = "d", big.mark = ",")))
    spct <- function(x) ifelse(is.na(x), "-", sprintf("%+.1f%%", x))
    spp  <- function(x) ifelse(is.na(x), "-", sprintf("%+.1fpp", x))
    tick <- toupper(p$ticker)
    cid  <- id_of[tick]
    # per-row checkbox (col 0). data-row is the 1-based position index into p and
    # rides with the row through any sort; the callback JS collects the checked
    # ones into input$lcPosChecked and injects a select-all box into the header.
    # Re-renders restore the checked state from lc_checked_ticks (isolate: a
    # check alone must not re-render the whole table).
    chk <- isolate(lc_checked_ticks())
    sel_box <- sprintf('<input type="checkbox" class="pfrow" data-row="%d"%s aria-label="select row">',
                       seq_len(nrow(p)),
                       ifelse(toupper(p$ticker) %in% chk, " checked", ""))
    show <- data.frame(
      ` `      = sel_box,
      id       = ifelse(is.na(cid), "-", as.character(cid)),
      Ticker   = tick,
      State    = states,
      `$/buy`  = round(as.numeric(p$amount_usd), 2),
      Cadence  = p$cadence,
      # Start = the row's FIRST ACTUAL FILL under the active basis (from the
      # series, whose first point per ticker is its first buy). Falls back to
      # the stored plan start while a row has no fills yet.
      Start    = { s <- if (!is.null(perf) && "since" %in% names(perf))
                     as.character(perf$since)[match(p$id, perf$id)] else rep(NA_character_, nrow(p))
                   ifelse(is.na(s), as.character(p$start_date), s) },
      Invested = usd(invested),
      Value    = usd(value),
      `P&L $`  = susd(pnl),
      Return   = spct(ret),
      `vs SPY` = spp(vspp),
      check.names = FALSE, stringsAsFactors = FALSE)
    editcol <- which(names(show) == "$/buy") - 1L   # 0-based; only this col editable
    pf_js <- DT::JS(
      "var el = $(table.table().node());",
      "var th = el.find('thead th').first();",
      "if (th.find('input.pfall').length === 0) {",
      "  th.empty().append('<input type=\"checkbox\" class=\"pfall\" title=\"Select all\" aria-label=\"select all\">');",
      "}",
      "function pfsync(){",
      "  var boxes = el.find('input.pfrow'); var sel = [];",
      "  boxes.each(function(){ if(this.checked) sel.push(parseInt(this.getAttribute('data-row'),10)); });",
      "  Shiny.setInputValue('lcPosChecked', sel, {priority:'event'});",
      "  var all = el.find('input.pfall');",
      "  all.prop('checked', boxes.length>0 && sel.length===boxes.length);",
      "  all.prop('indeterminate', sel.length>0 && sel.length<boxes.length);",
      "}",
      "el.off('change.pf').on('change.pf', 'input.pfrow', pfsync);",
      "el.off('change.pfa').on('change.pfa', 'input.pfall', function(){",
      "  var c=this.checked; el.find('input.pfrow').prop('checked', c); pfsync();",
      "});",
      "pfsync();")
    DT::datatable(show, rownames = FALSE, selection = "none", class = "compact",
      escape = FALSE,   # render the checkbox column HTML; all cell values are controlled
      editable = list(target = "cell",
        disable = list(columns = setdiff(seq_len(ncol(show)) - 1L, editcol))),
      callback = pf_js,
      # sortable, default sorted by cluster id ascending. data-row / cell_edit both
      # key on the data index (stable under sort), so the checkbox->chart map and
      # the $/buy edit stay correct however the user orders the rows.
      options = list(dom = "t", ordering = TRUE, pageLength = 50,
        order = list(list(1, "asc")),
        columnDefs = list(
          list(targets = 0, orderable = FALSE, className = "dt-center pf-sel"),
          list(targets = 1, className = "dt-center", render = DT::JS(
            "function(data,type,row){if(type==='sort'||type==='type'){var n=parseInt(data,10);return isNaN(n)?9999:n;}return data;}"))))) %>%
      DT::formatStyle("State", fontWeight = "600",
        color = DT::styleEqual(c("buy", "hold", "sell", "closed"),
                               c("#10b981", "#eab308", "#dc2626", "#64748b")))
  }, server = FALSE)

  # Inline edit of $/buy (the only editable column): validate, persist, and the
  # perf columns + chart recompute off the new amount.
  observeEvent(input$lcPosTable_cell_edit, {
    info <- input$lcPosTable_cell_edit
    i <- info$row; v <- suppressWarnings(as.numeric(info$value))
    p <- lc_pos(); if (is.null(p) || is.null(i) || i < 1 || i > nrow(p)) return()
    if (is.na(v) || v <= 0) { showNotification("Enter a positive $ amount.", type = "warning"); return() }
    p$amount_usd[i] <- v
    persist_positions(p); lc_pos(p)
  })

  # Buy -> Hold -> Sell transition timeline, read from portfolio.state_history
  # (the per-Generate snapshots). One row per ticker, consecutive same-states
  # collapsed into the change points, so the trail reads as the position's path.
  output$lcTransitionsTable <- DT::renderDT({
    note <- function(txt) DT::datatable(data.frame(Note = txt), rownames = FALSE,
      selection = "none", options = list(dom = "t", ordering = FALSE))
    if (!identical(lc_backend(), "db"))
      return(note("Transitions record to the DB - connect to a portfolio-schema DB and Generate."))
    p <- lc_pos(); if (is.null(p) || !nrow(p)) return(note("No positions yet."))
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    hz <- if (!is.null(dv)) dv$hz else 12L
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) return(note("DB unreachable."))
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    h <- tryCatch(dbGetQuery(con, "
        SELECT ticker, to_char(as_of,'YYYY-MM-DD') AS as_of, state
        FROM portfolio.state_history WHERE hz = $1 ORDER BY ticker, as_of",
        params = list(as.integer(hz))), error = function(e) NULL)
    if (is.null(h) || !nrow(h))
      return(note("No snapshots yet - re-Generate over the coming days to build each trail."))
    parts <- lapply(split(seq_len(nrow(h)), h$ticker), function(ix) {
      g <- h[ix, , drop = FALSE]
      keep <- c(TRUE, g$state[-1] != g$state[-nrow(g)])   # change points only
      ch <- g[keep, , drop = FALSE]
      data.frame(Ticker = g$ticker[1],
        Trail   = paste(sprintf("%s (%s)", ch$state, ch$as_of), collapse = "  →  "),
        Current = g$state[nrow(g)], Since = ch$as_of[nrow(ch)],
        stringsAsFactors = FALSE)
    })
    out <- do.call(rbind, parts)
    ord <- c(sell = 1, hold = 2, buy = 3, closed = 4)
    out <- out[order(ord[out$Current], out$Ticker), , drop = FALSE]
    DT::datatable(out, rownames = FALSE, selection = "none", class = "compact",
      options = list(dom = "t", ordering = FALSE, pageLength = 50)) %>%
      DT::formatStyle("Current", fontWeight = "600",
        color = DT::styleEqual(c("buy", "hold", "sell", "closed"),
                               c("#10b981", "#eab308", "#dc2626", "#64748b")))
  }, server = FALSE)

  # Chart display state, persisted OUTSIDE the widgets so re-renders (Generate,
  # a $/buy edit) don't wipe the user's narrowing:
  #  - lc_chart_ids_sel: NULL = "all" (untouched, or everything selected - keeps
  #    newly appearing ids auto-included); character(0) = explicit deselect-all
  #    (chart shows only the SPY benchmark); otherwise the selected id set.
  #  - lc_checked_ticks: tickers of the checked rows (explicit isolation; wins
  #    over the id filter). Tickers, not indices, so sorts/removals can't shift
  #    the meaning.
  lc_chart_ids_sel <- reactiveVal(NULL)
  observeEvent(input$lcChartIds, {
    v <- if (is.null(input$lcChartIds)) character(0) else as.character(input$lcChartIds)
    allc <- as.character(isolate(lc_holding_cids()))
    if (length(allc) && setequal(v, allc)) v <- NULL   # full set -> sticky "all"
    if (!identical(v, lc_chart_ids_sel())) lc_chart_ids_sel(v)
  }, ignoreNULL = FALSE, ignoreInit = TRUE)
  lc_checked_ticks <- reactiveVal(character(0))
  observeEvent(input$lcPosChecked, {
    p <- isolate(lc_pos())
    v <- suppressWarnings(as.integer(input$lcPosChecked))
    v <- v[!is.na(v) & v >= 1 & v <= if (is.null(p)) 0L else nrow(p)]
    tks <- sort(unique(toupper(as.character(p$ticker[v]))))
    if (!identical(tks, lc_checked_ticks())) lc_checked_ticks(tks)
  }, ignoreNULL = FALSE, ignoreInit = TRUE)

  # By-id filter + tracking-basis radio, sitting right above the chart (reuses
  # the board's cluster-id filter pattern). Selections survive re-renders via
  # the reactiveVals above (isolate() so re-render doesn't loop).
  output$lcChartFilter <- renderUI({
    p <- lc_pos(); d <- app_dataLC()
    if (is.null(p) || !nrow(p) || is.null(d)) return(NULL)
    id_of <- if (!is.null(d$gate$id))
      setNames(suppressWarnings(as.integer(d$gate$id)), toupper(d$gate$ticker)) else integer(0)
    cids <- sort(unique(stats::na.omit(unname(id_of[toupper(p$ticker)]))))
    if (!length(cids)) return(NULL)
    sel_now <- isolate(lc_chart_ids_sel())
    sel_use <- if (is.null(sel_now)) as.character(cids) else intersect(sel_now, as.character(cids))
    basis_now <- isolate(if (identical(input$lcTrackBasis, "stored")) "stored" else "epoch")
    div(style = "margin:0.5rem 0 0.1rem;",
      tags$label("Track model holdings",
                 style = "color:#94a3b8; font-size:0.72rem; font-weight:600; display:block; margin-bottom:0.15rem;"),
      radioButtons("lcTrackBasis", NULL,
        choices = c("Strategy since model epoch" = "epoch",
                    "My money since I added" = "stored"),
        selected = basis_now, inline = TRUE),
      tags$label("Show cluster ids on chart (all on; uncheck to narrow; checked rows above override)",
                 style = "color:#94a3b8; font-size:0.72rem; font-weight:600; display:block; margin-bottom:0.3rem;"),
      checkboxGroupInput("lcChartIds", NULL, choices = cids, selected = sel_use, inline = TRUE),
      actionButton("lcChartIdsAll", "Select all",
                   style = "padding:2px 10px; font-size:0.72rem; margin-right:0.35rem;"),
      actionButton("lcChartIdsNone", "Deselect all",
                   style = "padding:2px 10px; font-size:0.72rem;"))
  })
  lc_holding_cids <- reactive({
    p <- lc_pos(); d <- app_dataLC()
    if (is.null(p) || !nrow(p) || is.null(d) || is.null(d$gate$id)) return(integer(0))
    id_of <- setNames(suppressWarnings(as.integer(d$gate$id)), toupper(d$gate$ticker))
    sort(unique(stats::na.omit(unname(id_of[toupper(p$ticker)]))))
  })
  observeEvent(input$lcChartIdsAll, {
    updateCheckboxGroupInput(session, "lcChartIds", selected = as.character(lc_holding_cids()))
  })
  observeEvent(input$lcChartIdsNone, {
    updateCheckboxGroupInput(session, "lcChartIds", selected = character(0))
  })

  # Holdings chart. SPY (same cash) benchmark is ALWAYS drawn. Which holdings
  # show = the by-id filter above (all on by default) intersected with any
  # checked rows (check rows to isolate). Each line starts at 0% on its first buy.
  output$lcPortfolioChart <- renderPlotly({
    p <- lc_pos()
    if (is.null(p) || nrow(p) == 0)
      return(empty_plot("Adopt a model BUY or add a manual plan to see each holding vs SPY."))
    if (is.null(input$db_pass) || input$db_pass == "")
      return(empty_plot("Connect to price your holdings."))
    ser <- tryCatch(lc_ticker_series(), error = function(e) NULL)
    if (is.null(ser) || nrow(ser) == 0)
      return(empty_plot("No priced history yet - a position needs at least one trading day since its first buy. Tip: add a plan with an earlier start date to see a full curve now."))
    ser$d <- as.Date(ser$d); ser$ret_pct <- as.numeric(ser$ret_pct)
    ser <- ser[order(ser$ticker, ser$d), , drop = FALSE]
    all_ticks <- sort(unique(ser$ticker[ser$ticker != "__SPY__"]))
    # ticker -> cluster id, for the by-id chart filter above the chart.
    d <- app_dataLC()
    id_of <- if (!is.null(d) && !is.null(d$gate$id))
      setNames(suppressWarnings(as.integer(d$gate$id)), toupper(d$gate$ticker)) else integer(0)
    cid_of <- id_of[all_ticks]
    # Explicit row checks WIN (show exactly those, id filter ignored). Otherwise
    # the id filter applies: NULL = untouched -> all; character(0) = explicit
    # deselect-all -> only the SPY benchmark; else the checked-id set (narrowing
    # hides unclustered holdings - reach those via row checks).
    checked_tk <- intersect(lc_checked_ticks(), all_ticks)
    if (length(checked_tk)) {
      sel_ticks <- checked_tk
    } else {
      selids <- lc_chart_ids_sel()
      sel_ticks <- if (is.null(selids)) all_ticks else
        all_ticks[!is.na(cid_of) & as.character(cid_of) %in% selids]
    }
    # stable color per ticker across check/uncheck (index into the full set)
    pal <- c("#38bdf8","#34d399","#f59e0b","#f472b6","#a78bfa","#fb7185","#22d3ee",
             "#4ade80","#fbbf24","#e879f9","#60a5fa","#2dd4bf","#facc15","#f87171",
             "#c084fc","#818cf8","#fca5a5","#5eead4","#fde047","#93c5fd")
    fig <- plot_ly()
    for (tkr in sel_ticks) {
      sub <- ser[ser$ticker == tkr, , drop = FALSE]; if (!nrow(sub)) next
      col <- pal[(match(tkr, all_ticks) - 1) %% length(pal) + 1]
      fig <- add_trace(fig, x = sub$d, y = sub$ret_pct, type = "scatter",
        mode = if (nrow(sub) <= 1) "markers" else "lines+markers",
        name = tkr, legendgroup = tkr,
        line = list(color = col, width = 1.8), marker = list(color = col, size = 6),
        hovertemplate = paste0(tkr, "<br>%{x|%b %d %Y}: %{y:+.1f}%<extra></extra>"))
    }
    sp <- ser[ser$ticker == "__SPY__", , drop = FALSE]
    if (nrow(sp)) fig <- add_trace(fig, x = sp$d, y = sp$ret_pct, type = "scatter",
      mode = if (nrow(sp) <= 1) "markers" else "lines+markers",
      name = "SPY (same cash)", legendgroup = "SPY",
      line = list(color = "#e2e8f0", width = 3, dash = "dash"),
      marker = list(color = "#e2e8f0", size = 7),
      hovertemplate = "SPY (same cash)<br>%{x|%b %d %Y}: %{y:+.1f}%<extra></extra>")
    # date x-axis; pad a single-day range so plotly does not fall back to
    # sub-second ticks (the "23:59:59.999" axis on a one-point series).
    ad <- sort(unique(ser$d))
    xax <- list(title = "", color = "#cbd5e1", type = "date", tickformat = "%b %d",
                gridcolor = "rgba(148,163,184,0.10)", zeroline = FALSE)
    if (length(ad) <= 1) xax$range <- c(as.character(min(ad) - 3), as.character(max(ad) + 2))
    dark_layout(fig,
      title = list(text = paste0("Each holding's return vs same-cash SPY - ",
                     if (identical(lc_track_basis(), "stored"))
                       "your money since you added" else "strategy since model epoch"),
                   font = list(color = "#f8fafc", size = 13), x = 0.5),
      xaxis = xax,
      yaxis = list(title = "Return (%)", color = "#cbd5e1", ticksuffix = "%",
                   gridcolor = "rgba(148,163,184,0.10)", zeroline = TRUE,
                   zerolinecolor = "rgba(148,163,184,0.28)"),
      legend = list(font = list(color = "#e2e8f0", size = 10), orientation = "h",
                    x = 0, y = -0.2),
      hovermode = "closest", margin = list(l = 56, r = 20, t = 44, b = 72))
  })

  output$lcPortfolioNote <- renderUI({
    ser <- lc_port_series()
    if (is.null(ser) || nrow(ser) == 0) return(NULL)
    last <- ser[nrow(ser), ]
    extra <- ""
    sk <- attr(ser, "model_skipped")
    if (!is.null(sk) && length(sk))
      extra <- paste0(extra, sprintf(" Model rows (%s) price after Generate.",
                                     paste(sk, collapse = ", ")))
    lad <- tryCatch(lc_sell_events(), error = function(e) NULL)
    if (!is.null(lad) && !is.null(lad$meta) &&
        any(!lad$meta$has_hist & lad$meta$reason != "delisted", na.rm = TRUE))
      extra <- paste0(extra, " Some ladders use a time-based fallback (thin history).")
    div(style = "color:#94a3b8; font-size:0.78rem; margin:0.3rem 0 0.2rem;",
      sprintf(paste0("Invested $%s, now $%s (%+.1f%%). Same-cash SPY $%s (%+.1f%%). ",
                     "Edge %+.1fpp. Simulated fills at daily closes. Rows and chart ",
                     "are the accumulation view; this line applies the sell ladder.%s"),
        format(round(last$invested), big.mark = ","),
        format(round(last$value), big.mark = ","), last$ret_pct,
        format(round(last$spy_value), big.mark = ","), last$spy_ret_pct,
        last$ret_pct - last$spy_ret_pct, extra))
  })

  output$lcPortSummary <- renderUI({
    p <- lc_pos(); n <- if (is.null(p)) 0L else nrow(p)
    ser <- tryCatch(lc_port_series(), error = function(e) NULL)
    txt <- if (is.null(ser) || !nrow(ser)) sprintf(" - %d position%s", n, if (n == 1) "" else "s")
      else { last <- ser[nrow(ser), ]
        sprintf(" - %d pos, $%s (%+.1f%%), %+.1fpp vs SPY", n,
                format(round(last$value), big.mark = ","), last$ret_pct,
                last$ret_pct - last$spy_ret_pct) }
    span(txt, style = "color:#94a3b8; font-weight:400; font-size:0.78rem;")
  })

  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgLC("Error: Password is not set."); return() }
    lc_board_ids_sel(NULL)   # new environment -> new universe -> reset the id filter
    status_msgLC("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      b <- dbGetQuery(con, "
        SELECT COUNT(*) AS n, COUNT(DISTINCT prediction_date) AS d,
               MIN(prediction_date) AS lo, MAX(prediction_date) AS hi
        FROM monitoring.prediction_ledger")
      status_msgLC(sprintf("Connected - %s recorded calls across %d snapshots (%s to %s).",
                           as.numeric(b$n[1]), as.integer(b$d[1]), b$lo[1], b$hi[1]))
    }, error = function(e) { status_msgLC(paste("Error:", e$message)) })
  })

  # Auto-refresh: while the toggle is on, re-run the Generate fetch on a timer so
  # the board tracks the DB without a click. Ticks are suppressed (but the clock
  # keeps running) when the Lifecycle tab is not active or no password is set, so
  # hidden tabs and unconnected sessions never query. Returning to the tab
  # triggers an immediate refresh (mainNav is a reactive dep); the password is
  # isolate()d so typing it does not fire per-keystroke fetches.
  observe({
    if (!isTRUE(input$lcAutoRefresh)) return()
    invalidateLater(20000)
    if (!identical(input$mainNav, "Lifecycle")) return()
    if (isolate(is.null(input$db_pass) || input$db_pass == "")) return()
    isolate(lcAutoTick(lcAutoTick() + 1))
  })
  observeEvent(list(input$execute_LC, lcAutoTick()), {
    if (input$db_pass == "") { status_msgLC("Error: Password is not set."); return() }
    # Every Generate re-queries fresh, so the board always reflects the current
    # DB - no stale cache to reconnect past. (Horizon changes still re-derive in
    # memory from app_dataLC without a re-query.)
    status_msgLC("Loading the signal record...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      # Portfolio store: if this DB has the portfolio schema, adopt it as the
      # authoritative store (one shared copy across dashboards) and migrate any
      # local/parquet rows into it once, so nothing typed before connecting is
      # lost. Otherwise the app keeps using the parquet fallback.
      if (pf_db_ready(con)) {
        lc_backend("db")
        dbpos <- db_load_positions(con); dbdis <- db_load_dismissed(con)
        loc   <- lc_pos()
        extra <- if (!is.null(loc) && nrow(loc))
                   loc[!(loc$id %in% dbpos$id), , drop = FALSE] else loc[0, ]
        merged <- if (!is.null(extra) && nrow(extra)) rbind(dbpos, extra) else dbpos
        if (!is.null(extra) && nrow(extra))
          tryCatch(db_save_positions(con, merged), error = function(e) NULL)
        lc_pos(merged)
        alldis <- union(dbdis, lc_dismissed())
        if (length(setdiff(alldis, dbdis)))
          tryCatch(db_save_dismissed(con, alldis), error = function(e) NULL)
        lc_dismissed(alldis)
      }
      # Floored at LEDGER_EPOCH: pre-epoch BUYs came from the retired gate, and
      # letting them into the walk would start maturity clocks (and therefore
      # "matured"/"sell" states) from entries the current model never made.
      led <- dbGetQuery(con, gsub("__EPOCH__", LEDGER_EPOCH, "
        SELECT prediction_date::text AS d, ticker, global_action
        FROM monitoring.prediction_ledger
        WHERE prediction_date >= '__EPOCH__'
        ORDER BY ticker, prediction_date", fixed = TRUE))
      if (nrow(led) == 0) { status_msgLC("No recorded snapshots yet."); return() }
      gate <- dbGetQuery(con, "
        SELECT ticker, global_action AS gate_today, id
        FROM serving.return_cluster_ticker_global_action_current")
      # raw.ticker_metadata holds ONLY delisted names -> presence = delisted
      meta <- tryCatch(dbGetQuery(con, "
        SELECT ticker, name AS company_name, delisting_category,
               delisted_utc::date::text AS delisted_date
        FROM raw.ticker_metadata"), error = function(e) NULL)
      # shortlist evidence PER HORIZON: the ticker's walk-forward rank bin at
      # the latest cutoff and that bin's realized win rate - the SAME rule as
      # the Shortlist tab (>= 55% win on >= 100 graded observations). One row
      # per (ticker, fut_lag); DISTINCT ON keeps the best-evidence bin when a
      # ticker sits in two clusters.
      sl <- tryCatch(dbGetQuery(con, "
        WITH mx AS (
            SELECT fut_lag, MAX(train_cutoff_date) AS cut
            FROM validation.walk_forward_ticker_rank
            WHERE fut_lag IN (1,2,4,7,12,20,33)
            GROUP BY fut_lag
        ), wfbin AS (
            SELECT r.fut_lag, m.id AS eid, r.ticker,
                   NTILE(20) OVER (PARTITION BY r.fut_lag, m.id
                     ORDER BY r.ticker_score DESC, r.n_weighted DESC, r.ticker
                   )::int AS wf_bin
            FROM validation.walk_forward_ticker_rank r
            JOIN mx ON mx.fut_lag = r.fut_lag AND mx.cut = r.train_cutoff_date
            JOIN validation.walk_forward_cluster_id_map m
              ON m.train_cutoff_date = r.train_cutoff_date
             AND m.cluster_id       = r.cluster_id
            WHERE r.ticker_score IS NOT NULL AND r.ticker_score <> 0
        )
        SELECT DISTINCT ON (w.ticker, w.fut_lag) w.ticker, w.fut_lag,
               ROUND(ps.hit_rate::numeric * 100, 1) AS bin_win_pct,
               ps.n_obs::int AS bin_n
        FROM wfbin w
        LEFT JOIN validation.walk_forward_pctile_summary ps
          ON ps.id = w.eid AND ps.fut_lag = w.fut_lag AND ps.pctile_bin = w.wf_bin
        ORDER BY w.ticker, w.fut_lag, ps.hit_rate DESC NULLS LAST"),
        error = function(e) NULL)
      # the model's own historical entry record: at each quarterly walk-forward
      # cutoff inside the longest hold window, the top vingtile per LONG id
      # (wf_bin = 1, eid <= 12) whose bin evidence passes the Shortlist rule.
      # All 7 horizons load at once; derivedLC filters fut_lag == hz so the
      # hold-length selector re-derives without re-querying.
      coh <- tryCatch(dbGetQuery(con, "
        WITH wfbin AS (
            SELECT r.train_cutoff_date, r.fut_lag, m.id AS eid, r.ticker,
                   NTILE(20) OVER (PARTITION BY r.train_cutoff_date, r.fut_lag, m.id
                     ORDER BY r.ticker_score DESC, r.n_weighted DESC, r.ticker
                   )::int AS wf_bin
            FROM validation.walk_forward_ticker_rank r
            JOIN validation.walk_forward_cluster_id_map m
              ON m.train_cutoff_date = r.train_cutoff_date
             AND m.cluster_id       = r.cluster_id
            WHERE r.fut_lag IN (1,2,4,7,12,20,33)
              AND r.ticker_score IS NOT NULL AND r.ticker_score <> 0
              AND r.train_cutoff_date >= (CURRENT_DATE - INTERVAL '33 months')::date
        )
        SELECT w.train_cutoff_date::text AS d, w.fut_lag, w.ticker, w.eid AS id,
               ROUND(ps.hit_rate::numeric * 100, 1) AS bin_win_pct,
               ps.n_obs::int AS bin_n
        FROM wfbin w
        JOIN validation.walk_forward_pctile_summary ps
          ON ps.id = w.eid AND ps.fut_lag = w.fut_lag AND ps.pctile_bin = w.wf_bin
        WHERE w.wf_bin = 1 AND w.eid <= 12
          AND ps.hit_rate >= 0.55 AND ps.n_obs >= 100
          -- the WF ranking covers the full universe incl. delisted names (the
          -- survivorship fix); a buyable cohort must drop names already dead
          -- at the cutoff
          AND NOT EXISTS (SELECT 1 FROM raw.ticker_metadata tm
                          WHERE tm.ticker = w.ticker
                            AND tm.delisted_utc::date <= w.train_cutoff_date)"),
        error = function(e) NULL)
      # qualstream qualitative grades: latest non-vetoed scorecard per ticker
      # from the semi-annual rubric grader (qualstream repo writes
      # qual.ticker_scorecards; overall is 0-100). Absent table or no rows ->
      # dormant; once grades exist the board marks the top-20 with an orange +.
      # qualstream grades: pinned to the LIVE series (buy_decision_v1 -- other
      # rubric_versions score on different scales and must not mix into one
      # ranking) and bounded by freshness (150d ~ the 4-month cadence + slack)
      # so an orange + can expire instead of living forever.
      qs <- tryCatch(dbGetQuery(con, "
        SELECT DISTINCT ON (ticker) ticker,
               overall AS grade, as_of::text AS as_of,
               graded_at::date::text AS graded_at
        FROM qual.ticker_scorecards
        WHERE NOT veto
          AND rubric_version = 'buy_decision_v1'
          AND as_of >= CURRENT_DATE - 150
        ORDER BY ticker, as_of DESC, graded_at DESC"),
        error = function(e) NULL)
      # descriptive qualstream comparison series (LC_QS_COMPARE_SQL): current
      # BUYs qualstream graded vs the >= 68 subset vs SPY, equal-weight from the
      # current 4-month window start = the last Jan/May/Sep checkpoint.
      ck_all <- as.Date(sprintf("%d-%02d-01",
                  rep(as.integer(format(Sys.Date(), "%Y")) + c(-1, 0, 1), each = 3),
                  c(1, 5, 9)))
      qs_anchor <- max(ck_all[ck_all <= Sys.Date()])
      qscmp <- tryCatch(coerce_numeric_cols(
        dbGetQuery(con, gsub("__ANCHOR__", format(qs_anchor, "%Y-%m-%d"),
                             LC_QS_COMPARE_SQL, fixed = TRUE)),
        c("graded_pct", "passed_pct", "spy_pct")), error = function(e) NULL)
      dat <- list(led = led, gate = gate, meta = meta, sl = sl, coh = coh,
                  qs = qs, qscmp = qscmp, qscmp_anchor = qs_anchor)
      app_dataLC(dat)
      status_msgLC(sprintf(
        "Loaded - %d proven cohort picks across %d quarterly cutoffs + %d live series across %d snapshots.",
        if (is.null(coh)) 0L else nrow(coh),
        if (is.null(coh)) 0L else length(unique(coh$d)),
        length(unique(led$ticker[led$global_action == "BUY"])),
        length(unique(led$d))))
    }, error = function(e) { status_msgLC(paste("Error:", e$message)) })
  }, ignoreInit = TRUE)

  # State machine + summary, re-derived live on horizon changes without
  # re-querying. The snapshot stream at horizon hz is the model's own record:
  # quarterly walk-forward cohort picks (fut_lag == hz) followed by the daily
  # live ledger. The walk is unchanged; only the inputs got deeper.
  derivedLC <- reactive({
    d <- app_dataLC(); req(d)
    hz <- suppressWarnings(as.integer(input$holdLC)); if (is.na(hz)) hz <- 12L
    led <- d$led
    ch  <- if (!is.null(d$coh) && nrow(d$coh) > 0)
             d$coh[d$coh$fut_lag == hz, , drop = FALSE]
           else NULL
    coh_dates <- if (!is.null(ch) && nrow(ch) > 0) sort(unique(ch$d)) else character(0)
    dates <- sort(unique(c(coh_dates, led$d)))
    tickers <- sort(unique(c(led$ticker[led$global_action == "BUY"],
                             if (!is.null(ch)) ch$ticker else character(0))))
    if (length(tickers) == 0) return(NULL)
    # ledger actions first; cohort picks add a BUY at their cutoff date unless
    # the ledger already recorded that (ticker, date) - the recorded call wins.
    act <- setNames(led$global_action, paste(led$ticker, led$d))
    if (!is.null(ch) && nrow(ch) > 0) {
      ck <- paste(ch$ticker, ch$d)
      ck <- ck[!(ck %in% names(act))]
      if (length(ck)) act <- c(act, setNames(rep("BUY", length(ck)), ck))
    }
    dl   <- if (!is.null(d$meta) && nrow(d$meta) > 0)
              setNames(suppressWarnings(as.Date(d$meta$delisted_date)), d$meta$ticker)
            else setNames(as.Date(character(0)), character(0))
    # proven-at-hz evidence (today's rank bin at the latest cutoff, this lag):
    # the Shortlist rule that gates the board's buy section
    slh <- if (!is.null(d$sl) && nrow(d$sl) > 0 && "fut_lag" %in% names(d$sl))
             d$sl[d$sl$fut_lag == hz, , drop = FALSE]
           else NULL
    prov_of <- if (!is.null(slh) && nrow(slh) > 0)
                 setNames(!is.na(slh$bin_win_pct) & slh$bin_win_pct >= 55 &
                          !is.na(slh$bin_n)       & slh$bin_n >= 100, slh$ticker)
               else setNames(logical(0), character(0))
    wpct <- if (!is.null(slh) && nrow(slh) > 0)
              setNames(as.numeric(slh$bin_win_pct), slh$ticker) else numeric(0)
    provf <- function(t) t %in% names(prov_of) && isTRUE(prov_of[[t]])
    M <- matrix("", nrow = length(tickers), ncol = length(dates),
                dimnames = list(tickers, dates))
    entry_of <- setNames(rep(NA_character_, length(tickers)), tickers)
    exit_of  <- setNames(rep(NA_character_, length(tickers)), tickers)
    reason_of <- setNames(rep("", length(tickers)), tickers)
    src_of  <- setNames(rep(NA_character_, length(tickers)), tickers)
    mat_of  <- setNames(rep(as.Date(NA), length(tickers)), tickers)
    for (t in tickers) {
      open <- FALSE; sold <- FALSE; entry_d <- NA_character_; mat_d <- as.Date(NA)
      dl_d <- if (t %in% names(dl)) dl[[t]] else as.Date(NA)
      for (j in seq_along(dates)) {
        D <- dates[j]; a <- act[paste(t, D)]
        if (!open && !sold) {                       # flat: wait for an entry
          if (!is.na(a) && a == "BUY") {
            open <- TRUE; entry_d <- D
            mat_d <- seq(as.Date(D), by = paste(hz, "months"), length.out = 2)[2]
            M[t, j] <- "buy"; reason_of[[t]] <- "new signal"
            src_of[[t]] <- if (D %in% coh_dates) "cohort" else "ledger"
          }
        } else if (open && !sold) {                 # held: check exit triggers
          if (!is.na(a) && a == "SELL") {
            sold <- TRUE; open <- FALSE; M[t, j] <- "sell"
            reason_of[[t]] <- "gate flipped"; exit_of[[t]] <- D
          } else if ((!is.na(dl_d) && dl_d <= as.Date(D)) || as.Date(D) >= mat_d) {
            # both can have elapsed between sparse snapshots: earlier event wins
            sold <- TRUE; open <- FALSE; M[t, j] <- "sell"
            reason_of[[t]] <- if (!is.na(dl_d) && dl_d <= as.Date(D) && dl_d <= mat_d)
              "delisted" else "matured"
            exit_of[[t]] <- D
          } else if (!is.na(a) && a == "BUY") {
            M[t, j] <- "buy"; reason_of[[t]] <- "signal continuing"   # gate still says BUY
          } else {
            M[t, j] <- "hold"; reason_of[[t]] <- "maturing"          # washed to SKIP, still held
          }
        } else {                                    # sold: sticky until re-entry
          if (!is.na(a) && a == "BUY") {
            open <- TRUE; sold <- FALSE; entry_d <- D
            mat_d <- seq(as.Date(D), by = paste(hz, "months"), length.out = 2)[2]
            M[t, j] <- "buy"; reason_of[[t]] <- "re-entry"; exit_of[[t]] <- NA_character_
            src_of[[t]] <- if (D %in% coh_dates) "cohort" else "ledger"
          } else M[t, j] <- "sell"
        }
      }
      entry_of[[t]] <- entry_d
      mat_of[[t]]   <- mat_d
    }
    # current-decision bucket (the board's truth), 4 states as of TODAY:
    #   sell   - action item: matured/delisted/gate-flipped within ~a month
    #   buy    - proven rank slot backed by the MAJORITY of the last month's
    #            recorded runs (rolling monthly evaluation: one off-day cannot
    #            demote a durable rec, one on-day cannot promote a flicker)
    #   hold   - open position below the monthly majority
    #   closed - the exit is > 30 days old; history, not an action item
    today <- Sys.Date()
    led_dd <- as.Date(led$d)
    d30 <- sort(unique(led_dd[led_dd >= today - 30]))
    b30 <- led[led_dd >= today - 30 & led$global_action == "BUY", , drop = FALSE]
    share_of <- if (nrow(b30) > 0) {
      fb <- tapply(as.Date(b30$d), b30$ticker, min)   # first BUY inside window
      nb <- tapply(b30$d, b30$ticker, length)
      # denominator starts at the name's first window appearance so a fresh
      # entrant (3/3 runs) qualifies while a long flicker (8/21) does not
      setNames(mapply(function(n, f) n / max(1L, sum(d30 >= f)), nb, fb),
               names(nb))
    } else setNames(numeric(0), character(0))
    majf <- function(t) t %in% names(share_of) && share_of[[t]] >= 0.5
    gate_now <- setNames(as.character(d$gate$gate_today), d$gate$ticker)
    fin <- M[, ncol(M)]
    state_now <- setNames(rep("hold", length(tickers)), tickers)
    why_now   <- setNames(rep("washed out to SKIP", length(tickers)), tickers)
    for (t in tickers) {
      e <- as.Date(entry_of[[t]])
      mat_d <- mat_of[[t]]
      dl_d <- if (t %in% names(dl)) dl[[t]] else as.Date(NA)
      g <- if (t %in% names(gate_now)) gate_now[[t]] else NA_character_
      if (fin[[t]] == "sell") {                     # recorded exit in the walk
        r <- reason_of[[t]]
        # date the exit really happened: true maturity/delist date, not the
        # (possibly much later) snapshot that first observed it
        ref_d <- if (r == "matured") mat_d
                 else if (r == "delisted" && !is.na(dl_d)) dl_d
                 else suppressWarnings(as.Date(exit_of[[t]]))
        stale <- !is.na(ref_d) && as.numeric(today - ref_d) > 30
        state_now[[t]] <- if (stale) "closed" else "sell"
        why_now[[t]]   <- if (is.na(ref_d)) r else sprintf("%s %s", r, format(ref_d))
        next
      }
      if (!is.na(dl_d) && dl_d <= today) {          # open but delisted by now
        state_now[[t]] <- if (as.numeric(today - dl_d) > 30) "closed" else "sell"
        why_now[[t]]   <- sprintf("delisted %s", format(dl_d))
      } else if (!is.na(mat_d) && today >= mat_d) { # open but horizon elapsed
        state_now[[t]] <- if (as.numeric(today - mat_d) > 30) "closed" else "sell"
        why_now[[t]]   <- sprintf("matured %s", format(mat_d))
      } else if (!is.na(g) && g == "SELL") {
        state_now[[t]] <- "sell"; why_now[[t]] <- "gate flipped (today)"
      } else if (majf(t) && provf(t)) {
        state_now[[t]] <- "buy"
        why_now[[t]] <- if (!is.na(e) && as.numeric(today - e) <= 7) "new entry"
                        else sprintf("held since %s", entry_of[[t]])
      } else {                                      # hold: open, below majority
        d2m <- if (is.na(mat_d)) NA_real_ else as.numeric(mat_d - today)
        why_now[[t]] <- if (!is.na(d2m) && d2m <= 30)
            sprintf("matures %s", format(mat_d))
          else if (majf(t)) "signal live (unproven slot)"
          else if (!is.na(g) && g == "BUY") "flickering (below monthly majority)"
          else "washed out to SKIP"
      }
    }
    # board membership: cohort entries earned their slot at entry; ledger names
    # must be proven at this horizon today. Everything else is table-only.
    board_ok <- setNames(
      vapply(tickers, function(t)
        identical(src_of[[t]], "cohort") || provf(t), logical(1)),
      tickers)
    # signal persistence over the review window (trailing 4 months of daily
    # runs, floored at the regime epoch): on how many recorded runs was this name
    # a BUY? The period's real buy recs are the durable ones - this ranks the buy
    # section and rides on every chip, so a day-one flicker can't outrank a name
    # endorsed all period. The floor matters HERE most of all: 686 names held
    # pre-epoch BUY runs, and unfloored they bank persistence the current model
    # never awarded them. It cut both ways - CARR/HWM/NXT read 25% (10/40) while
    # the live gate has not bought them once (0/24), and genuinely durable names
    # were held down (ICHR 88% -> 100%, PENG 83% -> 96%).
    win_lo <- max(today - 122, as.Date(LEDGER_EPOCH))
    in_win <- as.Date(led$d) >= win_lo
    runs_tot <- length(unique(led$d[in_win]))
    pb <- led[in_win & led$global_action == "BUY", ]
    runs_of <- table(factor(pb$ticker, levels = tickers))
    runs_of <- setNames(as.integer(runs_of), names(runs_of))
    # qualstream grade per ticker + the orange-+ set (board-universe BUYs graded
    # >= QS_MIN, top QS_CAP by grade), resolved HERE so the board mark and the
    # portfolio default-seed share one definition. Deliberately independent of the
    # board's transient cluster-id view filter, so narrowing the view never
    # shrinks the seeded portfolio.
    qs_grade <- if (!is.null(d$qs) && nrow(d$qs) > 0 &&
                    all(c("ticker", "grade") %in% names(d$qs)))
                  setNames(suppressWarnings(as.numeric(d$qs$grade)), d$qs$ticker)
                else setNames(numeric(0), character(0))
    buy_univ <- tickers[board_ok[tickers] & state_now[tickers] == "buy"]
    bg_all   <- qs_grade[buy_univ]
    qs_ok    <- buy_univ[!is.na(bg_all) & bg_all >= QS_MIN]
    qs_buys  <- utils::head(qs_ok[order(-qs_grade[qs_ok], qs_ok)], QS_CAP)
    list(M = M, dates = dates, tickers = tickers,
         entry_of = entry_of, exit_of = exit_of, reason_of = reason_of, hz = hz,
         state_now = state_now, why_now = why_now,
         mat_of = mat_of, src_of = src_of, prov_of = prov_of, wpct = wpct,
         board_ok = board_ok, coh_dates = coh_dates,
         coh_n = if (is.null(ch)) 0L else nrow(ch),
         runs_of = runs_of, runs_tot = runs_tot,
         qs_grade = qs_grade, qs_buys = qs_buys)
  })

  # The decision board: three name sections ordered by action (exit list first,
  # then today's endorsed buys, then the maturing holds). Every ticker is
  # readable directly; the table below adds detail, filters, and CSV.
  # cluster-id filter (mirrors the Predictions tab): checkboxes for the ids
  # present in the loaded universe, all on by default; uncheck to narrow. The
  # board and the table both read input$idsLC.
  output$idFilterLC <- renderUI({
    d <- app_dataLC(); if (is.null(d) || is.null(d$gate$id)) return(NULL)
    ids <- sort(unique(stats::na.omit(c(
      as.integer(d$gate$id),
      if (!is.null(d$coh)) as.integer(d$coh$id)))))
    if (!length(ids)) return(NULL)
    # survive data refreshes: rebuild with the user's stored selection (isolate,
    # same pattern as lcChartFilter) instead of resetting to all-on
    sel_now <- isolate(lc_board_ids_sel())
    sel_use <- if (is.null(sel_now)) as.character(ids)
               else intersect(sel_now, as.character(ids))
    div(style = "margin-bottom:0.6rem;",
      tags$label("Cluster id filter (all on; uncheck to narrow)",
                 style = paste0("color:#94a3b8; font-size:0.72rem; font-weight:600;",
                                " display:block; margin-bottom:0.3rem;")),
      checkboxGroupInput("idsLC", NULL, choices = ids, selected = sel_use, inline = TRUE),
      # buttons, not links: same control pair as the Predictions/Forecast tabs
      actionButton("idsAllLC", "Select all",
                   style = "padding:2px 10px; font-size:0.72rem; margin-right:0.35rem;"),
      actionButton("idsNoneLC", "Deselect all",
                   style = "padding:2px 10px; font-size:0.72rem;"))
  })
  observeEvent(input$idsAllLC, {
    d <- app_dataLC(); req(d)
    # as.character: checkbox values are strings client-side; integers can miss
    updateCheckboxGroupInput(session, "idsLC",
      selected = as.character(sort(unique(stats::na.omit(as.integer(d$gate$id))))))
  })
  observeEvent(input$idsNoneLC, {
    updateCheckboxGroupInput(session, "idsLC", selected = character(0))
  })

  output$boardLC <- renderUI({
    dv <- derivedLC(); if (is.null(dv)) return(div(
      style = "color:#64748b; padding:1rem; font-size:0.85rem;",
      "Connect and Generate to load the signal record."))
    d <- app_dataLC()
    del_ticks <- if (!is.null(d$meta)) d$meta$ticker else character(0)
    st <- dv$state_now; why <- dv$why_now; hz <- dv$hz
    col_of <- c(buy = "#10b981", hold = "#eab308", sell = "#dc2626")
    # qualstream grades per ticker (latest non-vetoed); the top-20 AMONG THE
    # BUY SECTION get an orange +. Resolved after the buy set is known below;
    # empty until the qualstream table exists and holds grades.
    qs_grade <- if (!is.null(d$qs) && nrow(d$qs) > 0 &&
                    all(c("ticker", "grade") %in% names(d$qs)))
      setNames(suppressWarnings(as.numeric(d$qs$grade)), d$qs$ticker)
    else setNames(numeric(0), character(0))
    qs_top <- character(0)
    chipf <- function(t, colr, note = "", strike = FALSE, plus = FALSE) span(
      style = sprintf(paste0("display:inline-block; background:%s14; color:%s;",
                             " border:1px solid %s44; border-radius:5px; padding:2px 8px;",
                             " margin:2px; font-size:0.78rem; font-weight:600;%s"),
                      colr, colr, colr,
                      if (strike) " text-decoration:line-through; opacity:0.7;" else ""),
      if (nzchar(note)) sprintf("%s · %s", t, note) else t,
      if (plus) span("+", style = "color:#fb923c; font-weight:800; margin-left:3px;"))
    section <- function(title, colr, ticks, notes, max_h = NA) {
      div(style = "margin-bottom:1rem;",
        div(style = sprintf("color:%s; font-weight:700; margin-bottom:0.35rem;", colr),
            sprintf("%s (%d)", title, length(ticks))),
        div(style = if (!is.na(max_h))
              sprintf("display:flex; flex-wrap:wrap; max-height:%dpx; overflow-y:auto;", max_h)
            else "display:flex; flex-wrap:wrap;",
          mapply(function(t, n) chipf(t, colr, n, t %in% del_ticks,
                                      plus = t %in% qs_pass),
                 ticks, notes, SIMPLIFY = FALSE, USE.NAMES = FALSE)))
    }
    note_line <- function(txt) div(
      style = "color:#64748b; font-size:0.72rem; margin:0.15rem 0 0.6rem;", txt)
    # board universe: cohort entries + proven-at-hz ledger names; the unproven
    # rest of the gate lives in the table below, flagged
    bt <- names(st)[dv$board_ok[names(st)]]
    # cluster-id filter: keep only selected ids; names with no current cluster
    # (delisted, ~1%) always show. NULL before the checkboxes first render
    # keeps everything; NULL after that is a real deselect-all -> empty board.
    id_of <- if (!is.null(d$gate$id))
               setNames(suppressWarnings(as.integer(d$gate$id)), d$gate$ticker)
             else setNames(integer(0), character(0))
    if (!is.null(d$coh) && nrow(d$coh) > 0) {   # cohort id fills gate gaps
      ex <- d$coh[!(d$coh$ticker %in% names(id_of)), c("ticker", "id")]
      ex <- ex[!duplicated(ex$ticker), , drop = FALSE]
      if (nrow(ex)) id_of <- c(id_of, setNames(as.integer(ex$id), ex$ticker))
    }
    # NULL = filter never touched -> show all; character(0) = deselect-all ->
    # empty board; else narrow. The stored val survives refetch re-renders.
    sel_id <- lc_board_ids_sel()
    keep_id <- function(v) {
      if (is.null(sel_id)) return(v)
      if (!length(sel_id)) return(v[FALSE])
      v[is.na(id_of[v]) | id_of[v] %in% as.integer(sel_id)]
    }
    bt <- keep_id(bt)
    sells  <- sort(bt[st[bt] == "sell"])
    buys   <- bt[st[bt] == "buy"]
    holds  <- bt[st[bt] == "hold"]
    closed <- sort(bt[st[bt] == "closed"])
    # buys = the period's surviving recs: ordered by signal persistence over
    # the review window (BUY on how many recorded runs), then win rate. The
    # chip carries both, so a day-one flicker reads differently from a name
    # endorsed the entire period.
    wp <- dv$wpct
    rn <- dv$runs_of; rt <- max(1L, dv$runs_tot)
    rb <- ifelse(is.na(rn[buys]), 0L, rn[buys])
    buys <- buys[order(-rb, -ifelse(is.na(wp[buys]), 0, wp[buys]), buys)]
    b_note <- sprintf("%.0f%% · %d/%d runs", wp[buys],
                      ifelse(is.na(rn[buys]), 0L, rn[buys]), rt)
    b_note <- ifelse(why[buys] == "new entry", paste0(b_note, " · new"), b_note)
    # orange + = qualstream's endorsement within the buy section: a GRADE
    # THRESHOLD, not a fixed top-N. A rank cut splits ties arbitrarily (grades
    # are coarse -- 11 names once tied on 62, so head(.., 20) marked 5 of them
    # and dropped 6 identical ones on alphabetical order), and a fixed N always
    # marks N names however weak the crop. The cap is only a backstop against a
    # future re-calibration inflating every grade.
    qs_min <- QS_MIN; qs_cap <- QS_CAP
    bg <- qs_grade[buys]
    qs_ok <- buys[!is.na(bg) & bg >= qs_min]
    qs_top <- utils::head(qs_ok[order(-qs_grade[qs_ok], qs_ok)], qs_cap)
    # Buy column display (sidebar-driven): default to the qualstream-passed names
    # only (the orange +), best grade first = "better positioned"; the "show all"
    # box falls back to the full runs-ordered buy list. With no qualstream grades
    # there is nothing to filter to, so show all rather than an empty column.
    buys_all   <- buys
    # default ON: show only qualstream-passed names in ALL three columns
    # (Kevin's final call 2026-08-11, after trying buy-only scoping on prod:
    # he wants the ticked state strictly qualstream). Known trade-off, accepted:
    # qualstream grades current buy candidates and grades expire (150d), so
    # held/sold positions routinely lack a fresh grade and hide while ticked -
    # UNTICK to see every open position/exit. With no grades at all there is
    # nothing to filter to, so everything shows.
    buys_qsonly <- isTRUE(input$lcBuyQSonly) && length(qs_top) > 0
    # every ticker qualstream graded at/above the orange-+ threshold: filters
    # hold/sell membership while ticked, and marks the + on chips everywhere.
    qs_pass <- names(qs_grade)[!is.na(qs_grade) & qs_grade >= qs_min]
    # shared sort: order a column by its shown date, most recent first (undated
    # sinks to the bottom), ticker as the tiebreak.
    date_desc <- function(ticks, dates) {
      dd <- suppressWarnings(as.Date(dates)); dd[is.na(dd)] <- as.Date("1900-01-01")
      ticks[order(-as.numeric(dd), ticks)]
    }
    buys_shown <- if (buys_qsonly) qs_top else buys_all
    buys_shown <- date_desc(buys_shown, dv$entry_of[buys_shown])
    # Chip note: entry "since" date by default; win% · runs appended only when the
    # sidebar detail box is ticked. A brand-new entry with no real tenure says so.
    b_since <- ifelse(is.na(dv$entry_of[buys_shown]), "", as.character(dv$entry_of[buys_shown]))
    if (isTRUE(input$lcBuyStats)) {
      b_stat <- sprintf("%.0f%% · %d/%d runs", wp[buys_shown],
                        ifelse(is.na(rn[buys_shown]), 0L, rn[buys_shown]), rt)
      b_note2 <- ifelse(nzchar(b_since), paste0(b_since, " · ", b_stat), b_stat)
    } else {
      b_note2 <- b_since
    }
    b_note2 <- ifelse(why[buys_shown] == "new entry",
                      ifelse(nzchar(b_note2), paste0(b_note2, " · new"), "new entry"),
                      b_note2)
    note_fmt <- if (isTRUE(input$lcBuyStats)) "entry date · win% · runs" else "entry date"
    buy_sub <- if (!length(qs_top)) paste0("no qualstream grades yet · all shown · ", note_fmt)
               else if (buys_qsonly) paste0("qualstream + only · ", note_fmt)
               else paste0("all standing recs · ", note_fmt)
    buy_title_stacked <- if (!length(qs_top)) "buy - all standing recs (no qualstream grades)"
                         else if (buys_qsonly) "buy - qualstream-passed picks only (untick for all)"
                         else "buy - all standing recs"
    holds_all <- holds
    holds <- if (buys_qsonly) holds[holds %in% qs_pass] else holds
    holds <- date_desc(holds, dv$entry_of[holds])
    hz_days <- round(hz * 30.44)
    h_pct <- pmin(100L, as.integer(round(100 *
               as.numeric(Sys.Date() - as.Date(dv$entry_of[holds])) / hz_days)))
    h_note <- ifelse(grepl("^matures", why[holds]), unname(why[holds]),
                     sprintf("%s · %d%%", dv$entry_of[holds], h_pct))
    # sell column: same qualstream filter while ticked, ordered by the exit
    # date embedded in the reason string ("gate flipped/matured/delisted DATE");
    # "(today)" has no date so it counts as today = newest.
    sells_all <- sells
    sells <- if (buys_qsonly) sells[sells %in% qs_pass] else sells
    sell_dt <- as.Date(vapply(unname(why[sells]), function(s) {
      m <- regmatches(s, regexpr("[0-9]{4}-[0-9]{2}-[0-9]{2}", s))
      if (length(m)) m else NA_character_ }, character(1)))
    sell_dt[is.na(sell_dt)] <- Sys.Date()
    sells <- sells[order(-as.numeric(sell_dt), sells)]
    # checkpoint calendar: qualstream runs every 4 months (Jan/May/Sep 1, in
    # sync with the qual_scorecards_4monthly DAG); the buy/prune window is the
    # few days right AFTER the run, so grades inform the prune. Amber while
    # the window is open, grey while waiting.
    ck_all <- as.Date(sprintf("%d-%02d-01",
                rep(as.integer(format(Sys.Date(), "%Y")) + c(-1, 0, 1), each = 3),
                c(1, 5, 9)))
    ck_last <- max(ck_all[ck_all <= Sys.Date()])
    ck_next <- min(ck_all[ck_all > Sys.Date()])
    ck_open <- Sys.Date() <= ck_last + 4
    # Did qualstream ACTUALLY run? Read MAX(as_of) from the loaded grades rather
    # than asserting it from the calendar -- a silently failed DAG must not be
    # announced as a completed run.
    qs_ran <- if (!is.null(d$qs) && nrow(d$qs) > 0 && "as_of" %in% names(d$qs))
                suppressWarnings(max(as.Date(d$qs$as_of))) else as.Date(NA)
    ck_note <- if (ck_open && !is.na(qs_ran) && qs_ran >= ck_last) div(
        style = "color:#fbbf24; font-size:0.78rem; font-weight:700; margin:0.15rem 0 0.6rem;",
        sprintf(paste("CHECKPOINT WINDOW OPEN - qualstream ran %s: prune",
                      "(matured / vetoed / washed-out) and buy the new cohort by %s.",
                      "Sells in the red section never wait for this window."),
                format(qs_ran), format(ck_last + 4)))
      else if (ck_open) div(
        style = "color:#f87171; font-size:0.78rem; font-weight:700; margin:0.15rem 0 0.6rem;",
        sprintf(paste("CHECKPOINT WINDOW OPEN - but NO qualstream grades recorded",
                      "for %s (latest grades: %s). Check the qual_scorecards_4monthly",
                      "DAG before pruning; sells in the red section never wait."),
                format(ck_last),
                if (is.na(qs_ran)) "none" else format(qs_ran)))
      else note_line(sprintf(paste(
        "Next checkpoint: qualstream runs %s (in %d days); buy/prune window %s",
        "to %s - act after the run so its grades inform the prune. Gate flips,",
        "delistings and maturities in the sell section do not wait.%s"),
        format(ck_next), as.integer(ck_next - Sys.Date()),
        format(ck_next + 1), format(ck_next + 4),
        if (is.na(qs_ran)) "" else sprintf(" Latest recorded grades: %s.", format(qs_ran))))
    # honesty notes: where the record is thin, say so instead of implying signal
    gap_lo <- if (length(dv$coh_dates)) max(dv$coh_dates) else NULL
    led_lo <- min(d$led$d)
    notes <- tagList(
      ck_note,
      if (dv$coh_n == 0) note_line(sprintf(paste(
        "No %d-month rank slot has ever passed the evidence gate (win rate >=",
        "55%% on >= 100 graded picks), so there are no historical entries at",
        "this horizon - the model's proven horizons are 12 months and shorter."),
        hz)),
      if (!is.null(gap_lo)) note_line(sprintf(paste(
        "Entry record: quarterly walk-forward cohorts to %s, then a gap with",
        "no recorded signals until the daily ledger, read from %s."),
        gap_lo, led_lo))
      else note_line(sprintf(
        "Entry record: daily ledger only, read from %s.", led_lo)),
      note_line(sprintf(paste(
        "The ledger physically starts 2026-06-16, but everything above is read",
        "from %s: the BUY gate was replaced on 2026-07-02 after the old one was",
        "measured anti-correlated with realized returns and had stopped emitting",
        "buys entirely. Pre-epoch rows are kept as history and excluded from run",
        "counts, entries and the ledger-vs-benchmark chart, so nothing here",
        "averages two different systems together."), led_lo)),
      if (length(qs_top)) note_line(sprintf(paste(
        "Orange + = qualstream grade >= %d among the buys (%d of %d graded).",
        "A threshold, not a top-N: it never splits a tie, and in a weak period",
        "fewer names qualify."), qs_min, length(qs_top), sum(!is.na(bg)))),
      if (length(qs_top)) note_line(tagList(
        span("+", style = "color:#fb923c; font-weight:800;"),
        sprintf(" passed qualstream: %s.",
          paste(sprintf("%s %d", qs_top,
                        as.integer(round(qs_grade[qs_top]))), collapse = ", ")))))
    # Kanban column: same chips as the stacked sections (chipf reused verbatim, so
    # orange +, delisted strikethrough and the notes are identical), only laid out
    # side by side. Chips still wrap inside each column and the column scrolls
    # independently, so a 90-name hold list stays compact instead of a giant tower.
    kanban_col <- function(title, colr, ticks, ch_notes, sub = "", max_h = 520, n_total = NA) {
      hdr <- if (!is.na(n_total) && n_total != length(ticks))
               sprintf("%s (%d of %d)", title, length(ticks), n_total)
             else sprintf("%s (%d)", title, length(ticks))
      div(style = paste0("min-width:0; background:rgba(148,163,184,0.03);",
                         " border:1px solid #1e293b; border-radius:8px; padding:0.5rem 0.6rem;"),
        div(style = sprintf(paste0("color:%s; font-weight:700; font-size:0.92rem;",
                                   " border-bottom:2px solid %s55; padding-bottom:0.25rem;"),
                            colr, colr),
            hdr),
        if (nzchar(sub))
          div(style = "color:#64748b; font-size:0.68rem; margin:0.15rem 0 0.4rem;", sub)
        else div(style = "margin-bottom:0.4rem;"),
        div(style = sprintf(paste0("display:flex; flex-direction:column; align-items:flex-start;",
                                   " gap:2px; max-height:%dpx; overflow-y:auto;"), max_h),
          if (length(ticks))
            mapply(function(t, n) chipf(t, colr, n, t %in% del_ticks, plus = t %in% qs_pass),
                   ticks, ch_notes, SIMPLIFY = FALSE, USE.NAMES = FALSE)
          else div(style = "color:#475569; font-size:0.75rem; font-style:italic; padding:0.3rem;",
                   "none")))
    }
    closed_foot <- div(style = "color:#64748b; font-weight:600; font-size:0.85rem; margin:0.4rem 0 0.5rem;",
          sprintf("closed - exited over a month ago (%d) · detail in the table below",
                  length(closed)))
    if (identical(input$lcBoardLayout, "stacked")) {
      tagList(
        notes,
        section("sell - exit now", col_of[["sell"]], sells, unname(why[sells])),
        section(sprintf("%s (%d of %d shown)", buy_title_stacked,
                        length(buys_shown), length(buys_all)),
                col_of[["buy"]], buys_shown, b_note2),
        section("hold - the period's dropped recs, still open (entry · % of horizon)",
                col_of[["hold"]], holds, h_note, max_h = 220),
        closed_foot)
    } else {
      # Lifecycle order left to right: buy -> hold -> sell (sell = the terminal
      # "exit now" state, kept red so urgency still reads at a glance).
      tagList(
        notes,
        div(style = paste0("display:grid; grid-template-columns:repeat(3, minmax(0,1fr));",
                           " gap:0.6rem; align-items:stretch; margin-bottom:0.5rem;"),
          kanban_col("buy", col_of[["buy"]], buys_shown, b_note2, buy_sub,
                     n_total = length(buys_all)),
          kanban_col("hold", col_of[["hold"]], holds, h_note,
                     if (buys_qsonly) "qualstream + only · entry · % of horizon"
                     else "dropped recs · entry · % of horizon",
                     n_total = length(holds_all)),
          kanban_col("sell", col_of[["sell"]], sells, unname(why[sells]),
                     if (buys_qsonly) "qualstream + only · exit now · reason"
                     else "exit now · reason",
                     n_total = length(sells_all))),
        closed_foot)
    }
  })

  # Does the qualstream marking help? Equal-weight return of the current BUYs
  # qualstream graded vs the >= 68 (orange +) subset vs SPY, over the current
  # 4-month window. Descriptive only: see the caption + LC_QS_COMPARE_SQL.
  output$qsCompareLC <- renderPlotly({
    req(app_dataLC())
    d <- app_dataLC(); cmp <- d$qscmp
    if (is.null(cmp) || nrow(cmp) == 0 || all(is.na(cmp$graded_pct)))
      return(empty_plot("No qualstream-graded buys in range - run qualstream to populate this."))
    cmp$d <- as.Date(cmp$d)
    qs_g <- if (!is.null(d$qs)) suppressWarnings(as.numeric(d$qs$grade)) else numeric(0)
    n_g  <- if (!is.null(d$qs)) length(d$qs$ticker) else 0L
    n_p  <- if (!is.null(d$qs)) sum(qs_g >= 68, na.rm = TRUE) else 0L
    has_pass <- any(!is.na(cmp$passed_pct))
    fig <- plot_ly()
    fig <- add_trace(fig, x = cmp$d, y = cmp$spy_pct, type = "scatter", mode = "lines",
      name = "Benchmark (SPY)", line = list(color = "#3b82f6", width = 2.5),
      hovertemplate = "SPY<br>%{x|%b %d}: %{y:.1f}%<extra></extra>")
    fig <- add_trace(fig, x = cmp$d, y = cmp$graded_pct, type = "scatter", mode = "lines",
      name = sprintf("Graded buys (%d)", n_g), line = list(color = "#f59e0b", width = 3),
      hovertemplate = "Graded buys<br>%{x|%b %d}: %{y:.1f}%<extra></extra>")
    if (has_pass)
      fig <- add_trace(fig, x = cmp$d, y = cmp$passed_pct, type = "scatter", mode = "lines",
        name = sprintf("Passed qualstream ≥ 68 (%d)", n_p),
        line = list(color = "#10b981", width = 3),
        hovertemplate = "Passed ≥68<br>%{x|%b %d}: %{y:.1f}%<extra></extra>")
    dark_layout(fig,
      title = list(text = sprintf(
          "Does the qualstream marking help? Current buys vs benchmark (from %s, descriptive)",
          format(d$qscmp_anchor, "%b %d")),
        font = list(color = "#f8fafc", size = 13), x = 0.5),
      xaxis = list(title = "", color = "#cbd5e1", type = "date",
                   gridcolor = "rgba(148,163,184,0.10)", zeroline = FALSE),
      yaxis = list(title = "Equal-weight return (%)", color = "#cbd5e1",
                   gridcolor = "rgba(148,163,184,0.10)", zeroline = TRUE,
                   zerolinecolor = "rgba(148,163,184,0.30)", ticksuffix = "%"),
      legend = list(font = list(color = "#e2e8f0"), orientation = "h", x = 0, y = -0.35),
      hovermode = "x unified", margin = list(l = 60, r = 20, t = 44, b = 70))
  })

  # Honesty caption: the comparison is a single-grade-run snapshot with partial
  # coverage, so it describes this batch, it does not validate qualstream.
  output$qsCompareNoteLC <- renderUI({
    req(app_dataLC())
    d <- app_dataLC()
    if (is.null(d$qscmp) || nrow(d$qscmp) == 0 || all(is.na(d$qscmp$graded_pct))) return(NULL)
    qs_g  <- if (!is.null(d$qs)) suppressWarnings(as.numeric(d$qs$grade)) else numeric(0)
    n_g   <- if (!is.null(d$qs)) length(d$qs$ticker) else 0L
    n_p   <- if (!is.null(d$qs)) sum(qs_g >= 68, na.rm = TRUE) else 0L
    qs_ran <- if (!is.null(d$qs) && nrow(d$qs) > 0 && "as_of" %in% names(d$qs))
                suppressWarnings(max(as.Date(d$qs$as_of))) else as.Date(NA)
    div(style = "color:#64748b; font-size:0.72rem; margin:0.15rem 0 0.6rem;",
      sprintf(paste("Descriptive, not a walk-forward test: qualstream has a single grade run",
                    "(as of %s) applied across the whole window. The basket is qualstream's %d graded",
                    "standing-rec buys (the board's buy list = the orange + universe); the green line is",
                    "the %d that scored >= 68. Both are equal-weight, held from %s (the current 4-month",
                    "window). Membership is retroactive (today's buys applied backward), which flatters",
                    "both against SPY; the graded-vs-passed comparison is unaffected since both carry the",
                    "same tilt. One retroactive grade set cannot yet prove qualstream adds return; that",
                    "needs several cadence cycles of point-in-time grades."),
              if (is.na(qs_ran)) "n/a" else format(qs_ran), n_g, n_p, format(d$qscmp_anchor)))
  })

  # The full record: every company the model ever entered (cohort or ledger)
  # with its CURRENT state - including the unproven gate names the board hides
  # (flagged in the Proven column) and the closed history. Primary reading
  # surface for detail; the board above is the at-a-glance summary.
  output$tableLC <- DT::renderDT({
    dv <- derivedLC()
    if (is.null(dv)) return(DT::datatable(
      data.frame(Note = "Connect and Generate to load the signal record."),
      rownames = FALSE, selection = "none", options = list(dom = "t", ordering = FALSE)))
    d <- app_dataLC()
    gate_v <- setNames(as.character(d$gate$gate_today), d$gate$ticker)
    # cluster id per ticker (stable 1-19 key; gate + cohort fallback)
    id_map <- if (!is.null(d$gate$id)) {
      setNames(suppressWarnings(as.integer(d$gate$id)), d$gate$ticker)
    } else setNames(integer(0), character(0))
    if (!is.null(d$coh) && nrow(d$coh) > 0) {
      ex2 <- d$coh[!(d$coh$ticker %in% names(id_map)), c("ticker", "id")]
      ex2 <- ex2[!duplicated(ex2$ticker), , drop = FALSE]
      if (nrow(ex2)) id_map <- c(id_map, setNames(as.integer(ex2$id), ex2$ticker))
    }
    held <- ifelse(is.na(dv$entry_of), NA,
                   as.integer(Sys.Date() - as.Date(dv$entry_of)))
    hz_days <- round(dv$hz * 30.44)
    df <- data.frame(
      Ticker = dv$tickers,
      id     = unname(id_map[dv$tickers]),
      State  = unname(dv$state_now),
      Entry  = ifelse(is.na(dv$entry_of), "", dv$entry_of),
      Source = ifelse(is.na(dv$src_of), "", unname(dv$src_of)),
      `Held (d)` = held,
      `% of horizon` = ifelse(is.na(held), NA,
                              pmin(100L, as.integer(round(100 * held / hz_days)))),
      Matures = ifelse(is.na(dv$mat_of), "", format(dv$mat_of)),
      Why    = unname(dv$why_now),
      Proven = ifelse(dv$tickers %in% names(dv$prov_of) & dv$prov_of[dv$tickers],
                      "yes", "no"),
      `Gate today` = ifelse(dv$tickers %in% names(gate_v), gate_v[dv$tickers], "-"),
      stringsAsFactors = FALSE, check.names = FALSE)
    # the rank slot's realized win rate at THIS horizon (the board's buy gate)
    df$`Bin win %` <- ifelse(df$Ticker %in% names(dv$wpct),
                             as.numeric(dv$wpct[df$Ticker]), NA_real_)
    # signal persistence over the trailing review window (BUY on n of N runs)
    rr <- ifelse(is.na(dv$runs_of[df$Ticker]), 0L, dv$runs_of[df$Ticker])
    # not "(4mo)" any more: the window is floored at the regime epoch, so it is
    # 4 months OR the time since the gate change, whichever is shorter.
    df$`Runs` <- sprintf("%d/%d", rr, max(1L, dv$runs_tot))
    # qualstream grade column only once grades exist (dormant until then)
    has_qs <- !is.null(d$qs) && nrow(d$qs) > 0 &&
              all(c("ticker", "grade") %in% names(d$qs))
    if (has_qs) {
      qi <- match(df$Ticker, d$qs$ticker)
      df$`QS grade` <- suppressWarnings(as.numeric(d$qs$grade))[qi]
    }
    # delist coloring on the Ticker column (same pattern as the ledger table)
    df$delisted <- if (!is.null(d$meta)) df$Ticker %in% d$meta$ticker else FALSE
    if (!is.null(d$meta) && nrow(d$meta) > 0) {
      mi <- match(df$Ticker, d$meta$ticker)
      df$delisting_category <- d$meta$delisting_category[mi]
      df$company_name       <- d$meta$company_name[mi]
      df$delisted_date      <- d$meta$delisted_date[mi]
    }
    df <- delist_enrich(df)
    df$del_class[!df$delisted] <- ""
    # cluster-id filter (same control + semantics as the board); idless names
    # always show; deselect-all empties the table too
    sel_id <- lc_board_ids_sel()
    if (is.null(sel_id)) {
      # filter never touched -> show all
    } else if (!length(sel_id)) {
      df <- df[0, , drop = FALSE]
    } else if (!is.null(d$gate$id)) {
      id_of <- setNames(suppressWarnings(as.integer(d$gate$id)), d$gate$ticker)
      if (!is.null(d$coh) && nrow(d$coh) > 0) {   # cohort id fills gate gaps
        ex <- d$coh[!(d$coh$ticker %in% names(id_of)), c("ticker", "id")]
        ex <- ex[!duplicated(ex$ticker), , drop = FALSE]
        if (nrow(ex)) id_of <- c(id_of, setNames(as.integer(ex$id), ex$ticker))
      }
      tid <- id_of[df$Ticker]
      df <- df[is.na(tid) | tid %in% as.integer(sel_id), , drop = FALSE]
    }
    keep <- c("Ticker", "id", "State", "Entry", "% of horizon",
              "Why", "Proven", "Gate today", "Bin win %",
              "Runs", if (has_qs) "QS grade", "del_class")
    df <- df[order(factor(df$State, levels = c("sell", "buy", "hold", "closed")),
                   df$Ticker), keep]
    # right-align targets resolved BY NAME so column trims can't misalign them
    num_t <- match(c("% of horizon", "Bin win %", if (has_qs) "QS grade"), keep) - 1L
    num_t <- num_t[!is.na(num_t)]
    del_t <- length(keep) - 1L
    DT::datatable(
      df, selection = "none", rownames = FALSE, class = "compact",
      extensions = "Buttons", filter = "top",
      options = list(
        pageLength = 25, lengthMenu = c(10, 25, 50, 100, 1000),
        dom = "Bftip", buttons = c("copy", "csv"), ordering = TRUE,
        columnDefs = list(
          list(className = "dt-right", targets = num_t),
          list(visible = FALSE, targets = del_t))
      )
    ) %>%
      DT::formatStyle("State", fontWeight = "600",
        color = DT::styleEqual(c("buy", "hold", "sell", "closed"),
                               c("#10b981", "#eab308", "#dc2626", "#64748b"))) %>%
      DT::formatStyle("Ticker", valueColumns = "del_class",
        color = DT::styleEqual(names(DELIST_CLASSES), unname(DELIST_CLASSES)))
  }, server = FALSE)
}

# Run the application
# Bind address/port are env-overridable (SHINY_HOST / SHINY_PORT) so the same
# script serves local and the prod server; defaults keep the historical 0.0.0.0:3838.
.shiny_port <- suppressWarnings(as.integer(Sys.getenv("SHINY_PORT", "3838")))
if (is.na(.shiny_port)) .shiny_port <- 3838L
runApp(list(ui = ui, server = server),
       host = Sys.getenv("SHINY_HOST", "0.0.0.0"), port = .shiny_port)
