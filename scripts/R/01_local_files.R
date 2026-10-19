# AlphaStream dashboard - Local parquet files: QA history, portfolio fallback, qualstream constants, market tags. Part of app.R, split 2026-10-19: files load in name order (see app.R).
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
# QS_MIN marks the orange + on board chips (grade threshold, Kevin's call
# 2026-08-11). The SEED no longer gates on it: one retroactive grade vintage
# cannot validate a 68 cut, so the seed takes every validated-board BUY,
# ranked by grade (ungraded rank after, by signal persistence), vetoed names
# excluded, capped at QS_CAP overall and QS_CLUSTER_CAP per cluster id
# (concentration guard: the seed set has clustered ~7+7 across two cluster
# models before, so one cluster-gate flip would move a large block of the board
# at once; the per-id cap bounds that blast radius).
QS_MIN <- 68L
QS_CAP <- 25L
QS_CLUSTER_CAP <- 5L
# grade freshness in days - ONE source for every SQL copy (board mark, seed
# ranking, comparison chart; twin: board_state.py QS_MAX_AGE_DAYS). 150 ~ the
# 4-month cadence + slack, so a mark expires instead of living forever. NOTE:
# all current grades share one as_of, so expiry is a synchronized cliff until
# grading runs monthly/incrementally - the caption warns as it approaches.
QS_MAX_AGE_DAYS <- 150L

# Market-shift tags (qualstream category.py -> qual.ticker_market_tags, v2): three
# model answers per graded name, voted in code: does the company lead a market
# shift that is still under way, by CREATING a category or REDEFINING an existing
# market (the disruptor ◆), and, separately, is it an ENABLER (⚙: it sells a
# hard-to-replace input to someone else's large, fast-growing market; Kevin
# 2026-10-14 "i want to know the disruptors and enabler")? DISPLAY-ONLY (Kevin 2026-10-13: a marker next to the ticker and a way to
# view only those names across every cluster): filled ◆ = a shift in its growth
# phase in a market forecast at $10B+ a year, hollow ◇ = a pre-commercial builder with a dated milestone. It never feeds
# a grade, the 68 bar, the seed/adopt queue or any BUY/HOLD rule, and it is never
# backtestable (the tag comes from the model's own knowledge, so a past-dated tag
# is hindsight). v1 rows (qual.ticker_category_tags, new_category_v1) are history
# and no longer read. Latest tag per ticker, no age window: the tagger asks about
# each name once, so a window would drop every mark a year after its only tag
# (a deliberate refresh writes a newer row, which wins). NULL when the table is absent
# or empty (dev, staging, or before the tagger is deployed): no marker, no
# checkbox, no narrowing.
NC_TAG_VERSION  <- "market_shift_v2"
lc_load_category_tags <- function(con) {
  x <- tryCatch(dbGetQuery(con, sprintf("
    SELECT DISTINCT ON (ticker) ticker, marker, kind, category, parent_market,
           prior_option, adoption_stage, adoption_evidence, potential_size,
           potential_evidence, scale_year,
           first_sale_year, company_rank, is_creator, votes_marked, votes_total,
           milestone, enabler, enabler_votes, supplies_to, supply_evidence,
           supplied_stage, supply_role, supply_share, supplied_size,
           rationale, model, as_of::text AS as_of
    FROM qual.ticker_market_tags
    WHERE tag_version = '%s'
    ORDER BY ticker, as_of DESC, tagged_at DESC", NC_TAG_VERSION)),
    error = function(e) NULL)
  if (is.null(x) || !nrow(x)) return(NULL)
  x$ticker <- toupper(x$ticker)
  x$disruptor <- x$marker %in% c("filled", "hollow")
  x$enabler   <- x$enabler %in% TRUE
  x$marked    <- x$disruptor | x$enabler     # in the view: a disruptor or an enabler
  x
}
# The glyph(s) after a ticker: ◆ disruptor (◇ not selling yet), ⚙ enabler, both
# when both apply. U+FE0E keeps the gear a text glyph, never an emoji.
NC_GEAR <- "\u2699\ufe0e"
lc_nc_glyph <- function(marker, enabler) {
  d <- ifelse(marker %in% "hollow", "\u25c7", ifelse(marker %in% "filled", "\u25c6", ""))
  paste0(d, ifelse(enabler %in% TRUE, NC_GEAR, ""))
}
# Hover text for marked rows: kind, category, what buyers used before, adoption
# stage with its dated figure, timing, rank, the vote and the knowledge date.
lc_nc_tip <- function(x) {
  nz <- function(v) { v <- as.character(v); v[is.na(v)] <- ""; v }
  col <- function(f) if (is.null(x[[f]])) rep("", nrow(x)) else nz(x[[f]])
  # enabler part (⚙): what it supplies, how replaceable, how much of the company
  en  <- if (is.null(x$enabler)) rep(FALSE, nrow(x)) else x$enabler %in% TRUE
  sev <- col("supply_evidence")
  en_txt <- ifelse(en, sprintf("Enabler: a %s supplier to %s (%s)%s, %s to the company%s.",
                               ifelse(col("supply_role") == "bottleneck", "hard-to-replace", "leading"),
                               col("supplies_to"), gsub("_", " ", col("supplied_stage")),
                               ifelse(nzchar(sev) & sev != "no figure known", paste0("; ", sev), ""),
                               col("supply_share"),
                               ifelse(nzchar(col("enabler_votes")), sprintf(", votes %s/%s",
                                      col("enabler_votes"), col("votes_total")), "")), "")
  dis <- x$marker %in% c("filled", "hollow")
  hollow <- x$marker %in% "hollow"
  kind <- ifelse(hollow, "Building a new market (not selling yet)",
                 ifelse(x$kind %in% "REDEFINE", "Redefining a market", "Creating a category"))
  sy <- suppressWarnings(as.integer(x$scale_year)); fy <- suppressWarnings(as.integer(x$first_sale_year))
  when <- ifelse(!is.na(sy) & sy > 0, paste("at scale since", sy),
                 ifelse(!is.na(fy) & fy > 0, paste("first sold", fy), "no sales yet"))
  rank <- ifelse(x$company_rank %in% c("1", "2"), paste0(", rank #", x$company_rank), "")
  ev <- nz(x$adoption_evidence)
  stage <- paste0(gsub("_", " ", nz(x$adoption_stage)), ifelse(nzchar(ev), paste0(" (", ev, ")"), ""))
  nxt <- ifelse(hollow & nzchar(nz(x$milestone)), paste0(" Next: ", nz(x$milestone), "."), "")
  # the forecast behind "large growth potential" ($10B+ a year, decided in the tagger)
  psz  <- if (is.null(x$potential_size)) rep("", nrow(x)) else nz(x$potential_size)
  pev  <- if (is.null(x$potential_evidence)) rep("", nrow(x)) else nz(x$potential_evidence)
  plab <- c(under_1b = "under $1B", "1b_to_10b" = "$1B to $10B",
            "10b_to_100b" = "$10B to $100B", over_100b = "over $100B")[psz]
  pot  <- ifelse(is.na(plab), "", paste0(" Market potential: ", plab, " a year",
                                         ifelse(nzchar(pev), paste0(" (", pev, ")"), ""), "."))
  dis_txt <- sprintf("%s: %s (in %s). Before: %s. Stage: %s; %s%s.%s%s Votes %s/%s. %s",
          kind, nz(x$category), nz(x$parent_market), nz(x$prior_option), stage, when, rank,
          pot, nxt, nz(x$votes_marked), nz(x$votes_total), nz(x$rationale))
  trimws(paste0(ifelse(dis, paste0(dis_txt, " "), ""), ifelse(en, paste0(en_txt, " "), ""),
                sprintf("Model knowledge, tagged %s.", nz(x$as_of))))
}

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

