# AlphaStream dashboard - DB-backed portfolio store (portfolio.* schema). Part of app.R, split 2026-10-19: files load in name order (see app.R).
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

# Delta-safe save: upsert the session's rows by id, delete ONLY drop_ids (ids
# this session loaded and then removed). Never a whole-table replace - the
# daily portfolio_sync DAG is a second writer since 2026-08-21, and rows it
# adds/archives mid-session must survive a dashboard save untouched.
# recoup_pinged_at is DAG-owned and deliberately absent so a save can't reset it.
db_save_positions <- function(con, df, drop_ids = NULL) {
  drop_ids <- as.character(drop_ids[!is.na(drop_ids) & nzchar(drop_ids)])
  # a duplicated id in one multi-row upsert aborts the whole statement
  # ("cannot affect row a second time") - keep the last occurrence
  if (!is.null(df) && nrow(df) > 0)
    df <- df[!duplicated(as.character(df$id), fromLast = TRUE), , drop = FALSE]
  DBI::dbWithTransaction(con, {
    if (length(drop_ids))
      DBI::dbExecute(con, "DELETE FROM portfolio.positions WHERE id = $1",
                     params = list(drop_ids))
    if (!is.null(df) && nrow(df) > 0)
      DBI::dbExecute(con, "
        INSERT INTO portfolio.positions
          (id,ticker,amount_usd,cadence,day1,day2,start_date,end_date,
           sold_date,sold_fraction,mode,adopted_at,created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        ON CONFLICT (id) DO UPDATE SET
          ticker = EXCLUDED.ticker, amount_usd = EXCLUDED.amount_usd,
          cadence = EXCLUDED.cadence, day1 = EXCLUDED.day1, day2 = EXCLUDED.day2,
          start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date,
          sold_date = EXCLUDED.sold_date, sold_fraction = EXCLUDED.sold_fraction,
          mode = EXCLUDED.mode, adopted_at = EXCLUDED.adopted_at,
          created_at = EXCLUDED.created_at",
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

# Same delta discipline as db_save_positions: additive insert, delete only the
# explicit drop set (tickers this session un-dismissed). portfolio_sync also
# writes dismissals (hand-sold-on-BUY names), which a full replace would wipe.
db_save_dismissed <- function(con, tickers, drop = NULL) {
  tickers <- unique(toupper(tickers[nzchar(tickers)]))
  drop <- setdiff(unique(toupper(as.character(drop[!is.na(drop) & nzchar(drop)]))), tickers)
  DBI::dbWithTransaction(con, {
    if (length(drop))
      DBI::dbExecute(con, "DELETE FROM portfolio.dismissed WHERE upper(ticker) = $1",
                     params = list(drop))
    if (length(tickers))
      DBI::dbExecute(con, "
        INSERT INTO portfolio.dismissed (ticker) VALUES ($1)
        ON CONFLICT (ticker) DO NOTHING",
                     params = list(tickers))
  })
  invisible(tickers)
}

# Additive tables that arrived after the original schema (archive + settings).
# Idempotent, dashboard-owned schema only; mirrors tools/create_portfolio_schema.sql
# so an existing DB self-upgrades on connect and a fresh one still bootstraps
# from the SQL file. (Kevin authorized this DDL 2026-08-21.)
pf_ensure_extras <- function(con) {
  tryCatch({
    DBI::dbExecute(con, "
      CREATE TABLE IF NOT EXISTS portfolio.closed_positions (
        id text NOT NULL, ticker text NOT NULL, cluster_id integer,
        cadence text, amount_usd numeric, invested_usd numeric, value_usd numeric,
        pnl_usd numeric, ret_pct numeric, spy_ret_pct numeric, vs_spy_pp numeric,
        vs_lump_pp numeric, entry_date date, sold_date date NOT NULL,
        outcome text NOT NULL CHECK (outcome IN ('profit','loss')),
        archived_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (id, sold_date))")
    DBI::dbExecute(con, "
      CREATE TABLE IF NOT EXISTS portfolio.app_settings (
        key text PRIMARY KEY, value text NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now())")
    # portfolio_sync extras: the once-per-position recoup stamp and the run
    # log / ping outbox. The app itself never writes recoup_pinged_at or
    # sync_runs, but provisioning here keeps every connected DB (dev included,
    # where the env-guarded DAG never runs) on the same schema.
    DBI::dbExecute(con, "
      ALTER TABLE portfolio.positions
        ADD COLUMN IF NOT EXISTS recoup_pinged_at date")
    DBI::dbExecute(con, "
      CREATE TABLE IF NOT EXISTS portfolio.sync_runs (
        as_of date PRIMARY KEY, qs_buys text[],
        n_adopt integer, n_snap integer, n_arch integer, n_recoup integer,
        msgs jsonb, sent boolean NOT NULL DEFAULT false,
        created_at timestamptz NOT NULL DEFAULT now())")
    # per-message delivery cursor + give-up counter (outbox audit 2026-08-23:
    # the per-day sent flag re-delivered the already-sent prefix on retry)
    DBI::dbExecute(con, "
      ALTER TABLE portfolio.sync_runs
        ADD COLUMN IF NOT EXISTS sent_n integer NOT NULL DEFAULT 0")
    DBI::dbExecute(con, "
      ALTER TABLE portfolio.sync_runs
        ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0")
    # re-entry announcement dedup (policy 2026-08-28): sync-owned, but the
    # Adopt handler deletes from it, so provision it here too.
    DBI::dbExecute(con, "
      CREATE TABLE IF NOT EXISTS portfolio.reentry_pinged (
        ticker text NOT NULL, episode int NOT NULL,
        pinged_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (ticker, episode))")
    TRUE
  }, error = function(e) FALSE)
}
# Once per process and database: the DDL above is idempotent, but it ran 7
# statements (and their NOTICE lines) on every Generate of every tab.
.pf_extras_done <- new.env(parent = emptyenv())
pf_ensure_extras_once <- function(con) {
  info <- tryCatch(DBI::dbGetInfo(con), error = function(e) list())
  key <- paste(info$host, info$port, info$dbname, sep = "|")
  if (isTRUE(.pf_extras_done[[key]])) return(TRUE)
  ok <- pf_ensure_extras(con)
  if (isTRUE(ok)) .pf_extras_done[[key]] <- TRUE
  ok
}

db_get_setting <- function(con, key, default = "") {
  v <- tryCatch(DBI::dbGetQuery(con,
    "SELECT value FROM portfolio.app_settings WHERE key = $1",
    params = list(key))$value, error = function(e) NULL)
  if (is.null(v) || !length(v) || is.na(v[1]) || !nzchar(v[1])) default else v[1]
}

db_set_setting <- function(con, key, value) {
  tryCatch(DBI::dbExecute(con, "
    INSERT INTO portfolio.app_settings (key, value, updated_at)
    VALUES ($1, $2, now())
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
    params = list(key, as.character(value))), error = function(e) NULL)
  invisible(value)
}

db_archive_positions <- function(con, rows) {
  if (is.null(rows) || !nrow(rows)) return(invisible(0L))
  DBI::dbExecute(con, "
    INSERT INTO portfolio.closed_positions
      (id, ticker, cluster_id, cadence, amount_usd, invested_usd, value_usd,
       pnl_usd, ret_pct, spy_ret_pct, vs_spy_pp, vs_lump_pp, entry_date,
       sold_date, outcome)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::date,$14::date,$15)
    ON CONFLICT (id, sold_date) DO NOTHING",
    params = list(as.character(rows$id), toupper(rows$ticker),
      as.integer(rows$cluster_id), as.character(rows$cadence),
      as.numeric(rows$amount_usd), as.numeric(rows$invested_usd),
      as.numeric(rows$value_usd), as.numeric(rows$pnl_usd),
      as.numeric(rows$ret_pct), as.numeric(rows$spy_ret_pct),
      as.numeric(rows$vs_spy_pp), as.numeric(rows$vs_lump_pp),
      .pf_nz(rows$entry_date), .pf_nz(rows$sold_date), as.character(rows$outcome)))
  invisible(nrow(rows))
}

db_load_closed <- function(con) {
  tryCatch(DBI::dbGetQuery(con, "
    SELECT id, ticker, cluster_id, to_char(entry_date,'YYYY-MM-DD') AS entry_date,
           to_char(sold_date,'YYYY-MM-DD') AS sold_date, invested_usd, value_usd,
           pnl_usd, ret_pct, spy_ret_pct, vs_spy_pp, vs_lump_pp, outcome
    FROM portfolio.closed_positions ORDER BY sold_date DESC, ticker"),
    error = function(e) NULL)
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

