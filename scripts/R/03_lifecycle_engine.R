# AlphaStream dashboard - Lifecycle engine: schedules, trails, pins, episodes, per-load memo, board table, price cache. Part of app.R, split 2026-10-19: files load in name order (see app.R).
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
# For recurring cadences this filter yields pause (name off the buy list),
# resume (BUY returns), buy-off during a wind-down, and a fresh accumulation
# leg after re-entry. ONE-SHOT rows defer instead of drop: a "once" plan whose
# single candidate date fails the gate enters at the FIRST later ledger run
# where the model says BUY - dropping its only date would mean the position
# silently never exists. Manual rows are unchanged. Model rows need `led`;
# any model row that ends up contributing no fills (no ledger yet, or gated
# out entirely) lands in attr(out, "model_skipped") so the UI can say so.
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
      ix <- lc_led_idx(led)                                      # built once per load
      run_dates <- ix$run_d
      buy_keys  <- ix$buy_key
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
        if (!nrow(cand)) {
          skipped <- c(skipped, toupper(trimws(as.character(r$ticker)))); next }
        cd   <- as.Date(cand$d)
        idx  <- findInterval(cd, run_dates)                 # latest run <= D (0 if none)
        keep <- idx >= 1 &
                as.numeric(cd - run_dates[pmax(idx, 1L)]) <= LC_SIGNAL_MAX_AGE &
                paste(toupper(cand$ticker), format(run_dates[pmax(idx, 1L)])) %in% buy_keys
        if (!any(keep) && identical(as.character(r$cadence), "once")) {
          # one-shot defers, never drops: enter at the first run on/after the
          # candidate date where this ticker is a BUY (bounded by end_date/today)
          tk      <- toupper(trimws(as.character(cand$ticker[1])))
          end_raw <- as.character(r$end_date)
          stop_r  <- if (!is.na(r$end_date) && nzchar(end_raw))
                       min(as.Date(end_raw), today) else today
          later <- run_dates[run_dates >= cd[1] & run_dates <= stop_r &
                             paste(tk, format(run_dates)) %in% buy_keys]
          if (length(later)) { cand$d <- format(min(later)); keep <- TRUE }
        }
        if (any(keep)) parts[[length(parts) + 1]] <- cand[keep, , drop = FALSE]
        else skipped <- c(skipped, toupper(trimws(as.character(r$ticker))))
      }
      if (length(parts)) out <- rbind(out, do.call(rbind, parts))
    }
  }
  attr(out, "model_skipped") <- unique(skipped)
  out
}

# Lifecycle memo shared by every browser session (2026-10-19). One R process
# serves all tabs, and a new tab or a reload used to redo the whole Lifecycle
# load: about 14 s of single-threaded R, most of it per-name trail replays and
# schedule expansions, while every other tab waited. A load is keyed by the data
# signature (DB target, latest ledger day, latest recorded state, latest grade,
# latest nested run, today): a click on Generate always reloads; the auto-start
# and the hourly tick reuse a load younger than LC_MEMO_TTL_SEC when the
# signature still matches. The trail and derivedLC caches hang off the load's
# stamp, so a new load drops them.
LC_MEMO_TTL_SEC <- 600
.lc_memo <- new.env(parent = emptyenv())
lc_data_signature <- function(con) {
  q1 <- function(sql) tryCatch(as.character(DBI::dbGetQuery(con, sql)[[1]][1]),
                               error = function(e) "none")
  info <- tryCatch(DBI::dbGetInfo(con), error = function(e) list())
  paste(info$host, info$port, info$dbname,
        q1("SELECT MAX(prediction_date)::text FROM monitoring.prediction_ledger"),
        q1("SELECT MAX(captured_at)::text FROM portfolio.state_history"),
        q1("SELECT MAX(graded_at)::text FROM qual.ticker_scorecards"),
        q1("SELECT MAX(run_date)::text FROM monitoring.nested_board_shadow_runs"),
        format(Sys.Date()), sep = "|")
}
lc_bundle_memo_get <- function(sig) {
  m <- .lc_memo$bundle
  if (is.null(m) || !identical(m$sig, sig)) return(NULL)
  if (as.numeric(difftime(Sys.time(), m$at, units = "secs")) > LC_MEMO_TTL_SEC) return(NULL)
  m$dat
}
lc_bundle_memo_put <- function(sig, dat) {
  dat$memo_stamp <- paste(sig, format(Sys.time(), "%Y%m%d%H%M%OS3"), sep = "|")
  .lc_memo$bundle <- list(sig = sig, at = Sys.time(), dat = dat)
  .lc_memo$dv <- NULL; .lc_memo$dv_key <- NULL
  .lc_memo$trails <- new.env(parent = emptyenv()); .lc_memo$trails_stamp <- dat$memo_stamp
  .lc_memo$px <- NULL; .lc_memo$px_from <- NULL; .lc_memo$px_spy_max <- NULL
  dat
}
lc_loaded_msg <- function(d) sprintf(
  "Loaded - %d proven cohort picks across %d quarterly cutoffs + %d live series across %d snapshots.",
  if (is.null(d$coh)) 0L else nrow(d$coh),
  if (is.null(d$coh)) 0L else length(unique(d$coh$d)),
  length(unique(d$led$ticker[d$led$global_action == "BUY"])),
  length(unique(d$led$d)))

# Ledger lookups built once per load and attached to d$led (attribute lc_idx):
# every trail replay and schedule expansion used to re-scan the ~40k ledger rows
# and re-sort its run days per name. A ledger without the attribute (a subset,
# a test) gets the same lookups built on the spot.
lc_led_index <- function(led) {
  ldd <- if (!is.null(led$dd)) led$dd else as.Date(led$d)
  b <- led$global_action == "BUY"; s <- led$global_action == "SELL"
  list(run_d    = sort(unique(ldd)),
       buy_d    = lapply(split(ldd[b], led$ticker[b]), sort),
       buy_key  = paste(toupper(led$ticker), format(ldd))[b],
       sell_key = paste(led$ticker[s], as.character(led$d[s]), sep = "|"),
       empty_d  = ldd[0])
}
lc_led_idx <- function(led) {
  ix <- attr(led, "lc_idx")
  if (is.null(ix)) lc_led_index(led) else ix
}
# Delisting date per upper-cased ticker, built once per load (d$meta_dl); the
# first row wins, as the per-name scans of d$meta it replaces did.
lc_meta_dl <- function(meta) {
  if (is.null(meta) || !nrow(meta)) return(NULL)
  u <- toupper(meta$ticker); keep <- !duplicated(u)
  setNames(suppressWarnings(as.Date(as.character(meta$delisted_date[keep]))), u[keep])
}
lc_dl_of <- function(d, key) {
  m <- d$meta_dl; if (is.null(m)) m <- lc_meta_dl(d$meta)
  i <- if (is.null(m)) NA_integer_ else match(key, names(m))
  if (is.na(i)) as.Date(NA) else m[[i]]
}

# Price rows shared by every Lifecycle reader and session (2026-10-19): the pins,
# pick lines, checked lines, master list and track record each pulled overlapping
# cdm.ingest_combined ranges on every render. Rows are cached per ticker from the
# earliest date asked, per database, and reset with each new load; every read
# returns what its own query did: date >= from, optionally capped at SPY's last
# date, ordered by ticker and date.
lc_px_get <- function(con, tks, from, cap_spy = TRUE) {
  tks <- unique(as.character(tks)); from <- format(as.Date(from))
  info <- tryCatch(DBI::dbGetInfo(con), error = function(e) list())
  db <- paste(info$host, info$port, info$dbname, sep = "|")
  if (!identical(.lc_memo$px_db, db)) {
    .lc_memo$px <- NULL; .lc_memo$px_from <- NULL; .lc_memo$px_spy_max <- NULL; .lc_memo$px_db <- db
  }
  have <- .lc_memo$px_from; if (is.null(have)) have <- setNames(character(0), character(0))
  need <- tks[!(tks %in% names(have)) | (unname(have[tks]) > from) %in% TRUE]
  if (length(need)) {
    inl <- paste(sprintf("'%s'", gsub("'", "''", need, fixed = TRUE)), collapse = ",")
    new <- DBI::dbGetQuery(con, sprintf(
      "SELECT ticker, date::text AS d, adj_close AS px FROM cdm.ingest_combined
       WHERE ticker IN (%s) AND date >= '%s' ORDER BY ticker, date", inl, from))
    old <- .lc_memo$px
    if (!is.null(old)) old <- old[!(old$ticker %in% need), , drop = FALSE]
    .lc_memo$px <- rbind(old, new)
    have[need] <- from
    .lc_memo$px_from <- have
  }
  if (cap_spy && is.null(.lc_memo$px_spy_max))
    .lc_memo$px_spy_max <- DBI::dbGetQuery(con,
      "SELECT MAX(date)::text AS d FROM cdm.ingest_combined WHERE ticker = 'SPY'")$d[1]
  px <- .lc_memo$px
  if (is.null(px)) return(data.frame(ticker = character(0), d = character(0), px = numeric(0),
                                     stringsAsFactors = FALSE))
  keep <- px$ticker %in% tks & px$d >= from
  if (cap_spy) keep <- keep & !is.na(.lc_memo$px_spy_max) & px$d <= .lc_memo$px_spy_max
  out <- px[keep, , drop = FALSE]
  out <- out[order(out$ticker, out$d), , drop = FALSE]
  rownames(out) <- NULL
  out
}

# The board from the pipeline (elt-inference-models scripts/board_daily.py, run
# daily after grading): the same walk, cascade, adopt queue and trails as
# derivedLC + lc_board_trail, computed once per day instead of on every page load,
# with history frozen from the table's go-live (an evidence re-mint no longer
# repaints old pins). Used when its latest run covers the latest ledger day;
# otherwise (tables missing, the day's run not in yet, dev/staging DBs) the
# dashboard derives the board itself as before.
lc_board_load <- function(con, led_max) {
  run <- tryCatch(DBI::dbGetQuery(con, "
    SELECT as_of::text AS as_of, go_live::text AS go_live, runs_tot, coh_n,
           array_to_string(coh_dates, ' ') AS coh_dates
    FROM monitoring.board_daily_runs WHERE hz = 12 ORDER BY as_of DESC LIMIT 1"),
    error = function(e) NULL)
  if (is.null(run) || !nrow(run) || is.na(led_max) || run$as_of[1] < led_max) return(NULL)
  daily <- DBI::dbGetQuery(con, "
    SELECT ticker, state, why, src, proven, board_ok, entry_date::text AS entry_date,
           exit_date::text AS exit_date, exit_reason, maturity_date::text AS maturity_date,
           run_days, qs_rank, win_pct::float8 AS win_pct
    FROM monitoring.board_daily WHERE as_of = $1::date AND hz = 12",
    params = list(run$as_of[1]))
  trail <- DBI::dbGetQuery(con, "
    SELECT ticker, d::text AS date, state FROM monitoring.board_trail
    WHERE as_of = $1::date AND hz = 12 ORDER BY ticker, seq",
    params = list(run$as_of[1]))
  if (!nrow(daily)) return(NULL)
  # what the board said each day since its go-live, for every name: the pins'
  # recorded flag and fold reasons read it like portfolio.state_history
  hist <- DBI::dbGetQuery(con, "
    SELECT ticker, as_of::text AS as_of, hz, state, why FROM monitoring.board_daily
    WHERE hz = 12 AND as_of >= $1::date ORDER BY ticker, as_of",
    params = list(run$go_live[1]))
  list(run = as.list(run[1, ]), daily = daily, trail = trail, hist = hist)
}
# Recorded board states = portfolio.state_history (tracked names, since 2026-08-07)
# plus, from the board table's go-live, the table's own day for every name. A day
# the user moved by hand (paused/sold by user) keeps the user's row.
lc_rec_with_board <- function(rec, hist) {
  if (is.null(hist) || !nrow(hist)) return(rec)
  cols <- c("ticker", "as_of", "hz", "state", "why")
  hist <- hist[, cols]
  if (is.null(rec) || !nrow(rec)) return(hist)
  key <- function(x) paste(toupper(x$ticker), x$as_of, x$hz)
  user <- !is.na(rec$why) & rec$why %in% c("paused by user", "sold by user")
  out <- rbind(rec[!(key(rec) %in% key(hist)) | user, cols, drop = FALSE],
               hist[!(key(hist) %in% key(rec[user, , drop = FALSE])), , drop = FALSE])
  out <- out[order(out$ticker, out$as_of, out$hz), , drop = FALSE]
  rownames(out) <- NULL
  out
}
# derivedLC's list, built from the table rows: the same names, types and order, so
# every reader keeps working. M carries no walk, only the names and (attribute
# lc_trails) each name's stored trail, which lc_board_trail serves directly.
lc_dv_from_board <- function(b, d) {
  x <- b$daily[order(b$daily$ticker), , drop = FALSE]
  tk <- x$ticker
  nm <- function(v) setNames(v, tk)
  M <- matrix(character(0), nrow = length(tk), ncol = 0, dimnames = list(tk, NULL))
  tr <- b$trail
  attr(M, "lc_trails") <- if (nrow(tr))
    lapply(split(seq_len(nrow(tr)), tr$ticker), function(i)
      data.frame(date = tr$date[i], state = tr$state[i], stringsAsFactors = FALSE)) else list()
  coh_dates <- if (!is.null(b$run$coh_dates) && !is.na(b$run$coh_dates) && nzchar(b$run$coh_dates))
                 strsplit(b$run$coh_dates, " ", fixed = TRUE)[[1]] else character(0)
  qr <- suppressWarnings(as.integer(x$qs_rank))
  list(M = M, dates = sort(unique(c(coh_dates, d$led$d))), tickers = tk,
       entry_of = nm(as.character(x$entry_date)), exit_of = nm(as.character(x$exit_date)),
       reason_of = nm(ifelse(is.na(x$exit_reason), "", as.character(x$exit_reason))), hz = 12L,
       state_now = nm(as.character(x$state)), why_now = nm(as.character(x$why)),
       mat_of = nm(suppressWarnings(as.Date(x$maturity_date))), src_of = nm(as.character(x$src)),
       prov_of = nm(as.logical(x$proven)), wpct = nm(suppressWarnings(as.numeric(x$win_pct))),
       board_ok = nm(as.logical(x$board_ok)), coh_dates = coh_dates,
       coh_n = as.integer(b$run$coh_n), runs_of = nm(as.integer(x$run_days)),
       runs_tot = as.integer(b$run$runs_tot),
       # grades as derivedLC reads them (live, any graded name), queue as the run ranked it
       qs_grade = if (!is.null(d$qs) && nrow(d$qs) > 0 && all(c("ticker", "grade") %in% names(d$qs)))
                    setNames(suppressWarnings(as.numeric(d$qs$grade)), d$qs$ticker)
                  else setNames(numeric(0), character(0)),
       qs_buys = tk[!is.na(qr)][order(qr[!is.na(qr)])])
}

# Board-faithful trail for the chart overlay: the walk's daily state row
# collapsed to its changes, but post-epoch buy/hold days are REPLAYED under the chip rule (monthly
# majority x proven slot) instead of the raw per-run walk. The raw walk marks
# an open name "hold" on any run day it is merely absent from - exactly the
# one-off-day demotion state_now smooths away - so raw pins flip buy<->hold on
# noise and can even END contradicting the chip. The cohort era (pre-epoch,
# sparse cutoffs) passes through verbatim. Maturity/delist sells are re-dated
# to the TRUE exit (entry + hz months, or the delist date) instead of the
# later walk snapshot that first observed them - the same ref_d correction
# state_now applies; a recorded SELL action (gate flip) keeps its date.
# now_state is the chip's state today: the final open day is pinned to it so
# trail and chip agree.
lc_board_trail <- function(M, dates, tk, led, epoch, prov, now_state,
                           hz = 12L, dl_d = as.Date(NA), rec = NULL) {
  # board from the pipeline: the trail was computed (or frozen) there
  stored <- attr(M, "lc_trails")
  if (!is.null(stored)) return(stored[[tk]])
  # memo per load (M carries the load stamp): six readers ask for the same trails
  st  <- attr(M, "lc_memo")
  key <- if (!is.null(st) && identical(.lc_memo$trails_stamp, st))
           paste(tk, isTRUE(prov), now_state, hz, format(dl_d), is.null(rec), epoch, sep = "|")
  if (!is.null(key) && exists(key, envir = .lc_memo$trails, inherits = FALSE))
    return(get(key, envir = .lc_memo$trails, inherits = FALSE))
  out <- lc_board_trail_compute(M, dates, tk, led, epoch, prov, now_state, hz, dl_d, rec)
  if (!is.null(key)) assign(key, out, envir = .lc_memo$trails)
  out
}
lc_board_trail_compute <- function(M, dates, tk, led, epoch, prov, now_state, hz, dl_d, rec) {
  out <- lc_board_trail_replay(M, dates, tk, led, epoch, prov, now_state, hz, dl_d)
  # Seam fix (2026-10-06, widened 2026-10-12): the replay judges buy vs hold
  # with TODAY's proven flag. A name whose FIRST daily record is BUY was proven
  # when the board first looked at it (the record says "held since <entry>"),
  # so replay the days before its own first record as proven. Without this a
  # name unproven today pinned HOLD at entry and "flipped" to BUY on the day its
  # recording began (its adoption day), a change that never happened: 16 names
  # on 2026-08-07 and 21 adopted later (ENVA/PTGX Aug 14, Kevin 2026-10-12).
  # Days from the first record on come from the records (lc_merge_recorded).
  if (!isTRUE(prov) && !is.null(rec) && nrow(rec)) {
    r <- rec[toupper(rec$ticker) == toupper(tk) & rec$hz == hz &
             rec$state %in% c("buy", "hold", "sell"), , drop = FALSE]
    if (nrow(r)) {
      r  <- r[order(r$as_of), , drop = FALSE]
      r0 <- as.Date(r$as_of[1])                     # this name's own first record
      if (identical(r$state[1], "buy")) {
        alt <- lc_board_trail_replay(M, dates, tk, led, epoch, TRUE, now_state, hz, dl_d)
        if (!is.null(alt) && nrow(alt)) {
          pre  <- alt[as.Date(alt$date) < r0, c("date", "state"), drop = FALSE]
          rest <- if (!is.null(out) && nrow(out))
                    out[as.Date(out$date) >= r0, c("date", "state"), drop = FALSE] else NULL
          out <- rbind(pre, rest)
        }
      }
    }
  }
  lc_merge_recorded(out, rec, tk, hz)
}

# Overlay the RECORDED board states (portfolio.state_history, captured daily
# since 2026-08-07) on the replayed trail. The replay decides buy vs hold with
# `prov` = proven under TODAY's evidence, so after the 2026-10-01 re-mint
# de-gated id 9 every past BUY of its names replayed as HOLD (24 names, all
# one HOLD pin each). Recorded days show what the board actually said then;
# days before the first capture keep the replay. No rows -> replay unchanged.
lc_merge_recorded <- function(trail, rec, tk, hz) {
  if (is.null(rec) || !nrow(rec)) return(trail)
  r <- rec[toupper(rec$ticker) == toupper(tk) & rec$hz == hz &
           rec$state %in% c("buy", "hold", "sell"), , drop = FALSE]
  if (!nrow(r)) return(trail)
  r <- r[order(r$as_of), , drop = FALSE]
  r0 <- as.Date(r$as_of[1])
  pre <- if (!is.null(trail) && nrow(trail))
           trail[as.Date(trail$date) < r0, c("date", "state"), drop = FALSE] else NULL
  chg <- c(TRUE, r$state[-1] != r$state[-nrow(r)])
  post <- data.frame(date = as.character(r$as_of[chg]), state = r$state[chg],
                     stringsAsFactors = FALSE)
  # after the last capture, keep any later replayed change (e.g. today's chip)
  last_r <- as.Date(r$as_of[nrow(r)])
  tail_t <- if (!is.null(trail) && nrow(trail))
              trail[as.Date(trail$date) > last_r, c("date", "state"), drop = FALSE] else NULL
  out <- rbind(pre, post, tail_t)
  if (nrow(out) > 1)
    out <- out[c(TRUE, out$state[-1] != out$state[-nrow(out)]), , drop = FALSE]
  rownames(out) <- NULL
  out
}

lc_board_trail_replay <- function(M, dates, tk, led, epoch, prov, now_state,
                                  hz = 12L, dl_d = as.Date(NA)) {
  if (is.null(M) || is.null(dates) || !(tk %in% rownames(M))) return(NULL)
  row <- M[tk, ]; dd <- as.Date(dates)
  ix <- lc_led_idx(led)                                      # built once per load
  run_d <- ix$run_d
  tkb <- ix$buy_d[[tk]]; if (is.null(tkb)) tkb <- ix$empty_d
  open_j <- which(row %in% c("buy", "hold") & dd >= as.Date(epoch))
  for (j in open_j) {
    D <- dd[j]
    bw <- tkb[tkb >= D - 30 & tkb <= D]
    # mirrors derivedLC share_of: denominator starts at the name's first BUY
    # inside the window, so a fresh entrant (1/1) qualifies on day one
    maj <- length(bw) > 0 &&
      length(bw) / max(1, sum(run_d >= min(bw) & run_d <= D)) >= 0.5
    row[j] <- if (maj && isTRUE(prov)) "buy" else "hold"
  }
  if (length(open_j) && !is.na(now_state) && now_state %in% c("buy", "hold"))
    row[open_j[length(open_j)]] <- now_state
  # A current SELL with no model sell in the matrix is a hand-sold / gate-flip-
  # today name (a dev override, or state_now flipping to sell before the ledger
  # records it). Pin it at the last open day so the sketch shows the sell instead
  # of silently dropping the name to its last buy/hold state.
  if (length(open_j) && identical(now_state, "sell") && !any(row == "sell"))
    row[open_j[length(open_j)]] <- "sell"
  keep <- which(nzchar(row))
  if (!length(keep)) return(NULL)
  st <- unname(row[keep]); dt <- as.character(dates[keep])
  chg <- c(TRUE, st[-1] != st[-length(st)])
  out <- data.frame(date = dt[chg], state = st[chg], stringsAsFactors = FALSE)
  idx <- seq_len(nrow(out))
  for (k in idx[out$state == "sell"]) {
    wd <- out$date[k]
    if (paste(tk, wd, sep = "|") %in% ix$sell_key) next   # gate flip: real that day
    ps <- idx[out$state == "sell" & idx < k]          # this run's entry = first
    lo <- if (length(ps)) max(ps) else 0L             # buy after the prior sell
    eb <- idx[out$state == "buy" & idx > lo & idx < k]
    if (!length(eb)) next
    mat_d <- seq(as.Date(out$date[min(eb)]), by = paste(hz, "months"),
                 length.out = 2)[2]
    true_d <- suppressWarnings(min(c(mat_d, dl_d), na.rm = TRUE))
    if (is.finite(true_d) && true_d < as.Date(wd)) out$date[k] <- format(true_d)
  }
  out
}

# Settled-state pins (display only, 2026-10-12). The board re-decides daily and a
# single-vote name on the 0.50 share line flips every time one vote lands or
# lapses (AXON: six pins in 15 days). The sketch therefore pins a state change
# only once the board kept that state for PIN_DWELL_RUNDAYS ledger run-days
# (the Prometheus "for" / integrating-debounce pattern): entries and sells always
# pin (money moved), a kept pin that repeats the last pinned state merges into it
# (that merge, not the constant, is what folds AXON's Sep 6 BUY), every dropped
# flip is listed in the next pin's hover, and the open run is flagged unsettled
# (drawn hollow) until it has lasted the dwell. The model, the trail, the table
# and the STATE column are untouched; this only chooses which flips get a marker.
# Measured 2026-10-12 on the recorded board (92 runs): 27 runs of 1 to 4 days are
# all single-vote crossings or one-day re-mint blips; k = 4 leaks a 4-day wobble,
# k = 5 to 7 give the same pins, k = 10 leaves 12 names unsettled. Re-check k
# after another quarter of capture. Comparison is dwell >= PIN_DWELL_RUNDAYS.
PIN_DWELL_RUNDAYS <- 5L
lc_pin_reason <- function(why) {
  w <- tolower(ifelse(is.na(why), "", why))
  ifelse(grepl("unproven", w), "de-proved at re-mint, votes unchanged",
  ifelse(grepl("washed|flicker", w), "washed out, share below 0.50", ""))
}

# Lifecycle chart lines (sketch + portfolio chart) draw as a gentle spline through
# the SAME daily points: display only, so every value, hover, pin and line end is
# unchanged. Plotly smoothing runs 0 (straight segments) to 1.3; 0.5 only rounds
# the corners between days, with at most a slight overshoot right after a big
# one-day jump. Kevin 2026-10-16: "very subtle smoothing".
LC_LINE_SMOOTHING <- 0.5
lc_line <- function(...) list(..., shape = "spline", smoothing = LC_LINE_SMOOTHING)

# Bundle lines (one faint line per name that never carries pins: cluster grey
# lines, qualstream pick lines, market-creator lines) also get a light centered
# average over LC_BUNDLE_SMOOTH_K sessions so the bundle reads calmer; the spline
# alone was invisible at full range (Kevin 2026-10-16: 3, then "slightly more"
# -> 5). Averaged over trading days only (weekend rows hold Monday's close). The
# window narrows toward both ends, so the first and last points stay real: each
# line still starts at 0% and ends at its real return (= the master list Return),
# without a kink next to the ends. Hover shows the real value. Lines that carry
# pins (SPY, the baskets, checked names, the chip overlay) stay exact.
LC_BUNDLE_SMOOTH_K <- 5L
lc_soft_line <- function(ln, k = LC_BUNDLE_SMOOTH_K) {
  if (is.null(ln) || !nrow(ln)) return(ln)
  wd <- as.integer(format(as.Date(ln$d), "%u"))
  keep <- wd <= 5; keep[c(1, length(keep))] <- TRUE
  ln <- ln[keep, , drop = FALSE]
  ln$raw <- ln$ret
  n <- nrow(ln); h <- (k - 1L) %/% 2L
  if (n > 2 && h > 0 && !anyNA(ln$ret)) {
    cs <- c(0, cumsum(ln$ret))
    i <- seq_len(n); w <- pmin(h, i - 1L, n - i)   # half-window, 0 at both ends
    ln$ret <- (cs[i + w + 1] - cs[i - w]) / (2 * w + 1)
    ln$ret[c(1, n)] <- ln$raw[c(1, n)]             # exact, not cumsum-rounded
  }
  ln
}

# Strong smoothing, the sketch's "Smooth lines" button (Kevin 2026-10-19: "in the
# graph i would like an option like a button to smooth the lines very noticely").
# Off by default. The same tapered centered average as above, run twice over
# LC_STRONG_SMOOTH_K trading days (a triangular kernel about a month wide), on
# EVERY sketch line including SPY and the baskets. Display only, like the light
# one: each line still starts at 0% and ends at its real return, hovers show the
# real value, pins sit on the smoothed line at their own date, and the y axis keeps
# the unsmoothed range, so toggling never zooms (Kevin 2026-10-13: no axis zoom).
LC_STRONG_SMOOTH_K <- 11L
lc_taper_avg <- function(x, k) {
  n <- length(x); h <- (k - 1L) %/% 2L
  if (n <= 2 || h <= 0) return(x)
  cs <- c(0, cumsum(x)); i <- seq_len(n); w <- pmin(h, i - 1L, n - i)
  y <- (cs[i + w + 1] - cs[i - w]) / (2 * w + 1)
  y[c(1, n)] <- x[c(1, n)]
  y
}
lc_smooth2 <- function(v, k = LC_STRONG_SMOOTH_K) {
  ok <- which(!is.na(v)); if (length(ok) < 3) return(v)
  v[ok] <- lc_taper_avg(lc_taper_avg(v[ok], k), k)
  v
}
lc_weekdays <- function(df, col = "d") {             # trading days, first and last kept
  if (is.null(df) || !nrow(df)) return(df)
  keep <- as.integer(format(as.Date(df[[col]]), "%u")) <= 5; keep[c(1, length(keep))] <- TRUE
  df[keep, , drop = FALSE]
}
lc_strong_line <- function(ln, k = LC_STRONG_SMOOTH_K) {
  if (is.null(ln) || !nrow(ln)) return(ln)
  ln <- lc_weekdays(ln); ln$raw <- ln$ret; ln$ret <- lc_smooth2(ln$ret, k)
  ln
}
lc_pin_debounce <- function(ev, run_d, last_d, min_days = PIN_DWELL_RUNDAYS,
                            every = FALSE, why_of = NULL) {
  if (is.null(ev) || !nrow(ev)) return(ev)
  ev <- ev[order(as.Date(ev$date)), , drop = FALSE]
  n  <- nrow(ev); d <- as.Date(ev$date); st <- as.character(ev$state)
  nxt <- c(d[-1], as.Date(NA))
  dwell <- vapply(seq_len(n), function(i)
    if (is.na(nxt[i])) sum(run_d >= d[i] & run_d <= last_d)
    else sum(run_d >= d[i] & run_d < nxt[i]), numeric(1))
  # money moved: every sell/closed, and the first BUY of each position (the first
  # buy after the start or after a sell/closed, even when a replayed hold precedes it)
  money <- logical(n); seen_buy <- FALSE
  for (i in seq_len(n)) {
    if (st[i] %in% c("sell", "closed")) { money[i] <- TRUE; seen_buy <- FALSE }
    else if (st[i] == "buy" && !seen_buy) { money[i] <- TRUE; seen_buy <- TRUE }
  }
  settled <- dwell >= min_days
  keep <- if (isTRUE(every)) rep(TRUE, n) else (money | settled)
  keep[n] <- TRUE                                   # the open run always has a pin (hollow if young)
  surv <- rep(FALSE, n); last_state <- NA_character_
  for (i in seq_len(n)) {
    if (!keep[i]) next
    if (!isTRUE(every) && !is.na(last_state) && identical(st[i], last_state)) next
    surv[i] <- TRUE; last_state <- st[i]
  }
  desc <- function(idx) {
    why <- if (!is.null(why_of)) unname(why_of[format(d[idx])]) else rep(NA_character_, length(idx))
    rs  <- lc_pin_reason(why)
    sprintf("%s %s (%dd%s)", st[idx], format(d[idx], "%b %d"), as.integer(dwell[idx]),
            ifelse(nzchar(rs), paste0(", ", rs), ""))
  }
  hidden <- character(n); n_hidden <- integer(n)
  if (!isTRUE(every) && any(surv)) {
    pend <- integer(0)
    for (i in seq_len(n)) {
      if (surv[i]) {
        if (length(pend)) { hidden[i] <- paste(desc(pend), collapse = ", "); n_hidden[i] <- length(pend) }
        pend <- integer(0)
      } else pend <- c(pend, i)
    }
    if (length(pend)) {                             # trailing dropped rows ride the last pin
      j <- max(which(surv))
      hidden[j] <- paste(c(if (nzchar(hidden[j])) hidden[j], desc(pend)), collapse = ", ")
      n_hidden[j] <- n_hidden[j] + length(pend)
    }
  }
  ev$dwell <- as.integer(dwell)
  ev$settled <- if (isTRUE(every)) rep(TRUE, n) else (settled | money)   # raw view draws solid
  ev$hidden <- hidden; ev$n_hidden <- n_hidden
  ev[surv, , drop = FALSE]
}

# Pair a per-ticker board trail (lc_board_trail output) into buy->sell episodes =
# realized trades. An open buy with a hand-sold override (sold_over, from the
# positions table) closes at that date; otherwise it stays open and is marked to
# last_d. Returns a list of list(entry, exit, open). Shared by the flip-history
# modal and the board-wide track-record strip so a clicked chip always reconciles.
lc_trade_episodes <- function(trail, sold_over = as.Date(NA), last_d = as.Date(NA)) {
  ev <- if (!is.null(trail) && nrow(trail))
          trail[trail$state %in% c("buy", "hold", "sell"), , drop = FALSE] else NULL
  eps <- list(); open_e <- as.Date(NA)
  if (!is.null(ev) && nrow(ev)) for (i in seq_len(nrow(ev))) {
    s <- ev$state[i]; dt <- as.Date(ev$date[i])
    if (s == "buy" && is.na(open_e)) open_e <- dt
    else if (s == "sell" && !is.na(open_e)) {
      eps[[length(eps) + 1]] <- list(entry = open_e, exit = dt, open = FALSE); open_e <- as.Date(NA) }
  }
  if (!is.na(open_e)) {
    if (!is.na(sold_over) && sold_over >= open_e)
      eps[[length(eps) + 1]] <- list(entry = open_e, exit = sold_over, open = FALSE)
    else eps[[length(eps) + 1]] <- list(entry = open_e, exit = last_d, open = TRUE)
  }
  eps
}

# Buy-RUN windows: segment the trail into contiguous BUY runs; each window ends
# at its transition -> kind "hold" (downgraded to hold), "sell" (exited straight
# from buy), or "current" (still buying, run open to today). A re-buy after a
# hold opens a NEW window. A sell that fires from a hold state is not a buy-run,
# so it is not counted as buy->sell. Same row shape as lc_trade_episodes so
# lc_price_episodes prices each window vs SPY over its own dates.
lc_buyrun_episodes <- function(trail, sold_over = as.Date(NA), last_d = as.Date(NA)) {
  ev <- if (!is.null(trail) && nrow(trail))
          trail[trail$state %in% c("buy", "hold", "sell"), , drop = FALSE] else NULL
  eps <- list(); run_start <- as.Date(NA)
  if (!is.null(ev) && nrow(ev)) for (i in seq_len(nrow(ev))) {
    s <- ev$state[i]; dt <- as.Date(ev$date[i])
    if (s == "buy") { if (is.na(run_start)) run_start <- dt }
    else if (s == "hold") { if (!is.na(run_start)) {
      eps[[length(eps) + 1]] <- list(entry = run_start, exit = dt, kind = "hold", open = FALSE)
      run_start <- as.Date(NA) } }
    else if (s == "sell") { if (!is.na(run_start)) {
      eps[[length(eps) + 1]] <- list(entry = run_start, exit = dt, kind = "sell", open = FALSE)
      run_start <- as.Date(NA) } }
  }
  if (!is.na(run_start)) {
    if (!is.na(sold_over) && sold_over >= run_start)
      eps[[length(eps) + 1]] <- list(entry = run_start, exit = sold_over, kind = "sell", open = FALSE)
    else
      eps[[length(eps) + 1]] <- list(entry = run_start, exit = last_d, kind = "current", open = TRUE)
  }
  eps
}

# Price a list of episodes for one ticker. tp / sp = the ticker's and SPY's
# (d, px) frames (d = Date, px = numeric, sorted). Returns one row per episode:
# entry, exit ("open" if still held), days, ret (own %), spy (%), vs (pp), open.
lc_price_episodes <- function(eps, tp, sp) {
  if (!length(eps)) return(NULL)
  asof <- function(df, dd) { s <- df$px[df$d <= dd]; if (!length(s)) NA_real_ else s[length(s)] }
  rows <- lapply(eps, function(e) {
    ep <- asof(tp, e$entry); xp <- asof(tp, e$exit)
    es <- asof(sp, e$entry); xs <- asof(sp, e$exit)
    ret <- if (is.na(ep) || ep == 0) NA_real_ else (xp / ep - 1) * 100
    spy <- if (is.na(es) || es == 0) NA_real_ else (xs / es - 1) * 100
    data.frame(entry = format(e$entry), exit = if (e$open) "open" else format(e$exit),
               days = as.integer(e$exit - e$entry), ret = ret, spy = spy, vs = ret - spy,
               open = isTRUE(e$open), stringsAsFactors = FALSE)
  })
  do.call(rbind, rows)
}

# Aggregate the round-trip table (A) and buy-run window table (B) into the strip's
# summary numbers. Split out so the whole-board record (lc_track_record) and the
# cluster-filtered view (the strip) compute identically. Windows are
# DURATION-WEIGHTED (weight = days held, >= 1) so a brief buy<->hold flip barely
# counts; nm = distinct tickers per kind, exposing flip churn.
lc_track_agg <- function(A, B) {
  n_vs  <- if (is.null(A)) 0L else sum(!is.na(A$vs))
  n_win <- if (is.null(A)) 0L else sum(A$vs > 0, na.rm = TRUE)
  vsk <- function(k) {
    if (is.null(B)) return(list(v = NA_real_, n = 0L, nm = 0L))
    sub <- B[B$kind == k & !is.na(B$vs), , drop = FALSE]
    if (!nrow(sub)) return(list(v = NA_real_, n = 0L, nm = 0L))
    w <- pmax(sub$days, 1)
    list(v = sum(sub$vs * w) / sum(w), n = nrow(sub), nm = length(unique(sub$ticker))) }
  wh <- vsk("hold"); ws <- vsk("sell"); wc <- vsk("current")
  list(n = if (is.null(A)) 0L else nrow(A),
       closed = if (is.null(A)) 0L else sum(!A$open),
       open   = if (is.null(A)) 0L else sum(A$open),
       avg_ret = if (is.null(A)) NA_real_ else mean(A$ret, na.rm = TRUE),
       avg_vs  = if (is.null(A)) NA_real_ else mean(A$vs,  na.rm = TRUE),
       n_vs = n_vs, n_win = n_win,
       win = if (n_vs) n_win / n_vs * 100 else NA_real_,
       vs_hold = wh$v, n_hold = wh$n, nm_hold = wh$nm,
       vs_sell = ws$v, n_sell = ws$n, nm_sell = ws$nm,
       vs_now  = wc$v, n_now  = wc$n, nm_now  = wc$nm)
}


# What the board's own calls would have earned over a range (Kevin 2026-10-19).
# Money goes in on a schedule and each deposit is split equally across the names
# the board says BUY that day (`states`: the per-name trails, change points of
# buy/hold/sell, as on the pins); a purchase is held until that name's next SELL,
# then kept as cash. A deposit on a day with no BUY waits for the next run day
# that has one (to the end if none comes). SPY gets the same deposits on the same
# days and is held to the end. Prices are as-of closes (`px`: ticker, d, px incl.
# SPY). One row per schedule, each investing `total` in all.
lc_strategy_schedules <- function(states, px, run_d, start, end, total = 1000) {
  start <- as.Date(start); end <- as.Date(end)
  rd <- sort(unique(as.Date(run_d)[as.Date(run_d) >= start & as.Date(run_d) <= end]))
  states <- Filter(function(s) !is.null(s) && nrow(s) > 0, states)
  if (!length(rd) || is.null(px) || !nrow(px)) return(NULL)
  by_tk <- lapply(split(px[, c("d", "px")], px$ticker), function(p) p[order(p$d), , drop = FALSE])
  price_at <- function(tk, x) {
    p <- by_tk[[tk]]; if (is.null(p)) return(NA_real_)
    i <- findInterval(as.numeric(x), as.numeric(p$d)); if (i < 1) NA_real_ else p$px[i]
  }
  tks <- names(states)
  state_on <- function(tk) {                          # state on every run day in range
    s <- states[[tk]]; i <- findInterval(as.numeric(rd), as.numeric(s$date))
    out <- rep(NA_character_, length(rd)); out[i > 0] <- s$state[i[i > 0]]; out
  }
  st <- if (length(tks)) vapply(tks, state_on, character(length(rd))) else
          matrix(character(0), nrow = length(rd), ncol = 0)
  st <- matrix(st, nrow = length(rd), dimnames = list(NULL, tks))
  buys_on <- lapply(seq_along(rd), function(j) {      # priced BUY names per run day
    b <- tks[st[j, ] %in% "buy"]
    b[!is.na(vapply(b, function(tk) price_at(tk, rd[j]), numeric(1)))]
  })
  has_buy <- lengths(buys_on) > 0
  exit_of <- function(tk, D) {
    s <- states[[tk]]; k <- which(s$date > D & s$state == "sell")
    if (length(k)) s$date[k[1]] else as.Date(NA)
  }
  spy_end <- price_at("SPY", end)
  first_on_or_after <- function(x) {
    i <- findInterval(as.numeric(x) - 1, as.numeric(rd)) + 1
    rd[i[i <= length(rd)]]
  }
  sched <- list(
    "Lump sum at the start" = start,
    "Every run day"         = rd,
    "Every week"            = seq(start, end, by = "week"),
    "Every 2 weeks"         = seq(start, end, by = "2 weeks"),
    "Every month"           = seq(start, end, by = "month"))
  rows <- lapply(names(sched), function(nm) {
    dep <- first_on_or_after(sched[[nm]])
    if (!length(dep)) return(NULL)
    a <- total / length(dep)
    val <- spy <- 0; waited <- cashed <- sold <- 0L; names_bought <- character(0)
    for (x in as.list(dep)) {
      sp0 <- price_at("SPY", x)
      spy <- spy + if (is.na(sp0) || is.na(spy_end)) a else a * spy_end / sp0
      j <- which(rd >= x & has_buy)
      if (!length(j)) { val <- val + a; cashed <- cashed + 1L; next }
      D <- rd[j[1]]; if (D > x) waited <- waited + 1L
      b <- buys_on[[j[1]]]; names_bought <- union(names_bought, b)
      for (tk in b) {
        p0 <- price_at(tk, D); ex <- exit_of(tk, D)
        out_d <- if (!is.na(ex) && ex <= end) { sold <- sold + 1L; ex } else end
        p1 <- price_at(tk, out_d); if (is.na(p1)) p1 <- p0
        val <- val + (a / length(b)) * p1 / p0
      }
    }
    data.frame(schedule = nm, deposits = length(dep), invested = total, value = val,
               ret_pct = (val / total - 1) * 100, spy_value = spy, spy_pct = (spy / total - 1) * 100,
               waited = waited, cashed = cashed, sold = sold, names = length(names_bought),
               stringsAsFactors = FALSE)
  })
  out <- do.call(rbind, rows)
  if (is.null(out)) return(NULL)
  attr(out, "buy_days") <- sum(has_buy); attr(out, "run_days") <- length(rd)
  attr(out, "first_day") <- rd[1]                     # deposits start at the first run day
  attr(out, "last_buy") <- if (any(has_buy)) max(rd[has_buy]) else as.Date(NA)
  out
}
