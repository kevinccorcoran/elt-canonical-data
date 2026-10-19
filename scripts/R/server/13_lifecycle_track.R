# AlphaStream dashboard - Lifecycle: realized archive, track record, positions table. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # ── Realized archive ────────────────────────────────────────────────────────
  lc_closed <- reactiveVal(NULL)

  # Persist the auto-adopt $ whenever the user changes it (DB backend only; the
  # parquet fallback keeps it session-local). Loaded back on connect below.
  observeEvent(input$lcSeedAmt, {
    if (!identical(lc_backend(), "db")) return()
    a <- suppressWarnings(as.numeric(input$lcSeedAmt))
    if (length(a) != 1 || !is.finite(a) || a < 1) return()
    con <- tryCatch(get_con(input), error = function(e) NULL); if (is.null(con)) return()
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    db_set_setting(con, "auto_adopt_amount", a)
  }, ignoreInit = TRUE)

  observeEvent(input$lcRecoupThr, {
    if (!identical(lc_backend(), "db")) return()
    a <- suppressWarnings(as.numeric(input$lcRecoupThr))
    if (length(a) != 1 || !is.finite(a) || a < 10) return()
    con <- tryCatch(get_con(input), error = function(e) NULL); if (is.null(con)) return()
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    db_set_setting(con, "recoup_threshold_pct", a)
  }, ignoreInit = TRUE)

  # Archive sold = move ACTUALLY-sold rows (Sell stamped) into
  # portfolio.closed_positions, frozen at their current simulated-fill numbers,
  # and drop them from the active book. Dismissed too, so the next Generate does
  # not instantly re-seed a name the gate still lists as a buy. Losses archive
  # exactly like profits (decision 2026-08-21: follow the model, no
  # hold-until-breakeven).
  observeEvent(input$lcPosArchive, {
    if (!identical(lc_backend(), "db")) {
      showNotification("Archive needs the DB portfolio store (connect to a DB with the portfolio schema).",
                       type = "warning"); return()
    }
    sel <- suppressWarnings(as.integer(input$lcPosChecked)); cur <- lc_pos()
    if (is.null(cur) || !nrow(cur)) return()
    sel <- sel[!is.na(sel) & sel >= 1 & sel <= nrow(cur)]
    if (!length(sel)) { showNotification("Check one or more SOLD rows to archive.", type = "message"); return() }
    sd <- as.character(cur$sold_date[sel])
    unsold <- sel[is.na(sd) | !nzchar(sd)]
    if (length(unsold)) {
      showNotification(sprintf("Not marked sold yet (use Sell selected first): %s",
        paste(toupper(cur$ticker[unsold]), collapse = ", ")), type = "warning")
      sel <- setdiff(sel, unsold)
      if (!length(sel)) return()
    }
    perf <- tryCatch(lc_row_perf(), error = function(e) NULL)
    d    <- app_dataLC()
    id_of <- if (!is.null(d) && !is.null(d$gate$id))
      setNames(suppressWarnings(as.integer(d$gate$id)), toupper(d$gate$ticker)) else integer(0)
    pc <- function(col, ids) if (is.null(perf) || !nrow(perf)) rep(NA_real_, length(ids)) else
      as.numeric(perf[[col]])[match(ids, perf$id)]
    ids <- as.character(cur$id[sel])
    inv <- pc("invested", ids); val <- pc("value", ids); spyv <- pc("spy_value", ids)
    lmp <- pc("lump_ret", ids)
    # a row with no priced fills has no realized numbers - archiving it would
    # fabricate a $0 "loss"; keep it active and say so instead
    unpriced <- which(is.na(val) | is.na(inv) | inv <= 0)
    if (length(unpriced)) {
      showNotification(sprintf("No priced fills yet - not archived: %s",
        paste(toupper(cur$ticker[sel][unpriced]), collapse = ", ")), type = "warning")
      keep <- setdiff(seq_along(sel), unpriced)
      if (!length(keep)) return()
      sel <- sel[keep]; ids <- ids[keep]
      inv <- inv[keep]; val <- val[keep]; spyv <- spyv[keep]; lmp <- lmp[keep]
    }
    since <- if (!is.null(perf) && "since" %in% names(perf))
      as.character(perf$since)[match(ids, perf$id)] else rep(NA_character_, length(ids))
    ret  <- 100 * (val / inv - 1); spyr <- 100 * (spyv / inv - 1)
    # partial-sale history: slices already archived realize their own money, so
    # the final exit archives only the REMAINING stake (mirrors portfolio_sync)
    sfA <- suppressWarnings(as.numeric(cur$sold_fraction[sel]))
    rem <- ifelse(!is.na(sfA) & sfA > 0 & sfA < 1, 1 - sfA, 1)
    pnl  <- (val - inv) * rem
    rows <- data.frame(
      id = ids, ticker = toupper(cur$ticker[sel]),
      cluster_id = unname(id_of[toupper(cur$ticker[sel])]),
      cadence = as.character(cur$cadence[sel]),
      amount_usd = as.numeric(cur$amount_usd[sel]),
      invested_usd = inv * rem, value_usd = val * rem, pnl_usd = pnl,
      ret_pct = ret, spy_ret_pct = spyr, vs_spy_pp = ret - spyr,
      vs_lump_pp = ret - lmp,
      entry_date = ifelse(is.na(since), as.character(cur$start_date[sel]), since),
      sold_date = as.character(cur$sold_date[sel]),
      outcome = ifelse(!is.na(pnl) & pnl > 0, "profit", "loss"),
      stringsAsFactors = FALSE)
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) { showNotification("No DB connection - connect first.", type = "error"); return() }
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    ok <- tryCatch({ db_archive_positions(con, rows); TRUE },
                   error = function(e) { showNotification(paste("Archive failed:",
                     conditionMessage(e)), type = "error"); FALSE })
    if (!ok) return()
    merged <- cur[setdiff(seq_len(nrow(cur)), sel), , drop = FALSE]
    persist_positions(merged); lc_pos(merged)
    nd <- union(lc_dismissed(), toupper(cur$ticker[sel]))
    persist_dismissed(nd); lc_dismissed(nd)
    lc_closed(db_load_closed(con))
    npro <- sum(rows$outcome == "profit"); nlos <- sum(rows$outcome == "loss")
    showNotification(sprintf("Archived %d position(s): %d in profit, %d at a loss.",
                             nrow(rows), npro, nlos), type = "message")
  })

  output$lcClosedTable <- DT::renderDT({
    cl <- lc_closed()
    if (is.null(cl) || !nrow(cl))
      return(DT::datatable(
        data.frame(Note = "Nothing archived yet - Sell a row, then Archive once you have actually exited."),
        rownames = FALSE, selection = "none", options = list(dom = "t", ordering = FALSE)))
    usd  <- function(x) ifelse(is.na(x), "-", paste0("$", formatC(round(as.numeric(x)), format = "d", big.mark = ",")))
    susd <- function(x) ifelse(is.na(x), "-", sprintf("%s$%s", ifelse(as.numeric(x) < 0, "-", "+"),
                        formatC(abs(round(as.numeric(x))), format = "d", big.mark = ",")))
    spct <- function(x) ifelse(is.na(x), "-", sprintf("%+.1f%%", as.numeric(x)))
    spp  <- function(x) ifelse(is.na(x), "-", sprintf("%+.1fpp", as.numeric(x)))
    show <- data.frame(
      id = ifelse(is.na(cl$cluster_id), "-", as.character(cl$cluster_id)),
      Ticker = cl$ticker, Entry = cl$entry_date, Sold = cl$sold_date,
      Invested = usd(cl$invested_usd), Exit = usd(cl$value_usd),
      `P&L $` = susd(cl$pnl_usd), Return = spct(cl$ret_pct),
      `vs SPY` = spp(cl$vs_spy_pp), `vs HODL` = spp(cl$vs_lump_pp),
      Outcome = cl$outcome,
      check.names = FALSE, stringsAsFactors = FALSE)
    DT::datatable(show, rownames = FALSE, selection = "none", class = "compact",
      options = list(dom = "t", ordering = TRUE, pageLength = 100, scrollX = TRUE,
                     order = list(list(3, "desc")))) %>%
      DT::formatStyle("Outcome", fontWeight = "600",
        color = DT::styleEqual(c("profit", "loss"), c("#34d399", "#f87171")))
  }, server = FALSE)

  # Realized board track record (Kevin 2026-08-18): aggregate stats for every name
  # the board has traded since the ledger epoch, for the top-of-board strip. One
  # trade = a buy->sell episode (buy<->hold flips are NOT closes - the position is
  # held through); sold trades banked, open trades marked to today. Reuses the exact
  # per-ticker episode logic as the flip-history modal (lc_trade_episodes /
  # lc_price_episodes) so a clicked chip's numbers reconcile with the aggregate.
  # Episodes whose entry predates the epoch (old walk-forward cohort picks) are
  # dropped: this is the LIVE following record, not the backtest. Its own reactive
  # so board layout / cluster-filter toggles don't trigger the price query + rebuild.
  lc_track_record <- reactive({
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    d  <- tryCatch(app_dataLC(), error = function(e) NULL)
    if (is.null(dv) || is.null(dv$M) || is.null(d)) return(NULL)
    if (is.null(input$db_pass) || input$db_pass == "") return(NULL)
    # Universe = names that actually SHOW on the board (proven at horizon, board_ok),
    # not the full raw gate in state_now (~972 single-run blips since the epoch). This
    # is Kevin's "every ticker that showed on the board at one point". Not narrowed by
    # the cluster-id filter - the record is the whole board since inception.
    all_nm <- names(dv$state_now)
    ok <- if (!is.null(dv$board_ok)) dv$board_ok[all_nm] else rep(TRUE, length(all_nm))
    universe <- toupper(all_nm[!is.na(ok) & ok])
    # + every name the board RECORDED as a BUY (portfolio.state_history, daily
    # since 2026-08-07). board_ok is judged on TODAY's proven evidence, so the
    # 2026-10-01 re-mint (id 9 de-gated) dropped every traded name: board_ok 17,
    # qs-passed 23, overlap 0 -> empty universe -> the strip read "No board trades
    # priced yet" and the portfolio's Model % column went blank (2026-10-06).
    rec_buy <- if (!is.null(d$rec_states) && nrow(d$rec_states))
                 unique(toupper(d$rec_states$ticker[d$rec_states$state == "buy"])) else character(0)
    universe <- unique(c(universe, intersect(rec_buy, toupper(all_nm))))
    # "only the qs are trades" (Kevin 2026-08-18): follow the board's qualstream+picks
    # filter. On (default) -> count only qualstream-passed names (grade >= QS_MIN, the
    # orange +), matching the columns; untick it to count every proven board trade.
    if (isTRUE(input$lcBuyQSonly) && !is.null(dv$qs_grade) && length(dv$qs_grade)) {
      g  <- setNames(suppressWarnings(as.numeric(dv$qs_grade)), toupper(names(dv$qs_grade)))
      qp <- names(g)[!is.na(g) & g >= QS_MIN]
      universe <- intersect(universe, qp)
    }
    if (!length(universe)) return(NULL)
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) return(NULL)
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    px <- tryCatch(lc_px_get(con, c(universe, "SPY"), "2026-06-01", cap_spy = FALSE),
                   error = function(e) NULL)
    if (is.null(px) || !nrow(px) || !any(toupper(px$ticker) == "SPY")) return(NULL)
    px$d <- as.Date(px$d); px$px <- as.numeric(px$px)
    sp <- px[toupper(px$ticker) == "SPY", c("d", "px")]
    last_d <- suppressWarnings(max(sp$d))
    pmap <- split(px[, c("d", "px")], toupper(px$ticker))
    pp <- tryCatch(lc_pos(), error = function(e) NULL)
    sold_map <- if (!is.null(pp) && nrow(pp))
      setNames(suppressWarnings(as.Date(as.character(pp$sold_date))), toupper(pp$ticker)) else NULL
    meta_dl <- if (!is.null(d$meta) && "delisted_date" %in% names(d$meta))
      setNames(suppressWarnings(as.Date(as.character(d$meta$delisted_date))), toupper(d$meta$ticker)) else NULL
    epoch <- as.Date(LEDGER_EPOCH)
    rows <- list(); brows <- list()
    for (tk in universe) {
      tp <- pmap[[tk]]; if (is.null(tp) || !nrow(tp)) next
      prov <- !is.null(dv$prov_of) && tk %in% names(dv$prov_of) && isTRUE(dv$prov_of[[tk]])
      ns   <- if (tk %in% names(dv$state_now)) dv$state_now[[tk]] else NA_character_
      dl   <- if (!is.null(meta_dl) && tk %in% names(meta_dl)) meta_dl[[tk]] else as.Date(NA)
      trail <- tryCatch(lc_board_trail(dv$M, dv$dates, tk, d$led, LEDGER_EPOCH,
                                       prov, ns, dv$hz, dl, rec = d$rec_states), error = function(e) NULL)
      so  <- if (!is.null(sold_map) && tk %in% names(sold_map)) sold_map[[tk]] else as.Date(NA)
      eps <- lc_trade_episodes(trail, so, last_d)
      eps <- Filter(function(e) !is.na(e$entry) && e$entry >= epoch, eps)
      rr  <- lc_price_episodes(eps, tp, sp)
      if (!is.null(rr) && nrow(rr)) { rr$ticker <- tk; rows[[length(rows) + 1]] <- rr }
      # buy-RUN windows (buy->hold / buy->sell / buy->now), each vs SPY same dates
      beps <- lc_buyrun_episodes(trail, so, last_d)
      beps <- Filter(function(e) !is.na(e$entry) && e$entry >= epoch, beps)
      if (length(beps)) {
        br <- lc_price_episodes(beps, tp, sp)
        if (!is.null(br) && nrow(br)) {
          br$kind <- vapply(beps, function(e) e$kind, ""); br$ticker <- tk
          brows[[length(brows) + 1]] <- br }
      }
    }
    if (!length(rows)) return(NULL)
    A <- do.call(rbind, rows)
    B <- if (length(brows)) do.call(rbind, brows) else NULL
    # Whole-board record; the strip re-aggregates a cluster-filtered slice via
    # lc_track_agg on A/B, so this reactive stays independent of the cluster filter.
    c(lc_track_agg(A, B), list(asof = last_d, table = A, wtable = B))
  })

  # Tracking basis for MODEL rows (radio next to the chart filter): "epoch" =
  # the strategy's record since the regime epoch (default); "stored" = only the
  # user's own cash flows since each row was added. Threaded into every
  # gated_expand_schedule call so table, chart, note and sells stay consistent.
  lc_track_basis <- reactive({
    # default = "stored" (my money since I added): since 2026-08-22 the book is
    # Kevin's real positions, so the personal basis is what the table opens on;
    # the epoch basis stays one click away for judging the strategy itself.
    if (identical(input$lcTrackBasis, "epoch")) "epoch" else "stored"
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
  # signature so re-Generates with the same triggers are free.
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

  # Positions AS DISPLAYED: honors the "Show qualstream + picks only" checkbox
  # (default on) so the table, the headline count and the money curve/chart all
  # filter to qualstream passers (grade >= QS_MIN) together, and show every
  # position when it is unticked. DISPLAY ONLY - the seed, adopt, remove/hold/
  # sell and persistence paths keep reading lc_pos() (the full book). Guarded on
  # a loaded grade set so a grade-expiry cliff never blanks the view.
  lc_pos_disp <- reactive({
    # The personal book is NEVER filtered: every row is Kevin's real money
    # (rebase 2026-08-22). The adopt queue gates at grade >= QS_MIN
    # (policy 2026-08-23), but deliberately KEPT below-bar holdings (CVNA/HWM
    # at 62) live here too - two rounds of "why is my position missing"
    # (2026-08-22/23) both traced to a display filter on this book; the
    # qualstream checkbox governs the BOARD columns only.
    lc_pos()
  })

  lc_port_series <- reactive({
    p <- lc_pos_disp()
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
               spy_value = as.numeric(last$spy_value),
               lump_ret = if ("lump_ret_pct" %in% names(last))
                 as.numeric(last$lump_ret_pct) else rep(NA_real_, nrow(last)),
               peak_vs = if ("peak_vs" %in% names(last))
                 as.numeric(last$peak_vs) else rep(NA_real_, nrow(last)),
               trough_vs = if ("trough_vs" %in% names(last))
                 as.numeric(last$trough_vs) else rep(NA_real_, nrow(last)),
               stringsAsFactors = FALSE)
  })

  # Per-ticker cumulative-return series for the holdings chart. Same gated buy
  # expansion as lc_row_perf, run through LC_PORTFOLIO_TICKER_SQL: tidy
  # (d, ticker, ret_pct) with a '__SPY__' benchmark line.
  lc_ticker_series <- reactive({
    p <- lc_pos_disp(); if (is.null(p) || !nrow(p)) return(NULL)
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
    praw <- lc_pos()
    if (is.null(praw) || nrow(praw) == 0)
      return(DT::datatable(
        data.frame(Note = "No positions yet - Generate to auto-load the qualstream buys, or adopt a model BUY / add a manual plan above."),
        rownames = FALSE, selection = "none", options = list(dom = "t", ordering = FALSE)))
    p <- lc_pos_disp()   # = the full book; the display filter was removed 2026-08-23
    dv  <- tryCatch(derivedLC(), error = function(e) NULL)
    d   <- app_dataLC()
    perf <- tryCatch(lc_row_perf(), error = function(e) NULL)
    id_of <- if (!is.null(d) && !is.null(d$gate$id))
      setNames(suppressWarnings(as.integer(d$gate$id)), toupper(d$gate$ticker)) else integer(0)
    # State: money-rule or raw gate (toggle via lcStateView) + manual positions
    # show "-" (not adopted). The master list uses the same state so buy/hold/sell
    # is identical in both tables and independent of horizon selector (retired
    # 2026-08-26). Book overrides (paused -> hold, sold -> sell) still win.
    st <- lc_state_for_display(dv, d, p)
    states <- vapply(seq_len(nrow(p)), function(i) {
      if (!identical(p$mode[i], "model")) return("-")
      tk <- toupper(p$ticker[i])
      if (tk %in% names(st)) st[[tk]] else ""
    }, character(1))
    pcol <- function(col) if (is.null(perf) || !nrow(perf)) rep(NA_real_, nrow(p)) else
      as.numeric(perf[[col]])[match(p$id, perf$id)]
    invested <- pcol("invested"); value <- pcol("value"); spyv <- pcol("spy_value")
    # partial sales: an active row with sold_fraction in (0,1) runs on the
    # REMAINING stake (the sold slices sit in the realized archive; "Banked $"
    # shows their proceeds). Percent columns are scale-invariant and unchanged.
    sfr  <- suppressWarnings(as.numeric(p$sold_fraction))
    sold <- { sd <- as.character(p$sold_date); !is.na(sd) & nzchar(sd) }
    part <- !is.na(sfr) & sfr > 0 & sfr < 1 & !sold
    remf <- ifelse(part, 1 - sfr, 1)
    invested <- invested * remf; value <- value * remf; spyv <- spyv * remf
    ret  <- 100 * (value / invested - 1)
    spyr <- 100 * (spyv  / invested - 1)
    pnl  <- value - invested
    vspp <- ret - spyr
    # vs HODL: this row's (possibly staged) buys against the whole stake lumped
    # in at the first fill - the entry-policy audit's lump benchmark. One-off
    # rows read ~0.0pp by construction (they ARE the lump).
    lmp   <- pcol("lump_ret")
    vslmp <- ret - lmp
    # Model %: the open model-trade's own price move since ITS entry (the same
    # number the board chip shows). Persists through a buy->hold flip, so a
    # held row still answers "how has it done since the model bought it".
    mret <- {
      out <- rep(NA_real_, nrow(p))
      tt <- tryCatch(lc_track_record()$table, error = function(e) NULL)
      if (!is.null(tt) && nrow(tt)) {
        op <- tt[tt$open %in% TRUE & !is.na(tt$ret), , drop = FALSE]
        if (nrow(op)) {
          keep <- !duplicated(toupper(op$ticker), fromLast = TRUE)
          m <- setNames(op$ret[keep], toupper(op$ticker)[keep])
          out <- unname(m[toupper(p$ticker)])
        }
      }
      out
    }
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
      # today's capped adopt queue (dv$qs_buys): which holdings a fresh dollar
      # would still pick. Entry gate only - dropping off the queue is NOT an
      # exit signal (exits = model sell/washout), so unmarked buys stay held.
      Queue    = ifelse(tick %in% toupper(if (!is.null(dv)) dv$qs_buys else character(0)),
                        "✓", ""),
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
      `vs HODL` = spp(vslmp),
      `Model %` = spct(mret),
      # daily-exact extremes of the vs-SPY edge since first fill (lump basis).
      # Calibration (226 picks, 12mo windows): peak excess median +25pp,
      # p75 +47pp, p90 +84pp; trough median -13pp, p25 -32pp. Context column -
      # the Recoup rule is the act signal.
      `Peak/Low vsSPY` = {
        pk <- pcol("peak_vs"); tr <- pcol("trough_vs")
        ifelse(is.na(pk) | is.na(tr), "-",
               sprintf("%+.0f / %+.0f pp", pk, tr))
      },
      # never-lose-money rule: progress toward the recoup threshold; once hit,
      # the cell says the sale that returns the full stake (sell 1/(1+r) of the
      # position). Audit-backed default +100% (below that, trimming costs edge).
      Recoup   = {
        thr <- suppressWarnings(as.numeric(input$lcRecoupThr))
        if (length(thr) != 1 || !is.finite(thr) || thr < 10) thr <- 100
        out <- ifelse(is.na(ret), "-",
          ifelse(ret >= thr,
            sprintf("SELL %d%% → stake back", round(100 / (1 + ret / 100))),
            sprintf("%d%% of +%d%%", pmax(0, round(ret)), round(thr))))
        out[part] <- sprintf("banked %d%% ✓", round(100 * sfr[part]))
        out
      },
      # proceeds of this row's archived partial-sale slices (realized money).
      # Slices archive under suffixed ids (<id>~p1, ~p2 - the same-day
      # trim+archive PK collision fix, 2026-08-23), so sum by the base id.
      Banked   = {
        cl <- lc_closed()
        b <- rep(NA_real_, nrow(p))
        if (!is.null(cl) && nrow(cl) && "id" %in% names(cl)) {
          base <- sub("~p[0-9]+$", "", as.character(cl$id))
          agg <- tapply(as.numeric(cl$value_usd), base, sum, na.rm = TRUE)
          b <- as.numeric(agg[as.character(p$id)])
        }
        ifelse(is.finite(b) & b > 0, sprintf("$%.2f", b), "-")
      },
      pnl_num  = ifelse(is.na(pnl), 0, pnl),   # hidden: drives P&L color
      rec_hit  = {
        thr <- suppressWarnings(as.numeric(input$lcRecoupThr))
        if (length(thr) != 1 || !is.finite(thr) || thr < 10) thr <- 100
        as.integer(!is.na(ret) & ret >= thr & !part)   # hidden: drives Recoup color
      },
      check.names = FALSE, stringsAsFactors = FALSE)
    editcol <- which(names(show) == "$/buy") - 1L   # 0-based; only this col editable
    pnlcol  <- which(names(show) == "pnl_num") - 1L
    reccol  <- which(names(show) == "rec_hit") - 1L
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
      options = list(dom = "t", ordering = TRUE, pageLength = 50, scrollX = TRUE,
        autoWidth = FALSE, order = list(list(1, "asc")),
        columnDefs = list(
          list(targets = 0, orderable = FALSE, className = "dt-center pf-sel"),
          list(targets = pnlcol, visible = FALSE),
          list(targets = reccol, visible = FALSE),
          list(targets = 1, className = "dt-center", render = DT::JS(
            "function(data,type,row){if(type==='sort'||type==='type'){var n=parseInt(data,10);return isNaN(n)?9999:n;}return data;}"))))) %>%
      DT::formatStyle("State", fontWeight = "600",
        color = DT::styleEqual(c("buy", "hold", "sell", "closed"),
                               c("#10b981", "#eab308", "#dc2626", "#64748b"))) %>%
      # profit flag: P&L cell green in profit, red in loss (0/NA stays neutral)
      DT::formatStyle("P&L $", valueColumns = "pnl_num", fontWeight = "600",
        color = DT::styleInterval(c(-1e-9, 1e-9), c("#f87171", "#94a3b8", "#34d399"))) %>%
      # recoup trigger: gold + bold once the threshold is crossed
      DT::formatStyle("Recoup", valueColumns = "rec_hit", fontWeight = "600",
        color = DT::styleEqual(c(0L, 1L), c("#64748b", "#fbbf24")))
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
    # history accumulates across book rebases and archived names - show only
    # tickers in the CURRENT book, and under the "my money" basis clip each
    # trail to that position's own start date so the pre-entry (or pre-rebase
    # simulated) era doesn't open the trail (audit 2026-08-23)
    if (!is.null(h) && nrow(h)) {
      h <- h[toupper(h$ticker) %in% toupper(as.character(p$ticker)), , drop = FALSE]
      if (identical(lc_track_basis(), "stored") && nrow(h)) {
        st0 <- tapply(as.character(p$start_date), toupper(as.character(p$ticker)),
                      function(x) suppressWarnings(min(as.Date(x), na.rm = TRUE)))
        lo <- as.numeric(st0[toupper(h$ticker)])
        lo[!is.finite(lo)] <- NA_real_
        keep <- is.na(lo) | as.Date(h$as_of) >= as.Date(lo, origin = "1970-01-01")
        h <- h[keep, , drop = FALSE]
      }
    }
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

  # Reset every tab's viz back to its default (blank) state when the Environment
  # (database) is switched, so a chart from the previous DB never lingers until
  # the next Generate. Companion to setup_env_switcher (which reseeds the connect
  # fields). ignoreInit: fire only on a real switch, not the initial page load.
  observeEvent(input$db_env, {
    app_dataT(NULL);         status_msgT("Ready")
    app_dataV(NULL);         status_msgV("Ready")
    app_dataK(NULL);         status_msgK("Not connected.")
    app_dataP(NULL);         status_msgP("Ready")
    app_dataRS_slot(NULL);   app_dataRS_heatmap(NULL); app_dataRS_meta(NULL)
    app_dataRS_allIds(NULL); status_msgRS("Ready")
    app_dataMV_ic(NULL);     app_dataMV_payoff(NULL);  app_dataMV_tiers(NULL)
    app_dataMV_forest(NULL); status_msgMV("Ready")
    app_dataBL(NULL);        chart_rowsBL(0);          status_msgBL("Ready")
    app_dataFC(NULL);        status_msgFC("Ready")
    app_dataLC(NULL);        lc_chart_ids_sel(NULL);   status_msgLC("Ready")
  }, ignoreInit = TRUE)

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
    basis_now <- isolate(if (identical(input$lcTrackBasis, "epoch")) "epoch" else "stored")
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
  # ---- Sketch window slider (Kevin 2026-10-14) --------------------------------
  # Two-handle date slider above the sketch: drag either end and the sketch runs
  # from the left handle, every line at 0% there (lc_window_sketch), to the right
  # one. Both handles at the ends = today's full 120-day chart, untouched
  # (lc_sketch_data() is then app_dataLC() itself). An end left at its edge is
  # kept as NA, so it follows the rolling range when the data reloads.
  lc_sketch_win    <- reactiveVal(NULL)
  lc_sketch_bounds <- reactiveVal(NULL)
  observe({
    d  <- app_dataLC()
    dd <- if (!is.null(d) && !is.null(d$qscmp) && nrow(d$qscmp)) as.Date(d$qscmp$d) else NULL
    b  <- if (length(dd) && any(!is.na(dd))) range(dd, na.rm = TRUE) else NULL
    if (!identical(b, isolate(lc_sketch_bounds()))) lc_sketch_bounds(b)
  })
  observeEvent(input$lcSketchWin, {
    b <- lc_sketch_bounds(); w <- suppressWarnings(as.Date(input$lcSketchWin))
    if (is.null(b) || length(w) != 2 || anyNA(w)) return()
    v <- c(if (w[1] <= b[1]) as.Date(NA) else w[1], if (w[2] >= b[2]) as.Date(NA) else w[2])
    lc_sketch_win(if (all(is.na(v))) NULL else v)
  })
  observeEvent(input$lcSketchWinReset, {
    lc_sketch_win(NULL)
    b <- lc_sketch_bounds()
    if (!is.null(b)) updateSliderInput(session, "lcSketchWin", value = b)
  })
  output$lcSketchWinUI <- renderUI({
    b <- lc_sketch_bounds(); req(b)
    w <- isolate(lc_sketch_win())
    val <- if (is.null(w)) b else c(if (is.na(w[1])) b[1] else max(w[1], b[1]),
                                    if (is.na(w[2])) b[2] else min(w[2], b[2]))
    # side margins match the sketch's plot area (layout margin l = 60, r = 120)
    div(style = "margin:0.4rem 120px 0 60px;",
      sliderInput("lcSketchWin", NULL, min = b[1], max = b[2], value = val, step = 1,
                  timeFormat = "%b %d", ticks = FALSE, width = "100%"),
      div(style = "color:#64748b; font-size:0.7rem; margin-top:-0.7rem;",
        "Drag either end of the window. Every line restarts at 0% on the start date. ",
        actionLink("lcSketchWinReset", "Full 120 days", style = "color:#94a3b8;")))
  })
  # what the sketch and its lines/pins read: app_dataLC() re-anchored to the window
  lc_sketch_data <- reactive({
    d <- app_dataLC(); w <- lc_sketch_win()
    if (is.null(d) || is.null(w) || is.null(d$qscmp) || !nrow(d$qscmp)) return(d)
    out <- lc_window_sketch(d, w)
    if (is.null(out)) { d$qscmp_short <- TRUE; return(d) }
    out
  })

