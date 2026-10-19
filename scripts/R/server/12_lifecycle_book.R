# AlphaStream dashboard - Lifecycle: the book (positions, adopt, sell). Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # ── Personal portfolio: strategy follower (model DCA + ladder sells) ──────
  .init_pf  <- load_portfolio()
  .init_dis <- load_dismissed()
  lc_pos <- reactiveVal(.init_pf)
  lc_dismissed <- reactiveVal(.init_dis)
  lc_backend <- reactiveVal("parquet")   # flips to "db" once a portfolio-schema DB is seen
  # Ids/tickers this session has actually SEEN. Saves delete only from this set,
  # so rows the portfolio_sync DAG adds mid-session are never clobbered by a
  # whole-frame save. Plain env on purpose - no reactivity wanted.
  .lc_known <- new.env(parent = emptyenv())
  .lc_known$ids <- if (!is.null(.init_pf) && nrow(.init_pf)) as.character(.init_pf$id) else character(0)
  .lc_known$dis <- .init_dis

  # Route a positions/dismissed save to the DB (authoritative, shared) when the
  # connected DB has the portfolio schema, else to the local parquet files. The
  # observers below compute a full merged df and call these, unchanged otherwise.
  persist_positions <- function(df) {
    ids <- if (!is.null(df) && nrow(df)) as.character(df$id) else character(0)
    if (!identical(lc_backend(), "db")) {
      save_portfolio(df); .lc_known$ids <- ids; return(invisible(TRUE)) }
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) {
      showNotification("DB unreachable - change not saved.", type = "error"); return(invisible(FALSE)) }
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    tryCatch({
      # Resurrection guard (audit 2026-08-23): a row this session loaded from
      # the DB (.lc_known$ids) that is now GONE from the DB was archived by
      # the daily sync mid-session - re-upserting it would bring it back as a
      # live zombie. Skip those rows (they vanish from the view on the next
      # Generate); keep them in .lc_known$ids so every later save keeps
      # skipping them. Rows new to the session (not in known) are real
      # inserts and pass through untouched.
      db_ids <- tryCatch(as.character(
        DBI::dbGetQuery(con, "SELECT id FROM portfolio.positions")$id),
        error = function(e) NULL)
      gone <- if (is.null(db_ids)) character(0)
              else setdiff(intersect(.lc_known$ids, ids), db_ids)
      df_save <- if (length(gone) && !is.null(df) && nrow(df))
        df[!(as.character(df$id) %in% gone), , drop = FALSE] else df
      if (length(gone))
        showNotification(sprintf(
          "%s archived by the daily sync - not re-saved; Generate to refresh.",
          paste(unique(toupper(df$ticker[as.character(df$id) %in% gone])),
                collapse = ", ")), type = "warning")
      db_save_positions(con, df_save, drop_ids = setdiff(.lc_known$ids, ids))
      .lc_known$ids <- union(ids, gone)
      invisible(TRUE) },
      error = function(e) {
        showNotification(paste("Save failed:", e$message), type = "error"); invisible(FALSE) })
  }
  persist_dismissed <- function(v) {
    v <- unique(toupper(v[nzchar(v)]))
    if (!identical(lc_backend(), "db")) {
      save_dismissed(v); .lc_known$dis <- v; return(invisible(TRUE)) }
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) return(invisible(FALSE))
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    tryCatch({
      db_save_dismissed(con, v, drop = setdiff(.lc_known$dis, v))
      .lc_known$dis <- v
      invisible(TRUE) }, error = function(e) invisible(FALSE))
  }
  # Adopting a name resets its re-entry announcement state: without this, a
  # manual Adopt followed by a hand-DELETE (no archive) would leave the
  # (ticker, episode) dedup row in portfolio.reentry_pinged and the next
  # re-fire of that same episode would stay silent forever. DB backend only
  # (the table is sync-owned); missing table / connection errors are ignored.
  pf_clear_reentry <- function(tk) {
    if (!identical(lc_backend(), "db")) return(invisible(FALSE))
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) return(invisible(FALSE))
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    tryCatch({
      DBI::dbExecute(con,
        "DELETE FROM portfolio.reentry_pinged WHERE upper(ticker) = $1",
        params = list(toupper(tk)))
      invisible(TRUE) }, error = function(e) invisible(FALSE))
  }

  # Adoption is portfolio_sync's job now: the daily DAG adopts every queue name
  # at the auto_adopt_amount setting and pings Telegram. The dashboard-side
  # auto-seed observer that used to live here double-inserted across sessions
  # (its dedup ran against the session snapshot, not the DB - 9 doubled tickers
  # on prod by 2026-08-21) and fired on every backend, which is also what wrote
  # the "mystery" dev adopt batches of Aug 14-20. Removed 2026-08-21; manual
  # Add/Adopt/Sell/Archive remain the dashboard's write surface.

  # Transition log: the user's own moves (paused / sold by user) go to
  # portfolio.state_history, upserted per (ticker, as_of, hz). The engine's daily
  # state is recorded by the pipeline (portfolio_sync for tracked names,
  # monitoring.board_daily for every name); this observer used to write it too,
  # overwriting the sync's row whenever the dashboard was open (2026-10-19).
  # DB-backend only (needs the schema).
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
    # user-paused/sold rows record as hold/sell (matches the table's display
    # override), so the trail shows the user's moves even if the board says buy
    held <- { e <- as.character(p$end_date); sd <- as.character(p$sold_date)
              !is.na(e) & nzchar(e) & (is.na(sd) | !nzchar(sd)) }
    heldtk <- toupper(p$ticker[held])
    ih <- rows$ticker %in% heldtk
    rows$state[ih] <- "hold"; rows$why[ih] <- "paused by user"
    sold <- { sd <- as.character(p$sold_date); !is.na(sd) & nzchar(sd) }
    soldtk <- toupper(p$ticker[sold])
    is2 <- rows$ticker %in% soldtk
    rows$state[is2] <- "sell"; rows$why[is2] <- "sold by user"
    rows <- rows[ih | is2, , drop = FALSE]                                # user moves only
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
               # one-shot is the policy default: lump-at-entry measured +3.9pp
               # over monthly installments on the proven cohort record
               choices = c("One-off" = "once", "Monthly" = "monthly"), selected = "once")),
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
    pf_clear_reentry(tk)   # fresh episode: reset the re-entry announcement dedup
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
  # Hold = pause accumulation: stamp end_date so no further buys fire, while the
  # stake already bought keeps being valued in the table and chart. Resume clears
  # the stamp and buying continues per cadence (and the model gate, for model rows).
  observeEvent(input$lcPosHold, {
    sel <- input$lcPosChecked; cur <- lc_pos()
    if (is.null(cur) || !nrow(cur)) return()
    sel <- suppressWarnings(as.integer(sel))
    sel <- sel[!is.na(sel) & sel >= 1 & sel <= nrow(cur)]
    if (!length(sel)) { showNotification("Check one or more rows to move to hold.", type = "message"); return() }
    cur$end_date[sel] <- format(Sys.Date())
    persist_positions(cur); lc_pos(cur)
    showNotification(sprintf("%s moved to hold - buying paused, position keeps valuing.",
                             paste(toupper(cur$ticker[sel]), collapse = ", ")), type = "message")
  })
  # Sell = record a full exit by hand: stamp sold_date + sold_fraction (and
  # end_date, a sold position never keeps buying). Displays as sell everywhere;
  # Resume clears every stamp and the row rejoins the model's flow.
  observeEvent(input$lcPosSell, {
    sel <- input$lcPosChecked; cur <- lc_pos()
    if (is.null(cur) || !nrow(cur)) return()
    sel <- suppressWarnings(as.integer(sel))
    sel <- sel[!is.na(sel) & sel >= 1 & sel <= nrow(cur)]
    if (!length(sel)) { showNotification("Check one or more rows to sell.", type = "message"); return() }
    cur$sold_date[sel] <- format(Sys.Date())
    # sold_fraction in (0,1) records earlier PARTIAL sales (their slices are
    # already archived) - the final exit realizes only the remaining stake, so
    # keep that history; a plain 1 means the whole stake sold at once.
    sfS <- suppressWarnings(as.numeric(cur$sold_fraction[sel]))
    cur$sold_fraction[sel] <- ifelse(!is.na(sfS) & sfS > 0 & sfS < 1, sfS, 1)
    cur$end_date[sel] <- format(Sys.Date())
    persist_positions(cur); lc_pos(cur)
    showNotification(sprintf("%s marked sold - buying stopped, exit recorded.",
                             paste(toupper(cur$ticker[sel]), collapse = ", ")), type = "message")
  })
  # Sell part = record a hand TRIM (the recoup-ping action): archive the sold
  # slice into closed_positions at today's numbers and stamp the cumulative
  # sold_fraction; the row stays active on the remaining stake, so the table
  # keeps telling the truth after a stake-back sale. DB backend only (slices
  # live in the realized archive). One slice per position per day - the
  # (id, sold_date) key - so a second same-day trim is refused, not merged.
  observeEvent(input$lcPosSellPart, {
    if (!identical(lc_backend(), "db")) {
      showNotification("Sell part needs the DB portfolio store (connect to a DB with the portfolio schema).",
                       type = "warning"); return()
    }
    pctv <- suppressWarnings(as.numeric(input$lcPartPct))
    if (length(pctv) != 1 || !is.finite(pctv) || pctv < 1 || pctv > 95) {
      showNotification("Sell part needs a percent between 1 and 95 (selling 100% = use Sell selected).",
                       type = "warning"); return()
    }
    sel <- suppressWarnings(as.integer(input$lcPosChecked)); cur <- lc_pos()
    if (is.null(cur) || !nrow(cur)) return()
    sel <- sel[!is.na(sel) & sel >= 1 & sel <= nrow(cur)]
    if (!length(sel)) { showNotification("Check one or more rows to trim.", type = "message"); return() }
    sd0 <- as.character(cur$sold_date[sel])
    fully <- !(is.na(sd0) | !nzchar(sd0))
    if (any(fully)) {
      showNotification(sprintf("Already fully sold - not trimmed: %s",
        paste(toupper(cur$ticker[sel][fully]), collapse = ", ")), type = "warning")
      sel <- sel[!fully]
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
    unpriced <- which(is.na(val) | is.na(inv) | inv <= 0)
    if (length(unpriced)) {
      showNotification(sprintf("No priced fills yet - not trimmed: %s",
        paste(toupper(cur$ticker[sel][unpriced]), collapse = ", ")), type = "warning")
      keep <- setdiff(seq_along(sel), unpriced)
      if (!length(keep)) return()
      sel <- sel[keep]; ids <- ids[keep]
      inv <- inv[keep]; val <- val[keep]; spyv <- spyv[keep]; lmp <- lmp[keep]
    }
    con <- tryCatch(get_con(input), error = function(e) NULL)
    if (is.null(con)) { showNotification("No DB connection - connect first.", type = "error"); return() }
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    inlist <- paste(sprintf("'%s'", gsub("'", "''", ids, fixed = TRUE)), collapse = ",")
    # slices archive under suffixed ids (<id>~p1, ~p2 ...) so match on the base
    already <- tryCatch(as.character(DBI::dbGetQuery(con, sprintf(
      "SELECT regexp_replace(id, '~p[0-9]+$', '') AS id
       FROM portfolio.closed_positions
       WHERE sold_date = CURRENT_DATE
         AND regexp_replace(id, '~p[0-9]+$', '') IN (%s)",
      inlist))$id), error = function(e) character(0))
    if (length(already)) {
      hit <- ids %in% already
      showNotification(sprintf("Already trimmed today - not trimmed again: %s",
        paste(toupper(cur$ticker[sel][hit]), collapse = ", ")), type = "warning")
      keep <- which(!hit)
      if (!length(keep)) return()
      sel <- sel[keep]; ids <- ids[keep]
      inv <- inv[keep]; val <- val[keep]; spyv <- spyv[keep]; lmp <- lmp[keep]
    }
    sf0 <- suppressWarnings(as.numeric(cur$sold_fraction[sel]))
    sf0[is.na(sf0) | sf0 < 0 | sf0 >= 1] <- 0
    slice <- (1 - sf0) * (pctv / 100)   # fraction of the ORIGINAL stake sold now
    ret  <- 100 * (val / inv - 1); spyr <- 100 * (spyv / inv - 1)
    since <- if (!is.null(perf) && "since" %in% names(perf))
      as.character(perf$since)[match(ids, perf$id)] else rep(NA_character_, length(ids))
    # Suffixed slice ids: the closed_positions PK is (id, sold_date), so a
    # bare-id slice would COLLIDE with a same-day full archive and the final
    # exit's money silently vanished under ON CONFLICT DO NOTHING (audit
    # 2026-08-23). ~p<n> continues from the archived slice count per base id.
    nprev <- tryCatch({
      sc <- DBI::dbGetQuery(con, sprintf(
        "SELECT regexp_replace(id, '~p[0-9]+$', '') AS base, COUNT(*) AS n
         FROM portfolio.closed_positions
         WHERE id ~ '~p[0-9]+$'
           AND regexp_replace(id, '~p[0-9]+$', '') IN (%s)
         GROUP BY 1", inlist))
      setNames(as.integer(sc$n), as.character(sc$base))
    }, error = function(e) integer(0))
    kslice <- ifelse(is.na(nprev[ids]), 0L, nprev[ids]) + 1L
    rows <- data.frame(
      id = sprintf("%s~p%d", ids, kslice), ticker = toupper(cur$ticker[sel]),
      cluster_id = unname(id_of[toupper(cur$ticker[sel])]),
      cadence = as.character(cur$cadence[sel]),
      amount_usd = as.numeric(cur$amount_usd[sel]),
      invested_usd = inv * slice, value_usd = val * slice,
      pnl_usd = (val - inv) * slice,
      ret_pct = ret, spy_ret_pct = spyr, vs_spy_pp = ret - spyr,
      vs_lump_pp = ret - lmp,
      entry_date = ifelse(is.na(since), as.character(cur$start_date[sel]), since),
      sold_date = format(Sys.Date()),
      outcome = ifelse((val - inv) * slice > 0, "profit", "loss"),
      stringsAsFactors = FALSE)
    ok <- tryCatch({ db_archive_positions(con, rows); TRUE },
                   error = function(e) { showNotification(paste("Trim failed:",
                     conditionMessage(e)), type = "error"); FALSE })
    if (!ok) return()
    cur$sold_fraction[sel] <- sf0 + slice
    persist_positions(cur); lc_pos(cur)
    lc_closed(db_load_closed(con))
    showNotification(sprintf("Partial sale recorded: %s - %.0f%% of the current stake (≈%s banked).",
      paste(toupper(cur$ticker[sel]), collapse = ", "), pctv,
      paste(sprintf("$%.2f", val * slice), collapse = ", ")), type = "message")
  })
  observeEvent(input$lcPosResume, {
    sel <- input$lcPosChecked; cur <- lc_pos()
    if (is.null(cur) || !nrow(cur)) return()
    sel <- suppressWarnings(as.integer(sel))
    sel <- sel[!is.na(sel) & sel >= 1 & sel <= nrow(cur)]
    if (!length(sel)) { showNotification("Check one or more rows to resume.", type = "message"); return() }
    cur$end_date[sel] <- ""
    cur$sold_date[sel] <- ""
    # Recurring rows KEEP partial-sale history: their stake spans episodes and
    # wiping the fraction would double-count already-archived slices.
    sfR <- suppressWarnings(as.numeric(cur$sold_fraction[sel]))
    cur$sold_fraction[sel] <- ifelse(!is.na(sfR) & sfR > 0 & sfR < 1, sfR, NA_real_)
    # one-shot resume DEFERS FORWARD: re-stamp start_date to today so the single
    # buy prices at the re-commit date, not a backfilled sat-out price. Resume
    # means "I'm in now" - backdating a fill the user never made would fabricate
    # a hindsight-selected entry. The epoch/strategy basis ignores stored start
    # (tracks from the regime epoch regardless), so only the "since I added"
    # personal view changes - and it becomes truthful. Recurring cadences keep
    # their stored start; their next scheduled buy resumes per cadence + gate.
    once <- sel[as.character(cur$cadence[sel]) == "once"]
    if (length(once)) {
      cur$start_date[once] <- format(Sys.Date())
      # a resumed one-off is a FRESH full-amount entry: the banked slices
      # belong to the closed episode (still in the archive), so carrying the
      # fraction forward under-displayed the new stake (audit 2026-08-23)
      cur$sold_fraction[once] <- NA_real_
    }
    persist_positions(cur); lc_pos(cur)
    showNotification(sprintf("%s resumed - buying continues per cadence/model.",
                             paste(toupper(cur$ticker[sel]), collapse = ", ")), type = "message")
  })
  # Clear all = reset to defaults: wipe positions AND dismissals. The wipe is
  # DB-wide, not session-wide: rows the sync DAG adopted after this session
  # connected are not in .lc_known, and without the union below they would
  # survive the Clear and reappear on the next Generate (audit 2026-08-23).
  observeEvent(input$lcPosClear, {
    if (identical(lc_backend(), "db")) {
      con <- tryCatch(get_con(input), error = function(e) NULL)
      if (!is.null(con)) {
        on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
        db_ids <- tryCatch(as.character(
          DBI::dbGetQuery(con, "SELECT id FROM portfolio.positions")$id),
          error = function(e) character(0))
        db_dis <- db_load_dismissed(con)
        .lc_known$ids <- union(.lc_known$ids, db_ids)
        .lc_known$dis <- union(.lc_known$dis, db_dis)
      }
    }
    persist_positions(portfolio_empty()); lc_pos(portfolio_empty())
    persist_dismissed(character(0)); lc_dismissed(character(0))
  })

