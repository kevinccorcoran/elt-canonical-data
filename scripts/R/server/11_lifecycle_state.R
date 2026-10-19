# AlphaStream dashboard - Lifecycle: session state, auto-start. Part of app.R, split 2026-10-19: files load in name order (see app.R).
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
  # master-list checkboxes -> the set of tickers to draw as their own lines on the
  # sketch (persisted across re-renders/hourly refresh so a checked name stays on).
  lc_sketch_ticks <- reactiveVal(character(0))
  # Unticking the last row sends an empty list, which arrives as NULL, so NULL
  # must clear the lines (Kevin 2026-10-19: with every row unticked the last
  # name's white line stayed on). At startup NULL just sets the empty default.
  observeEvent(input$lcSketchChecked, {
    v <- unlist(input$lcSketchChecked)
    lc_sketch_ticks(if (is.null(v)) character(0) else toupper(as.character(v)))
  }, ignoreNULL = FALSE)
  lcAutoTick   <- reactiveVal(0)
  # Generate trigger bookkeeping for the cross-session memo: a click on the
  # button forces a reload; the auto-start and hourly ticks may reuse a load
  lc_gen <- new.env(parent = emptyenv()); lc_gen$exec_seen <- 0
  # Auto-start (2026-08-22): opening the app should need zero clicks - land on
  # Lifecycle, connect to the default environment (Production) with the
  # container-held credential, and Generate. The db_env init observer prefills
  # db_pass from the env credential; the first time that lands non-empty (and
  # the env is still the default) we switch tabs and bump lcAutoTick, which is
  # exactly the auto-refresh timer's path into the Generate observer. Fires
  # once per browser session; without a resolvable credential it stays quiet.
  # Latches only on SUCCESS (lc_backend flips to "db"): the old version
  # latched before Generate ran, so one DB blip at boot silently left the
  # whole session on the parquet fallback with no retry (audit 2026-08-23).
  # Retries every 15s, caps at 20 tries; only the first try switches tabs so
  # a failing DB can't keep yanking the user back to Lifecycle.
  .lc_auto_started <- reactiveVal(FALSE)
  .lc_auto_tries   <- reactiveVal(0)
  observe({
    if (isTRUE(.lc_auto_started())) return()
    if (identical(lc_backend(), "db")) { .lc_auto_started(TRUE); return() }
    invalidateLater(15000, session)
    if (is.null(input$db_pass) || !nzchar(input$db_pass)) return()
    if (!identical(input$db_env, DB_ENV_DEFAULT)) return()
    tries <- isolate(.lc_auto_tries())
    if (tries >= 20) { .lc_auto_started(TRUE); return() }
    .lc_auto_tries(tries + 1)
    if (tries == 0)
      updateNavbarPage(session, "mainNav", selected = "Lifecycle")
    lcAutoTick(isolate(lcAutoTick()) + 1)
  })
  observeEvent(input$idsLC, {
    lc_board_ids_sel(if (is.null(input$idsLC)) character(0)
                     else as.character(input$idsLC))
  }, ignoreNULL = FALSE, ignoreInit = TRUE)
  output$statusMessageLC <- renderText({ status_msgLC() })

