# AlphaStream dashboard - Lifecycle: Connect, hourly refresh, Generate. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgLC("Error: Password is not set."); return() }
    lc_board_ids_sel(NULL)   # new environment -> new universe -> reset the id filter
    status_msgLC("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      b <- dbGetQuery(con, sprintf("
        SELECT COUNT(*) AS n, COUNT(DISTINCT prediction_date) AS d,
               MIN(prediction_date) AS lo, MAX(prediction_date) AS hi,
               COUNT(DISTINCT prediction_date)
                 FILTER (WHERE prediction_date >= DATE '%s') AS d_epoch
        FROM monitoring.prediction_ledger", LEDGER_EPOCH))
      status_msgLC(sprintf(
        "Connected - %s recorded calls across %d snapshots (%s to %s); the board uses the %d since the %s epoch (earlier ones = retired gate).",
        as.numeric(b$n[1]), as.integer(b$d[1]), b$lo[1], b$hi[1],
        as.integer(b$d_epoch[1]), LEDGER_EPOCH))
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
    # Hourly (2026-08-26, Kevin): keep the board current against PROD while the
    # laptop is on and connected, without a click. The clock keeps running on
    # hidden tabs but the tick is suppressed there (returning to Lifecycle fires
    # an immediate refresh anyway, mainNav is a reactive dep), and an unset
    # password (not connected) short-circuits, so a disconnected session never
    # queries. 3600000 ms = 1 hour.
    invalidateLater(3600000)
    if (!identical(input$mainNav, "Lifecycle")) return()
    if (isolate(is.null(input$db_pass) || input$db_pass == "")) return()
    isolate(lcAutoTick(lcAutoTick() + 1))
  })
  observeEvent(list(input$execute_LC, lcAutoTick()), {
    if (input$db_pass == "") { status_msgLC("Error: Password is not set."); return() }
    # A click on Generate always re-queries; the auto-start and hourly ticks may
    # reuse a load under LC_MEMO_TTL_SEC old with the same data signature (.lc_memo). (Horizon changes still re-derive in
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
        # additive archive/settings tables self-provision (idempotent), then the
        # saved auto-adopt $ and the realized archive load back into the session
        pf_ensure_extras_once(con)
        samt <- suppressWarnings(as.numeric(db_get_setting(con, "auto_adopt_amount", "100")))
        if (length(samt) == 1 && is.finite(samt) && samt >= 1)
          updateNumericInput(session, "lcSeedAmt", value = samt)
        rthr <- suppressWarnings(as.numeric(db_get_setting(con, "recoup_threshold_pct", "100")))
        if (length(rthr) == 1 && is.finite(rthr) && rthr >= 10)
          updateNumericInput(session, "lcRecoupThr", value = rthr)
        lc_closed(db_load_closed(con))
        # The DB is authoritative on every Generate: no session/local merge-back.
        # The old parquet-migration merge re-upserted anything an open session
        # still remembered, which resurrected server-side deletions (the deduped
        # duplicate rows came back this way, seen live 2026-08-21). The one-time
        # parquet migration it existed for is long done; stale parquet books on
        # any machine now simply get ignored once a portfolio DB is present.
        dbpos <- db_load_positions(con)
        lc_pos(dbpos)
        .lc_known$ids <- if (!is.null(dbpos) && nrow(dbpos)) as.character(dbpos$id) else character(0)
        dbdis <- db_load_dismissed(con)
        lc_dismissed(dbdis)
        .lc_known$dis <- dbdis
      }
      # Model data: reuse the last load (any session) while the data signature
      # still matches and it is under LC_MEMO_TTL_SEC old; the portfolio rows
      # above are always read fresh. A click on Generate always reloads.
      ex <- input$execute_LC; if (is.null(ex)) ex <- 0
      forced <- !identical(ex, lc_gen$exec_seen)
      lc_gen$exec_seen <- ex
      sig <- lc_data_signature(con)
      memo <- if (forced) NULL else lc_bundle_memo_get(sig)
      if (!is.null(memo)) {
        app_dataLC(memo)
        status_msgLC(lc_loaded_msg(memo))
        return()
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
      # parsed ONCE here: every trail / schedule reader used to re-parse the
      # ~40k date strings per name (profiled 2026-10-12: a big share of the
      # 60s+ first page load). Readers fall back to as.Date(led$d) if absent.
      led$dd <- as.Date(led$d)
      attr(led, "lc_idx") <- lc_led_index(led)
      gate <- dbGetQuery(con, "
        SELECT ticker, global_action AS gate_today, id
        FROM serving.return_cluster_ticker_global_action_current")
      # raw.ticker_metadata holds ONLY delisted names -> presence = delisted
      meta <- tryCatch(dbGetQuery(con, "
        SELECT ticker, name AS company_name, delisting_category,
               delisted_utc::date::text AS delisted_date
        FROM raw.ticker_metadata"), error = function(e) NULL)
      # shortlist evidence PER HORIZON: the ticker's walk-forward rank bin at
      # the latest cutoff and that bin's realized win rate. One row per
      # (ticker, fut_lag); DISTINCT ON prefers a cell that passes the FULL
      # cohort standard (top vingtile of a long id, >= 55% win on >= 100 obs)
      # when a ticker sits in two clusters, then best win rate. rank_bin/eid
      # ride along so the prov gate can hold the bin-1 line - 28 (id,bin)
      # cells pass 55/100 pooled, but only bin 1 is what the cohort record
      # validated; mid-table passes are multiple-comparisons noise (id 6's
      # bin 1 FAILS at 52 while its bins 2/6/7/9 'pass').
      sl <- tryCatch(dbGetQuery(con, "
        WITH mx AS (
            SELECT fut_lag, MAX(train_cutoff_date) AS cut
            FROM validation.walk_forward_ticker_rank
            WHERE fut_lag IN (1,2,4,7,12,20,33)
            GROUP BY fut_lag
        ), wfbin AS (
            SELECT r.fut_lag, m.id AS eid, r.ticker,
                   NTILE(20) OVER (PARTITION BY r.fut_lag, m.id
                     ORDER BY r.ticker_score DESC, r.tiebreak_score DESC NULLS LAST,
                              r.n_weighted DESC, r.ticker
                   )::int AS wf_bin
            FROM validation.walk_forward_ticker_rank r
            JOIN mx ON mx.fut_lag = r.fut_lag AND mx.cut = r.train_cutoff_date
            JOIN validation.walk_forward_cluster_id_map m
              ON m.train_cutoff_date = r.train_cutoff_date
             AND m.cluster_id       = r.cluster_id
            WHERE r.ticker_score IS NOT NULL AND r.ticker_score <> 0
        )
        SELECT DISTINCT ON (w.ticker, w.fut_lag) w.ticker, w.fut_lag,
               w.wf_bin AS rank_bin, w.eid,
               ROUND(ps.hit_rate::numeric * 100, 1) AS bin_win_pct,
               ps.n_obs::int AS bin_n
        FROM wfbin w
        LEFT JOIN validation.walk_forward_pctile_summary ps
          ON ps.id = w.eid AND ps.fut_lag = w.fut_lag AND ps.pctile_bin = w.wf_bin
        ORDER BY w.ticker, w.fut_lag,
               (w.wf_bin = 1 AND w.eid <= 12 AND ps.hit_rate >= 0.55
                AND ps.n_obs >= 100) DESC,
               ps.hit_rate DESC NULLS LAST"),
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
                     ORDER BY r.ticker_score DESC, r.tiebreak_score DESC NULLS LAST,
                              r.n_weighted DESC, r.ticker
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
      # ranking) and bounded by freshness (150d ~ the daily grader's 90d
      # re-grade cadence + slack) so an orange + can expire instead of living
      # forever if the grader dies.
      qs <- tryCatch(dbGetQuery(con, sprintf("
        SELECT DISTINCT ON (ticker) ticker,
               overall AS grade, as_of::text AS as_of,
               graded_at::date::text AS graded_at
        FROM qual.ticker_scorecards
        WHERE NOT veto
          AND rubric_version = 'buy_decision_v1'
          AND as_of >= CURRENT_DATE - %d
        ORDER BY ticker, as_of DESC, graded_at DESC", QS_MAX_AGE_DAYS)),
        error = function(e) NULL)
      # vetoed names (latest scorecard, any age): excluded from the auto-seed
      # even though a grade no longer hard-gates entry - a veto is an active
      # thumbs-down, the absence of a grade is not.
      qsv <- tryCatch(dbGetQuery(con, "
        SELECT DISTINCT ON (ticker) ticker, veto, as_of::text AS as_of
        FROM qual.ticker_scorecards
        WHERE rubric_version = 'buy_decision_v1'
        ORDER BY ticker, as_of DESC, graded_at DESC"),
        error = function(e) NULL)
      # descriptive qualstream comparison series (LC_QS_COMPARE_SQL): current
      # BUYs qualstream graded vs the >= 68 subset vs SPY, equal-weight over a
      # ROLLING 120-day lookback (2026-09-01): the old anchor snapped to the
      # last Jan/May/Sep checkpoint, a leftover of the retired 4-monthly
      # grading calendar - it wiped the sketch to an empty zero-day window
      # three times a year (blank chart every checkpoint morning). Rolling
      # keeps a full ~4-month window on screen every day; the lines re-zero
      # from a day-later start each morning, invisible for a hindsight sketch.
      qs_anchor <- Sys.Date() - 120L
      qscmp <- tryCatch(coerce_numeric_cols(
        dbGetQuery(con, gsub("__QS_AGE__", QS_MAX_AGE_DAYS,
                        gsub("__ANCHOR__", format(qs_anchor, "%Y-%m-%d"),
                             LC_QS_COMPARE_SQL, fixed = TRUE), fixed = TRUE)),
        c("graded_pct", "passed_pct", "spy_pct")), error = function(e) NULL)
      # per-ticker detail (ticker, id, passed, ret) behind the same series, so the
      # sketch can re-aggregate the graded/passed lines for a selected cluster live
      # in R without a re-query. Same anchor/age substitution as qscmp above.
      qscmp_detail <- tryCatch({
        dd <- dbGetQuery(con, gsub("__QS_AGE__", QS_MAX_AGE_DAYS,
                          gsub("__ANCHOR__", format(qs_anchor, "%Y-%m-%d"),
                               LC_QS_COMPARE_DETAIL_SQL, fixed = TRUE), fixed = TRUE))
        if (!is.null(dd) && nrow(dd)) {
          dd$ret    <- suppressWarnings(as.numeric(dd$ret))
          dd$id     <- suppressWarnings(as.integer(dd$id))
          dd$passed <- as.logical(dd$passed)
        }
        dd
      }, error = function(e) NULL)
      # Nested-dwell SHADOW (decisive line, pre-registered 2026-10-12; written
      # daily by scripts/nested_board_shadow.py in elt-inference-models). Only
      # its entries, exits and the latest run's rows are read: the STATE radio's
      # "Nested shadow" option shows its state and draws these as the sketch
      # pins. Rows with proven_src 'backfill' are context, not live evidence.
      # NULL until the table exists; the engine view is untouched either way.
      nested <- tryCatch(dbGetQuery(con, "
        SELECT ticker, run_date::text AS run_date, state, entered_at::text AS entered_at,
               exit_reason, proven_src
        FROM monitoring.nested_board_shadow
        WHERE state = 'EXIT' OR (state = 'BUY' AND run_date = entered_at)
           OR run_date = (SELECT MAX(run_date) FROM monitoring.nested_board_shadow_runs)
        ORDER BY ticker, run_date"),
        error = function(e) NULL)
      # What the board ACTUALLY said each day (daily capture since 2026-08-07),
      # last capture per (ticker, day, horizon). lc_board_trail prefers these over
      # its replay, which re-derives "proven" from TODAY's evidence and so repaints
      # a de-gated cluster's past BUYs as HOLD after every re-mint. NULL where the
      # portfolio schema is absent (dev) -> replay only, as before.
      rec_states <- tryCatch(dbGetQuery(con, "
        SELECT DISTINCT ON (ticker, as_of, hz) ticker, as_of::text AS as_of, hz, state, why
        FROM portfolio.state_history
        ORDER BY ticker, as_of, hz, captured_at DESC"),
        error = function(e) NULL)
      # market-shift tags: display-only marker + market-creators view (see NC_TAG_VERSION)
      nc <- lc_load_category_tags(con)
      # the day's board from the pipeline (NULL -> derivedLC walks it here)
      board <- tryCatch(lc_board_load(con, max(led$d)), error = function(e) NULL)
      if (!is.null(board)) rec_states <- lc_rec_with_board(rec_states, board$hist)
      dat <- list(led = led, gate = gate, meta = meta, meta_dl = lc_meta_dl(meta), sl = sl, coh = coh,
                  board = board,
                  qs = qs, qscmp = qscmp, qscmp_detail = qscmp_detail, qscmp_anchor = qs_anchor,
                  nested = nested, rec_states = rec_states,
                  nc = nc,
                  qs_veto = if (!is.null(qsv) && nrow(qsv) > 0)
                              toupper(qsv$ticker[qsv$veto %in% TRUE])
                            else character(0),
                  # latest grade date per ticker ANY verdict (vetoes included):
                  # the grading-gap tripwire needs "was it graded", not "did it
                  # pass" - built from d$qs alone, a vetoed name looked ungraded
                  # and false-fired the GRADING GAP banner (FTDR/TSM, 2026-09-01)
                  qs_any_asof = if (!is.null(qsv) && nrow(qsv) > 0)
                                  setNames(suppressWarnings(as.Date(qsv$as_of)),
                                           toupper(qsv$ticker))
                                else NULL)
      dat <- lc_bundle_memo_put(sig, dat)
      app_dataLC(dat)
      status_msgLC(lc_loaded_msg(dat))
    }, error = function(e) { status_msgLC(paste("Error:", e$message)) })
  }, ignoreInit = TRUE)

