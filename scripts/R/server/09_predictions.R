# AlphaStream dashboard - Predictions (buy list) tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
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
          -- tickers get NULL bin -> 'no evidence' downstream. NTILE key list
          -- mirrors walk_forward_pctile_summary: score, tiebreak (F1, NULLS
          -- LAST -> no-op on pre-F1 generations), n_weighted, ticker.
          SELECT m.id AS eid, r.ticker,
                 NTILE(20) OVER (PARTITION BY m.id
                   ORDER BY r.ticker_score DESC, r.tiebreak_score DESC NULLS LAST,
                            r.n_weighted DESC, r.ticker
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

