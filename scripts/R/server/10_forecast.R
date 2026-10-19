# AlphaStream dashboard - Forecast tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
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
      # back to the default on exit: the connection is shared by every tab and session
      on.exit(try(DBI::dbExecute(con, sprintf("SET statement_timeout = '%s'", DB_STATEMENT_TIMEOUT)),
                  silent = TRUE), add = TRUE)
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
        if ("qs_pct" %in% names(ls0)) ls0$qs_pct <- as.numeric(ls0$qs_pct)
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
    # Look-ahead label (Kevin 2026-10-20, after elt-inference-models
    # docs/lookahead_audit_2026-10-20.md): the selections, Typical and the
    # projection all come from walk_forward_top_picks, whose scores used data
    # dated after each as-of date. Only the Live log is out of sample.
    tagList(
    tags$div(style = paste("color:#fbbf24; background:rgba(251,191,36,0.08); border:1px solid rgba(251,191,36,0.35);",
                           "border-radius:6px; padding:6px 10px; font-size:0.78rem; margin-bottom:0.5rem; line-height:1.45;"),
      HTML(paste("<b>Look-ahead warning (audit Oct 20, 2026).</b> These backtest selections were scored with data",
                 "dated after their own cutoff, often years later, so for most past dates they were not knowable",
                 "then. Where an honest test is possible,",
                 "the same picks returned about -2pp vs Benchmark over 12 months instead of about +19pp. Read the",
                 "edge shown in the selections, Typical and the projection as mostly look-ahead. The Live log is",
                 "the real record."))),
    tags$div(style = "color:#cbd5e1; font-size:0.82rem; margin-bottom:0.6rem; line-height:1.5;",
      HTML(sprintf(paste0(
        "<b>As of %s</b> (%s): the selections returned <b>%+.1f%%</b> vs Benchmark <b>%+.1f%%</b> = <b>%+.1fpp</b>. ",
        "Typically +%.1fpp at 12mo (avg of all starts, not this window). &nbsp;|&nbsp; <b>Live log</b> (live out-of-sample, since %s): ",
        "<b>%+.1f%%</b> vs Benchmark <b>%+.1f%%</b> = <b>%+.1fpp</b>. A fixed hold makes as-of dates comparable; ",
        "'to today' lets old dates run 8-13 years (dropped series get frozen and drag them)."),
        format(d$anchor, "%b %Y"), win, pend, spend, beat_live, beat12,
        lg$entry_d[1], lg$basket_ret_pct[1], lg$spy_ret_pct[1], lg$basket_ret_pct[1] - lg$spy_ret_pct[1]))))
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
    # qs >= 68 picks line: NULL before the first grade, so has_qs gates the whole line.
    # q0 = first graded date (where the line begins), qi = last (the end label).
    has_qs <- !is.null(ls$qs_pct) && any(!is.na(ls$qs_pct))
    q0 <- if (has_qs) min(which(!is.na(ls$qs_pct))) else NA_integer_
    qi <- if (has_qs) max(which(!is.na(ls$qs_pct))) else NA_integer_
    qend <- if (has_qs) ls$qs_pct[qi] else NA_real_
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
    # qs >= 68 PICKS, point-in-time: each name enters at its own first-grade date at
    # 0% (not backdated to Jun), prunes on veto/<68. Independent of the frozen basket,
    # so its level is on a DIFFERENT (grade-date) basis than the orange/blue lines --
    # read its slope vs them, not the absolute gap. connectgaps = FALSE so the
    # pre-grade NULLs stay blank instead of drawing a phantom segment back to Jun.
    if (has_qs)
      fig <- add_trace(fig, x = ls$d, y = ls$qs_pct, type = "scatter", mode = "lines",
        name = "qs ≥ 68 picks (from grade date)", legendgroup = "q", connectgaps = FALSE,
        line = list(color = "#10b981", width = 3),
        hovertemplate = "qs ≥ 68 picks<br>%{x|%b %d}: %{y:.1f}%<br>(since each name's own grade date)<extra></extra>")
    tdy <- format(today, "%Y-%m-%d")
    ann <- list(
      list(x = tdy, y = 1, yref = "paper", text = "today", showarrow = FALSE,
           font = list(color = "#94a3b8", size = 10), yanchor = "bottom"),
      list(x = tdy, y = lend, text = sprintf("%+.1f%%", lend), showarrow = FALSE,
           xanchor = "left", xshift = 6, font = list(color = "#f59e0b", size = 12)),
      list(x = tdy, y = send, text = sprintf("%+.1f%%", send), showarrow = FALSE,
           xanchor = "left", xshift = 6, font = list(color = "#3b82f6", size = 12)))
    # qs end-value label + a small marker where the line begins, so the mid-chart
    # start reads as "grades began here", not "qs was flat then jumped".
    if (has_qs) ann <- c(ann, list(
      list(x = tdy, y = qend, text = sprintf("%+.1f%%", qend), showarrow = FALSE,
           xanchor = "left", xshift = 6, font = list(color = "#10b981", size = 12)),
      list(x = format(ls$d[q0], "%Y-%m-%d"), y = ls$qs_pct[q0], text = "qs grades begin",
           showarrow = TRUE, arrowhead = 0, ax = 0, ay = -22,
           font = list(color = "#6ee7b7", size = 9), arrowcolor = "rgba(16,185,129,0.55)")))
    dark_layout(fig,
      title = list(text = "REAL SCOREBOARD - live picks vs qs ≥ 68 picks vs Benchmark (since Jun 2026)",
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

