# AlphaStream dashboard - Lifecycle: portfolio chart, note, summary. Part of app.R, split 2026-10-19: files load in name order (see app.R).
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
        line = lc_line(color = col, width = 1.8), marker = list(color = col, size = 6),
        hovertemplate = paste0(tkr, "<br>%{x|%b %d %Y}: %{y:+.1f}%<extra></extra>"))
    }
    sp <- ser[ser$ticker == "__SPY__", , drop = FALSE]
    if (nrow(sp)) fig <- add_trace(fig, x = sp$d, y = sp$ret_pct, type = "scatter",
      mode = if (nrow(sp) <= 1) "markers" else "lines+markers",
      name = "SPY (same cash)", legendgroup = "SPY",
      line = lc_line(color = "#e2e8f0", width = 3, dash = "dash"),
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
      extra <- paste0(extra, sprintf(
        " Model rows (%s) have no fills yet - run Generate, or the model has not said BUY since their start.",
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
    p <- lc_pos_disp(); n <- if (is.null(p)) 0L else nrow(p)
    ser <- tryCatch(lc_port_series(), error = function(e) NULL)
    txt <- if (is.null(ser) || !nrow(ser)) sprintf(" - %d position%s", n, if (n == 1) "" else "s")
      else { last <- ser[nrow(ser), ]
        sprintf(" - %d pos, $%s (%+.1f%%), %+.1fpp vs SPY", n,
                format(round(last$value), big.mark = ","), last$ret_pct,
                last$ret_pct - last$spy_ret_pct) }
    span(txt, style = "color:#94a3b8; font-weight:400; font-size:0.78rem;")
  })

