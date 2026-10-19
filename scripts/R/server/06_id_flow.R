# AlphaStream dashboard - ID Flow tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # ── ID FLOW: id-level in -> out X-ray ──
  idf_fed     <- reactiveVal(NULL)   # scored-cell grid (bucket x fut_lag)
  idf_out     <- reactiveVal(NULL)   # served tickers + board action
  idf_verdict <- reactiveVal(NULL)   # one-row: tradeable + IC evidence
  status_msgIDF <- reactiveVal("Ready")
  output$statusMessageIDF <- renderText({ status_msgIDF() })

  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgIDF("Error: Password is not set."); return() }
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals <- dbGetQuery(con,
        "SELECT DISTINCT id FROM serving.return_cluster_ticker_summary_current ORDER BY 1")
      updateSelectInput(session, "id_valIDF", choices = id_vals[[1]], selected = id_vals[[1]][1])
      status_msgIDF("Filters loaded!")
    }, error = function(e) status_msgIDF(paste("Error:", e$message)))
  })

  observeEvent(input$execute_IDF, {
    if (input$db_pass == "") { status_msgIDF("Error: Password is not set."); return() }
    if (is.null(input$id_valIDF) || input$id_valIDF == "") {
      status_msgIDF("Error: Select a cluster first."); return()
    }
    status_msgIDF("Running query...")
    idv <- input$id_valIDF
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)

      # FED: matched-horizon scored cells with their full past/future
      # distributions - drawn Transition-Range style, one panel per horizon.
      # fut_lag <= 33 = the app-wide horizon cap: the scoring table still
      # carries legacy 54-376mo rows that were never validated (can't finish
      # inside the holdout) - every tab filters them out.
      fed <- dbGetQuery(con, sprintf("
        SELECT fut_lag,
               past_excess_return_z_bucket_num AS bucket,
               past_excess_return_z_bucket     AS bucket_label,
               past_lo, past_q1, past_median AS past_med, past_q3, past_hi,
               future_lo, future_q1, future_median AS future_med,
               future_q3, future_hi,
               past_record_count AS past_count,
               future_record_count AS future_count,
               net_score, recommendation, n_observations
        FROM scoring.return_cluster_cell_score_extended
        WHERE id = %s AND past_lag = fut_lag AND fut_lag <= 33
        ORDER BY fut_lag, past_excess_return_z_bucket_num;", idv))
      idf_fed(fed)

      # COMES OUT: served tickers + live board action
      out <- dbGetQuery(con, sprintf("
        SELECT s.agg_rank,
               s.ticker,
               COALESCE(g.global_action, '-') AS action,
               ROUND(s.agg_directional_score::numeric, 3) AS dir_score,
               g.buy_votes, g.sell_votes,
               s.coverage_cell_count AS cells,
               s.net_long_density_long_h,
               s.horizon_coherence_flag,
               s.evidence_status
        FROM serving.return_cluster_ticker_summary_current s
        LEFT JOIN serving.return_cluster_ticker_global_action_current g
          ON g.id = s.id AND g.ticker = s.ticker
        WHERE s.id = %s
        ORDER BY s.agg_rank;", idv))
      out$agg_rank <- as.integer(out$agg_rank)
      # Horizon read (audit 2026-09-22): a BUY whose >=20mo legs would not
      # clear the 0.3 density floor on their own is timed entirely by the
      # 1-7mo legs (the pop-chasing signature the horizon A/B tests).
      lh <- suppressWarnings(as.numeric(out$net_long_density_long_h))
      fc <- out$horizon_coherence_flag
      if (is.character(fc)) fc <- fc %in% c("t", "true", "TRUE")
      out$horizon_flag <- ifelse(out$action == "BUY" & (is.na(lh) | lh < 0.3),
                                 "SHORT-ONLY",
                                 ifelse(!is.na(fc) & !fc, "mixed", ""))
      idf_out(out)

      # VERDICT: tradeable flag (from serving) + walk-forward IC gate evidence
      vr <- dbGetQuery(con, sprintf("
        SELECT DISTINCT cluster_is_tradeable, cluster_untradeable_reason,
               evidence_id
        FROM serving.return_cluster_ticker_summary_current
        WHERE id = %s LIMIT 1;", idv))
      ic <- dbGetQuery(con, sprintf("
        SELECT ROUND(mean_ic::numeric,4) AS mean_ic,
               ROUND(median_ic::numeric,4) AS median_ic,
               n_cohorts, n_positive,
               ROUND(mean_decile_spread::numeric,4) AS spread
        FROM validation.walk_forward_id_ic
        WHERE id = %s AND fut_lag = 12;", idv))
      idf_verdict(list(id = idv, vr = vr, ic = ic,
                       n_out = nrow(out),
                       n_buy = sum(out$action == "BUY", na.rm = TRUE),
                       n_hold = sum(out$action == "HOLD", na.rm = TRUE),
                       n_sell = sum(out$action == "SELL", na.rm = TRUE)))

      status_msgIDF(sprintf("Loaded id %s: %d cells fed, %d tickers out.",
                            idv, nrow(fed), nrow(out)))
    }, error = function(e) {
      idf_fed(NULL); idf_out(NULL); idf_verdict(NULL)
      status_msgIDF(paste("Error:", e$message))
    })
  })

  output$idfVerdict <- renderUI({
    v <- idf_verdict()
    if (is.null(v)) return(HTML("Select a cluster id and Generate."))
    tradeable <- !is.null(v$vr) && nrow(v$vr) && isTRUE(v$vr$cluster_is_tradeable[1])
    badge <- if (tradeable)
      "<span style='color:#4ade80;font-weight:700;'>TRADEABLE</span>"
    else sprintf("<span style='color:#f87171;font-weight:700;'>NOT TRADEABLE</span>%s",
      if (!is.null(v$vr) && nrow(v$vr) && !is.na(v$vr$cluster_untradeable_reason[1]))
        sprintf(" <span style='color:#94a3b8;'>(%s)</span>", v$vr$cluster_untradeable_reason[1]) else "")
    evid <- if (!is.null(v$vr) && nrow(v$vr) && !is.na(v$vr$evidence_id[1]))
      sprintf("  ·  evidence id %s", v$vr$evidence_id[1]) else ""
    icline <- if (!is.null(v$ic) && nrow(v$ic) && !is.na(v$ic$mean_ic[1]))
      sprintf("walk-forward IC (12mo): mean %.3f, median %.3f, positive %d/%d, decile spread %.3f",
              v$ic$mean_ic[1], v$ic$median_ic[1], as.integer(v$ic$n_positive[1]),
              as.integer(v$ic$n_cohorts[1]), v$ic$spread[1])
    else "walk-forward IC (12mo): no evidence row for this id"
    HTML(sprintf(
      "Cluster %s  ·  %s%s<br>%s<br>Emits %d ticker(s): <b style='color:#4ade80;'>%d BUY</b>, %d HOLD, <b style='color:#f87171;'>%d SELL</b>.",
      v$id, badge, evid, icline, v$n_out, v$n_buy, v$n_hold, v$n_sell))
  })

  output$idfFedPlot <- renderPlotly({
    fed <- idf_fed()
    if (is.null(fed) || nrow(fed) == 0)
      return(empty_plot("Generate to load the transition panels."))
    for (cn in c("fut_lag","bucket","past_lo","past_q1","past_med","past_q3",
                 "past_hi","future_lo","future_q1","future_med","future_q3",
                 "future_hi","past_count","future_count","net_score"))
      fed[[cn]] <- as.numeric(fed[[cn]])
    lags <- sort(unique(fed$fut_lag))
    # one Transition-Range-style panel per horizon (Kevin 2026-08-23: same
    # picture as the Transition tab, all legs at once). Same colors: purple
    # past box, sky-blue future box; buckets ordered most negative -> most
    # positive left to right (bucket_num 1 = most positive, so reverse).
    panels <- lapply(lags, function(lg) {
      d <- fed[fed$fut_lag == lg, , drop = FALSE]
      d <- d[order(-d$bucket), , drop = FALSE]
      xcat <- factor(d$bucket_label, levels = d$bucket_label)
      d$hover <- sprintf(paste0(
        "<b>%s · fut_lag %d</b><br>",
        "past   med %.1f%% [%.1f .. %.1f]<br>",
        "future med %.1f%% [%.1f .. %.1f]<br>",
        "n_obs %d · %s (net %.1f)"),
        d$bucket_label, lg,
        d$past_med, d$past_q1, d$past_q3,
        d$future_med, d$future_q1, d$future_q3,
        as.integer(as.numeric(d$n_observations)), d$recommendation, d$net_score)
      first <- lg == lags[1]
      plot_ly(d) %>%
        add_trace(type = 'box', name = 'Past', x = xcat,
          q1 = ~past_q1, median = ~past_med, q3 = ~past_q3,
          lowerfence = ~past_lo, upperfence = ~past_hi,
          marker = list(color = '#a855f7'), line = list(color = '#a855f7', width = 2),
          fillcolor = 'rgba(167,139,250,0.4)', hoverinfo = "skip",
          offsetgroup = '1', legendgroup = 'past', showlegend = first) %>%
        add_trace(type = 'box', name = 'Future', x = xcat,
          q1 = ~future_q1, median = ~future_med, q3 = ~future_q3,
          lowerfence = ~future_lo, upperfence = ~future_hi,
          marker = list(color = '#0ea5e9'), line = list(color = '#0ea5e9', width = 2),
          fillcolor = 'rgba(14,165,233,0.4)', hoverinfo = "skip",
          offsetgroup = '2', legendgroup = 'future', showlegend = first) %>%
        add_trace(type = 'scatter', mode = 'markers', x = xcat, y = ~future_med,
          marker = list(color = 'rgba(14,165,233,0)', size = 40),
          text = ~hover, hovertemplate = "%{text}<extra></extra>",
          showlegend = FALSE,
          hoverlabel = list(font = list(family = 'Courier New, monospace', size = 12))) %>%
        layout(
          boxmode = "group",
          xaxis = list(color = "#94a3b8", gridcolor = "rgba(255,255,255,0.08)",
                       tickfont = list(size = 10)),
          yaxis = list(title = sprintf("fut_lag %d", lg), color = "#94a3b8",
                       gridcolor = "rgba(255,255,255,0.08)", zeroline = TRUE,
                       zerolinewidth = 2, zerolinecolor = "rgba(255,255,255,0.25)"))
    })
    subplot(panels, nrows = length(panels), shareX = FALSE, shareY = FALSE,
            titleY = TRUE, margin = 0.012) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        showlegend = TRUE,
        legend = list(font = list(color = "#f8fafc"), orientation = "h", y = 1.02),
        margin = list(l = 70, r = 30, b = 40, t = 30))
  })

  output$idfOutTable <- DT::renderDT({
    out <- idf_out()
    if (is.null(out) || nrow(out) == 0)
      return(DT::datatable(data.frame(Note = "Generate to load the served tickers."),
        rownames = FALSE, selection = "none", options = list(dom = "t", ordering = FALSE)))
    show <- data.frame(
      Rank = out$agg_rank, Ticker = toupper(out$ticker), Action = out$action,
      Horizon = if ("horizon_flag" %in% names(out)) out$horizon_flag else "",
      `Dir score` = as.numeric(out$dir_score),
      `Buy votes` = suppressWarnings(as.integer(out$buy_votes)),
      `Sell votes` = suppressWarnings(as.integer(out$sell_votes)),
      Cells = suppressWarnings(as.integer(out$cells)),
      Evidence = out$evidence_status,
      check.names = FALSE, stringsAsFactors = FALSE)
    DT::datatable(show, rownames = FALSE, selection = "none", class = "compact",
      options = list(dom = "tp", pageLength = 25, order = list(list(0, "asc")))) %>%
      DT::formatStyle("Action", fontWeight = "600",
        color = DT::styleEqual(c("BUY","SELL","HOLD"),
                               c("#4ade80","#f87171","#fbbf24"), default = "#94a3b8")) %>%
      DT::formatStyle("Horizon", fontWeight = "600",
        color = DT::styleEqual(c("SHORT-ONLY","mixed"),
                               c("#f87171","#fbbf24"), default = "#64748b"))
  })

