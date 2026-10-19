# AlphaStream dashboard - Clusters tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # ── CLUSTERS ──
  app_dataK   <- reactiveVal(NULL)
  status_msgK <- reactiveVal("Not connected.")
  output$statusMessageK <- renderText({ status_msgK() })

  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgK("Error: Password is not set."); return() }
    status_msgK("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM analysis.ticker_cluster_segments")$n[1]
      status_msgK(sprintf("Connected — %s tickers clustered.", n))
    }, error = function(e) { status_msgK(paste("Error:", e$message)) })
  })

  observeEvent(input$execute_K, {
    if (input$db_pass == "") { status_msgK("Error: Password is not set."); return() }
    status_msgK("Loading clusters...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      df <- dbGetQuery(con, "
        SELECT s.ticker,
               v.id::int AS id,   -- cast bigint->int: integer64 breaks the per-id for-loop (subscript out of bounds)
               s.months_count,
               s.growth_pct_per_month
        FROM analysis.ticker_cluster_segments s
        JOIN clustering.ticker_cluster_volatility_summary v
          ON v.cluster_id = s.cluster_id
        WHERE s.months_count > 0 AND s.growth_pct_per_month IS NOT NULL")
      app_dataK(df)
      status_msgK(sprintf("Loaded %d tickers across %d clusters.",
                          nrow(df), length(unique(df$id))))
    }, error = function(e) { app_dataK(NULL); status_msgK(paste("Error:", e$message)) })
  })

  output$clusterPlot <- renderPlotly({
    req(app_dataK())
    df <- app_dataK()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))

    df <- df[is.finite(df$months_count) & is.finite(df$growth_pct_per_month), ]
    palette20 <- c("#ef4444","#f97316","#eab308","#84cc16","#22c55e","#10b981",
                   "#14b8a6","#06b6d4","#0ea5e9","#3b82f6","#6366f1","#8b5cf6",
                   "#a855f7","#d946ef","#ec4899","#f43f5e","#fbbf24","#a3e635",
                   "#2dd4bf","#7c3aed")
    id_levels <- sort(unique(df$id))
    color_map <- setNames(palette20[((seq_along(id_levels) - 1) %% length(palette20)) + 1],
                          as.character(id_levels))
    # Clip y-axis (p0.5..p99.5, min ceiling 25) so high-growth clusters stay
    # visible while penny-stock crashes don't compress the bulk into y=0.
    y_lo <- min(-10, quantile(df$growth_pct_per_month, 0.005, na.rm = TRUE))
    y_hi <- max( 25, quantile(df$growth_pct_per_month, 0.995, na.rm = TRUE))
    x_hi <- quantile(df$months_count,         0.995, na.rm = TRUE)
    show_nums <- is.null(input$clusterNumbers) || isTRUE(input$clusterNumbers)

    # One markers trace + one centroid-text trace PER cluster, sharing a
    # legendgroup so toggling a cluster (legend click / Show-Hide all) also
    # drops its number. The number trace is only added when the checkbox is on.
    fig <- plot_ly()
    for (k in id_levels) {
      sub <- df[df$id == k, ]
      col <- color_map[[as.character(k)]]
      fig <- add_trace(fig, x = sub$months_count, y = sub$growth_pct_per_month,
        type = "scatter", mode = "markers", name = as.character(k), legendgroup = as.character(k),
        marker = list(color = col, size = 3, opacity = 0.6),
        text = sub$ticker, hovertemplate = paste0("%{text}<br>id ", k, "<extra></extra>"))
      if (show_nums) {
        fig <- add_trace(fig, x = median(sub$months_count, na.rm = TRUE),
          y = median(sub$growth_pct_per_month, na.rm = TRUE),
          type = "scatter", mode = "text", legendgroup = as.character(k), showlegend = FALSE,
          text = as.character(k), hoverinfo = "skip",
          textfont = list(color = "#f8fafc", size = 16))
      }
    }
    dark_layout(fig,
      xaxis = list(title = "Tenure (months)", range = c(0, x_hi), color = "#cbd5e1",
                   gridcolor = "#1e293b", zeroline = FALSE),
      yaxis = list(title = "Growth rate (%/mo)", range = c(y_lo, y_hi), color = "#cbd5e1",
                   gridcolor = "#1e293b", zeroline = FALSE),
      legend = list(font = list(color = "#e2e8f0"), title = list(text = "id"),
                    groupclick = "togglegroup"),
      margin = list(l = 55, r = 20, t = 15, b = 45)) %>%
      config(displayModeBar = FALSE)
  })

  observeEvent(input$clusterShowAll, {
    plotlyProxy("clusterPlot", session) %>%
      plotlyProxyInvoke("restyle", list(visible = TRUE))
  })

  observeEvent(input$clusterHideAll, {
    plotlyProxy("clusterPlot", session) %>%
      plotlyProxyInvoke("restyle", list(visible = "legendonly"))
  })

