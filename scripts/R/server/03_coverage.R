# AlphaStream dashboard - Coverage tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # ── COVERAGE: Reactive values ──
  app_dataV <- reactiveVal(NULL)
  status_msgV <- reactiveVal("Ready")
  output$statusMessageV <- renderText({ status_msgV() })

  # ── COVERAGE: Connect — just smoke-test the query source ──
  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgV("Error: Password is not set."); return() }
    status_msgV("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      n <- dbGetQuery(con,
        "SELECT COUNT(DISTINCT ticker) AS n FROM cdm.ingest_combined")$n[1]
      status_msgV(sprintf("Connected — %s distinct tickers.", n))
    }, error = function(e) { status_msgV(paste("Error:", e$message)) })
  })

  # ── COVERAGE: Execute — load min/max date per ticker ──
  observeEvent(input$execute_V, {
    if (input$db_pass == "") { status_msgV("Error: Password is not set."); return() }
    status_msgV("Loading coverage...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      df <- dbGetQuery(con, "
        SELECT ticker,
               MIN(\"date\")::date AS first_date,
               MAX(\"date\")::date AS last_date,
               COUNT(*) AS n_obs
        FROM cdm.ingest_combined
        GROUP BY ticker
        ORDER BY MIN(\"date\")")
      df$first_date <- as.Date(df$first_date)
      df$last_date  <- as.Date(df$last_date)
      df$n_obs      <- as.numeric(df$n_obs)
      app_dataV(df)
      status_msgV(sprintf("Loaded %d tickers.", nrow(df)))
    }, error = function(e) { app_dataV(NULL); status_msgV(paste("Error:", e$message)) })
  })

  # ── COVERAGE: Container with dynamic height ──
  output$coveragePlotContainer <- renderUI({
    req(app_dataV())
    ph <- max(3000, nrow(app_dataV()) * 14)
    plotlyOutput("coveragePlot", height = paste0(ph, "px"))
  })

  # ── COVERAGE: Render ──
  output$coveragePlot <- renderPlotly({
    req(app_dataV())
    df <- app_dataV()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))

    # Sort: earliest start at bottom (longest history), most recent start at top
    df <- df[order(df$first_date), ]
    df$ticker_f <- factor(df$ticker, levels = df$ticker)
    today <- Sys.Date()
    ph <- max(3000, nrow(df) * 14)

    hover <- sprintf(
      "<b>%s</b><br>%s → %s<br>%s obs<extra></extra>",
      df$ticker, df$first_date, df$last_date, format(df$n_obs, big.mark = ","))

    plot_ly(height = ph) %>%
      add_segments(
        x = df$first_date, xend = df$last_date,
        y = df$ticker_f,   yend = df$ticker_f,
        line = list(color = "#fb7185", width = 4),
        hovertemplate = hover,
        hoverlabel = list(font = list(family = "JetBrains Mono, monospace", size = 12))
      ) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        xaxis = list(title = "Observation date", color = "#94a3b8",
                     gridcolor = "rgba(255,255,255,0.08)",
                     zerolinecolor = "rgba(255,255,255,0.08)",
                     fixedrange = TRUE),
        yaxis = list(title = "", color = "#94a3b8", automargin = TRUE,
                     tickfont = list(size = 8),
                     categoryorder = "array",
                     categoryarray = levels(df$ticker_f),
                     fixedrange = TRUE),
        shapes = list(list(
          type = "line", x0 = today, x1 = today, y0 = 0, y1 = 1, yref = "paper",
          line = list(color = "#f8fafc", width = 2, dash = "dot"))),
        annotations = list(list(
          x = today, y = 1.005, yref = "paper", xanchor = "right", yanchor = "bottom",
          text = sprintf("today (%s)", today), showarrow = FALSE,
          font = list(color = "#94a3b8", size = 11, family = "Inter"))),
        margin = list(l = 80, r = 40, t = 40, b = 60),
        showlegend = FALSE
      ) %>%
      config(scrollZoom = FALSE, displayModeBar = FALSE)
  })

