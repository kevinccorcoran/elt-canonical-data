# AlphaStream dashboard - Model Validation tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # ── MODEL VALIDATION: Reactive values ──
  app_dataMV_ic     <- reactiveVal(NULL)
  app_dataMV_payoff <- reactiveVal(NULL)
  app_dataMV_tiers  <- reactiveVal(NULL)
  app_dataMV_forest <- reactiveVal(NULL)
  status_msgMV <- reactiveVal("Ready")
  output$statusMessageMV <- renderText({ status_msgMV() })

  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgMV("Error: Password is not set."); return() }
    status_msgMV("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals <- dbGetQuery(con,
        "SELECT DISTINCT id FROM validation.cell_credibility
         UNION SELECT DISTINCT id FROM validation.return_cluster_payoff_backtest
         ORDER BY 1")
      updateSelectInput(session, "id_valMV",
        choices = c("All ids" = "ALL",
                    setNames(as.character(id_vals$id), as.character(id_vals$id))),
        selected = "ALL")
      past_vals <- dbGetQuery(con,
        "SELECT DISTINCT past_lag FROM validation.cell_credibility ORDER BY 1")
      fut_vals <- dbGetQuery(con,
        "SELECT DISTINCT fut_lag FROM validation.cell_credibility ORDER BY 1")
      updateSelectInput(session, "past_lagMV",
                        choices = as.character(past_vals$past_lag), selected = "12")
      updateSelectInput(session, "fut_lagMV",
                        choices = as.character(fut_vals$fut_lag), selected = "12")
      status_msgMV("Filters loaded!")
    }, error = function(e) { status_msgMV(paste("Error:", e$message)) })
  })

  observeEvent(input$execute_MV, {
    if (input$db_pass == "") { status_msgMV("Error: Password is not set."); return() }
    status_msgMV("Running queries...")
    id_filter <- if (is.null(input$id_valMV) || input$id_valMV %in% c("", "ALL")) "" else
      sprintf("AND id = %d", as.integer(input$id_valMV))
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)

      ic_df <- dbGetQuery(con, "
        SELECT id, fut_lag, weighted_ic, mean_ic, median_ic,
               n_positive, n_cohorts, mean_decile_spread
        FROM validation.walk_forward_id_ic
        ORDER BY id, fut_lag;")
      ic_df$id          <- as.integer(ic_df$id)
      ic_df$fut_lag     <- as.integer(ic_df$fut_lag)
      ic_df$weighted_ic <- as.numeric(ic_df$weighted_ic)
      ic_df$mean_decile_spread <- as.numeric(ic_df$mean_decile_spread)
      # COUNT(*) columns arrive as integer64 (see the RS n_obs note above);
      # cast to double before any arithmetic or sprintf.
      ic_df$n_positive <- as.numeric(ic_df$n_positive)
      ic_df$n_cohorts  <- as.numeric(ic_df$n_cohorts)
      app_dataMV_ic(ic_df)

      payoff_df <- dbGetQuery(con, sprintf("
        SELECT id, past_lag, fut_lag, n_holdout, win_pct, expectancy,
               avg_win, avg_loss, median_ret, p90_ret, max_win
        FROM validation.return_cluster_payoff_backtest
        WHERE 1=1 %s
        ORDER BY id, past_lag, fut_lag;", id_filter))
      payoff_df$id        <- as.integer(payoff_df$id)
      payoff_df$past_lag  <- as.integer(payoff_df$past_lag)
      payoff_df$fut_lag   <- as.integer(payoff_df$fut_lag)
      payoff_df <- coerce_numeric_cols(payoff_df, c(
        "n_holdout", "win_pct", "expectancy", "avg_win", "avg_loss",
        "median_ret", "p90_ret", "max_win"))
      app_dataMV_payoff(payoff_df)

      tier_df <- dbGetQuery(con, "
        SELECT id, tier, COUNT(*) AS n_cells
        FROM validation.cell_credibility
        GROUP BY id, tier
        ORDER BY id, tier;")
      tier_df$id      <- as.integer(tier_df$id)
      tier_df$n_cells <- as.numeric(tier_df$n_cells)           # integer64
      app_dataMV_tiers(tier_df)

      if (!input$id_valMV %in% c("", "ALL") &&
          isTruthy(input$past_lagMV) && isTruthy(input$fut_lagMV)) {
        forest_df <- dbGetQuery(con, sprintf("
          SELECT vol_bucket_num, bucket, n_cutoffs, wf_n_holdout, n_scored,
                 wf_agreement, wilson_lower, wilson_upper, tier, credibility_weight
          FROM validation.cell_credibility
          WHERE id = %d AND past_lag = %d AND fut_lag = %d
          ORDER BY vol_bucket_num, bucket;",
          as.integer(input$id_valMV), as.integer(input$past_lagMV),
          as.integer(input$fut_lagMV)))
        forest_df <- coerce_numeric_cols(forest_df, c(
          "n_cutoffs", "wf_n_holdout", "n_scored",
          "wf_agreement", "wilson_lower", "wilson_upper", "credibility_weight"))
        app_dataMV_forest(forest_df)
      } else {
        app_dataMV_forest(data.frame())
      }
      status_msgMV(sprintf("Loaded: %d IC rows, %d payoff rows, %d forest rows.",
                           nrow(ic_df), nrow(payoff_df),
                           nrow(app_dataMV_forest())))
    }, error = function(e) {
      app_dataMV_ic(NULL); app_dataMV_payoff(NULL)
      app_dataMV_tiers(NULL); app_dataMV_forest(NULL)
      status_msgMV(paste("Error:", e$message))
    })
  })

  output$icHeatmapMV <- renderPlotly({
    req(app_dataMV_ic())
    df <- app_dataMV_ic()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    fut_lags <- sort(unique(df$fut_lag))
    ids      <- sort(unique(df$id))
    z_mat <- matrix(NA_real_, nrow = length(ids), ncol = length(fut_lags),
                    dimnames = list(as.character(ids), as.character(fut_lags)))
    cd_mat <- matrix("", nrow = length(ids), ncol = length(fut_lags))
    for (i in seq_len(nrow(df))) {
      r <- which(ids == df$id[i])
      c <- which(fut_lags == df$fut_lag[i])
      if (length(r) && length(c)) {
        z_mat[r, c] <- df$weighted_ic[i]
        cd_mat[r, c] <- sprintf("%d/%d cohorts positive | decile spread %.3f",
                                as.integer(df$n_positive[i]),
                                as.integer(df$n_cohorts[i]),
                                df$mean_decile_spread[i])
      }
    }
    # symmetric zmin/zmax so IC = 0 sits at the palette midpoint
    colorscale <- DIVERGING_COLORSCALE
    max_abs <- max(abs(z_mat), na.rm = TRUE)
    if (!is.finite(max_abs) || max_abs == 0) max_abs <- 0.1
    text_mat <- matrix(rep(as.character(fut_lags), each = nrow(z_mat)),
                       nrow = nrow(z_mat))
    plot_ly(
      x = fut_lags, y = as.character(ids), z = z_mat, type = "heatmap",
      colorscale = colorscale,
      xgap = 1, ygap = 1,
      zmin = -max_abs, zmax = max_abs,
      customdata = cd_mat,
      text = text_mat,
      hovertemplate = paste0(
        "id: %{y}<br>",
        "fut_lag: %{text}<br>",
        "weighted IC: %{z:.3f}<br>",
        "%{customdata}<extra></extra>"),
      colorbar = list(
        title = list(text = "Weighted\nIC", font = list(color = "#f8fafc")),
        tickfont = list(color = "#94a3b8"))
    ) %>% layout(
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
      xaxis = list(title = "fut_lag (months, to scale)", type = "linear",
                   tickvals = fut_lags, ticktext = as.character(fut_lags),
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "id (1-12 long, 13-19 short)", type = "category",
                   autorange = "reversed",
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      margin = list(l = 90, r = 60, b = 60, t = 30))
  })

  output$payoffScatterMV <- renderPlotly({
    req(app_dataMV_payoff())
    df <- app_dataMV_payoff()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    df$hover <- sprintf(
      "id %d | past %d -> fut %d<br>n holdout: %d<br>avg win %.3f | avg loss %.3f<br>p90 %.3f",
      df$id, df$past_lag, df$fut_lag, as.integer(df$n_holdout),
      df$avg_win, df$avg_loss, df$p90_ret)
    plot_ly(df, x = ~win_pct, y = ~expectancy,
            color = ~factor(fut_lag),
            colors = c('#38bdf8', '#a855f7', '#f59e0b', '#10b981',
                       '#dc2626', '#c4b5fd', '#facc15'),
            size = ~n_holdout, sizes = c(6, 30),
            type = "scatter", mode = "markers",
            marker = list(sizemode = "area", opacity = 0.75),
            customdata = ~hover,
            hovertemplate = paste0(
              "win %: %{x:.1f}<br>",
              "expectancy: %{y:.3f}<br>",
              "%{customdata}<extra></extra>")) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        legend = list(title = list(text = "fut_lag"),
                      font = list(color = "#94a3b8")),
        shapes = list(
          list(type = "line", x0 = 50, x1 = 50, yref = "paper", y0 = 0, y1 = 1,
               line = list(dash = "dash", color = "rgba(255,255,255,0.25)")),
          list(type = "line", y0 = 0, y1 = 0, xref = "paper", x0 = 0, x1 = 1,
               line = list(dash = "dash", color = "rgba(255,255,255,0.25)"))),
        xaxis = list(title = "Win % (holdout, coin flip = 50)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        yaxis = list(title = "Expectancy (mean period return)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        margin = list(l = 70, r = 30, b = 50, t = 30))
  })

  output$payoffHeatmapMV <- renderPlotly({
    req(app_dataMV_payoff())
    df <- app_dataMV_payoff()
    sel <- input$id_valMV
    if (is.null(sel) || sel %in% c("", "ALL")) return(empty_plot("Select a specific id"))
    df <- df[df$id == as.integer(sel), ]
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    fut_lags  <- sort(unique(df$fut_lag))
    past_lags <- sort(unique(df$past_lag))
    z_mat <- matrix(NA_real_, nrow = length(past_lags), ncol = length(fut_lags),
                    dimnames = list(as.character(past_lags), as.character(fut_lags)))
    cd_mat <- matrix("", nrow = length(past_lags), ncol = length(fut_lags))
    for (i in seq_len(nrow(df))) {
      r <- which(past_lags == df$past_lag[i])
      c <- which(fut_lags == df$fut_lag[i])
      if (length(r) && length(c)) {
        z_mat[r, c] <- df$expectancy[i]
        cd_mat[r, c] <- sprintf("win %.1f%% | n %d",
                                df$win_pct[i], as.integer(df$n_holdout[i]))
      }
    }
    colorscale <- DIVERGING_COLORSCALE
    max_abs <- max(abs(z_mat), na.rm = TRUE)
    if (!is.finite(max_abs) || max_abs == 0) max_abs <- 0.1
    text_mat <- matrix(rep(as.character(fut_lags), each = nrow(z_mat)),
                       nrow = nrow(z_mat))
    plot_ly(
      x = fut_lags, y = as.character(past_lags), z = z_mat, type = "heatmap",
      colorscale = colorscale,
      xgap = 1, ygap = 1,
      zmin = -max_abs, zmax = max_abs,
      customdata = cd_mat,
      text = text_mat,
      hovertemplate = paste0(
        "past_lag: %{y}<br>",
        "fut_lag: %{text}<br>",
        "expectancy: %{z:.3f}<br>",
        "%{customdata}<extra></extra>"),
      colorbar = list(
        title = list(text = "Expectancy", font = list(color = "#f8fafc")),
        tickfont = list(color = "#94a3b8"))
    ) %>% layout(
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
      xaxis = list(title = "fut_lag (months, to scale)", type = "linear",
                   tickvals = fut_lags, ticktext = as.character(fut_lags),
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "past_lag (months)", type = "category",
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      margin = list(l = 90, r = 60, b = 60, t = 30))
  })

  output$tierBarMV <- renderPlotly({
    req(app_dataMV_tiers())
    df <- app_dataMV_tiers()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    tier_colors <- TIER_COLORS
    df$tier <- factor(df$tier, levels = names(tier_colors))
    plot_ly(df, x = ~factor(id), y = ~n_cells, color = ~tier,
            colors = tier_colors, type = "bar",
            hovertemplate = "id %{x}<br>%{fullData.name}: %{y} cells<extra></extra>") %>%
      layout(
        barmode = "stack",
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        legend = list(font = list(color = "#94a3b8")),
        xaxis = list(title = "id (1-12 long, 13-19 short)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        yaxis = list(title = "Cells (vol x past x fut x z-bucket)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        margin = list(l = 70, r = 30, b = 50, t = 30))
  })

  output$forestPlotMV <- renderPlotly({
    req(app_dataMV_forest())
    df <- app_dataMV_forest()
    if (nrow(df) == 0) return(empty_plot("Select a specific id (+ past/fut lag), then Generate"))
    df$cell <- sprintf("vol %d | z %d",
                       as.integer(df$vol_bucket_num), as.integer(df$bucket))
    df$hover <- sprintf(
      "n scored: %d | cutoffs: %d | holdout: %d<br>credibility weight: %.3f",
      as.integer(df$n_scored), as.integer(df$n_cutoffs),
      as.integer(df$wf_n_holdout), df$credibility_weight)
    tier_colors <- TIER_COLORS
    df$tier <- factor(df$tier, levels = names(tier_colors))
    plot_ly(df, x = ~wf_agreement, y = ~cell, color = ~tier,
            colors = tier_colors,
            type = "scatter", mode = "markers",
            marker = list(size = 9),
            error_x = list(type = "data", symmetric = FALSE,
                           array = ~(wilson_upper - wf_agreement),
                           arrayminus = ~(wf_agreement - wilson_lower),
                           color = "rgba(148,163,184,0.6)"),
            customdata = ~hover,
            hovertemplate = paste0(
              "%{y}<br>agreement: %{x:.3f}<br>%{customdata}<extra></extra>")) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        legend = list(font = list(color = "#94a3b8")),
        shapes = list(list(type = "line", x0 = 0.5, x1 = 0.5, yref = "paper",
                           y0 = 0, y1 = 1,
                           line = list(dash = "dash",
                                       color = "rgba(255,255,255,0.4)"))),
        xaxis = list(title = "Walk-forward sign agreement (Wilson 95% CI)",
                     range = c(-0.05, 1.05),
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        yaxis = list(title = "", type = "category",
                     categoryorder = "array",
                     categoryarray = rev(sort(unique(df$cell))),
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        margin = list(l = 110, r = 30, b = 50, t = 30))
  })

