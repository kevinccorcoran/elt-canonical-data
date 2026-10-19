# AlphaStream dashboard - Rank Stability tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # ── RANK STABILITY: Reactive values ──
  app_dataRS_slot     <- reactiveVal(NULL)
  app_dataRS_heatmap  <- reactiveVal(NULL)
  app_dataRS_meta     <- reactiveVal(NULL)
  status_msgRS <- reactiveVal("Ready")
  output$statusMessageRS <- renderText({ status_msgRS() })

  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgRS("Error: Password is not set."); return() }
    status_msgRS("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      # fut_lag <= 33 matches every render path in the tab: an id whose only
      # evidence is at the banned 54/88mo horizons (e.g. id 1) would otherwise
      # list here but chart empty, so keep it out of the selector.
      id_vals <- dbGetQuery(con,
        "SELECT DISTINCT id FROM validation.walk_forward_pctile_summary
         WHERE fut_lag <= 33 ORDER BY id")
      cl_choices <- c("All ids" = "ALL",
                      setNames(as.character(id_vals$id), as.character(id_vals$id)))
      updateSelectInput(session, "id_valRS", choices = cl_choices, selected = "ALL")
      cutoff_bounds <- dbGetQuery(con,
        "SELECT MIN(train_cutoff_date) AS min_d, MAX(train_cutoff_date) AS max_d
         FROM validation.walk_forward_ticker_rank")
      if (!is.na(cutoff_bounds$min_d[1]) && !is.na(cutoff_bounds$max_d[1])) {
        updateDateRangeInput(session, "cutoff_rangeRS",
                             start = cutoff_bounds$min_d[1],
                             end   = cutoff_bounds$max_d[1],
                             min   = cutoff_bounds$min_d[1],
                             max   = cutoff_bounds$max_d[1])
      }
      status_msgRS("Filters loaded!")
    }, error = function(e) { status_msgRS(paste("Error:", e$message)) })
  })

  # Auto-tier the display depth to the selected cluster's size: 5 / 10 / 20.
  observeEvent(input$id_valRS, {
    id_sel <- input$id_valRS
    if (is.null(id_sel) || id_sel == "" || id_sel == "ALL") {
      updateSliderInput(session, "top_n_valRS", value = 20); return()
    }
    con <- tryCatch(get_con(input), error = function(e) NULL)
    # register cleanup BEFORE the early return so a half-open connection
    # can't leak when con is NULL-but-partially-created
    on.exit({ if (!is.null(con) && DBI::dbIsValid(con)) try(dbDisconnect(con), silent = TRUE) }, add = TRUE)
    if (is.null(con)) return()
    med <- tryCatch(dbGetQuery(con, sprintf(
      "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY r.cluster_size) AS med
       FROM validation.walk_forward_ticker_rank r
       JOIN validation.walk_forward_cluster_id_map m
         ON m.train_cutoff_date = r.train_cutoff_date AND m.cluster_id = r.cluster_id
       WHERE m.id = %s AND r.fut_lag = 12;", id_sel))$med[1],
      error = function(e) NA_real_)
    if (length(med) == 0 || is.na(med)) return()
    tier <- if (med >= 20) 20L else if (med >= 10) 10L else 5L
    updateSliderInput(session, "top_n_valRS", value = tier)
  }, ignoreInit = TRUE)

  observeEvent(input$execute_RS, {
    if (input$db_pass == "") { status_msgRS("Error: Password is not set."); return() }
    if (is.null(input$id_valRS) || input$id_valRS == "") {
      status_msgRS("Error: Select a cluster (or 'All clusters') first."); return()
    }
    status_msgRS("Running queries...")
    date_lo <- format(input$cutoff_rangeRS[1], "%Y-%m-%d")
    date_hi <- format(input$cutoff_rangeRS[2], "%Y-%m-%d")
    slot_cluster_filter <- if (input$id_valRS == "ALL") "" else
      sprintf("AND m.id = %s", input$id_valRS)

    slot_query <- sprintf("
      SELECT
        r.rank_within_cluster,
        COUNT(*) AS n_obs,
        AVG(r.forward_return) AS mean_fwd,
        STDDEV(r.forward_return) AS sd_fwd
      FROM validation.walk_forward_ticker_rank r
      JOIN validation.walk_forward_cluster_id_map m
        ON m.train_cutoff_date = r.train_cutoff_date
       AND m.cluster_id = r.cluster_id
      WHERE r.train_cutoff_date BETWEEN '%s' AND '%s' %s
      GROUP BY r.rank_within_cluster
      ORDER BY r.rank_within_cluster;",
      date_lo, date_hi, slot_cluster_filter)

    summary_cluster_filter <- if (input$id_valRS == "ALL") "" else
      sprintf("AND id = %s", input$id_valRS)
    metric_col <- input$metric_valRS
    # combo_quadrants is computed in R (needs both hit_rate and median_return),
    # not a column in the table. Use a special SQL that returns both, then
    # combine in the render function.
    if (metric_col == "combo_quadrants") {
      # Align median per-cluster INSIDE the aggregation (long: +1, short: -1),
      # otherwise the weighted average of raw medians across long+short clusters
      # cancels out when ALL ids are selected.
      topn_query <- sprintf("
        SELECT
          pctile_bin AS rank_within_cluster,
          fut_lag,
          SUM(hit_rate * n_obs) / NULLIF(SUM(n_obs), 0) AS hit_rate,
          SUM(
            (CASE WHEN id <= 12 THEN median_return ELSE -median_return END)
            * n_obs
          ) / NULLIF(SUM(n_obs), 0) AS aligned_median,
          SUM(n_obs) AS n_tickers
        FROM validation.walk_forward_pctile_summary
        WHERE pctile_bin <= %d
          AND fut_lag <= 33
          %s
        GROUP BY pctile_bin, fut_lag
        ORDER BY pctile_bin, fut_lag;",
        input$top_n_valRS, summary_cluster_filter)
    } else if (metric_col == "combo_mean_quadrants") {
      # Same shape as combo_quadrants but uses mean_return instead of
      # median_return for the magnitude axis.
      topn_query <- sprintf("
        SELECT
          pctile_bin AS rank_within_cluster,
          fut_lag,
          SUM(hit_rate * n_obs) / NULLIF(SUM(n_obs), 0) AS hit_rate,
          SUM(
            (CASE WHEN id <= 12 THEN mean_return ELSE -mean_return END)
            * n_obs
          ) / NULLIF(SUM(n_obs), 0) AS aligned_median,
          SUM(n_obs) AS n_tickers
        FROM validation.walk_forward_pctile_summary
        WHERE pctile_bin <= %d
          AND fut_lag <= 33
          %s
        GROUP BY pctile_bin, fut_lag
        ORDER BY pctile_bin, fut_lag;",
        input$top_n_valRS, summary_cluster_filter)
    } else {
      metric_expr <- if (metric_col %in% c("hit_rate", "sharpe_like", "median_return")) {
        sprintf("AVG(%s)", metric_col)
      } else {
        sprintf("SUM(%s * n_obs) / NULLIF(SUM(n_obs), 0)", metric_col)
      }
      topn_query <- sprintf("
        SELECT
          pctile_bin AS rank_within_cluster,
          fut_lag,
          %s AS realized_return,
          SUM(n_obs) AS n_tickers
        FROM validation.walk_forward_pctile_summary
        WHERE pctile_bin <= %d
          AND fut_lag <= 33
          %s
        GROUP BY pctile_bin, fut_lag
        ORDER BY pctile_bin, fut_lag;",
        metric_expr, input$top_n_valRS, summary_cluster_filter)
    }

    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      slot_df <- dbGetQuery(con, slot_query)
      slot_df$rank_within_cluster <- as.integer(slot_df$rank_within_cluster)
      slot_df$mean_fwd <- as.numeric(slot_df$mean_fwd)
      slot_df$sd_fwd   <- as.numeric(slot_df$sd_fwd)
      slot_df$n_obs    <- as.integer(slot_df$n_obs)
      app_dataRS_slot(slot_df)

      hm_df <- dbGetQuery(con, topn_query)
      if (nrow(hm_df) == 0) {
        app_dataRS_heatmap(data.frame())
      } else {
        hm_df$fut_lag           <- as.integer(hm_df$fut_lag)
        hm_df$rank_within_cluster <- as.integer(hm_df$rank_within_cluster)
        hm_df$n_tickers         <- as.integer(hm_df$n_tickers)
        if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) {
          # Median was already aligned per-cluster inside the SQL aggregation.
          hm_df$hit_rate <- as.numeric(hm_df$hit_rate)
          hm_df$aligned_median <- as.numeric(hm_df$aligned_median)
          hm_df$realized_return <- ifelse(
            hm_df$hit_rate > 0.5 & hm_df$aligned_median > 0, 0.875,
            ifelse(hm_df$hit_rate <= 0.5 & hm_df$aligned_median > 0, 0.625,
              ifelse(hm_df$hit_rate > 0.5 & hm_df$aligned_median <= 0, 0.375,
                0.125)))
        } else {
          hm_df$realized_return <- as.numeric(hm_df$realized_return)
        }
        app_dataRS_heatmap(hm_df)
      }

      n_cut_filter <- if (input$id_valRS == "ALL") "" else
        sprintf("AND m.id = %s", input$id_valRS)
      n_cut <- dbGetQuery(con, sprintf("
        SELECT COUNT(DISTINCT r.train_cutoff_date) AS n
        FROM validation.walk_forward_ticker_rank r
        JOIN validation.walk_forward_cluster_id_map m
          ON m.train_cutoff_date = r.train_cutoff_date
         AND m.cluster_id = r.cluster_id
        WHERE r.train_cutoff_date BETWEEN '%s' AND '%s' %s;",
        date_lo, date_hi, n_cut_filter))$n[1]
      app_dataRS_meta(list(n_cutoffs = n_cut, lo = date_lo, hi = date_hi,
                           cluster = input$id_valRS,
                           metric = input$metric_valRS))
      status_msgRS(sprintf("Loaded: %d slot rows, %d cutoffs.",
                           as.integer(nrow(slot_df)), as.integer(n_cut)))
    }, error = function(e) {
      app_dataRS_slot(NULL); app_dataRS_heatmap(NULL); app_dataRS_meta(NULL)
      status_msgRS(paste("Error:", e$message))
    })
  })

  output$cutoffCountRS <- renderText({
    meta <- app_dataRS_meta()
    if (is.null(meta)) return("")
    sprintf("Cluster filter: %s   |   %d distinct cutoffs in window [%s -> %s]",
            meta$cluster, as.integer(meta$n_cutoffs), meta$lo, meta$hi)
  })

  output$slotPerfPlotRS <- renderPlotly({
    req(app_dataRS_slot())
    df <- app_dataRS_slot()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))
    df$se <- df$sd_fwd / sqrt(pmax(df$n_obs, 1))
    plot_ly(df, x = ~rank_within_cluster, y = ~mean_fwd,
            type = "scatter", mode = "lines+markers",
            error_y = list(type = "data", array = ~se,
                           color = "rgba(148,163,184,0.5)"),
            marker = list(size = 8, color = "#38bdf8"),
            line = list(color = "#38bdf8", width = 2),
            hovertemplate = paste0(
              "rank slot: %{x}<br>",
              "mean fwd return: %{y:.4f}<br>",
              "n obs: %{customdata}<extra></extra>"),
            customdata = ~n_obs) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        xaxis = list(title = "rank_within_cluster (1 = top)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
        yaxis = list(title = "Mean forward return (+/- SE)",
                     color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)",
                     zeroline = TRUE, zerolinecolor = "rgba(255,255,255,0.25)"),
        margin = list(l = 70, r = 30, b = 50, t = 30))
  })

  output$stabilityHeatmapRS <- renderPlotly({
    req(app_dataRS_heatmap())
    df <- app_dataRS_heatmap()
    if (nrow(df) == 0) return(empty_plot("No tickers met threshold."))
    fut_lags <- sort(unique(df$fut_lag))
    ranks    <- sort(unique(df$rank_within_cluster))
    z_mat <- matrix(NA_real_, nrow = length(ranks), ncol = length(fut_lags),
                    dimnames = list(as.character(ranks), as.character(fut_lags)))
    n_mat <- matrix(NA_integer_, nrow = length(ranks), ncol = length(fut_lags),
                    dimnames = list(as.character(ranks), as.character(fut_lags)))
    for (i in seq_len(nrow(df))) {
      r <- which(ranks == df$rank_within_cluster[i])
      c <- which(fut_lags == df$fut_lag[i])
      if (length(r) && length(c)) {
        z_mat[r, c] <- df$realized_return[i]
        n_mat[r, c] <- df$n_tickers[i]
      }
    }
    metric <- isolate(app_dataRS_meta()$metric)
    if (is.null(metric)) metric <- "mean_return"
    if (metric == "hit_rate") {
      colorscale <- DIVERGING_COLORSCALE   # centered at coin flip 0.5
      max_abs <- 0.2
      z_center <- 0.5
    } else if (metric == "sharpe_like") {
      colorscale <- list(
        c(0.00, '#dc2626'),
        c(0.25, '#f97316'),
        c(0.50, '#facc15'),
        c(0.75, '#84cc16'),
        c(1.00, '#16a34a')
      )
      max_abs <- 1
      z_center <- 0
    } else if (metric %in% c("combo_quadrants", "combo_mean_quadrants")) {
      # Discrete bands: gray (neither), blue (hit only),
      # gold (median only = Bessembinder), green (both high).
      # Short clusters (id > 12) get a purple palette so green never appears
      # on declining clusters; longs keep the green palette.
      sel_id <- suppressWarnings(as.integer(input$id_valRS))
      is_short_selected <- !is.na(sel_id) && sel_id > 12
      colorscale <- if (is_short_selected) {
        list(
          c(0.00, '#888888'), c(0.25, '#888888'),
          c(0.25, '#c4b5fd'), c(0.50, '#c4b5fd'),
          c(0.50, '#a78bfa'), c(0.75, '#a78bfa'),
          c(0.75, '#7c3aed'), c(1.00, '#7c3aed')
        )
      } else {
        list(
          c(0.00, '#888888'), c(0.25, '#888888'),
          c(0.25, '#3b82f6'), c(0.50, '#3b82f6'),
          c(0.50, '#f59e0b'), c(0.75, '#f59e0b'),
          c(0.75, '#10b981'), c(1.00, '#10b981')
        )
      }
      max_abs <- 0.5
      z_center <- 0.5
    } else {
      colorscale <- list(
        c(0.00, '#dc2626'),
        c(0.25, '#f97316'),
        c(0.50, '#facc15'),
        c(0.75, '#84cc16'),
        c(1.00, '#16a34a')
      )
      max_abs <- 50
      p95 <- quantile(abs(z_mat - 0), 0.95, na.rm = TRUE)
      if (is.finite(p95) && p95 > 0) max_abs <- min(max_abs, max(p95, 5))
      z_center <- 0
    }
    metric_label <- switch(metric,
      "hit_rate" = "Hit rate\n(coin flip = 0.5)",
      "sharpe_like" = "Sharpe-like\n(mean/sd)",
      "median_return" = "Median\nreturn",
      "combo_quadrants" = "Quadrant\n0.875=both high\n0.625=median only\n(Bessembinder)\n0.375=hit only\n0.125=neither",
      "combo_mean_quadrants" = "Quadrant (mean)\n0.875=both high\n0.625=mean only\n(Bessembinder)\n0.375=hit only\n0.125=neither",
      "Mean\nrealized\nreturn")
    fut_lag_pos <- fut_lags   # linear spacing: cell width proportional to actual horizon
    text_mat <- matrix(rep(as.character(fut_lags), each = nrow(z_mat)), nrow = nrow(z_mat))
    plot_ly(
      x = fut_lag_pos, y = as.character(ranks), z = z_mat, type = "heatmap",
      colorscale = colorscale,
      xgap = 1, ygap = 1,
      zmin = z_center - max_abs, zmax = z_center + max_abs,
      customdata = n_mat,
      text = text_mat,
      hovertemplate = paste0(
        "rank: %{y}<br>",
        "fut_lag: %{text}<br>",
        metric_label, ": %{z:.4f}<br>",
        "n observations: %{customdata}<extra></extra>"),
      colorbar = list(
        title = list(text = metric_label, font = list(color = "#f8fafc")),
        tickfont = list(color = "#94a3b8"))
    ) %>% layout(
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
      xaxis = list(title = "fut_lag (months, to scale)", type = "linear",
                   tickvals = fut_lag_pos, ticktext = as.character(fut_lags),
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "vingtile (1 = top 5%, 20 = bottom 5%)",
                   type = "category",
                   autorange = "reversed",
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      margin = list(l = 90, r = 60, b = 60, t = 30))
  })

  # ── RANK STABILITY: Small-multiples grid of all ids ──
  app_dataRS_allIds <- reactiveVal(NULL)

  observeEvent(input$execute_all_RS, {
    if (input$db_pass == "") { status_msgRS("Error: Password is not set."); return() }
    status_msgRS("Querying all ids...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      df <- dbGetQuery(con, "
        SELECT id, pctile_bin, fut_lag, mean_return, median_return, hit_rate, sharpe_like, n_obs
        FROM validation.walk_forward_pctile_summary
        WHERE fut_lag <= 33
        ORDER BY id, pctile_bin, fut_lag;")
      df$id <- as.integer(df$id)
      df$pctile_bin <- as.integer(df$pctile_bin)
      df$fut_lag <- as.integer(df$fut_lag)
      df$mean_return <- as.numeric(df$mean_return)
      df$median_return <- as.numeric(df$median_return)
      df$hit_rate <- as.numeric(df$hit_rate)
      df$sharpe_like <- as.numeric(df$sharpe_like)
      # n_obs arrives from Postgres as integer64 (bit64). Left as-is, it is the
      # weight in weighted.mean() during the adaptive rebin below, where bit64
      # arithmetic truncates the fractional metric to an integer BEFORE dividing
      # (hit_rate 0.75 -> 0), collapsing every long cell to hit<=0.5 and washing
      # all green into gold. Cast to double so the weighting is exact.
      df$n_obs <- as.numeric(df$n_obs)
      tiers <- dbGetQuery(con, "
        SELECT m.id AS id,
               CASE WHEN percentile_cont(0.5) WITHIN GROUP (ORDER BY r.cluster_size) >= 20 THEN 20
                    WHEN percentile_cont(0.5) WITHIN GROUP (ORDER BY r.cluster_size) >= 10 THEN 10
                    ELSE 5 END AS tier
        FROM validation.walk_forward_ticker_rank r
        JOIN validation.walk_forward_cluster_id_map m
          ON m.train_cutoff_date = r.train_cutoff_date AND m.cluster_id = r.cluster_id
        WHERE r.fut_lag = 12
        GROUP BY m.id;")
      tier_map <- setNames(as.integer(tiers$tier), as.character(as.integer(tiers$id)))
      df$tier <- tier_map[as.character(df$id)]
      df$tier[is.na(df$tier)] <- 20L
      df$active_metric <- input$metric_valRS
      app_dataRS_allIds(df)
      present_ids <- sort(unique(suppressWarnings(as.integer(df$id))))
      miss_ids <- setdiff(1:19, present_ids)
      msg <- sprintf("Loaded all ids: %d rows across %d distinct ids.",
                     as.integer(nrow(df)), length(present_ids))
      if (length(miss_ids))
        msg <- sprintf("%s Absent under the ≤33mo cap: id %s (evidence only at legacy 54/88mo).",
                       msg, paste(miss_ids, collapse = ", "))
      status_msgRS(msg)
    }, error = function(e) {
      app_dataRS_allIds(NULL)
      status_msgRS(paste("Error:", e$message))
    })
  })

  # Dynamic caption: the legend is fixed, but the lead line reports the LIVE id
  # count (of 19) and names any id absent under the ≤33mo cap (e.g. id 1, whose
  # only evidence is at the banned 54/88mo horizons), so it never lies "all 19".
  output$rsAllIdsCap <- renderUI({
    legend <- paste0("green = model was right (sign-adjusted: longs up = green, ",
      "shorts down = profitable short). Combo - longs (id 1-12): gray=neither, ",
      "blue=hit only, gold=Bessembinder, green=both high. Shorts (id 13-19): ",
      "gray=neither, light purple=hit only, medium purple=Bessembinder, ",
      "deep purple=both high (shorting worked).")
    df <- app_dataRS_allIds()
    lead <- if (is.null(df) || !nrow(df)) {
      "The 19 growth-vol ids (id 1-12 long / 13-19 short)"
    } else {
      present <- sort(unique(suppressWarnings(as.integer(df$id))))
      miss <- setdiff(1:19, present)
      base <- sprintf("Showing %d of 19 growth-vol ids (id 1-12 long / 13-19 short)", length(present))
      if (length(miss))
        sprintf("%s - absent under the ≤33mo cap: id %s (evidence only at legacy 54/88mo)",
                base, paste(miss, collapse = ", "))
      else base
    }
    h5(paste0(lead, " - ", legend), style = "color: #f8fafc; font-weight: 600;")
  })

  output$allIdsGridRS <- renderPlotly({
    req(app_dataRS_allIds())
    df_subset <- app_dataRS_allIds()
    if (nrow(df_subset) == 0) return(empty_plot("No data matched your filters."))

    # Adaptive bins: collapse the 20 vingtiles into each cluster's tier (5/10/20)
    # so small clusters render fewer rows. Rebin the raw metrics (weighted by
    # n_obs) BEFORE the signal is derived, so the quadrant recomputes on the
    # merged bin instead of averaging categorical codes.
    if (!is.null(df_subset$tier)) {
      df_subset$pctile_bin <- pmax(1L, as.integer(ceiling(df_subset$pctile_bin * df_subset$tier / 20)))
      df_subset <- df_subset %>%
        group_by(id, tier, pctile_bin, fut_lag, active_metric) %>%
        summarise(
          mean_return   = weighted.mean(mean_return,   n_obs, na.rm = TRUE),
          median_return = weighted.mean(median_return, n_obs, na.rm = TRUE),
          hit_rate      = weighted.mean(hit_rate,      n_obs, na.rm = TRUE),
          sharpe_like   = weighted.mean(sharpe_like,   n_obs, na.rm = TRUE),
          n_obs         = sum(n_obs, na.rm = TRUE),
          .groups = "drop") %>%
        as.data.frame()
    }

    metric_col <- df_subset$active_metric[1]
    if (is.null(metric_col) || is.na(metric_col)) metric_col <- "mean_return"

    if (metric_col == "hit_rate") {
      df_subset$signal <- df_subset$hit_rate - 0.5
      max_abs <- 0.2
      z_center <- 0
    } else if (metric_col == "sharpe_like") {
      df_subset$direction_sign <- ifelse(df_subset$id <= 12, 1, -1)
      df_subset$signal <- df_subset$direction_sign * df_subset$sharpe_like
      max_abs <- 1
      z_center <- 0
    } else if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) {
      # Quadrant categorical view: hit_rate crossed with aligned return.
      # combo_quadrants uses median_return; combo_mean_quadrants uses mean_return.
      df_subset$direction_sign <- ifelse(df_subset$id <= 12, 1, -1)
      df_subset$aligned_median <- if (metric_col == "combo_quadrants") {
        df_subset$direction_sign * df_subset$median_return
      } else {
        df_subset$direction_sign * df_subset$mean_return
      }
      df_subset$signal <- ifelse(
        df_subset$hit_rate > 0.5 & df_subset$aligned_median > 0, 0.875,
        ifelse(df_subset$hit_rate <= 0.5 & df_subset$aligned_median > 0, 0.625,
          ifelse(df_subset$hit_rate > 0.5 & df_subset$aligned_median <= 0, 0.375,
            0.125)))
      max_abs <- 1
      z_center <- 0.5
    } else {
      val_col <- if (metric_col == "median_return") "median_return" else "mean_return"
      df_subset$val <- df_subset[[val_col]]
      horizon_median <- aggregate(val ~ fut_lag, data = df_subset,
                                  FUN = function(x) median(x, na.rm = TRUE))
      names(horizon_median)[2] <- "horizon_median"
      df_subset <- merge(df_subset, horizon_median, by = "fut_lag", all.x = TRUE)
      df_subset$direction_sign <- ifelse(df_subset$id <= 12, 1, -1)
      df_subset$signal <- df_subset$direction_sign *
                         (df_subset$val - df_subset$horizon_median)
      max_abs <- 10
      p90 <- quantile(abs(df_subset$signal), 0.90, na.rm = TRUE)
      if (is.finite(p90) && p90 > 0) max_abs <- min(max_abs, max(p90, 3))
      z_center <- 0
    }
    colorscale <- if (metric_col == "hit_rate") {
      DIVERGING_COLORSCALE
    } else if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) list(
      # Four discrete bands: gray (neither), blue (hit only),
      # gold (median only = Bessembinder), green (both high)
      c(0.00, '#888888'), c(0.25, '#888888'),
      c(0.25, '#3b82f6'), c(0.50, '#3b82f6'),
      c(0.50, '#f59e0b'), c(0.75, '#f59e0b'),
      c(0.75, '#10b981'), c(1.00, '#10b981')
    ) else list(
      c(0.00, '#dc2626'),
      c(0.25, '#f97316'),
      c(0.50, '#facc15'),
      c(0.75, '#84cc16'),
      c(1.00, '#16a34a')
    )

    ids <- sort(unique(df_subset$id))
    n_ids <- length(ids)
    n_cols <- 4
    n_rows <- ceiling(n_ids / n_cols)

    # Consistent axes across all subplots: full vingtile range 1-20, fut_lag from data
    all_ranks <- 1:20
    all_fut_lags <- sort(unique(df_subset$fut_lag))
    all_fut_lag_pos <- sqrt(all_fut_lags)

    plots <- lapply(ids, function(this_id) {
      sub <- df_subset[df_subset$id == this_id, ]
      # Per-cluster row count: 5 / 10 / 20 by its tier (shadows the outer 1:20).
      this_tier <- if (!is.null(sub$tier) && length(sub$tier) && !is.na(sub$tier[1])) sub$tier[1] else 20L
      all_ranks <- 1:this_tier
      # Pad to full vingtile range so all subplots have same axis
      z_mat <- matrix(NA_real_, nrow = length(all_ranks), ncol = length(all_fut_lags),
                      dimnames = list(as.character(all_ranks), as.character(all_fut_lags)))
      for (i in seq_len(nrow(sub))) {
        r <- which(all_ranks == sub$pctile_bin[i])
        c <- which(all_fut_lags == sub$fut_lag[i])
        if (length(r) && length(c)) z_mat[r, c] <- sub$signal[i]
      }
      # Reverse rows so vingtile 1 is at top of plot (matches single-id view)
      z_mat_rev <- z_mat[rev(seq_len(nrow(z_mat))), , drop = FALSE]
      text_mat <- matrix(rep(as.character(all_fut_lags), each = nrow(z_mat_rev)),
                         nrow = nrow(z_mat_rev))
      # combo_quadrants uses categorical [0,1] range; others use centered [-max_abs, max_abs]
      zmin_val <- if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) 0 else -max_abs
      zmax_val <- if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) 1 else  max_abs
      # For combo_quadrants ONLY: short clusters (id > 12) get a purple
      # palette so green never appears on declining clusters. Same 4-tier
      # information (gray/light purple/medium purple/deep purple), distinct
      # palette from the long-cluster green family.
      this_colorscale <- if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants") && this_id > 12) {
        list(
          c(0.00, '#888888'), c(0.25, '#888888'),
          c(0.25, '#c4b5fd'), c(0.50, '#c4b5fd'),
          c(0.50, '#a78bfa'), c(0.75, '#a78bfa'),
          c(0.75, '#7c3aed'), c(1.00, '#7c3aed')
        )
      } else {
        colorscale
      }
      plot_ly(
        x = all_fut_lag_pos,
        y = rev(as.character(all_ranks)),
        z = z_mat_rev,
        type = "heatmap", colorscale = this_colorscale,
        zmin = zmin_val, zmax = zmax_val, showscale = FALSE,
        xgap = 1, ygap = 1,
        text = text_mat,
        hovertemplate = if (metric_col %in% c("combo_quadrants", "combo_mean_quadrants")) paste0(
          "id ", this_id,
          " (", ifelse(this_id <= 12, "long", "short"), ")",
          "<br>vingtile: %{y}<br>fut_lag: %{text}<br>",
          "0.125=neither | 0.375=hit-only | 0.625=median-only (Bessembinder) | 0.875=both-high",
          "<br>value: %{z:.3f}<extra></extra>") else paste0(
          "id ", this_id,
          " (", ifelse(this_id <= 12, "long", "short"), ")",
          "<br>vingtile: %{y}<br>fut_lag: %{text}<br>",
          "model-aligned signal: %{z:+.2f}<extra></extra>")
      ) %>% layout(
        annotations = list(
          list(text = paste0("id ", this_id),
               xref = "paper", yref = "paper", x = 0.5, y = 1.05,
               showarrow = FALSE,
               font = list(color = "#f8fafc", size = 11, family = "Inter"))
        ),
        yaxis = list(type = "category",
                     categoryorder = "array",
                     categoryarray = rev(as.character(all_ranks)),
                     tickvals = c("1","5","10","15","20"),
                     showgrid = FALSE, zeroline = FALSE,
                     color = "#94a3b8", tickfont = list(size = 8)),
        xaxis = list(type = "linear",
                     tickvals = all_fut_lag_pos,
                     ticktext = as.character(all_fut_lags),
                     showgrid = FALSE, zeroline = FALSE,
                     color = "#94a3b8", tickfont = list(size = 8))
      )
    })

    subplot(plots, nrows = n_rows, shareX = FALSE, shareY = FALSE,
            margin = 0.025, titleY = TRUE) %>%
      layout(
        paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
        showlegend = FALSE,
        font = list(color = "#94a3b8", size = 9),
        margin = list(l = 40, r = 30, b = 40, t = 40))
  })

