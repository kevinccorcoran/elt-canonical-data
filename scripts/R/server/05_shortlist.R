# AlphaStream dashboard - Shortlist (top picks) tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # ── TOP PICKS: Reactive values ──
  app_dataP <- reactiveVal(NULL)
  cluster_ic_metaP <- reactiveVal(NULL)
  status_msgP <- reactiveVal("Ready")
  output$statusMessageP <- renderText({ status_msgP() })

  # ── TOP PICKS: Connect ──
  observeEvent(input$connect_btn, {
    if (input$db_pass == "") { status_msgP("Error: Password is not set."); return() }
    status_msgP("Connecting...")
    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      id_vals <- dbGetQuery(con,
        "SELECT DISTINCT id FROM serving.return_cluster_ticker_summary_current ORDER BY 1")
      updateSelectInput(session, "id_valP", choices = id_vals[[1]], selected = id_vals[[1]][1])
      status_msgP("Filters loaded!")
    }, error = function(e) { status_msgP(paste("Error:", e$message)) })
  })

  # ── TOP PICKS: Execute ──
  observeEvent(input$execute_P, {
    if (input$db_pass == "") { status_msgP("Error: Password is not set."); return() }
    if (input$id_valP == "") { status_msgP("Error: Select a cluster first."); return() }
    status_msgP("Running query...")

    query <- sprintf("
      WITH top_picks AS (
          SELECT ticker, agg_rank, coverage_cell_count, agg_directional_score
          FROM serving.return_cluster_ticker_summary_current
          WHERE id = %s AND agg_rank <= %d
      )
      SELECT
          tp.agg_rank,
          tp.ticker,
          tp.coverage_cell_count,
          tp.agg_directional_score,
          p.fut_lag,
          AVG(
              CASE
                  WHEN p.recommendation IN ('STRONG_PICK','BUY','OUTLIER_BUY')   THEN  c.wilson_lower - 0.5
                  WHEN p.recommendation IN ('AVOID','SIGNAL_TRAP','OUTLIER_AVOID') THEN -(c.wilson_lower - 0.5)
              END
          ) FILTER (
              WHERE c.wilson_lower IS NOT NULL
                AND p.recommendation IN
                    ('STRONG_PICK','BUY','AVOID','SIGNAL_TRAP','OUTLIER_BUY','OUTLIER_AVOID')
          ) AS signed_edge,
          AVG(c.wilson_lower) FILTER (
              WHERE c.wilson_lower IS NOT NULL
                AND p.recommendation IN
                    ('STRONG_PICK','BUY','AVOID','SIGNAL_TRAP','OUTLIER_BUY','OUTLIER_AVOID')
          ) AS avg_sign_agreement,
          COUNT(*) FILTER (
              WHERE c.wilson_lower IS NOT NULL
                AND p.recommendation IN
                    ('STRONG_PICK','BUY','AVOID','SIGNAL_TRAP','OUTLIER_BUY','OUTLIER_AVOID')
          ) AS n_directional_cells_with_cred,
          COUNT(*) FILTER (
              WHERE p.recommendation IN
                  ('STRONG_PICK','BUY','AVOID','SIGNAL_TRAP','OUTLIER_BUY','OUTLIER_AVOID')
          ) AS n_directional_cells_total,
          COUNT(*) AS n_total_cells
      FROM top_picks tp
      JOIN serving.return_cluster_ticker_pair_current p
        ON p.ticker = tp.ticker AND p.id = %s
      -- 2026-07-24 grain fix: cell_credibility no longer carries
      -- vol_bucket_num; join on the cell shape alone.
      LEFT JOIN validation.cell_credibility c
        ON c.id            = p.id
       AND c.past_lag      = p.past_lag
       AND c.fut_lag       = p.fut_lag
       AND c.bucket        = p.bucket
      GROUP BY tp.agg_rank, tp.ticker, tp.coverage_cell_count,
               tp.agg_directional_score, p.fut_lag
      ORDER BY tp.agg_rank, p.fut_lag;",
      input$id_valP, input$top_n_valP, input$id_valP)

    tryCatch({
      con <- get_con(input)
      on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
      res <- dbGetQuery(con, query)
      res$agg_rank <- as.integer(res$agg_rank)
      res$fut_lag <- as.integer(res$fut_lag)
      res$coverage_cell_count <- as.integer(res$coverage_cell_count)
      res$signed_edge <- as.numeric(res$signed_edge)
      res$avg_sign_agreement <- as.numeric(res$avg_sign_agreement)
      res$n_directional_cells_with_cred  <- as.integer(res$n_directional_cells_with_cred)
      res$n_directional_cells_total      <- as.integer(res$n_directional_cells_total)
      res$n_directional_cells            <- res$n_directional_cells_with_cred
      res$n_total_cells <- as.integer(res$n_total_cells)
      app_dataP(res)

      ic_query <- sprintf("
        SELECT
          ROUND(mean_ic::numeric, 4) AS mean_ic,
          ROUND(median_ic::numeric, 4) AS median_ic,
          n_cohorts,
          n_positive,
          ROUND(mean_decile_spread::numeric, 4) AS mean_decile_spread
        FROM validation.walk_forward_id_ic
        WHERE id = %s AND fut_lag = 12;",
        input$id_valP)
      ic_df <- tryCatch(dbGetQuery(con, ic_query), error = function(e) NULL)
      cluster_ic_metaP(ic_df)

      status_msgP(paste("Loaded", nrow(res), "rows."))
    }, error = function(e) {
      app_dataP(NULL); cluster_ic_metaP(NULL)
      status_msgP(paste("Error:", e$message))
    })
  })

  output$clusterIcDisplayP <- renderText({
    df <- cluster_ic_metaP()
    if (is.null(df) || nrow(df) == 0 || is.na(df$mean_ic[1])) {
      return("Walk-forward IC: no data for this cluster (run walk_forward_ticker_ic.py)")
    }
    sprintf(
      "Walk-forward IC for cluster %s (fut_lag=12, %d cohorts): mean=%.4f  median=%.4f  positive=%d/%d  decile_spread=%.4f",
      input$id_valP,
      as.integer(df$n_cohorts[1]),
      as.numeric(df$mean_ic[1]),
      as.numeric(df$median_ic[1]),
      as.integer(df$n_positive[1]),
      as.integer(df$n_cohorts[1]),
      as.numeric(df$mean_decile_spread[1])
    )
  })

  # ── TOP PICKS: Render heatmap ──
  output$topPicksPlot <- renderPlotly({
    req(app_dataP())
    df <- app_dataP()
    if (nrow(df) == 0) return(empty_plot("No data matched your filters."))

    df <- df[order(df$agg_rank, df$fut_lag), ]
    row_keys <- unique(df[, c("agg_rank", "ticker", "coverage_cell_count")])
    row_keys <- row_keys[order(row_keys$agg_rank), ]
    row_labels <- sprintf("#%d %s (n=%d)",
                          row_keys$agg_rank, row_keys$ticker, row_keys$coverage_cell_count)

    fut_lag_vals <- sort(unique(df$fut_lag))
    # Proportional spacing via sqrt(fut_lag) so adjacent small lags (1,2) stay
    # similar in width while larger lags (20,33) get visibly wider cells.
    # Linear axis with sqrt-positioned ticks; tick labels show real fut_lag.
    fut_lag_pos  <- sqrt(fut_lag_vals)

    z_mat   <- matrix(NA_real_, nrow = nrow(row_keys), ncol = length(fut_lag_vals),
                      dimnames = list(row_labels, as.character(fut_lag_vals)))
    sa_mat  <- matrix(NA_real_, nrow = nrow(row_keys), ncol = length(fut_lag_vals),
                      dimnames = list(row_labels, as.character(fut_lag_vals)))
    n_mat   <- matrix(NA_integer_, nrow = nrow(row_keys), ncol = length(fut_lag_vals),
                      dimnames = list(row_labels, as.character(fut_lag_vals)))
    for (i in seq_len(nrow(df))) {
      row_i <- which(row_keys$agg_rank == df$agg_rank[i] & row_keys$ticker == df$ticker[i])
      col_i <- which(fut_lag_vals == df$fut_lag[i])
      if (length(row_i) > 0 && length(col_i) > 0) {
        z_mat[row_i, col_i]  <- df$signed_edge[i]
        sa_mat[row_i, col_i] <- df$avg_sign_agreement[i]
        n_mat[row_i, col_i]  <- df$n_directional_cells[i]
      }
    }

    max_abs <- 0.3

    colorscale <- list(
      c(0.00, '#dc2626'),
      c(0.25, '#f97316'),
      c(0.50, '#facc15'),
      c(0.75, '#84cc16'),
      c(1.00, '#16a34a')
    )

    custom_arr <- array(NA_real_, dim = c(dim(z_mat), 2))
    custom_arr[,,1] <- sa_mat
    custom_arr[,,2] <- n_mat

    plot_ly(
      x = fut_lag_pos, y = row_labels, z = z_mat, type = "heatmap",
      colorscale = colorscale, zmin = -max_abs, zmax = max_abs,
      customdata = custom_arr,
      text = matrix(rep(as.character(fut_lag_vals), each = nrow(z_mat)), nrow = nrow(z_mat)),
      hovertemplate = paste0(
        "%{y}<br>",
        "fut_lag: %{text}<br>",
        "signed edge (wilson_lower - 0.5) x dir: %{z:.3f}<br>",
        "avg wilson_lower: %{customdata[0]:.3f}<br>",
        "n directional cells: %{customdata[1]}<extra></extra>"),
      colorbar = list(
        title = list(text = "Signed edge\n(wilson_lower\n- 0.5, × dir;\n0 = coin flip)",
                     font = list(color = "#f8fafc")),
        tickfont = list(color = "#94a3b8"))
    ) %>% layout(
      title = list(
        text = sprintf("Cluster %s — Top %d picks: signed reliability edge (green=reliable BUY, red=reliable AVOID, yellow=coin flip)",
                       input$id_valP, input$top_n_valP),
        font = list(color = "#f8fafc", family = "Inter", size = 14)),
      paper_bgcolor = "rgba(0,0,0,0)", plot_bgcolor = "rgba(0,0,0,0)",
      xaxis = list(title = "Future lag (months, sqrt-spaced)", type = "linear",
                   tickvals = fut_lag_pos, ticktext = as.character(fut_lag_vals),
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      yaxis = list(title = "Series (by agg_rank)", type = "category",
                   autorange = "reversed",
                   color = "#94a3b8", gridcolor = "rgba(255,255,255,0.1)"),
      margin = list(l = 160, r = 60, b = 60, t = 60)
    )
  })

