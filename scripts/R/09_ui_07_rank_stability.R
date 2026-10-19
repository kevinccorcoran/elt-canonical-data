# AlphaStream dashboard - UI of the rank stability tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
ui_tab_rank_stability <- tabPanel("Rank Stability",
    sidebarLayout(
      make_sidebar("RS", "Rank Stability", tagList(
        selectInput("id_valRS", "Cluster ID",
                    choices = c("Connect first..." = ""), selected = ""),
        sliderInput("top_n_valRS", "Max vingtile to display (1 = top 5%, 20 = bottom 5%)",
                    min = 5, max = 20, value = 20, step = 1),
        selectInput("metric_valRS", "Metric",
                    choices = c("Mean return" = "mean_return",
                                "Median return" = "median_return",
                                "Hit rate (% correct direction)" = "hit_rate",
                                "Sharpe-like (mean/sd)" = "sharpe_like",
                                "Combo: hit + median (quadrants)" = "combo_quadrants",
                                "Combo: hit + mean (quadrants)" = "combo_mean_quadrants"),
                    selected = "mean_return"),
        dateRangeInput("cutoff_rangeRS", "Cutoff range",
                       start = NULL, end = NULL,
                       startview = "year", separator = " to ")
      )),
      mainPanel(div(class = "main-card",
        h4("Rank stability across walk-forward cohorts",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #f59e0b; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; line-height: 1.4;
                   margin-bottom: 0.75rem;",
          tags$b(style = "color: #fbbf24;", "Caveat: "),
          "cluster_id is NOT identity-stable across walk-forward refits. ",
          "Treat the cluster selector as a ", tags$i("growth-tier alignment"),
          " filter (tickers that landed in the same growth-vol regime as the selected cluster id at each cutoff), ",
          "not as a fixed-membership cohort. ",
          "Slot performance is cluster-agnostic by design (aggregated across all 84 cohorts)."
        ),
        textOutput("cutoffCountRS"),
        tags$br(),
        div(style = "display: flex; align-items: center; gap: 0.75rem;",
            h5("Slot performance - mean realized forward return per rank slot",
               style = "color: #f8fafc; font-weight: 600; margin: 0; flex: 1;"),
            checkboxInput("show_slot_perfRS", "Show", value = TRUE)
        ),
        conditionalPanel(
          condition = "input.show_slot_perfRS",
          plotlyOutput("slotPerfPlotRS", height = "380px")
        ),
        tags$br(),
        div(style = "display: flex; align-items: center; gap: 0.75rem;",
            h5("Vingtile (5% bin) vs fut_lag heatmap - mean realized return at each (vingtile, horizon) across 84 cohorts. Rank normalized to within-cluster vingtile so different cluster sizes compare fairly.",
               style = "color: #f8fafc; font-weight: 600; margin: 0; flex: 1;"),
            checkboxInput("show_vingtile_heatmapRS", "Show", value = TRUE)
        ),
        conditionalPanel(
          condition = "input.show_vingtile_heatmapRS",
          plotlyOutput("stabilityHeatmapRS", height = "800px")
        ),
        tags$br(),
        actionButton("execute_all_RS", "Generate small-multiples for ALL ids",
                     class = "btn-primary", style = "margin-bottom: 1rem;"),
        uiOutput("rsAllIdsCap"),
        plotlyOutput("allIdsGridRS", height = "1200px")
      ))
    )
  )

  # ── Tab 8: Model Validation ──
