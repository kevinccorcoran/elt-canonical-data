# AlphaStream dashboard - UI of the model validation tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
ui_tab_model_validation <- tabPanel("Model Validation",
    sidebarLayout(
      make_sidebar("MV", "Model Validation", tagList(
        selectInput("id_valMV", "Cluster ID",
                    choices = c("Connect first..." = ""), selected = ""),
        selectInput("past_lagMV", "Past lag (Wilson forest)",
                    choices = c("Connect first..." = ""), selected = ""),
        selectInput("fut_lagMV", "Future lag (Wilson forest)",
                    choices = c("Connect first..." = ""), selected = "")
      )),
      mainPanel(div(class = "main-card",
        h4("Walk-forward IC",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        plotlyOutput("icHeatmapMV", height = "600px"),
        tags$br(),
        h4("Holdout payoff backtest",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #f59e0b; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; line-height: 1.4;
                   margin-bottom: 0.75rem;",
          tags$b(style = "color: #fbbf24;", "Caveat: "),
          "read matched short horizons only (past_lag = fut_lag, fut_lag <= 12); ",
          "longer horizons have overlapping holdout windows and read noisy."
        ),
        plotlyOutput("payoffScatterMV", height = "460px"),
        tags$br(),
        h5("Expectancy heatmap (past_lag x fut_lag) - select a specific id",
           style = "color: #f8fafc; font-weight: 600;"),
        plotlyOutput("payoffHeatmapMV", height = "420px"),
        tags$br(),
        h4("Cell credibility",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        h5("Cells by tier per id (high/medium = actionable, thin = n<10, anti = reliably wrong)",
           style = "color: #f8fafc; font-weight: 600;"),
        plotlyOutput("tierBarMV", height = "380px"),
        tags$br(),
        h5("Wilson forest - selected (id, past_lag, fut_lag); dashed line = coin flip 0.5",
           style = "color: #f8fafc; font-weight: 600;"),
        plotlyOutput("forestPlotMV", height = "460px")
      ))
    )
  )

  # ── Tab 9: Buy List ──
