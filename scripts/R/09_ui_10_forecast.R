# AlphaStream dashboard - UI of the forecast tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
ui_tab_forecast <- tabPanel("Forecast",
    sidebarLayout(
      make_sidebar("FC", "Forecast", tagList(
        tags$label("As-of date (stand here in the past)", style = "color: #94a3b8; font-weight: 600;"),
        tags$style(HTML(".shiny-split-layout > div { overflow: visible; }")),
        splitLayout(cellWidths = c("30%", "38%", "32%"),
          selectInput("asof_dayFC", NULL, choices = 1:31, selected = 1),
          selectInput("asof_monthFC", NULL, choices = setNames(1:12, month.abb), selected = 1),
          selectInput("asof_yearFC", NULL, choices = c("Connect first..." = ""), selected = "")),
        actionButton("asofRecentFC", "12 months ago", class = "btn-primary",
                     style = "margin-bottom: 0.5rem; padding:3px 10px; font-size:0.72rem;"),
        selectInput("cutoffFC", "Jump to a backtest cutoff",
                    choices = c("Connect first..." = "")),
        uiOutput("cutoffResolvedFC"),
        selectInput("holdFC", "Hold length (model horizons)",
                    choices = c("4 months" = "4", "7 months" = "7", "12 months" = "12",
                                "20 months" = "20", "33 months" = "33", "To today" = "240"),
                    selected = "12"),
        tags$span(paste("How long you hold before measuring. These are the model's Fibonacci forecast",
                        "horizons (same set as the Predictions tab's replay), capped at 33mo - holding to a",
                        "horizon realizes that prediction. 'To today' holds past it, where the edge decays."),
                  style = "color:#64748b; font-size:0.7rem; display:block; margin-bottom:0.6rem;"),
        checkboxGroupInput("idsFC", "Cluster ids (all on; uncheck to narrow)",
                           choices = NULL, inline = TRUE),
        div(style = "margin-top:-0.3rem; margin-bottom:0.5rem;",
          actionButton("idsAllFC", "Select all", style = "padding:2px 10px; font-size:0.72rem; margin-right:0.35rem;"),
          actionButton("idsNoneFC", "Deselect all", style = "padding:2px 10px; font-size:0.72rem;"),
          tags$span("Connect to load ids.", style = "color:#64748b; font-size:0.7rem; margin-left:0.4rem;")),
        tags$p(paste("Stand at any past date: it takes the selections known then (all-strategy",
                     "phased entry) and tracks them to today in REAL prices vs Benchmark, with the backtest",
                     "as the expectation and the live log in its own panel below. Solid =",
                     "realized, dotted = forward. First Generate ~6s, then under a second."),
               style = "color: #64748b; font-size: 0.72rem; margin-bottom: 0.5rem;")
      )),
      mainPanel(div(class = "main-card",
        uiOutput("forecastNoteFC"),
        uiOutput("forecastRiskFC"),
        plotlyOutput("forecastPlotFC", height = "560px"),
        tags$br(),
        plotlyOutput("forecastLedgerFC", height = "300px"),
        tags$br(),
        uiOutput("forecastTableFC")
      ))
    )
  )

  # ── Tab: Lifecycle (buy/hold/sell transition grid of points: before -> now) ──
