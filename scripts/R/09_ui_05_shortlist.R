# AlphaStream dashboard - UI of the shortlist tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
ui_tab_shortlist <- tabPanel("Shortlist",
    sidebarLayout(
      make_sidebar("P", "Top Picks", tagList(
        selectInput("id_valP", "Cluster ID", choices = c("Connect first..." = ""), selected = ""),
        sliderInput("top_n_valP", "Top N tickers", min = 5, max = 400, value = 30, step = 5)
      )),
      mainPanel(div(class = "main-card",
        h4("Top-ranked across horizons",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #38bdf8; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; line-height: 1.4;
                   margin-bottom: 0.75rem;",
          "Rows: top-N tickers by agg_rank in the selected cluster (rank 1 at top). ",
          "Columns: fut_lag (1 to 33 months). ",
          "Color: SIGNED RELIABILITY EDGE = (wilson_lower - 0.5) x direction, averaged over cells with a directional vote. ",
          "wilson_lower is the conservative lower bound of the walk-forward agreement rate, so thin-sample cells get penalized vs dense-sample cells with the same raw agreement. ",
          "Bright green = reliable BUY with dense evidence. Bright red = reliable AVOID with dense evidence. Yellow = vote near coin flip OR thin-evidence cell whose raw agreement is high but wilson_lower is modest. Blank = no directional vote. ",
          "Scale fixed at -0.3 to +0.3 (clips extreme cells but spans p90 of high-tier cells). Row label format: #rank ticker (n=coverage_cell_count). Hover shows raw wilson_lower and cell count."
        ),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #fbbf24; border-radius: 4px;
                   color: #f8fafc; font-size: 0.85rem; line-height: 1.4;
                   margin-bottom: 0.75rem; font-family: 'JetBrains Mono', monospace;",
          textOutput("clusterIcDisplayP")
        ),
        plotlyOutput("topPicksPlot", height = "900px")
      ))
    )
  )

  # ── Tab: ID Flow (id-level in -> out X-ray) ──
