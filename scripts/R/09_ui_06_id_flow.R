# AlphaStream dashboard - UI of the id flow tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
ui_tab_id_flow <- tabPanel("ID Flow",
    sidebarLayout(
      make_sidebar("IDF", "ID Flow", tagList(
        selectInput("id_valIDF", "Cluster ID", choices = c("Connect first..." = ""), selected = "")
      )),
      mainPanel(div(class = "main-card",
        h4("One cluster, end to end: what feeds it, what it emits",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),

        # ---- verdict / evidence gate ----
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #fbbf24; border-radius: 4px;
                   color: #f8fafc; font-size: 0.85rem; line-height: 1.5;
                   margin-bottom: 1rem; font-family: 'JetBrains Mono', monospace;",
          uiOutput("idfVerdict")
        ),

        # ---- FED IN ----
        h5("Fed in - the scored signal per return-bucket at each horizon",
           style = "color: #38bdf8; font-weight: 600; margin-bottom: 0.4rem;"),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #38bdf8; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; line-height: 1.4;
                   margin-bottom: 0.75rem;",
          "Same picture as the Transition Range tab, but every horizon at once: one panel per fut_lag (1 to 33 months). ",
          "In each panel: x = past-return z-bucket (most negative left), purple box = how members in that bucket got there (past), blue box = what happened next (future). ",
          "Blue box above zero = the model reads that bucket as bullish at that horizon. ",
          "Matched horizon only (past window = future window). Hover a bucket for the medians, n_obs, and the cell's recommendation."
        ),
        plotlyOutput("idfFedPlot", height = "1500px"),

        # ---- COMES OUT ----
        h5("Comes out - the ranked tickers and their board action",
           style = "color: #4ade80; font-weight: 600; margin: 1.4rem 0 0.4rem 0;"),
        tags$div(
          style = "padding: 0.5rem 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #4ade80; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; line-height: 1.4;
                   margin-bottom: 0.75rem;",
          "Every ticker this cluster serves today, by agg_rank (1 = strongest). ",
          "Action is the live board call; dir score is agg_directional_score; votes are the BUY/SELL cell tallies behind it."
        ),
        DT::DTOutput("idfOutTable")
      ))
    )
  )

  # ── Tab: Rank Stability ──
