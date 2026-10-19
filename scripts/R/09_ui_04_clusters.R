# AlphaStream dashboard - UI of the clusters tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
ui_tab_clusters <- tabPanel("Clusters",
    sidebarLayout(
      make_sidebar("K", "Clusters", tagList()),
      mainPanel(div(class = "main-card",
        h4("Per-series scatter",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        tags$div(style = "margin-bottom: 0.35rem;",
          actionButton("clusterShowAll", "Show all", class = "btn-primary",
                       style = "margin-right: 0.5rem;"),
          actionButton("clusterHideAll", "Hide all", class = "btn-primary")
        ),
        checkboxInput("clusterNumbers", "Show cluster numbers (one per cluster; hidden clusters drop their number)", value = TRUE),
        plotlyOutput("clusterPlot", height = "600px")
      ))
    )
  )

  # ── Tab: Top Picks ──
