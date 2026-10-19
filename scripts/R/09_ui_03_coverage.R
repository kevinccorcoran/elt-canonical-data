# AlphaStream dashboard - UI of the coverage tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
ui_tab_coverage <- tabPanel("Coverage",
    sidebarLayout(
      make_sidebar("V", "Coverage", tagList(
        tags$div(
          style = "padding: 0.75rem; background: rgba(255,255,255,0.03);
                   border-left: 2px solid #64748b; border-radius: 4px;
                   color: #94a3b8; font-size: 0.75rem; font-family: 'Inter'; line-height: 1.5;",
          tags$div(style = "color: #f8fafc; font-weight: 600; margin-bottom: 0.4rem;", "What this shows"),
          "One horizontal bar per ticker in ", tags$code("cdm.ingest_combined"),
          ". Left edge = first observation, right edge = last observation. Dashed vertical line = today."
        )
      )),
      mainPanel(div(class = "main-card",
        h4("Series history coverage",
           style = "color: #f8fafc; margin-bottom: 1rem; font-weight: 600;"),
        uiOutput("coveragePlotContainer")
      ))
    )
  )

  # ── Tab: Clusters ──
