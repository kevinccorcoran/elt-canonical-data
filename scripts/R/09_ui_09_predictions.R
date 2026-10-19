# AlphaStream dashboard - UI of the predictions tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
ui_tab_predictions <- tabPanel("Predictions",
    sidebarLayout(
      make_sidebar("BL", "Buy List", tagList(
        radioButtons("date_modeBL", "As-of",
          choiceNames = list(
            tagList("Today (live)",
              tags$span("The model as of right now.",
                        style = "display:block; color:#64748b; font-size:0.68rem; font-weight:400; line-height:1.3; margin-top:0.1rem;")),
            tagList("Recorded day",
              tags$span("A recorded daily snapshot (since 2026-06-16).",
                        style = "display:block; color:#64748b; font-size:0.68rem; font-weight:400; line-height:1.3; margin-top:0.1rem;")),
            tagList("Backtest cutoff",
              tags$span("A quarterly walk-forward state - identical for the whole quarter-bundle.",
                        style = "display:block; color:#64748b; font-size:0.68rem; font-weight:400; line-height:1.3; margin-top:0.1rem;"))),
          choiceValues = c("today", "ledger", "backtest"),
          selected = "today"),
        conditionalPanel(
          # day-level precision is only offered where days actually differ: the
          # recorded BUY list is the one daily view. Picks/ladder snap to the
          # last walk-forward cutoff, so for them the picker is replaced by an
          # anchor note (ledger_anchorBL) instead of dead granularity.
          condition = "input.date_modeBL == 'ledger' && input.bl_viewBL == 'buys'",
          # bounds arrive from Connect via updateDateInput; until then the
          # widget defaults to today and asof_dateBL() returns NULL (no bounds)
          dateInput("ledger_dayBL", NULL, value = NULL, format = "yyyy-mm-dd")),
        conditionalPanel(
          condition = paste0("input.date_modeBL == 'ledger' && ",
                             "typeof input.bl_viewBL !== 'undefined' && ",
                             "input.bl_viewBL != 'buys'"),
          uiOutput("ledger_anchorBL")),
        conditionalPanel(
          condition = "input.date_modeBL == 'backtest'",
          selectInput("wf_cutoffBL", "Quarterly cutoff",
                      choices = c("Connect first..." = ""))),
        uiOutput("wf_resolvedBL"),
        tags$p(paste("Same three views at every date. The BUY list exists only",
                     "where a gate was recorded: live today and daily snapshots",
                     "since 2026-06-16; backtest dates show picks and ladder."),
               style = "color: #64748b; font-size: 0.7rem; margin-bottom: 0.75rem;"),
        # Unified 3-slot View (same menu at every date). Rendered server-side
        # so the BUY slot can grey out for pre-ledger dates: no recorded gate
        # exists before 2026-06-16 and reconstructing it would be lookahead
        # (see walk_forward_top_picks header for why that replay was rejected).
        uiOutput("blViewUI"),
        # BUY-view refinements (current date only): the decision shortlist is
        # a refinement of the BUY list, not a separate universe - so it lives
        # INSIDE the BUY view as a default-on toggle. Regular/Sparse toggles
        # only matter when the shortlist is off (sparse rows carry no bin).
        conditionalPanel(
          condition = "output.blCtlMode == 'current' && input.bl_viewBL == 'buys'",
          checkboxInput("buys_shortlistBL",
                        "Shortlist only - rank bins winning >= 55% (display filter, not the buy gate)",
                        value = TRUE),
          conditionalPanel(
            condition = "!input.buys_shortlistBL",
            checkboxGroupInput("show_catBL", NULL,
                               choices = c("Regular entries (BUYs)"      = "regular",
                                           "Sparse entries (young ids)"  = "sparse"),
                               selected = c("regular", "sparse")))),
        conditionalPanel(
          condition = "output.blCtlMode != 'current' && input.bl_viewBL == 'ladder'",
          # horizon/depth only steer the full-ladder replay; picks are a
          # top ~5% (tie-inclusive) slice graded at 12 months in the table.
          # Horizon is past-only by design: it selects a GRADING window, and
          # current-date rows have no realized outcomes to grade.
          selectInput("wf_horizon_valBL", "Replay horizon (months)",
                      choices = c("4", "7", "12", "20", "33"), selected = "12"),
          # replay's shortlist: the slider can't express "top X% of EACH
          # cluster" when clusters of different sizes are loaded together
          selectInput("wf_depth_valBL", "Per-cluster depth",
                      choices = c("All ranks"               = "all",
                                  "Top 5% of each cluster"  = "p5",
                                  "Top 10% of each cluster" = "p10",
                                  "Top 20% of each cluster" = "p20"),
                      selected = "all")),
        tags$div(
          style = paste0("margin: 0.25rem 0 0.75rem; padding: 0.5rem 0.75rem; ",
                         "background: rgba(255,255,255,0.03); ",
                         "border-left: 2px solid #64748b; border-radius: 4px; ",
                         "color: #94a3b8; font-size: 0.72rem; line-height: 1.5;"),
          tags$details(
            tags$summary("Glossary - the words on this tab"),
            tags$div(
              tags$div(HTML("<b>Walk-forward</b>: the backtest style here - train only on data before a cutoff, predict, then grade on what really happened after. No peeking ahead.")),
              tags$div(HTML("<b>Rank bin / slot</b>: each cluster's ranked list cut into 20 slots of 5% (slot 1 = top 5%). A ticker inherits its slot's history, not its own.")),
              tags$div(HTML("<b>Bin win rate</b>: of past walk-forward picks landing in this slot, the share that beat the benchmark. 50% = coin flip.")),
              tags$div(HTML("<b>Graded / holdout obs</b>: past predictions old enough to score against real later prices; 'n' counts them. Under 100 = 'no evidence'.")),
              tags$div(HTML("<b>Expectancy</b>: average return of past holdout trades, in pp per trade. Context only - the buy logic never reads it.")),
              tags$div(HTML("<b>pp</b>: percentage points - the gap between two percentages (55% is 5 pp above 50%).")),
              tags$div(HTML("<b>Serving rank vs pick rank</b>: serving rank = the model's position within the whole cluster today; pick rank = position inside the shown top-5% slice (r1 = best).")),
              tags$div(HTML("<b>Credibility</b>: a 0-1 weight each evidence cell earns from sample size and consistency; the serving BUY votes are credibility-weighted.")))))
      ),
      # rendered UNDER Generate Chart via make_sidebar's post_widgets slot:
      # both are post-load refinements (reactive on the loaded data, no
      # re-Generate needed), so they follow the button, not precede it.
      tagList(
        checkboxGroupInput("idsBL", "Cluster id filter (all on; uncheck to narrow)",
                           choices = NULL, inline = TRUE),
        div(style = "margin-top:-0.3rem; margin-bottom:0.5rem;",
          actionButton("idsAllBL", "Select all", style = "padding:2px 10px; font-size:0.72rem; margin-right:0.35rem;"),
          actionButton("idsNoneBL", "Deselect all", style = "padding:2px 10px; font-size:0.72rem;"),
          tags$span("Generate to load ids.", style = "color:#64748b; font-size:0.7rem; margin-left:0.4rem;")),
        sliderInput("flt_rank_rangeBL", "Rank range (within cluster)",
                    min = 1, max = 600, value = c(1, 600), step = 1)
      )),
      mainPanel(div(class = "main-card",
        uiOutput("modeNoteBL"),
        uiOutput("selStatsBL"),
        uiOutput("buyChartContainerBL"),
        tags$br(),
        DT::DTOutput("buyTableBL")
      ))
    )
  )

  # ── Tab: Forecast (backtest growth curve vs live ledger) ──
