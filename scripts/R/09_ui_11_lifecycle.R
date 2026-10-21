# AlphaStream dashboard - UI of the lifecycle tab. Part of app.R, split 2026-10-19: files load in name order (see app.R).
ui_tab_lifecycle <- tabPanel("Lifecycle",
    sidebarLayout(
      make_sidebar("LC", "Lifecycle", tagList(
        checkboxInput("lcBuyQSonly", "Qualstream + picks only", value = TRUE),
        checkboxInput("lcAllPins", "All qualstream pins", value = TRUE),
        checkboxInput("lcEveryFlip", "Show every flip", value = FALSE),
        checkboxInput("lcTopPerf", "Top performers (hindsight, any qs)", value = FALSE),
        checkboxInput("lcAllPickLines", "All qualstream pick lines (white)", value = FALSE),
        checkboxInput("lcAutoRefresh", "Auto-refresh (hourly)", value = TRUE),
        uiOutput("lcNewCatUI"),
        uiOutput("idFilterLC"),
        uiOutput("lcNcPausedUI")
      ), tagList(
        div(style = "color:#cbd5e1;font-size:0.72rem;font-weight:600;margin-bottom:0.3rem;",
            "STATE"),
        radioButtons("lcStateView", NULL,
          choices = c("Engine (money rule)" = "engine", "Raw gate" = "raw",
                      "Nested shadow (decisive)" = "nested"),
          selected = "engine", inline = FALSE),
        div(style = "color:#64748b;font-size:0.66rem;margin-top:-0.4rem;",
            paste("Changes the STATE column. Under Nested shadow the sketch pins follow",
                  "the shadow's decisive line too (▲ entry, ▼ exit, no hold circles).",
                  "Queue ✓ and pick lines always follow the engine - the rule the money uses."))
      )),
      mainPanel(div(class = "main-card",
        # Decision board leads the panel: the at-a-glance buy/hold/sell surface is
        # the primary artifact, so it sits at the top; benchmark + portfolio follow.
        uiOutput("boardLC"),
        tags$hr(style = "border-color:#1e293b; margin:0.9rem 0 0.9rem;"),
        # Cross-horizon best-bets matrix: one table, each name's top-slot status
        # + win% at 4/7/12mo, so a bet is chosen by the hold length it leads at.
        uiOutput("bestBetsLC"),
        tags$hr(style = "border-color:#1e293b; margin:0.9rem 0 1.1rem;"),
        # Realized track record (trades / avg return / vs SPY / win rate + buy
        # windows vs SPY) sits directly above the sketch it summarizes.
        uiOutput("lcTrackStripLC"),
        # Benchmark comparison (+ click a board chip to overlay that ticker here).
        # The window slider above it re-anchors the sketch (lc_window_sketch).
        uiOutput("lcSketchWinUI"),
        # "Smooth lines" button (strong display smoothing, lc_strong_line), on by
        # default: the server flips it per click and sends lcSmoothState to restyle it.
        # "Group lines" button beside it (lc_group_lines), also on by default.
        # "Ticker lines" button (lcNamesBtnUI) only while one cluster is ticked: it
        # hides or shows that cluster's own name lines.
        div(style = "display:flex; justify-content:flex-end; gap:0.4rem; margin:0.3rem 0 0.1rem;",
            uiOutput("lcNamesBtnUI", inline = TRUE),
            actionButton("lcGroupBtn", "Group lines: on", class = "on",
                         style = "padding:0.35rem 0.9rem !important; font-size:0.7rem !important;"),
            actionButton("lcSmoothBtn", "Smooth lines: on", class = "on",
                         style = "padding:0.35rem 0.9rem !important; font-size:0.7rem !important;")),
        tags$style(HTML(paste(
          "#lcSmoothBtn.on, #lcGroupBtn.on, #lcNamesBtn.on { background:#34d399 !important; }",
          ".lc-groupbox { margin:0.25rem 0 0.7rem; }",
          ".lc-groupbox .shiny-input-container { margin-bottom:0; width:auto; }",
          ".lc-groupbox label.checkbox-inline { text-transform:none !important; letter-spacing:0 !important;",
          "  font-size:0.8rem !important; line-height:1.4; margin:0.2rem 1.4rem 0.2rem 0 !important;",
          "  padding-left:1.3rem; position:relative; }",
          ".lc-groupbox label.checkbox-inline input[type=checkbox] { position:absolute; left:0; top:0.2rem;",
          "  margin:0 !important; width:0.85rem; height:0.85rem; accent-color:#34d399; }"))),
        tags$script(HTML(paste(
          "Shiny.addCustomMessageHandler('lcSmoothState', function(on) {",
          "  var b = document.getElementById('lcSmoothBtn'); if (!b) return;",
          "  b.classList.toggle('on', !!on);",
          "  b.textContent = on ? 'Smooth lines: on' : 'Smooth lines: off';",
          "});",
          "Shiny.addCustomMessageHandler('lcGroupState', function(on) {",
          "  var b = document.getElementById('lcGroupBtn'); if (!b) return;",
          "  b.classList.toggle('on', !!on);",
          "  b.textContent = on ? 'Group lines: on' : 'Group lines: off';",
          "});",
          "Shiny.addCustomMessageHandler('lcNamesState', function(on) {",
          "  var b = document.getElementById('lcNamesBtn'); if (!b) return;",
          "  b.classList.toggle('on', !!on);",
          "  b.textContent = on ? 'Ticker lines: on' : 'Ticker lines: off';",
          "});"))),
        plotlyOutput("qsCompareLC", height = "760px"),
        uiOutput("lcLineGroupsLC"),
        uiOutput("qsCompareNoteLC"),
        uiOutput("lcStrategyLC"),
        uiOutput("lcStrategyClustersLC"),
        tags$hr(style = "border-color:#1e293b; margin:0.9rem 0 1rem;"),
        # --- My portfolio: strategy follower (model-linked DCA + ladder sells) ---
        tags$details(open = NA, class = "pf-wide",
          style = "border:1px solid #1e293b; border-radius:8px; padding:0.6rem 0.9rem; margin-bottom:1rem;",
          tags$summary("My portfolio",
            uiOutput("lcPortSummary", inline = TRUE)),
          div(
            div(style = "color:#64748b; font-size:0.72rem; margin-bottom:0.6rem;",
                paste("Hands-off: daily portfolio_sync adopts every queue BUY at the $ below,",
                      "archives exits to Realized, and Telegram pings when to act.",
                      "Simulated fills at daily closes vs a same-cash SPY.")),
            div(style = "display:flex; gap:1rem; flex-wrap:wrap;",
              div(style = "max-width:220px;",
                  numericInput("lcSeedAmt", "$ per new BUY", value = 100, min = 1)),
              # audit 2026-08-21: trimming at +25/+50% costs -3.3/-1.5pp vs holding;
              # at +100% the recoup sale is performance-free (+0.3pp, coin flip),
              # so the never-lose-money rule defaults there.
              div(style = "max-width:220px;",
                  numericInput("lcRecoupThr", "Recoup alert at +%", value = 100, min = 10))),
            uiOutput("lcAdoptRow"),
            div(style = "color:#94a3b8; font-size:0.72rem; margin:0.5rem 0 0.15rem; font-weight:600;",
                "Manual plan"),
            fluidRow(
              column(2, textInput("lcPosTicker", "Ticker", "")),
              column(2, numericInput("lcPosAmt", "$ per buy", value = 100, min = 1)),
              column(3, selectInput("lcPosCadence", "Cadence",
                       choices = c("Monthly" = "monthly", "One-off" = "once"),
                       selected = "monthly")),
              column(2, numericInput("lcPosDay", "Day of month", value = 1, min = 1, max = 28)),
              column(3, dateInput("lcPosStart", "Start date", value = Sys.Date() - 365))
            ),
            div(style = "margin:0.1rem 0 0.6rem;",
              actionButton("lcPosAdd", "Add", class = "btn-primary", style = "margin-right:0.4rem;"),
              actionButton("lcPosRemove", "Remove", style = "margin-right:0.4rem;"),
              actionButton("lcPosHold", "Hold", style = "margin-right:0.4rem;",
                           title = "Pause buying; the position keeps valuing"),
              actionButton("lcPosSell", "Sell", style = "margin-right:0.4rem;",
                           title = "Record a full exit"),
              div(style = "display:inline-block;width:70px;margin-right:0.15rem;vertical-align:middle;",
                  numericInput("lcPartPct", NULL, value = 50, min = 1, max = 95, step = 1)),
              actionButton("lcPosSellPart", "Sell part %", style = "margin-right:0.4rem;",
                           title = "Record a partial sale (the recoup-ping action): the slice is archived as Banked $, the row keeps running on the rest"),
              actionButton("lcPosResume", "Resume", style = "margin-right:0.4rem;",
                           title = "Return the row to the model's flow"),
              actionButton("lcPosArchive", "Archive", style = "margin-right:0.4rem;",
                           title = "Move Sell-ed rows to the read-only Realized table, frozen at their exit numbers"),
              actionButton("lcPosClear", "Clear")),
            div(style = "color:#64748b; font-size:0.7rem; margin:0.1rem 0 0.35rem;",
                paste("id = cluster · check rows, then act (hover a button for what it does) ·",
                      "double-click $/buy to edit ·",
                      "vs SPY = same dollars in SPY · vs HODL = one lump at first fill ·",
                      "Model % = the model trade since its own entry.")),
            DT::DTOutput("lcPosTable"),
            uiOutput("lcChartFilter"),
            plotlyOutput("lcPortfolioChart", height = "340px"),
            uiOutput("lcPortfolioNote"),
            div(style = "color:#94a3b8; font-size:0.72rem; margin:0.8rem 0 0.15rem; font-weight:600;",
                "Transition history"),
            DT::DTOutput("lcTransitionsTable"),
            div(style = "color:#94a3b8; font-size:0.72rem; margin:0.9rem 0 0.15rem; font-weight:600;",
                "Realized · green = profit · red = loss"),
            DT::DTOutput("lcClosedTable")
          )
        )
        # Decision board moved to the top of this panel; the benchmark chart sits
        # just below it. "Same companies with detail" table removed from view
        # (still computed; re-add hr + caption + DT::DTOutput("tableLC") to restore).
      ))
    )
  )
