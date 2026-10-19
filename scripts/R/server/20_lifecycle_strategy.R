# AlphaStream dashboard - Lifecycle: what the board's calls would have earned (lump sum vs recurring deposits). Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # ── Board strategy by deposit schedule (Kevin 2026-10-19) ─────────────────────
  # Over the sketch window (the slider), the same names as the pins: each deposit
  # is split equally across that day's BUY names and held until each name's SELL;
  # SPY gets the same deposits. One row per schedule, $1,000 in total each.
  lc_strategy <- reactive({
    if (is.null(input$db_pass) || input$db_pass == "") return(NULL)
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    if (is.null(dv) || is.null(dv$M)) return(NULL)
    d <- lc_sketch_data()
    start <- if (!is.null(d) && !is.null(d$qscmp_anchor)) as.Date(d$qscmp_anchor) else NA
    if (is.na(start)) return(NULL)
    tks <- lc_scope_names(dv)
    if (!length(tks)) return(NULL)
    con <- tryCatch(get_con(input), error = function(e) NULL); if (is.null(con)) return(NULL)
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    px <- tryCatch(lc_px_get(con, c(tks, "SPY"), start), error = function(e) NULL)
    if (is.null(px) || !nrow(px)) return(NULL)
    px$d <- as.Date(px$d); px$px <- as.numeric(px$px)
    end <- if (!is.null(d$qscmp_to)) as.Date(d$qscmp_to) else max(px$d[px$ticker == "SPY"])
    states <- lapply(setNames(tks, tks), function(tk) {
      prov <- !is.null(dv$prov_of) && tk %in% names(dv$prov_of) && isTRUE(dv$prov_of[[tk]])
      ns   <- if (tk %in% names(dv$state_now)) dv$state_now[[tk]] else NA_character_
      tr <- tryCatch(lc_board_trail(dv$M, dv$dates, tk, d$led, LEDGER_EPOCH, prov, ns, dv$hz,
                                    lc_dl_of(d, toupper(tk)), rec = d$rec_states),
                     error = function(e) NULL)
      if (is.null(tr) || !nrow(tr)) NULL
      else data.frame(date = as.Date(tr$date), state = tr$state, stringsAsFactors = FALSE)
    })
    run_d <- if (!is.null(d$led$dd)) d$led$dd else as.Date(d$led$d)
    res <- tryCatch(lc_strategy_schedules(states, px, run_d, start, end), error = function(e) NULL)
    if (is.null(res)) return(NULL)
    attr(res, "start") <- start; attr(res, "end") <- end; attr(res, "n_names") <- length(tks)
    res
  })

  output$lcStrategyLC <- renderUI({
    r <- lc_strategy(); if (is.null(r) || !nrow(r)) return(NULL)
    th <- function(x, al = "right") tags$th(style = sprintf(
      "text-align:%s;color:#cbd5e1;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.04em;padding:3px 10px;border-bottom:1px solid #1e293b;", al), x)
    td <- function(x, col = "#e2e8f0", al = "right", wt = 400) tags$td(style = sprintf(
      "text-align:%s;color:%s;font-weight:%d;padding:3px 10px;border-bottom:1px solid #0f172a;font-variant-numeric:tabular-nums;", al, col, wt), x)
    sgn <- function(v) if (is.na(v)) "#94a3b8" else if (v >= 0) "#10b981" else "#dc2626"
    usd <- function(v) if (is.na(v)) "n/a" else paste0("$", formatC(v, format = "f", digits = 0, big.mark = ","))
    pct <- function(v) if (is.na(v)) "n/a" else sprintf("%+.1f%%", v)
    pp  <- function(v) if (is.na(v)) "n/a" else sprintf("%+.1fpp", v)
    rows <- lapply(seq_len(nrow(r)), function(i) {
      x <- r[i, ]; vs <- x$ret_pct - x$spy_pct
      note <- c(if (x$waited > 0) sprintf("%d waited for a BUY day", x$waited),
                if (x$cashed > 0) sprintf("%d never invested (no BUY after)", x$cashed))
      tags$tr(td(x$schedule, "#f1f5f9", "left", 600), td(x$deposits), td(usd(x$value)),
              td(pct(x$ret_pct), sgn(x$ret_pct), wt = 700), td(usd(x$spy_value)),
              td(pct(x$spy_pct), sgn(x$spy_pct)), td(pp(vs), sgn(vs), wt = 700),
              td(x$names), td(if (length(note)) paste(note, collapse = "; ") else "", "#94a3b8", "left"))
    })
    s <- attr(r, "first_day"); e <- attr(r, "end"); lb <- attr(r, "last_buy")
    tagList(
      tags$div(style = "color:#f1f5f9;font-weight:700;font-size:0.95rem;margin:0.6rem 0 0.1rem;",
               sprintf("What the board's calls would have earned, %s to %s", format(s, "%b %d"), format(e, "%b %d"))),
      tags$div(style = "color:#64748b;font-size:0.72rem;margin-bottom:0.4rem;",
        sprintf(paste("$1,000 in total per row. Each deposit is split equally across the names the board says",
                      "BUY that day (the pins' %d names: graded >= %d or held, ticked clusters), held until",
                      "that name's SELL, then kept as cash; a deposit on a day with no BUY waits for the next",
                      "one. SPY gets the same deposits on the same days. The board had a BUY on %d of %d run",
                      "days%s. Follows the slider."),
                attr(r, "n_names"), QS_MIN, attr(r, "buy_days"), attr(r, "run_days"),
                if (is.na(lb)) "" else sprintf(", the last on %s", format(lb, "%b %d")))),
      tags$div(style = "overflow-x:auto;",
        tags$table(style = "width:100%;border-collapse:collapse;font-size:0.8rem;",
          tags$thead(tags$tr(th("Schedule", "left"), th("Deposits"), th("Value now"), th("Return"),
                             th("SPY, same deposits"), th("SPY return"), th("vs SPY"), th("Names bought"),
                             th("Note", "left"))),
          tags$tbody(rows))),
      tags$div(style = "color:#64748b;font-size:0.7rem;margin:0.3rem 0 0.6rem;",
        paste("States before Aug 7 for names the board did not track are replayed with today's evidence,",
              "as on the pins; prices are closes, no costs or dividends. Not a forecast.")))
  })
