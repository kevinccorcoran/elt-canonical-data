# AlphaStream dashboard - Lifecycle: what the board's calls would have earned (lump sum vs recurring deposits). Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # ── Board strategy by deposit schedule (Kevin 2026-10-19) ─────────────────────
  # Over the sketch window (the slider), the same names as the pins: each deposit
  # is split equally across that day's BUY names and held until each name's SELL;
  # SPY gets the same deposits. One row per schedule, $1,000 in total each.
  # lc_strategy_base holds every name of that set before the cluster and marks
  # view filter, with its trail, prices and run days; both tables slice it.
  lc_strategy_base <- reactive({
    if (is.null(input$db_pass) || input$db_pass == "") return(NULL)
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    if (is.null(dv) || is.null(dv$M)) return(NULL)
    d <- lc_sketch_data()
    start <- if (!is.null(d) && !is.null(d$qscmp_anchor)) as.Date(d$qscmp_anchor) else NA
    if (is.na(start)) return(NULL)
    tks <- lc_scope_names(dv, narrow = FALSE)
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
    list(dv = dv, tks = tks, states = states, px = px, run_d = run_d, start = start, end = end)
  })
  lc_strategy <- reactive({
    b <- lc_strategy_base(); if (is.null(b)) return(NULL)
    tks <- lc_scope_names(b$dv)
    if (!length(tks)) return(NULL)
    res <- tryCatch(lc_strategy_schedules(b$states[tks], b$px, b$run_d, b$start, b$end),
                    error = function(e) NULL)
    if (is.null(res)) return(NULL)
    attr(res, "start") <- b$start; attr(res, "end") <- b$end; attr(res, "n_names") <- length(tks)
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

  # ── Each cluster box on its own (Kevin 2026-10-19: "for each checkbox cluster i
  # also want returns based on different scenarios, for example all but last, all
  # industries having markers only, stdv ticker returns") ──────────────────────
  # Same calls, window and deposit schedules as the table above, for what is
  # selected only (Kevin 2026-10-19: "i only want see what ever is selected"): one
  # block per ticked cluster, then those clusters together when more than one is
  # ticked; under the marks view, the marked names by cluster. Per block: all names; the
  # worst name left out ("all but last": the master list sorts by window return, so
  # its bottom row) and, beside it, the best name left out, because dropping only
  # the loser flatters the result; the diamond/gear marked names only (display
  # tags, never a rule); and the spread, the standard deviation of the names'
  # window returns (the master list's RETURN). SPY is one row: its deposits do not
  # depend on the names.
  lc_strategy_clusters <- reactive({
    b <- lc_strategy_base(); if (is.null(b)) return(NULL)
    U <- lc_scope_names(b$dv)                         # the selection, as the table above
    if (!length(U)) return(NULL)
    ids <- lc_live_ids()[toupper(U)]
    boxes <- sort(unique(as.integer(tryCatch(lc_board_filter_ids(), error = function(e) integer(0)))))
    nc_view <- !is.null(lc_nc_only())
    wr <- lc_ml_winret()
    wr <- setNames(as.numeric(wr[toupper(U)]), U)
    nc <- tryCatch(lc_nc_tags(), error = function(e) NULL)
    marked <- if (!is.null(nc) && nrow(nc)) toupper(nc$ticker) else character(0)
    run1 <- function(set) {
      r <- tryCatch(lc_strategy_schedules(b$states[set], b$px, b$run_d, b$start, b$end),
                    error = function(e) NULL)
      w <- wr[set]; w <- w[!is.na(w)]
      list(r = r, spread = if (length(w) >= 2) stats::sd(w) else NA_real_)
    }
    row <- function(label, set) c(list(label = label, n = length(set)), run1(set))
    scen <- function(set) {
      out <- list(row(if (length(set) == 1) paste("Only", set) else sprintf("All %d names", length(set)),
                      set))
      w <- wr[set]; w <- w[!is.na(w)]
      if (length(w) >= 2) {
        lo <- names(w)[which.min(w)]; hi <- names(w)[which.max(w)]
        out <- c(out, list(
          row(sprintf("Without the worst (%s, %+.1f%%)", lo, w[[lo]]), setdiff(set, lo)),
          row(sprintf("Without the best (%s, %+.1f%%)", hi, w[[hi]]), setdiff(set, hi))))
      }
      mk <- set[toupper(set) %in% marked]
      if (length(mk) && length(mk) < length(set))
        out <- c(out, list(row(sprintf("\u25c6 %s marked only (%s)", NC_GEAR, paste(mk, collapse = ", ")), mk)))
      out
    }
    present <- boxes[boxes %in% ids]
    and_join <- function(x) if (length(x) < 2) paste(x) else
      paste(paste(x[-length(x)], collapse = ", "), "and", x[length(x)])
    blocks <- lapply(present, function(k) {
      set <- U[!is.na(ids) & ids == k]
      list(title = sprintf("Cluster %d", k), n = length(set), rows = scen(set))
    })
    gear <- paste("\u25c6", NC_GEAR)
    if (length(present) != 1 || any(is.na(ids) | !(ids %in% present)))
      blocks <- c(blocks, list(list(
        title = if (nc_view) sprintf("Every cluster, %s names", gear)
                else if (is.null(lc_ids_effective())) "Every cluster"
                else sprintf("Clusters %s together", and_join(present)),
        n = length(U), rows = scen(U))))
    attr(blocks, "scope") <- if (nc_view) sprintf("the %s names, every cluster", gear)
      else if (length(present) == 1) sprintf("cluster %d", present)
      else sprintf("clusters %s", and_join(present))
    blocks
  })

  output$lcStrategyClustersLC <- renderUI({
    bl <- lc_strategy_clusters(); if (is.null(bl) || !length(bl)) return(NULL)
    ref <- bl[[length(bl)]]$rows[[1]]$r
    if (is.null(ref) || !nrow(ref)) return(NULL)
    sch <- ref$schedule
    th <- function(x, al = "right") tags$th(style = sprintf(
      "text-align:%s;color:#cbd5e1;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.04em;padding:3px 10px;border-bottom:1px solid #1e293b;", al), x)
    td <- function(x, col = "#e2e8f0", al = "right", wt = 400, tip = NULL) tags$td(title = tip, style = sprintf(
      "text-align:%s;color:%s;font-weight:%d;padding:3px 10px;border-bottom:1px solid #0f172a;font-variant-numeric:tabular-nums;", al, col, wt), x)
    sgn <- function(v) if (is.na(v)) "#94a3b8" else if (v >= 0) "#10b981" else "#dc2626"
    cells <- function(x) {
      r <- x$r
      if (is.null(r) || !nrow(r)) return(lapply(sch, function(s) td("n/a", "#64748b")))
      if (identical(as.integer(attr(r, "buy_days")), 0L))
        return(lapply(sch, function(s) td("no BUY", "#64748b", tip = "No name in this set had a BUY day in the window")))
      lapply(sch, function(s) {
        i <- match(s, r$schedule); v <- r$ret_pct[i]; vs <- v - r$spy_pct[i]
        td(sprintf("%+.1f%%", v), sgn(v), wt = 700, tip = sprintf(
          "$%s now from $1,000; SPY with the same deposits %+.1f%%, so %+.1fpp vs SPY; %d name%s bought",
          formatC(r$value[i], format = "f", digits = 0, big.mark = ","), r$spy_pct[i], vs,
          r$names[i], if (r$names[i] == 1) "" else "s"))
      })
    }
    spy_row <- tags$tr(td("SPY, same deposits", "#94a3b8", "left", 600),
                       lapply(ref$spy_pct, function(v) td(sprintf("%+.1f%%", v), sgn(v))), td(""))
    body <- lapply(bl, function(blk) tagList(
      tags$tr(tags$td(colspan = length(sch) + 2, style = paste0(
        "color:#f1f5f9;font-weight:700;font-size:0.78rem;padding:7px 10px 3px;",
        "border-bottom:1px solid #1e293b;"), sprintf("%s \u00b7 %d name%s", blk$title, blk$n,
                                                     if (blk$n == 1) "" else "s"))),
      lapply(blk$rows, function(x) tags$tr(
        td(x$label, "#e2e8f0", "left", 400), cells(x),
        td(if (is.na(x$spread)) "n/a" else sprintf("%.1fpp", x$spread), "#cbd5e1"))))
    )
    short <- c("Lump sum at the start" = "Lump sum")
    hdr <- ifelse(sch %in% names(short), short[sch], sch)
    s <- attr(ref, "first_day"); e <- lc_strategy_base()$end
    tagList(
      tags$div(style = "color:#f1f5f9;font-weight:700;font-size:0.95rem;margin:0.9rem 0 0.1rem;",
               sprintf("Scenarios for %s, %s to %s", attr(bl, "scope"), format(s, "%b %d"), format(e, "%b %d"))),
      tags$div(style = "color:#64748b;font-size:0.72rem;margin-bottom:0.4rem;",
        paste("Same $1,000 per cell and the same rules as the table above, for the ticked clusters,",
              "each on its own and together when more than one is ticked. Without the worst / best leaves out the",
              "name with the lowest / highest return over the window (the master list's RETURN).",
              "\u25c6", NC_GEAR, "marked only keeps the disruptor and enabler names (display tags,",
              "never a rule). Spread is the standard deviation of the names' window returns: how far",
              "apart the names landed. Hover a cell for dollars and vs SPY.")),
      tags$div(style = "overflow-x:auto;",
        tags$table(style = "width:100%;border-collapse:collapse;font-size:0.8rem;",
          tags$thead(tags$tr(th("Names", "left"), lapply(hdr, th), th("Spread (stdev)"))),
          tags$tbody(spy_row, body))))
  })
