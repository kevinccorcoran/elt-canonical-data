# AlphaStream dashboard - Lifecycle: the sketch and its note. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # Does the qualstream marking help? Equal-weight return of the current BUYs
  # qualstream graded vs the >= 68 (orange +) subset vs SPY, over the current
  # 4-month window. Descriptive only: see the caption + LC_QS_COMPARE_SQL.
  # "Smooth lines" button above the sketch (Kevin 2026-10-19): each click flips
  # strong display smoothing (lc_strong_line) for this tab; ON on every page load
  # (Kevin 2026-10-19: "smooth line to be on by default").
  lc_smooth_on <- reactiveVal(TRUE)
  observeEvent(input$lcSmoothBtn, {
    lc_smooth_on(!isTRUE(lc_smooth_on()))
    session$sendCustomMessage("lcSmoothState", isTRUE(lc_smooth_on()))
  })

  # "Group lines" button (Kevin 2026-10-19): colours the per-name lines by profile
  # over the window (lc_group_lines); on by default, a click turns it off.
  lc_group_on <- reactiveVal(TRUE)
  observeEvent(input$lcGroupBtn, {
    lc_group_on(!isTRUE(lc_group_on()))
    session$sendCustomMessage("lcGroupState", isTRUE(lc_group_on()))
  })
  # The groups for the lines on screen: the cluster lines or the pick lines (the
  # marks view keeps its own violet styling), cut to the window like the chart.
  lc_line_groups <- reactive({
    if (!isTRUE(lc_group_on())) return(NULL)
    pl <- tryCatch(lc_pick_lines(), error = function(e) NULL)
    if (is.null(pl) || !length(pl$lines) || identical(pl$mode, "nc")) return(NULL)
    d <- lc_sketch_data(); if (is.null(d) || is.null(d$qscmp) || !nrow(d$qscmp)) return(NULL)
    spy <- lc_win_cut(d$qscmp, d$qscmp_to)
    lines <- lapply(pl$lines, lc_win_cut, we = d$qscmp_to)
    tryCatch(lc_group_lines(lines, spy$d, spy$spy_pct), error = function(e) NULL)
  })

  # Group checkboxes under the sketch (Kevin 2026-10-19: "i want check boxes for
  # categories"): the labels of the unticked groups. Keyed by label, so a hidden
  # group stays hidden across slider moves while a group with that label exists
  # ("Like SPY" always does). All boxes unticked arrives as NULL; ignoreInit stops
  # the startup NULL (no boxes yet) from hiding everything.
  lc_group_hidden <- reactiveVal(character(0))
  observeEvent(input$lcGroupShow, {
    g <- lc_line_groups(); labs <- if (is.null(g)) character(0) else unique(g$label)
    lc_group_hidden(setdiff(labs, as.character(unlist(input$lcGroupShow))))
  }, ignoreNULL = FALSE, ignoreInit = TRUE)
  # The names in the unticked groups: the two return tables under the sketch leave
  # them out too, as the chart does (Kevin 2026-10-19: "table to update based on
  # selection"). Nothing unticked, or grouping off, drops nothing.
  lc_group_dropped <- reactive({
    hid <- lc_group_hidden(); if (!length(hid)) return(character(0))
    g <- lc_line_groups(); if (is.null(g) || !nrow(g)) return(character(0))
    toupper(g$ticker[g$label %in% hid])
  })

  output$qsCompareLC <- renderPlotly({
    req(lc_sketch_data())
    d <- lc_sketch_data(); cmp <- d$qscmp
    if (isTRUE(d$qscmp_short))
      return(empty_plot("The window holds under two trading days. Drag the handles further apart."))
    # (The Jan/May/Sep checkpoint-rollover empty state is gone with the rolling
    # 120d anchor - an empty chart now genuinely means no graded names in range.)
    if (is.null(cmp) || nrow(cmp) == 0 || all(is.na(cmp$graded_pct)))
      return(empty_plot("No qualstream-graded names in range - run qualstream to populate this."))
    cmp$d <- as.Date(cmp$d)
    # ◆/⚙ marks after tickers on the chart too (Kevin 2026-10-14): chips, the
    # cluster view's name labels and ticked rows. No tags loaded = no change.
    nc_tg <- tryCatch(lc_nc_tags(), error = function(e) NULL)
    nc_g  <- if (!is.null(nc_tg) && nrow(nc_tg))
               setNames(lc_nc_glyph(nc_tg$marker, nc_tg$enabler), nc_tg$ticker) else character(0)
    tk_g  <- function(tk) { g <- unname(nc_g[toupper(tk)]); ifelse(is.na(g), tk, paste0(tk, g)) }
    # Cluster narrowing (live on the buy-cluster checkboxes): recompute the graded
    # and passed lines for the selected cluster(s) from the per-ticker detail. SPY is
    # cluster-independent and always stays. Rules honour the "UNCHECK TO NARROW"
    # label: NULL (untouched) or all-available checked = full basket; a strict subset
    # = that cluster only; empty = nothing.
    sel_raw  <- lc_ids_effective()
    sel_ids  <- suppressWarnings(as.integer(sel_raw))
    avail_id <- tryCatch(lc_board_filter_ids(), error = function(e) integer(0))
    det      <- d$qscmp_detail
    cl_suffix <- ""; n_over <- NULL
    # Market-creators view (Kevin 2026-10-13): the baskets narrow to the marked
    # names from every cluster, like a cluster pick but by ticker; the cluster
    # boxes (and their deselect-all) are ignored while it is on.
    nc_set <- lc_nc_only()
    nc_view <- !is.null(nc_set)
    if (!nc_view && !is.null(sel_raw) && length(sel_raw) == 0L)
      return(empty_plot("No cluster selected - check a cluster to show the sketch."))
    narrowed <- nc_view || (!is.null(sel_raw) && length(sel_ids) > 0 &&
                !is.null(det) && nrow(det) > 0 && !setequal(sel_ids, avail_id))
    if (narrowed) {
      dsel <- if (is.null(det)) det[FALSE, , drop = FALSE]
              else if (nc_view) det[toupper(det$ticker) %in% nc_set, , drop = FALSE]
              else det[!is.na(det$id) & det$id %in% sel_ids, , drop = FALSE]
      if (nrow(dsel) == 0 && !nc_view)
        return(empty_plot(sprintf("No qualstream-graded names in cluster %s.",
                                  paste(sort(sel_ids), collapse = ", "))))
      key  <- as.character(cmp$d)
      gvec <- tapply(dsel$ret, dsel$d, function(x) mean(x, na.rm = TRUE) * 100)
      dpas <- dsel[dsel$passed %in% TRUE, , drop = FALSE]
      pvec <- if (nrow(dpas)) tapply(dpas$ret, dpas$d, function(x) mean(x, na.rm = TRUE) * 100)
              else numeric(0)
      cmp$graded_pct <- round(as.numeric(gvec[key]), 2)
      cmp$passed_pct <- round(as.numeric(pvec[key]), 2)
      n_over    <- c(g = length(unique(dsel$ticker)), p = length(unique(dpas$ticker)))
      # literal middle dot: plotly titles don't decode HTML entities (a raw
      # '&middot;' rendered verbatim), unlike Shiny HTML() blocks
      cl_suffix <- if (nc_view) paste0(" · \u25c6 disruptors + ", NC_GEAR, " enablers")
                   else sprintf(" · cluster %s", paste(sort(sel_ids), collapse = ", "))
    }
    qs_g <- if (!is.null(d$qs)) suppressWarnings(as.numeric(d$qs$grade)) else numeric(0)
    n_g  <- if (!is.null(n_over)) unname(n_over[["g"]])
            else if (!is.null(d$qs)) length(d$qs$ticker) else 0L
    n_p  <- if (!is.null(n_over)) unname(n_over[["p"]])
            else if (!is.null(d$qs)) sum(qs_g >= 68, na.rm = TRUE) else 0L
    has_pass <- any(!is.na(cmp$passed_pct))
    # the market-creators view can hold no graded series at all (every marked
    # name vetoed or ungraded): SPY + the per-name lines still draw
    has_graded <- any(!is.na(cmp$graded_pct))
    # strong smoothing (the button): the basket lines draw from cmpL; cmp keeps the
    # real values for the end labels, the hovers (cmpR, same rows as cmpL) and the
    # axis range, so a toggle never zooms
    smooth <- isTRUE(lc_smooth_on())
    cmpL <- cmp
    if (smooth) {
      cmpL <- lc_weekdays(cmp)
      for (cn in intersect(c("spy_pct", "graded_pct", "passed_pct"), names(cmpL)))
        cmpL[[cn]] <- lc_smooth2(cmpL[[cn]])
    }
    cmpR <- if (smooth) lc_weekdays(cmp) else cmp
    hov <- function(lbl, raw) if (smooth)
      list(customdata = raw, hovertemplate = paste0(lbl, "<br>%{x|%b %d}: %{customdata:.1f}%<extra></extra>"))
      else list(hovertemplate = paste0(lbl, "<br>%{x|%b %d}: %{y:.1f}%<extra></extra>"))
    fig <- plot_ly()
    # Build end-of-line label annotations
    endlabels <- list()
    last_x <- if (nrow(cmp) > 0) cmp$d[nrow(cmp)] else NULL
    if (!is.null(last_x) && !is.na(last_x)) {
      if (nrow(cmp) > 0 && !is.na(cmp$spy_pct[nrow(cmp)])) {
        endlabels[[length(endlabels) + 1]] <- list(
          x = last_x, y = cmp$spy_pct[nrow(cmp)], text = "SPY",
          textfont = list(color = "#3b82f6", size = 8), showarrow = FALSE,
          xanchor = "left", yanchor = "middle", xshift = 5)
      }
      if (nrow(cmp) > 0 && !is.na(cmp$graded_pct[nrow(cmp)])) {
        endlabels[[length(endlabels) + 1]] <- list(
          x = last_x, y = cmp$graded_pct[nrow(cmp)], text = "Graded",
          textfont = list(color = "#f59e0b", size = 8), showarrow = FALSE,
          xanchor = "left", yanchor = "middle", xshift = 5)
      }
      if (has_pass && nrow(cmp) > 0 && !is.na(cmp$passed_pct[nrow(cmp)])) {
        endlabels[[length(endlabels) + 1]] <- list(
          x = last_x, y = cmp$passed_pct[nrow(cmp)], text = "QS≥68",
          textfont = list(color = "#10b981", size = 8), showarrow = FALSE,
          xanchor = "left", yanchor = "middle", xshift = 5)
      }
    }
    fig <- do.call(add_trace, c(list(fig, x = cmpL$d, y = cmpL$spy_pct, type = "scatter", mode = "lines",
      name = "Benchmark (SPY)", line = lc_line(color = "#3b82f6", width = 2.5)),
      hov("SPY", cmpR$spy_pct)))
    if (has_graded)
      fig <- do.call(add_trace, c(list(fig, x = cmpL$d, y = cmpL$graded_pct, type = "scatter", mode = "lines",
        name = sprintf("%s graded names (%d)", if (nc_view) paste0("\u25c6", NC_GEAR) else "All", n_g),
        line = lc_line(color = "#f59e0b", width = 3)),
        hov("All graded names", cmpR$graded_pct)))
    if (has_pass) {
      fig <- do.call(add_trace, c(list(fig, x = cmpL$d, y = cmpL$passed_pct, type = "scatter", mode = "lines",
        name = sprintf("%sassed qualstream ≥ 68 (%d)", if (nc_view) paste0("\u25c6", NC_GEAR, " p") else "P", n_p),
        line = lc_line(color = "#10b981", width = 3)),
        hov("Passed ≥68", cmpR$passed_pct)))
    }
    # Top-performers HINDSIGHT ceiling (opt-in, off by default): top decile of
    # buys by realized return, any qs. Dashed + labelled "hindsight" because it
    # is outcome-selected and not achievable ex ante. Buy pins ride the line.
    tp <- tryCatch(lc_top_perf(), error = function(e) NULL)
    if (!is.null(tp)) tp$pins <- lc_win_cut(tp$pins, d$qscmp_to, "date")
    tp_suffix <- ""
    if (!is.null(tp) && !is.null(tp$line) && nrow(tp$line)) {
      tpl <- if (smooth) lc_strong_line(tp$line) else tp$line
      if (smooth && !is.null(tp$pins) && nrow(tp$pins))      # pins stay on the drawn line
        tp$pins$y <- approx(as.numeric(as.Date(tpl$d)), tpl$ret,
                            xout = as.numeric(as.Date(tp$pins$date)), rule = 2)$y
      fig <- do.call(add_trace, c(list(fig, x = tpl$d, y = tpl$ret, type = "scatter", mode = "lines",
        name = sprintf("Top performers ≥ mean+1σ · hindsight, any qs (%d)", tp$n),
        line = lc_line(color = "#e879f9", width = 2.5, dash = "dash")),
        hov("Top performers (hindsight)", tpl$raw)))
      # buy pins ride the line, ticker-labelled: green = bought in-window,
      # grey square = held since before the window (carried in at the anchor).
      if (!is.null(tp$pins) && nrow(tp$pins)) {
        bp <- tp$pins[tp$pins$kind == "buy", , drop = FALSE]
        cp <- tp$pins[tp$pins$kind == "carried", , drop = FALSE]
        if (nrow(bp))
          fig <- add_trace(fig, x = bp$date, y = bp$y, type = "scatter",
            mode = "markers+text", showlegend = FALSE, text = bp$ticker,
            textposition = "top center", textfont = list(color = "#34d399", size = 9),
            marker = list(color = "#34d399", size = 8, symbol = "circle",
                          line = list(color = "#0b1220", width = 1)),
            hovertext = sprintf("%s BUY %s", bp$ticker, format(bp$date)),
            hovertemplate = "%{hovertext}<extra></extra>")
        if (nrow(cp))
          fig <- add_trace(fig, x = cp$date, y = cp$y, type = "scatter",
            mode = "markers+text", showlegend = FALSE, text = cp$ticker,
            textposition = "top center", textfont = list(color = "#94a3b8", size = 9),
            marker = list(color = "#94a3b8", size = 9, symbol = "square-open",
                          line = list(color = "#0b1220", width = 1)),
            hovertext = sprintf("%s · held since before %s (bought pre-window)",
                                cp$ticker, format(as.Date(d$qscmp_anchor))),
            hovertemplate = "%{hovertext}<extra></extra>")
      }
      tp_suffix <- " · top performers"
    }
    # All qualstream pick lines (opt-in, off by default): every passed-qualstream
    # name as its OWN faint white return line - the green aggregate decomposed
    # into its constituents. Lines only, no pins; one shared legend row toggles
    # the whole bundle. A clicked chip's overlay draws brighter (below) on top so
    # the focused name stands out of the pack.
    pl <- tryCatch(lc_pick_lines(), error = function(e) NULL)
    if (!is.null(pl) && length(pl$lines) && !is.null(d$qscmp_to)) {
      pl$lines <- lapply(pl$lines, lc_win_cut, we = d$qscmp_to)
      pl$lines <- pl$lines[vapply(pl$lines, nrow, integer(1)) > 0]
    }
    pl_suffix <- ""
    if (!is.null(pl) && length(pl$lines) && identical(pl$mode, "nc")) {
      # market-creators view: every marked name as its own violet line with a
      # glyph end label; names off the master list dashed (chart only)
      first_on <- TRUE; first_off <- TRUE
      for (nm in names(pl$lines)) {
        ln <- if (smooth) lc_strong_line(pl$lines[[nm]]) else lc_soft_line(pl$lines[[nm]])
        off <- nm %in% pl$offlist
        glyph <- lc_nc_glyph(unname(pl$marker[nm]), isTRUE(unname(pl$enabler[nm])))
        fig <- add_trace(fig, x = ln$d, y = ln$ret, type = "scatter", mode = "lines",
          name = if (off) sprintf("%s off the master list (%d)", paste0("\u25c6", NC_GEAR), length(pl$offlist))
                 else sprintf("%s disruptors + enablers (%d)", paste0("\u25c6", NC_GEAR), pl$n - length(pl$offlist)),
          legendgroup = if (off) "nclines_off" else "nclines",
          showlegend = if (off) first_off else first_on,
          line = lc_line(color = if (off) "rgba(192,132,252,0.45)" else "rgba(192,132,252,0.75)",
                         width = if (off) 1 else 1.4, dash = if (off) "dash" else "solid"),
          hovertext = if (off) sprintf("%s %s (%s, chart only)", glyph, nm, pl$why[[nm]])
                      else sprintf("%s %s", glyph, nm),
          customdata = ln$raw,
          hovertemplate = paste0("%{hovertext}<br>%{x|%b %d}: %{customdata:.1f}%<extra></extra>"))
        if (off) first_off <- FALSE else first_on <- FALSE
        if (nrow(ln) > 0) {
          last_i <- nrow(ln)
          endlabels[[length(endlabels) + 1]] <- list(
            x = ln$d[last_i], y = ln$ret[last_i], text = paste(glyph, nm),
            textfont = list(color = "#c084fc", size = 8), showarrow = FALSE,
            xanchor = "left", yanchor = "middle", xshift = 5)
        }
      }
      pl_suffix <- paste0(" · \u25c6", NC_GEAR, " lines")
    } else if (!is.null(pl) && length(pl$lines)) {
      first <- TRUE
      # single-cluster view: light grey lines with a ticker end label per name;
      # checkbox view (all clusters): the faint white bundle as before
      pl_auto <- isTRUE(pl$auto)
      pl_col  <- if (pl_auto) "rgba(148,163,184,0.55)" else "rgba(248,250,252,0.30)"
      pl_name <- if (pl_auto) sprintf("Cluster %d names (%d)", pl$cluster, pl$n)
                 else sprintf("Qualstream pick lines (%d)", pl$n)
      grp <- tryCatch(lc_line_groups(), error = function(e) NULL); seen <- character(0)
      hid <- if (is.null(grp)) character(0) else lc_group_hidden()
      for (nm in names(pl$lines)) {
        gi <- if (is.null(grp)) NA_integer_ else match(nm, grp$ticker)
        if (!is.na(gi) && grp$label[gi] %in% hid) next   # unticked group: not drawn
        ln <- if (smooth) lc_strong_line(pl$lines[[nm]]) else lc_soft_line(pl$lines[[nm]])
        if (is.na(gi)) {
          fig <- add_trace(fig, x = ln$d, y = ln$ret, type = "scatter", mode = "lines",
            name = pl_name,
            legendgroup = "picklines", showlegend = first,
            line = lc_line(color = pl_col, width = 1),
            hovertext = nm, customdata = ln$raw,
            hovertemplate = paste0("%{hovertext}<br>%{x|%b %d}: %{customdata:.1f}%<extra></extra>"))
          first <- FALSE
        } else {                                          # coloured by profile group
          gk <- as.character(grp$grp[gi])
          fig <- add_trace(fig, x = ln$d, y = ln$ret, type = "scatter", mode = "lines",
            name = sprintf("%s (%d)", grp$label[gi], sum(grp$grp == grp$grp[gi])),
            legendgroup = paste0("linegrp_", gk), showlegend = !(gk %in% seen),
            legendrank = 1000 + grp$rank[gi], opacity = 0.85,
            line = lc_line(color = grp$col[gi], width = 1.3),
            hovertext = sprintf("%s \u00b7 %s", nm, grp$label[gi]), customdata = ln$raw,
            hovertemplate = paste0("%{hovertext}<br>%{x|%b %d}: %{customdata:.1f}%<extra></extra>"))
          seen <- c(seen, gk)
        }
        if (pl_auto && nrow(ln) > 0) {
          last_i <- nrow(ln)
          endlabels[[length(endlabels) + 1]] <- c(list(
            x = ln$d[last_i], y = ln$ret[last_i], text = tk_g(nm),
            textfont = list(color = if (is.na(gi)) "#94a3b8" else grp$col[gi], size = 8), showarrow = FALSE,
            xanchor = "left", yanchor = "middle", xshift = 5),
            # annotations colour by `font` (textfont is ignored there), so only the
            # grouped labels take their group's colour
            if (!is.na(gi)) list(font = list(color = grp$col[gi])))
        }
      }
      pl_suffix <- paste0(if (pl_auto) sprintf(" · cluster %d lines", pl$cluster)
                          else sprintf(" · %d pick lines", pl$n),
                          if (!is.null(grp)) " grouped" else "")
    }
    # Checked master-list names: each ticked row draws its own rebased return line
    # in a distinct color, with a legend entry, so you can compare a handful of
    # names head-to-head against SPY / the baskets. Lines only (no pins); a clicked
    # chip's white overlay still rides brightest on top.
    ckl <- tryCatch(lc_checked_lines(), error = function(e) NULL)
    if (!is.null(ckl) && !is.null(d$qscmp_to))
      ckl <- lapply(ckl, function(x) { x$line <- lc_win_cut(x$line, d$qscmp_to)
                                       x$ev <- lc_win_cut(x$ev, d$qscmp_to, "date"); x })
    ck_suffix <- ""
    if (!is.null(ckl) && length(ckl)) {
      # all checked lines draw white with a small white end-label (Kevin
      # 2026-08-28); names tell apart by label + pins, not color, so the old
      # per-name palette is gone
      ck_sym <- c(buy = "triangle-up", hold = "diamond", sell = "triangle-down", closed = "x")
      ck_pincol <- c(buy = "#22c55e", hold = "#f59e0b", sell = "#ef4444", closed = "#94a3b8")
      for (nm in names(ckl)) {
        ln <- ckl[[nm]]$line; ev <- ckl[[nm]]$ev
        if (smooth) {                                      # pins ride the drawn line, hover stays real
          ln <- lc_strong_line(ln)
          if (!is.null(ev) && nrow(ev)) {
            ev$raw <- ev$ret
            ev$ret <- approx(as.numeric(as.Date(ln$d)), ln$ret,
                             xout = as.numeric(as.Date(ev$date)), rule = 2)$y
          }
        }
        fig <- do.call(add_trace, c(list(fig, x = ln$d, y = ln$ret, type = "scatter", mode = "lines",
          name = nm, legendgroup = paste0("ck_", nm), showlegend = TRUE,
          line = lc_line(color = "#f8fafc", width = 2)), hov(nm, ln$raw)))
        if (nrow(ln) > 0) {
          last_i <- nrow(ln)
          endlabels[[length(endlabels) + 1]] <- list(
            x = ln$d[last_i], y = ln$ret[last_i], text = tk_g(nm),
            textfont = list(color = "#f8fafc", size = 8), showarrow = FALSE,
            xanchor = "left", yanchor = "middle", xshift = 5)
        }
        # dated buy/hold/sell pins riding this name's own line: big distinct
        # markers (buy = green up-triangle, hold = amber diamond, sell = red
        # down-triangle) with an always-visible state label so the lifecycle reads
        # off the line without hovering (sell labels above, buy/hold below).
        if (!is.null(ev) && nrow(ev)) {
          for (s in unique(ev$state)) {
            sub <- ev[ev$state == s, , drop = FALSE]
            pc  <- if (s %in% names(ck_pincol)) ck_pincol[[s]] else "#94a3b8"
            fig <- add_trace(fig, x = sub$date, y = sub$ret, type = "scatter",
              mode = "markers+text",
              legendgroup = paste0("ck_", nm), showlegend = FALSE,
              marker = list(
                symbol = if (s %in% names(ck_sym)) ck_sym[[s]] else "circle",
                color = pc, size = 14, line = list(color = "#0b1220", width = 1.4)),
              text = toupper(s),
              textposition = if (identical(s, "sell")) "top center" else "bottom center",
              textfont = list(color = pc, size = 10),
              cliponaxis = FALSE,
              hovertext = sprintf("%s %s %s", nm, toupper(s), format(sub$date)),
              hovertemplate = if (smooth) "%{hovertext}<br>%{customdata:.1f}%<extra></extra>"
                              else "%{hovertext}<br>%{y:.1f}%<extra></extra>",
              customdata = if (smooth) sub$raw)
          }
        }
      }
      ck_suffix <- sprintf(" · %d checked", length(ckl))
    }
    ann <- list(); ap_yrange <- NULL
    # all-pins mode (rebuilt 2026-10-06, Kevin: "need better way to represent").
    # ONE pin per (day, state), not one per name: a big symbol on the green line
    # (triangle-up buy, circle hold, triangle-down sell, white ring so it reads
    # on any line) plus ONE chip naming every ticker that changed that day, 4 to
    # a row. The old one-chip-per-name packer stacked 16-25 chips on busy days,
    # and the headroom reserved for those towers stretched the y-axis to 50%+
    # and squashed the lines into the bottom quarter. Chips are packed in pixel
    # space against an assumed plot area (PW x PH, on the small side) with the
    # lines and pins as obstacles, so no chip hides a line or another pin; the
    # y-range then grows only as much as the tallest real stack needs.
    ap_suffix <- ""
    ap <- tryCatch(lc_all_pins(), error = function(e) NULL)
    ap <- lc_win_cut(ap, d$qscmp_to, "date")
    if (!is.null(ap) && nrow(ap)) {
      apcol <- c(buy = "#34d399", hold = "#fbbf24", sell = "#f87171")
      apsym <- c(buy = "triangle-up", hold = "circle", sell = "triangle-down")
      apnam <- c(buy = "Buy", hold = "Hold", sell = "Sell")
      if (is.null(ap$prev)) ap$prev <- NA_character_
      if (is.null(ap$recorded)) ap$recorded <- FALSE
      if (is.null(ap$src)) ap$src <- "engine"
      if (is.null(ap$settled)) ap$settled <- TRUE
      if (is.null(ap$hidden)) ap$hidden <- ""
      if (is.null(ap$n_hidden)) ap$n_hidden <- 0L
      ap$settled <- ap$settled %in% TRUE
      ap <- ap[order(ap$date, match(ap$state, c("buy", "hold", "sell")), ap$ticker), , drop = FALSE]
      # an unsettled (hollow) pin never shares a chip with a settled one that day
      key <- paste(format(ap$date), ap$state, ap$settled)
      PER <- 4L                                       # tickers per chip row
      G <- do.call(rbind, lapply(unique(key), function(k) {
        s <- ap[key == k, , drop = FALSE]
        rows <- vapply(split(tk_g(s$ticker), ceiling(seq_len(nrow(s)) / PER)),
                       paste, character(1), collapse = " ")
        how <- ifelse(is.na(s$prev) | !(s$prev %in% c("buy", "hold", "sell")), "new",
               ifelse(s$prev == "sell", "re-entered", paste("from", s$prev)))
        fold <- ifelse(s$n_hidden > 0,
                       sprintf(" · %d quick flip%s hidden: %s", s$n_hidden,
                               ifelse(s$n_hidden == 1, "", "s"), s$hidden), "")
        hov <- paste0(
          sprintf("<b>%s · %s</b> · %d name%s", toupper(s$state[1]),
                  format(s$date[1], "%b %d"), nrow(s), if (nrow(s) == 1) "" else "s"),
          if (!isTRUE(s$settled[1]))
            sprintf("<br><i>current state, younger than %d run-days: not settled</i>", PIN_DWELL_RUNDAYS) else "",
          if (!any(s$recorded %in% TRUE))
            (if (any(s$src %in% "nested"))
               "<br><i>backfilled by the shadow (context, not live evidence)</i>"
             else "<br><i>replayed from the ledger (daily recording began later)</i>") else "",
          "<br>", paste(sprintf("%s · %s · %s own%s", tk_g(s$ticker), how,
                                ifelse(is.na(s$own), "-", sprintf("%+.1f%%", s$own)), fold),
                        collapse = "<br>"))
        data.frame(date = s$date[1], state = s$state[1], ret = s$ret[1], n = nrow(s),
                   settled = isTRUE(s$settled[1]),
                   txt = paste(rows, collapse = "<br>"), nl = length(rows),
                   nc = max(nchar(rows)), hov = hov, stringsAsFactors = FALSE)
      }))
      # pixel layout. Sizes match font 10 / borderpad 3 / border 1.5 below; the
      # plot area measured 1020 x 625 px in the 760px-tall widget at a ~1200px
      # card (headless Chrome, 2026-10-06) - assume a bit less so a narrower
      # window still clears.
      xlo <- as.numeric(min(cmp$d)); xhi <- as.numeric(max(cmp$d))
      span <- max(xhi - xlo, 1)
      PW <- 960; PH <- 600; CW <- 7.2; LH <- 14; PADW <- 14; PADH <- 9
      GAP <- 3; STEM <- 14; MD <- 15                  # MD = min px between pins
      # Pins closer than MD hid each other (Aug 12-14 held four within three
      # days; a BUY and a HOLD on one day sat on the same spot). Spread them the
      # least possible with order kept - an isotonic fit of x - k*MD, so each
      # moves under ~2 days - then sit each pin back ON the displayed green line
      # at its drawn x. The hover keeps the true date.
      p0 <- (as.numeric(G$date) - xlo) / span * PW
      k  <- seq_len(nrow(G)) - 1
      xp <- if (nrow(G) > 1) isoreg(p0 - k * MD)$yf + k * MD else p0
      if (nrow(G) > 1) {                              # keep the spread inside the axis
        xp[nrow(G)] <- min(xp[nrow(G)], PW)
        for (j in (nrow(G) - 1):1) xp[j] <- min(xp[j], xp[j + 1] - MD)
        xp[1] <- max(xp[1], 0)
        for (j in 2:nrow(G)) xp[j] <- max(xp[j], xp[j - 1] + MD)
      }
      G$xnum <- xlo + xp / PW * span
      G$xs <- format(as.POSIXct(G$xnum * 86400, origin = "1970-01-01", tz = "UTC"),
                     "%Y-%m-%d %H:%M:%S", tz = "UTC")
      # pins sit on the displayed line: passed, else graded, else SPY (the
      # market-creators view can hold no basket series at all)
      lineY <- if (any(!is.na(cmpL$passed_pct))) cmpL$passed_pct
               else if (any(!is.na(cmpL$graded_pct))) cmpL$graded_pct else cmpL$spy_pct
      okl <- !is.na(lineY)
      G$y <- if (sum(okl) >= 2)
               approx(as.numeric(cmpL$d)[okl], lineY[okl], xout = G$xnum, rule = 2)$y
             else G$ret
      for (s in c("buy", "hold", "sell")) for (stl in c(TRUE, FALSE)) {
        sub <- G[G$state == s & G$settled == stl, , drop = FALSE]; if (!nrow(sub)) next
        # POSIXct, not the text form the chips use: a text x next to the Date
        # lines made plotly warn "both discrete & non-discrete data" per render.
        # Unsettled = the open run, younger than the dwell: same glyph, hollow.
        fig <- add_markers(fig, x = as.POSIXct(sub$xnum * 86400, origin = "1970-01-01", tz = "UTC"),
          y = sub$y, name = if (stl) apnam[[s]] else paste(apnam[[s]], "(new)"),
          legendgroup = paste0("pin_", s, if (stl) "" else "_new"), showlegend = TRUE,
          marker = if (stl)
            list(color = apcol[[s]], size = 13, symbol = apsym[[s]],
                 line = list(color = "#f8fafc", width = 1.5))
          else
            list(color = apcol[[s]], size = 13, symbol = paste0(apsym[[s]], "-open"),
                 line = list(color = apcol[[s]], width = 2)),
          text = sub$hov, hovertemplate = "%{text}<extra></extra>")
      }
      # y-range from every drawn line (checked / pick / top-performer lines too,
      # so a name that dips below 0 or spikes high never clips)
      tpret <- if (!is.null(tp) && !is.null(tp$line)) tp$line$ret else numeric(0)
      plret <- if (!is.null(pl) && length(pl$lines))
                 unlist(lapply(pl$lines, function(x) x$ret), use.names = FALSE) else numeric(0)
      ckret <- if (!is.null(ckl) && length(ckl))
                 unlist(lapply(ckl, function(x) x$line$ret), use.names = FALSE) else numeric(0)
      yhi <- max(c(cmp$spy_pct, cmp$graded_pct, cmp$passed_pct, G$y, tpret, plret, ckret), na.rm = TRUE)
      ylo <- min(c(cmp$spy_pct, cmp$graded_pct, cmp$passed_pct, G$y, tpret, plret, ckret, 0), na.rm = TRUE)
      yspan <- max(yhi - ylo, 1)
      ylo2 <- ylo - yspan * 0.05
      yhi2 <- yhi + yspan * 0.06
      # chip packer (pixel space, chips above their pin)
      wp <- G$nc * CW + PADW
      hp <- G$nl * LH + PADH
      xa <- ifelse((PW - xp) / PW < 0.06, "right", ifelse(xp / PW < 0.04, "left", "center"))
      cx <- ifelse(xa == "right", xp - wp / 2 + 3, ifelse(xa == "left", xp + wp / 2 - 3, xp))
      # obstacles: the three basket lines (a thin box per day) + every pin
      lpts <- do.call(rbind, lapply(c("spy_pct", "graded_pct", "passed_pct"), function(cn)
        if (cn %in% names(cmpL)) data.frame(x = as.numeric(cmpL$d), y = cmpL[[cn]]) else NULL))
      lpts <- lpts[!is.na(lpts$y), , drop = FALSE]
      lw <- 2 * PW / max(nrow(cmpL), 1)
      off <- rep(STEM, nrow(G))
      for (it in 1:8) {
        R  <- yhi2 - ylo2
        yp <- (G$y - ylo2) / R * PH                     # pin height above plot bottom, px
        bx  <- c((lpts$x - xlo) / span * PW, xp)
        bw  <- c(rep(lw, nrow(lpts)), rep(14, nrow(G)))
        ly  <- (lpts$y - ylo2) / R * PH
        blo <- c(ly - 2, yp - 7); bhi <- c(ly + 2, yp + 7)
        for (i in seq_len(nrow(G))) {
          o <- STEM
          # the +0.5 px keeps a chip from re-clashing with the box it just
          # cleared on floating-point rounding (that looped forever); the cap
          # is a second guard
          for (step in 1:400) {
            lo <- yp[i] + o; hi <- lo + hp[i]
            hit <- which(abs(cx[i] - bx) < (wp[i] + bw) / 2 + GAP &
                         lo < bhi + GAP & hi > blo - GAP)
            if (!length(hit)) break
            o <- max(o + 1, max(bhi[hit]) + GAP + 0.5 - yp[i])
          }
          off[i] <- o
          bx <- c(bx, cx[i]); bw <- c(bw, wp[i])
          blo <- c(blo, yp[i] + o); bhi <- c(bhi, yp[i] + o + hp[i])
        }
        if (max(yp + off + hp) <= PH - 4) break
        # grow the top just enough for the tallest stack: a chip at data y whose
        # top sits h px above it needs (y - ylo2)/R*PH + h <= PH
        need <- max((G$y - ylo2) * PH / pmax(PH - off - hp - 4, PH * 0.25))
        yhi2 <- ylo2 + max(R * 1.02, need)
      }
      apann <- lapply(seq_len(nrow(G)), function(i) {
        cc <- apcol[[G$state[i]]]
        list(x = G$xs[i], y = G$y[i], xref = "x", yref = "y",
             text = G$txt[i], xanchor = xa[i], yanchor = "bottom", align = "left",
             showarrow = TRUE, arrowhead = 0, arrowwidth = 1.4, arrowcolor = cc,
             standoff = 7, ax = 0, ay = -off[i],
             font = list(color = cc, size = 10),
             # OPAQUE bg: the bright basket lines bleed through a translucent box
             opacity = 1, bgcolor = "#0b1220", bordercolor = cc, borderwidth = 1.5,
             borderpad = 3, hovertext = G$hov[i],
             hoverlabel = list(bgcolor = "rgba(2,6,23,0.95)", bordercolor = cc,
                               font = list(color = "#e2e8f0", size = 11)))
      })
      # draw the highest chips first: plotly paints annotations in order, so a
      # lower chip's opaque box then covers the stems of chips stacked above it
      # instead of those stems striking through its tickers
      ann <- c(ann, apann[order(-off)])
      ap_yrange <- c(ylo2, yhi2)
      ap_suffix <- " · all qualstream pins"
    }
    yax <- list(title = "Equal-weight return (%)", color = "#cbd5e1",
                gridcolor = "rgba(148,163,184,0.10)", zeroline = TRUE,
                zerolinecolor = "rgba(148,163,184,0.30)", ticksuffix = "%")
    if (!is.null(ap_yrange)) yax$range <- ap_yrange
    else if (smooth) {                                    # no zoom on toggle: the real lines' range
      rawy <- c(cmp$spy_pct, cmp$graded_pct, cmp$passed_pct,
                if (!is.null(tp) && !is.null(tp$line)) tp$line$ret,
                if (!is.null(pl) && length(pl$lines)) unlist(lapply(pl$lines, function(x) x$ret), use.names = FALSE),
                if (!is.null(ckl) && length(ckl)) unlist(lapply(ckl, function(x) x$line$ret), use.names = FALSE))
      ylo <- min(c(rawy, 0), na.rm = TRUE); yhi <- max(rawy, na.rm = TRUE); ysp <- max(yhi - ylo, 1)
      yax$range <- c(ylo - ysp * 0.05, yhi + ysp * 0.06)
    }
    dark_layout(fig,
      title = list(text = paste0(sprintf(
          "HINDSIGHT SKETCH (not a real score) - graded/passed vs benchmark, from %s%s",
          format(d$qscmp_anchor, "%b %d"),
          if (is.null(d$qscmp_to)) "" else format(d$qscmp_to, " to %b %d")), cl_suffix,
          ap_suffix, tp_suffix, pl_suffix, if (smooth) " · smoothed" else ""),
        font = list(color = "#f8fafc", size = 13), x = 0.5),
      xaxis = list(title = "", color = "#cbd5e1", type = "date",
                   gridcolor = "rgba(148,163,184,0.10)", zeroline = FALSE),
      yaxis = yax,
      # legend tucked just under the date labels (was y = -0.35: ~120px of dead
      # space that the plot can use for the pins instead)
      legend = list(font = list(color = "#e2e8f0"), orientation = "h", x = 0, y = -0.08,
                    yanchor = "top"),
      hovermode = "closest",
      hoverlabel = list(bgcolor = "rgba(2,6,23,0.92)", bordercolor = "#475569",
                        font = list(color = "#e2e8f0", size = 11)),
      margin = list(l = 60, r = 120, t = 44, b = 40),
      annotations = c(endlabels, ann))
  })

  # Who is in each profile group, one checkbox per group: untick to hide that
  # group's lines (the legend shows the groups; hover a line for its group too).
  # Re-rendered only when the groups change; the ticks come from lc_group_hidden.
  output$lcLineGroupsLC <- renderUI({
    g <- lc_line_groups(); if (is.null(g) || !nrow(g)) return(NULL)
    d <- lc_sketch_data()
    g <- g[order(g$rank), , drop = FALSE]
    labs <- unique(g$label)
    hidden <- isolate(lc_group_hidden())
    div(style = "color:#94a3b8; font-size:0.72rem; margin:0.1rem 0 0.2rem;",
        span(style = "color:#f8fafc; font-weight:700;", "Line groups"),
        sprintf(paste(" from %s%s, by how close each name's line runs to SPY's and to the others",
                      "(level and shape over the window, at most 4 groups). Untick a group to hide its lines."),
                format(as.Date(d$qscmp_anchor), "%b %d"),
                if (is.null(d$qscmp_to)) "" else format(as.Date(d$qscmp_to), " to %b %d")),
        div(class = "lc-groupbox",
            checkboxGroupInput("lcGroupShow", NULL, inline = TRUE,
              choiceNames = lapply(labs, function(l) tags$span(
                style = sprintf("color:%s;", g$col[g$label == l][1]),
                tags$b(l), paste0(": ", paste(g$ticker[g$label == l], collapse = ", ")))),
              choiceValues = labs, selected = setdiff(labs, hidden))))
  })

  # Honesty caption: the comparison is a single-grade-run snapshot with partial
  # coverage, so it describes this batch, it does not validate qualstream.
  output$qsCompareNoteLC <- renderUI({
    req(app_dataLC())
    d <- app_dataLC()
    if (is.null(d$qscmp) || nrow(d$qscmp) == 0 || all(is.na(d$qscmp$graded_pct))) return(NULL)
    qs_g  <- if (!is.null(d$qs)) suppressWarnings(as.numeric(d$qs$grade)) else numeric(0)
    n_g   <- if (!is.null(d$qs)) length(d$qs$ticker) else 0L
    n_p   <- if (!is.null(d$qs)) sum(qs_g >= 68, na.rm = TRUE) else 0L
    qs_ran <- if (!is.null(d$qs) && nrow(d$qs) > 0 && "as_of" %in% names(d$qs))
                suppressWarnings(max(as.Date(d$qs$as_of))) else as.Date(NA)
    # expiry-cliff warning: every grade shares one as_of, so the orange + and
    # this chart empty together QS_MAX_AGE_DAYS after that date. Surface the
    # countdown as it nears so a slipped grading run does not silently blank the
    # marks (board membership + seeding survive - they no longer gate on grade).
    expires <- if (is.na(qs_ran)) as.Date(NA) else qs_ran + QS_MAX_AGE_DAYS
    dleft   <- if (is.na(expires)) NA_integer_ else as.integer(expires - Sys.Date())
    cliff <- if (!is.na(dleft) && dleft <= 30)
               sprintf(" All %d grades share one as-of, so every orange + expires together on %s (%d days) unless a fresh grading run lands first.",
                       n_g, format(expires), dleft) else ""
    ap_note <- if (isTRUE(input$lcAllPins))
      div(style = "color:#94a3b8; font-size:0.72rem; margin:0.1rem 0 0.4rem;",
        span(style = "color:#f8fafc; font-weight:700;", "All qualstream pins on"),
        if (!is.null(lc_nc_only()))
          paste0(" - every \u25c6 disruptor and ", NC_GEAR, " enabler on the master list, from every cluster. ")
        else " - every name in the master list (graded >= 68, plus your open positions). ",
        "One marker per day on the green line, ",
        span(style = "color:#34d399;", "▲ buy"), " / ",
        span(style = "color:#fbbf24;", "● hold"), " / ",
        span(style = "color:#f87171;", "▼ sell"),
        ", and its chip lists every name that moved to that state that day. ",
        "Hover a marker or chip for each name's move.",
        if (isTRUE(input$lcEveryFlip))
          span(style = "color:#f8fafc;", " Every recorded flip is drawn.")
        else
          span(style = "color:#f8fafc;",
               sprintf(" A pin is a state the board then held for %d+ run-days; entries and sells always pin.",
                       PIN_DWELL_RUNDAYS),
               " Shorter flips are folded into the next pin's hover. A hollow marker is the current state,",
               sprintf(" younger than %d run-days, and may be withdrawn. Run-days are ledger run-days.", PIN_DWELL_RUNDAYS),
               " The table and the flip history still show every flip; tick Show every flip for the raw sequence."),
        if (identical(input$lcStateView, "nested"))
          span(style = "color:#f8fafc;",
               " STATE = Nested shadow: the pins are the shadow's entries (▲) and exits (▼),",
               " nothing in between; pins before Oct 12 2026 are backfilled context.")
        else NULL,
        local({
          r0 <- if (!is.null(d$rec_states) && nrow(d$rec_states))
                  suppressWarnings(min(as.Date(d$rec_states$as_of), na.rm = TRUE)) else as.Date(NA)
          if (is.na(r0)) NULL
          else paste0(" Before ", format(r0, "%b %d"),
                      ", when daily recording began, states are replayed from the ledger.")
        }),
        # never fail silently: say when nothing drew and the usual reason why
        local({
          ap_n <- tryCatch(lc_all_pins(), error = function(e) NULL)
          n_pins <- if (!is.null(ap_n)) nrow(ap_n) else 0L
          if (n_pins > 0) NULL
          else span(style = "color:#fb923c;",
            if (!is.null(lc_nc_only()))
              paste0(" 0 pins drawn - no \u25c6 disruptor or ", NC_GEAR, " enabler on the master list has a buy/hold/sell event in this window.")
            else " 0 pins drawn - no master-list name has a buy/hold/sell event in this window.")
        }))
      else NULL
    nc_now <- lc_nc_only()
    pl_note <- if (!is.null(nc_now)) local({
      # market-creators view: its own lines and baskets, every cluster
      pl_now <- tryCatch(lc_pick_lines(), error = function(e) NULL)
      det <- d$qscmp_detail
      n_ser <- if (!is.null(det) && nrow(det)) length(intersect(nc_now, toupper(det$ticker))) else 0L
      tp_gone <- isTRUE(input$lcTopPerf) &&
                 is.null(tryCatch(lc_top_perf(), error = function(e) NULL))
      div(style = "color:#94a3b8; font-size:0.72rem; margin:0.1rem 0 0.4rem;",
        span(style = "color:#f8fafc; font-weight:700;", paste0("\u25c6 Disruptors + ", NC_GEAR, " enablers view")),
        if (is.null(pl_now)) " - no marked name has prices in this window."
        else tagList(
          " - every marked name, from every cluster, is drawn as its own ",
          span(style = "color:#c084fc;", "violet line"),
          " with its ticker at the end, rebased to 0% at the anchor; names off the master list",
          " are dashed and appear on the chart only. "),
        if (n_ser > 0) tagList("The ", span(style = "color:#f59e0b;", "orange"), " and ",
                               span(style = "color:#10b981;", "green"),
                               sprintf(" lines are the marked names' own baskets (%d with a graded series: all graded / passed 68). ", n_ser))
        else " No marked name has a graded series in this window, so only SPY and the name lines draw. ",
        "Hover a line for its ticker and return.",
        if (tp_gone) span(style = "color:#fb923c;",
          " Top performers needs 4 or more marked names with a graded series; this view has ",
          sprintf("%d.", n_ser)))
    }) else if (isTRUE(input$lcAllPickLines))
      div(style = "color:#94a3b8; font-size:0.72rem; margin:0.1rem 0 0.4rem;",
        span(style = "color:#f8fafc; font-weight:700;", "All qualstream pick lines on"),
        " - every passed-qualstream (>= 68) board name as its own ",
        span(style = "color:#f8fafc;", "faint white line"),
        ", each rebased to 0% at the anchor. Hover a line for its ticker and return. ",
        "The ", span(style = "color:#10b981;", "green line"),
        " is their average - these are the paths behind it. Click a chip to brighten ",
        "one out of the pack.")
      else if (!is.na(lc_single_cluster()))
      div(style = "color:#94a3b8; font-size:0.72rem; margin:0.1rem 0 0.4rem;",
        span(style = "color:#f8fafc; font-weight:700;",
             sprintf("Cluster %d selected", lc_single_cluster())),
        " - each of its master-list names is drawn as its own ",
        span(style = "color:#f8fafc;", "light grey line"),
        " with its ticker at the end, rebased to 0% at the anchor, so a pin can be read ",
        "against the name's own path. The ", span(style = "color:#10b981;", "green line"),
        " is their average. Hover a line for its ticker and return; tick a row to draw it bright.")
      else NULL
    tagList(ap_note, pl_note,
    div(style = "color:#64748b; font-size:0.72rem; margin:0.15rem 0 0.6rem;",
      sprintf(paste("Descriptive, not a walk-forward test: one retroactive grade run (%s) applied",
                    "backward, so membership is hindsight-picked - trust green-vs-orange, not either",
                    "line vs SPY.%s"),
              if (is.na(qs_ran)) "n/a" else format(qs_ran), cliff)))
  })

  # The full-record table (output$tableLC) was removed with its UI element:
  # the server render survived as an orphan, recomputing id-map/delist/cluster
  # filtering per derive for output nobody displays - and it read dv$state_now
  # directly, bypassing lc_state_for_display (audit 2026-08-28). To restore it,
  # rebuild from git history AND route its State through lc_state_for_display.
