# AlphaStream dashboard - Lifecycle: sketch layers (pins, pick lines, checked names, top performers). Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # Names in the sketch's scope (the pins and the strategy table): the master
  # list's set, every name graded >= QS_MIN plus every open position, in a live
  # state. NOT gated on board_ok: after the 2026-10-01 re-mint de-gated id 9,
  # every held name fell off the gate's board, so the old board_ok rule left 0
  # pin candidates while the master list still showed all 25 holds (17 board_ok
  # names, 23 graded, overlap 0). Then the cluster-id view filter: only names in
  # the ticked clusters (by live id), owned or not (Kevin 2026-10-07, replacing
  # the 2026-09-01 owned-names exemption; every master-list cluster has its own
  # box). Untouched / all ticked = everything. narrow = FALSE skips that view
  # filter (the per-cluster strategy table splits the whole set itself).
  lc_scope_names <- function(dv, narrow = TRUE) {
    grade_u <- setNames(suppressWarnings(as.numeric(dv$qs_grade)), toupper(names(dv$qs_grade)))
    pp_univ <- tryCatch(lc_pos(), error = function(e) NULL)
    owned_univ <- if (!is.null(pp_univ) && nrow(pp_univ)) {
      sdu <- as.character(pp_univ$sold_date)
      toupper(pp_univ$ticker[is.na(sdu) | !nzchar(sdu)])
    } else character(0)
    tks <- dv$tickers[
      dv$state_now[dv$tickers] %in% c("buy", "hold", "sell", "closed") &
      ((!is.na(grade_u[toupper(dv$tickers)]) & grade_u[toupper(dv$tickers)] >= QS_MIN) |
       toupper(dv$tickers) %in% owned_univ)]
    if (narrow) tks[lc_in_sel(tks)] else tks
  }

  # "Show all pins" mode: every qualstream-pass board ticker's buy/hold/sell
  # entries as pins on the benchmark chart - NO lines, label = ticker only,
  # colored by state (buy green / hold orange / sell red). Reuses the exact
  # board-faithful trail + as-of price rebase the single-ticker overlay uses,
  # just mapped over the whole orange-+ set at once. Off unless input$lcAllPins.
  lc_all_pins <- reactive({
    if (!isTRUE(input$lcAllPins)) return(NULL)
    if (is.null(input$db_pass) || input$db_pass == "") return(NULL)
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    if (is.null(dv) || is.null(dv$M)) return(NULL)
    d <- lc_sketch_data()
    anchor <- if (!is.null(d) && !is.null(d$qscmp_anchor)) as.Date(d$qscmp_anchor) else NA
    if (is.na(anchor)) return(NULL)
    tks <- lc_scope_names(dv)
    if (!length(tks)) return(NULL)
    con <- tryCatch(get_con(input), error = function(e) NULL); if (is.null(con)) return(NULL)
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    px <- tryCatch(lc_px_get(con, c(tks, "SPY"), anchor), error = function(e) NULL)
    if (is.null(px) || !nrow(px)) return(NULL)
    px$d <- as.Date(px$d); px$px <- as.numeric(px$px)
    asof <- function(df, dd) { s <- df[df$d <= dd, , drop = FALSE]
      if (!nrow(s)) NA_real_ else s$px[nrow(s)] }
    # pins ride the passed-qualstream (green) line so the y-axis stays put and the
    # basket lines never compress: a pin's y is that line's value on the event
    # date, not the name's own return (kept as `own` for the hover). Fall back to
    # the graded (orange) line if nothing in range passed >= 68.
    ln <- d$qscmp
    ln <- if (!is.null(ln) && nrow(ln)) data.frame(d = as.Date(ln$d),
        y = suppressWarnings(as.numeric(
              if ("passed_pct" %in% names(ln) && any(!is.na(ln$passed_pct)))
                ln$passed_pct else ln$graded_pct)),
        stringsAsFactors = FALSE) else NULL
    if (!is.null(ln)) { ln <- ln[!is.na(ln$d) & !is.na(ln$y), , drop = FALSE]
                        ln <- ln[order(ln$d), , drop = FALSE] }
    if (is.null(ln) || !nrow(ln)) return(NULL)
    line_at <- function(dd) { s <- ln$y[ln$d <= dd]
      if (!length(s)) ln$y[1] else s[length(s)] }
    # User overrides live in the tracker (lc_pos sold_date), NOT in derivedLC's
    # pure model state_now, so the pin layer must apply them itself to agree with
    # the board's sell column. Map ticker -> sold_date for the hand-sold names.
    pp_sold  <- tryCatch(lc_pos(), error = function(e) NULL)
    pos_sold <- if (!is.null(pp_sold) && nrow(pp_sold)) {
      sdv <- suppressWarnings(as.Date(as.character(pp_sold$sold_date)))
      keep <- !is.na(sdv); setNames(sdv[keep], toupper(pp_sold$ticker)[keep])
    } else setNames(as.Date(character(0)), character(0))
    # settled-state pins: the dwell clock is the ledger's run-days, not calendar
    # days; "Show every flip" (lcEveryFlip) restores the raw change sequence
    run_d <- sort(unique(if (!is.null(d$led$dd)) d$led$dd else as.Date(d$led$d)))
    last_d <- if (length(run_d)) max(run_d) else Sys.Date()
    every_flip <- isTRUE(input$lcEveryFlip)
    out <- list()
    px_by <- split(px, toupper(px$ticker))           # one pass, not one scan per name
    for (tk in tks) {
      tkr <- px_by[[toupper(tk)]]
      if (is.null(tkr) || !nrow(tkr)) next
      tkr <- tkr[order(tkr$d), , drop = FALSE]
      base_tk <- asof(tkr, anchor); if (is.na(base_tk)) base_tk <- tkr$px[1]
      if (is.na(base_tk) || base_tk == 0) next
      now_state <- if (tk %in% names(dv$state_now)) dv$state_now[[tk]] else NA_character_
      prov_tk <- !is.null(dv$prov_of) && tk %in% names(dv$prov_of) && isTRUE(dv$prov_of[[tk]])
      dl_tk <- lc_dl_of(d, toupper(tk))
      # STATE radio = Nested shadow: the pins are the shadow's own entries (buy)
      # and exits (sell), nothing in between - its decisive line. Otherwise the
      # engine trail as before. Backfilled shadow rows count as not recorded.
      view <- if (!is.null(input$lcStateView)) input$lcStateView else "engine"
      nested_rec <- NULL
      if (identical(view, "nested")) {
        nz <- if (!is.null(d$nested) && nrow(d$nested))
                d$nested[toupper(d$nested$ticker) == toupper(tk), , drop = FALSE] else NULL
        if (is.null(nz) || !nrow(nz)) next
        nz <- nz[(nz$state == "BUY" & nz$run_date == nz$entered_at) | nz$state == "EXIT", , drop = FALSE]
        if (!nrow(nz)) next
        ev <- data.frame(date = as.Date(nz$run_date),
                         state = ifelse(nz$state == "EXIT", "sell", "buy"),
                         stringsAsFactors = FALSE)
        ev <- ev[order(ev$date), , drop = FALSE]
        # the shadow's ledger starts 2026-06-16, the board's at the epoch: like the
        # engine trail, show nothing before the epoch and pin a name the shadow
        # already held on the epoch day there (hover keeps it marked backfilled)
        ep  <- as.Date(LEDGER_EPOCH)
        pre <- ev[ev$date < ep, , drop = FALSE]
        ev  <- ev[ev$date >= ep, , drop = FALSE]
        if (nrow(pre) && identical(pre$state[nrow(pre)], "buy"))
          ev <- rbind(data.frame(date = ep, state = "buy", stringsAsFactors = FALSE), ev)
        if (!nrow(ev)) next
        nested_rec <- as.Date(nz$run_date[nz$proven_src %in% "resolver"])
      } else {
        trail <- lc_board_trail(dv$M, dv$dates, tk, d$led, LEDGER_EPOCH,
                                prov_tk, now_state, dv$hz, dl_tk, rec = d$rec_states)
        if (is.null(trail) || !nrow(trail)) next
        ev <- data.frame(date = as.Date(trail$date), state = trail$state,
                         stringsAsFactors = FALSE)
      }
      # hand-sold override: drop model events on/after the sell date and pin a
      # sell there, so the sketch matches the board's sell column for this name.
      sold_d <- if (toupper(tk) %in% names(pos_sold)) pos_sold[[toupper(tk)]] else as.Date(NA)
      if (!is.na(sold_d) && sold_d >= anchor)
        ev <- rbind(ev[ev$date < sold_d, , drop = FALSE],
                    data.frame(date = sold_d, state = "sell", stringsAsFactors = FALSE))
      # settled-state pins (lc_pin_debounce): keep entries, sells and states the
      # board then held for PIN_DWELL_RUNDAYS; fold the rest into the next pin's
      # hover with the recorded reason. Nested rows are entries/exits, so none fold.
      why_of <- if (!is.null(d$rec_states) && nrow(d$rec_states) && "why" %in% names(d$rec_states)) {
        rw <- d$rec_states[toupper(d$rec_states$ticker) == toupper(tk) & d$rec_states$hz == dv$hz, , drop = FALSE]
        setNames(as.character(rw$why), as.character(rw$as_of))
      } else NULL
      ev <- lc_pin_debounce(ev, run_d, last_d, PIN_DWELL_RUNDAYS, every_flip, why_of)
      if (is.null(ev) || !nrow(ev)) next
      # previous state (from the FULL kept sequence, so a window's first pin still
      # knows whether it was an entry or a flip) - shown in the pin hover
      ev$prev <- c(NA_character_, head(ev$state, -1))
      ev <- ev[ev$date >= anchor & ev$state %in% c("buy", "hold", "sell"), , drop = FALSE]
      if (!nrow(ev)) next
      # recorded = the board's own daily capture holds this (name, day);
      # otherwise the state is replayed from the ledger (pre-capture days)
      rec_d <- if (!is.null(d$rec_states) && nrow(d$rec_states))
                 as.Date(d$rec_states$as_of[toupper(d$rec_states$ticker) == toupper(tk) &
                                             d$rec_states$hz == dv$hz]) else as.Date(character(0))
      ev$recorded <- if (!is.null(nested_rec)) ev$date %in% nested_rec else ev$date %in% rec_d
      ev$src <- if (!is.null(nested_rec)) "nested" else "engine"
      ev$own <- vapply(ev$date, function(dd) { p <- asof(tkr, dd)
        if (is.na(p)) NA_real_ else (p / base_tk - 1) * 100 }, numeric(1))
      ev <- ev[!is.na(ev$own), , drop = FALSE]; if (!nrow(ev)) next
      ev$ret <- vapply(ev$date, line_at, numeric(1))   # sit on the green line
      ev$ticker <- toupper(tk)
      out[[tk]] <- ev
    }
    if (!length(out)) return(NULL)
    do.call(rbind, out)
  })

  # ── All qualstream pick lines (white spaghetti) ──────────────────────────
  # Opt-in (checkbox lcAllPickLines, OFF by default). Every passed-qualstream
  # board name drawn as its OWN faint white return line - the green aggregate
  # decomposed into its constituents, so the spread hiding behind the average
  # shows. Same name set + cluster filter as lc_all_pins, but lines, no pins.
  # A clicked chip's single overlay still draws bright white on top, so the
  # focused name stands out of the pack (both are unset here in focus mode? no -
  # unlike all-pins we do NOT hide in focus, the overlay just rides brighter).
  lc_pick_lines <- reactive({
    # on by the checkbox, or automatically (light grey, with end labels) while
    # exactly one cluster is ticked, so the status of each name in that cluster
    # can be read against its own path and not only the green average. The
    # market-creators view always draws its own lines (mode "nc", below).
    nc_set <- lc_nc_only()
    one_cl <- lc_single_cluster()
    auto <- !isTRUE(input$lcAllPickLines) && !is.na(one_cl)
    if (is.null(nc_set) && !isTRUE(input$lcAllPickLines) && !auto) return(NULL)
    if (is.null(input$db_pass) || input$db_pass == "") return(NULL)
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    if (is.null(dv) || is.null(dv$M)) return(NULL)
    d <- lc_sketch_data()
    anchor <- if (!is.null(d) && !is.null(d$qscmp_anchor)) as.Date(d$qscmp_anchor) else NA
    if (is.na(anchor)) return(NULL)
    if (!is.null(nc_set)) {
      # every marked name from the tags (not dv$tickers): master-list names solid,
      # the rest dashed and drawn on the chart only (Kevin 2026-10-13)
      if (!length(nc_set)) return(NULL)
      ml <- lc_ml_names()
      lines <- lc_price_lines(nc_set, anchor)
      if (!length(lines)) return(NULL)
      tg <- lc_nc_tags()
      mk <- setNames(tg$marker, tg$ticker)
      en <- setNames(tg$enabler %in% TRUE, tg$ticker)
      off <- setdiff(names(lines), ml)
      why <- setNames(lc_offlist_why(off), off)
      return(list(lines = lines, n = length(lines), auto = TRUE, cluster = NA_integer_,
                  mode = "nc", offlist = off, why = why, marker = mk[names(lines)],
                  enabler = en[names(lines)]))
    }
    # Same name set as the master list: every name graded >= QS_MIN, plus every
    # open position, in a live state. NOT gated on board_ok: after the 2026-10-01
    # re-mint de-gated id 9, every held name fell off the gate's board, so the
    # old board_ok rule left 0 pin candidates while the master list still showed
    # all 25 holds (17 board_ok names, 23 graded, overlap 0).
    grade_u <- setNames(suppressWarnings(as.numeric(dv$qs_grade)), toupper(names(dv$qs_grade)))
    pp_univ <- tryCatch(lc_pos(), error = function(e) NULL)
    owned_univ <- if (!is.null(pp_univ) && nrow(pp_univ)) {
      sdu <- as.character(pp_univ$sold_date)
      toupper(pp_univ$ticker[is.na(sdu) | !nzchar(sdu)])
    } else character(0)
    tks <- dv$tickers[
      dv$state_now[dv$tickers] %in% c("buy", "hold", "sell", "closed") &
      ((!is.na(grade_u[toupper(dv$tickers)]) & grade_u[toupper(dv$tickers)] >= QS_MIN) |
       toupper(dv$tickers) %in% owned_univ)]
    # cluster-id view filter (same rule as the pins and the master list)
    tks <- tks[lc_in_sel(tks)]
    if (!length(tks)) return(NULL)
    out <- lc_price_lines(tks, anchor)
    if (!length(out)) return(NULL)
    list(lines = out, n = length(out), auto = auto, cluster = one_cl)
  })
  # Each ticker's own return line from the anchor (rebased to 0% there), from the
  # cdm price table up to SPY's last date. Named list of data.frame(d, ret);
  # tickers without prices are dropped.
  lc_price_lines <- function(tks, anchor) {
    con <- tryCatch(get_con(input), error = function(e) NULL); if (is.null(con)) return(list())
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    px <- tryCatch(lc_px_get(con, tks, anchor), error = function(e) NULL)
    if (is.null(px) || !nrow(px)) return(list())
    px$d <- as.Date(px$d); px$px <- as.numeric(px$px)
    asof <- function(df, dd) { s <- df[df$d <= dd, , drop = FALSE]
      if (!nrow(s)) NA_real_ else s$px[nrow(s)] }
    out <- list()
    px_by <- split(px, toupper(px$ticker))           # one pass, not one scan per name
    for (tk in tks) {
      tkr <- px_by[[toupper(tk)]]
      if (is.null(tkr) || !nrow(tkr)) next
      tkr <- tkr[order(tkr$d), , drop = FALSE]
      base_tk <- asof(tkr, anchor); if (is.na(base_tk)) base_tk <- tkr$px[1]
      if (is.na(base_tk) || base_tk == 0) next
      tkr$ret <- (tkr$px / base_tk - 1) * 100          # rebase each to 0% at anchor
      lo <- tkr[tkr$d >= anchor, c("d", "ret"), drop = FALSE]
      if (!nrow(lo)) next
      out[[toupper(tk)]] <- lo
    }
    out
  }

  # ── Checked master-list names -> their own return lines ───────────────────
  # Driven by the master-list row checkboxes (lc_sketch_ticks). Each checked name
  # is priced from cdm.ingest_combined and rebased to 0% at the SAME anchor as the
  # benchmark lines, so a name's line is directly comparable to SPY / the baskets.
  # Independent of the qualstream filter; follows the cluster boxes like the rest
  # of the sketch (a checked name outside the ticked clusters is hidden with its row).
  lc_checked_lines <- reactive({
    tks <- toupper(lc_sketch_ticks()); tks <- tks[nzchar(tks)]
    tks <- tks[lc_in_sel(tks)]
    if (!length(tks)) return(NULL)
    if (is.null(input$db_pass) || input$db_pass == "") return(NULL)
    d <- lc_sketch_data()
    anchor <- if (!is.null(d) && !is.null(d$qscmp_anchor)) as.Date(d$qscmp_anchor) else NA
    if (is.na(anchor)) return(NULL)
    dv <- tryCatch(derivedLC(), error = function(e) NULL)   # for the buy/hold/sell pins
    con <- tryCatch(get_con(input), error = function(e) NULL); if (is.null(con)) return(NULL)
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    px <- tryCatch(lc_px_get(con, tks, anchor), error = function(e) NULL)
    if (is.null(px) || !nrow(px)) return(NULL)
    px$d <- as.Date(px$d); px$px <- as.numeric(px$px)
    asof <- function(df, dd) { s <- df[df$d <= dd, , drop = FALSE]
      if (!nrow(s)) NA_real_ else s$px[nrow(s)] }
    ret_asof <- function(lo, dd) { s <- lo[lo$d <= dd, , drop = FALSE]
      if (!nrow(s)) NA_real_ else s$ret[nrow(s)] }
    out <- list()
    for (tk in tks) {
      tkr <- px[toupper(px$ticker) == tk, , drop = FALSE]
      if (!nrow(tkr)) next
      tkr <- tkr[order(tkr$d), , drop = FALSE]
      base_tk <- asof(tkr, anchor); if (is.na(base_tk)) base_tk <- tkr$px[1]
      if (is.na(base_tk) || base_tk == 0) next
      tkr$ret <- (tkr$px / base_tk - 1) * 100
      lo <- tkr[tkr$d >= anchor, c("d", "ret"), drop = FALSE]
      if (!nrow(lo)) next
      # lifecycle pins (buy/hold/sell transitions) placed on THIS name's own line,
      # so a checked name shows when it was bought / held / sold, dated, in context.
      ev <- data.frame(date = as.Date(character(0)), state = character(0),
                       ret = numeric(0), stringsAsFactors = FALSE)
      if (!is.null(dv) && !is.null(dv$M) && tk %in% rownames(dv$M)) {
        now_state <- if (!is.null(dv$state_now) && tk %in% names(dv$state_now))
                       dv$state_now[[tk]] else NA_character_
        prov_tk <- !is.null(dv$prov_of) && tk %in% names(dv$prov_of) &&
                   isTRUE(dv$prov_of[[tk]])
        dl_tk <- lc_dl_of(d, tk)
        trail <- tryCatch(lc_board_trail(dv$M, dv$dates, tk, d$led, LEDGER_EPOCH,
                                         prov_tk, now_state, dv$hz, dl_tk, rec = d$rec_states), error = function(e) NULL)
        if (!is.null(trail) && nrow(trail)) {
          td <- as.Date(trail$date); keep <- td >= anchor      # only in-window pins
          if (any(keep)) {
            td <- td[keep]; ts <- trail$state[keep]
            rr <- vapply(td, function(x) ret_asof(lo, x), numeric(1))
            ev <- data.frame(date = td, state = ts, ret = rr, stringsAsFactors = FALSE)
            ev <- ev[!is.na(ev$ret), , drop = FALSE]
          }
        }
      }
      out[[tk]] <- list(line = lo, ev = ev)
    }
    if (!length(out)) return(NULL)
    out
  })

  # ── Top-performers (hindsight) line ──────────────────────────────────────
  # Opt-in (checkbox lcTopPerf, OFF by default). The top decile of the sketch's
  # BUY universe by REALIZED window return, ignoring qualstream pass/fail. This
  # is an explicit HINDSIGHT ceiling: it is selected on the outcome and is NOT
  # achievable ex ante, so the render draws it dashed and labels it "hindsight".
  # Universe = qscmp_detail (current BUYs that were qs-graded); "regardless of
  # qualstream" means the >=68 pass/fail split is ignored, NOT that buys
  # qualstream never graded are pulled in (that would need a wider source).
  # Buy pins reuse the board lifecycle trail (lc_board_trail), buy events only,
  # riding this line; best-effort - if the derived model isn't ready the line
  # still renders without pins.
  lc_top_perf <- reactive({
    if (!isTRUE(input$lcTopPerf)) return(NULL)
    d   <- tryCatch(lc_sketch_data(), error = function(e) NULL)
    det <- if (!is.null(d)) d$qscmp_detail else NULL
    cmp <- if (!is.null(d)) d$qscmp else NULL
    if (is.null(det) || !nrow(det) || is.null(cmp) || !nrow(cmp)) return(NULL)
    # mirror the sketch's narrowing so the line tracks the visible board: the
    # market-creators view (its names, every cluster) or else the cluster boxes
    nc_set <- lc_nc_only()
    if (!is.null(nc_set)) {
      det <- det[toupper(det$ticker) %in% nc_set, , drop = FALSE]
    } else {
      sel_raw  <- lc_ids_effective()
      sel_ids  <- suppressWarnings(as.integer(sel_raw))
      avail_id <- tryCatch(lc_board_filter_ids(), error = function(e) integer(0))
      if (!is.null(sel_raw) && length(sel_ids) > 0 && !setequal(sel_ids, avail_id))
        det <- det[!is.na(det$id) & det$id %in% sel_ids, , drop = FALSE]
    }
    if (!nrow(det)) return(NULL)
    # final realized return per ticker = its last non-NA ret in the window
    det <- det[order(det$ticker, det$d), , drop = FALSE]
    fin <- tapply(det$ret, det$ticker, function(x) { x <- x[!is.na(x)]
      if (length(x)) x[length(x)] else NA_real_ })
    fin <- fin[!is.na(fin)]
    if (length(fin) < 4) return(NULL)              # too few to define a tier
    # DYNAMIC cutoff: names whose realized return clears mean + 1 SD of the buy
    # universe - the count emerges from the distribution (wide when returns are
    # dispersed, tight when bunched), never a hand-picked N.
    mu <- mean(fin); sdv <- stats::sd(fin)
    if (is.na(sdv) || sdv <= 0) return(NULL)
    thr <- mu + sdv
    top_tk <- names(fin)[fin >= thr]
    if (!length(top_tk)) return(NULL)
    top_tk <- top_tk[order(fin[top_tk], decreasing = TRUE)]
    dtop <- det[det$ticker %in% top_tk, , drop = FALSE]
    tvec <- tapply(dtop$ret, dtop$d, function(x) mean(x, na.rm = TRUE) * 100)
    line <- data.frame(d = as.Date(cmp$d),
                       ret = round(as.numeric(tvec[as.character(cmp$d)]), 2),
                       stringsAsFactors = FALSE)
    line <- line[!is.na(line$ret), , drop = FALSE]
    if (!nrow(line)) return(NULL)
    pins <- tryCatch({
      dv <- derivedLC()
      anchor <- as.Date(d$qscmp_anchor)
      dvtk <- dv$tickers; names(dvtk) <- toupper(dvtk)
      line_at <- function(dd) { dd <- as.Date(dd, origin = "1970-01-01")
        s <- line$ret[line$d <= dd]
        if (length(s)) s[length(s)] else line$ret[1] }
      out <- list()
      for (tk0 in top_tk) {
        tk <- unname(dvtk[toupper(tk0)]); if (is.na(tk)) next
        now_state <- if (tk %in% names(dv$state_now)) dv$state_now[[tk]] else NA_character_
        prov_tk <- !is.null(dv$prov_of) && tk %in% names(dv$prov_of) && isTRUE(dv$prov_of[[tk]])
        dl_tk <- lc_dl_of(d, toupper(tk))
        trail <- tryCatch(lc_board_trail(dv$M, dv$dates, tk, d$led, LEDGER_EPOCH,
                            prov_tk, now_state, dv$hz, dl_tk, rec = d$rec_states), error = function(e) NULL)
        if (is.null(trail) || !nrow(trail)) next
        allbuys <- as.Date(trail$date[trail$state == "buy"])
        allbuys <- allbuys[!is.na(allbuys)]
        if (!length(allbuys)) next
        inwin <- sort(allbuys[allbuys >= anchor])
        if (length(inwin))                          # bought within the window
          out[[length(out) + 1L]] <- data.frame(ticker = toupper(tk), date = inwin,
              y = vapply(inwin, line_at, numeric(1)), kind = "buy",
              stringsAsFactors = FALSE)
        else                                        # held since before the window
          out[[length(out) + 1L]] <- data.frame(ticker = toupper(tk), date = anchor,
              y = line_at(anchor), kind = "carried", stringsAsFactors = FALSE)
      }
      if (length(out)) do.call(rbind, out) else NULL
    }, error = function(e) NULL)
    list(line = line, pins = pins, n = length(top_tk), tickers = top_tk,
         rets = round(fin[top_tk] * 100, 1))   # window return %, named by ticker
  })

