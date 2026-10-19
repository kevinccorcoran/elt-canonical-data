# AlphaStream dashboard - Lifecycle: board notes, track strip, state view, master list. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  output$boardLC <- renderUI({
    dv <- derivedLC(); if (is.null(dv)) return(div(
      style = "color:#64748b; padding:1rem; font-size:0.85rem;",
      "Connect and Generate to load the signal record."))
    d <- app_dataLC()
    st <- dv$state_now; hz <- dv$hz
    # user override: tracker rows moved to hold (end_date stamped, not sold)
    # count as hold whatever the model says; hand-sold rows count as sell -
    # same rule as the tracker table and the transition history. (Only the
    # bucket membership matters here now; the retired kanban chips carried
    # the per-name why labels.)
    pp <- lc_pos()
    if (!is.null(pp) && nrow(pp)) {
      pe <- as.character(pp$end_date); ps <- as.character(pp$sold_date)
      heldtk <- toupper(pp$ticker[!is.na(pe) & nzchar(pe) & (is.na(ps) | !nzchar(ps))])
      heldtk <- intersect(heldtk, names(st))
      if (length(heldtk)) st[heldtk] <- "hold"
      soldtk <- toupper(pp$ticker[!is.na(ps) & nzchar(ps)])
      soldtk <- intersect(soldtk, names(st))
      if (length(soldtk)) st[soldtk] <- "sell"
    }
    # qualstream grades per ticker (latest non-vetoed); resolved after the buy
    # set is known below; empty until the qualstream table exists and holds grades.
    qs_grade <- if (!is.null(d$qs) && nrow(d$qs) > 0 &&
                    all(c("ticker", "grade") %in% names(d$qs)))
      setNames(suppressWarnings(as.numeric(d$qs$grade)), d$qs$ticker)
    else setNames(numeric(0), character(0))
    qs_top <- character(0)
    note_line <- function(txt) div(
      style = "color:#64748b; font-size:0.72rem; margin:0.15rem 0 0.6rem;", txt)
    # board universe: cohort entries + proven-at-hz ledger names; the unproven
    # rest of the gate lives in the table below, flagged
    bt <- names(st)[dv$board_ok[names(st)]]
    # cluster-id filter: keep only selected ids; names with no current cluster
    # (delisted, ~1%) always show. NULL before the checkboxes first render
    # keeps everything; NULL after that is a real deselect-all -> empty board.
    id_of <- if (!is.null(d$gate$id))
               setNames(suppressWarnings(as.integer(d$gate$id)), d$gate$ticker)
             else setNames(integer(0), character(0))
    if (!is.null(d$coh) && nrow(d$coh) > 0) {   # cohort id fills gate gaps
      ex <- d$coh[!(d$coh$ticker %in% names(id_of)), c("ticker", "id")]
      ex <- ex[!duplicated(ex$ticker), , drop = FALSE]
      if (nrow(ex)) id_of <- c(id_of, setNames(as.integer(ex$id), ex$ticker))
    }
    # NULL = filter never touched -> show all; character(0) = deselect-all ->
    # empty board; else narrow. The stored val survives refetch re-renders.
    # The market-creators view ignores the boxes (every cluster), so the board
    # and the grading-gap tripwire stay whole; its passed list is narrowed below.
    sel_id <- if (is.null(lc_nc_only())) lc_ids_effective() else NULL
    keep_id <- function(v) {
      if (is.null(sel_id)) return(v)
      if (!length(sel_id)) return(v[FALSE])
      v[is.na(id_of[v]) | id_of[v] %in% as.integer(sel_id)]
    }
    bt <- keep_id(bt)
    buys   <- bt[st[bt] == "buy"]
    closed <- sort(bt[st[bt] == "closed"])
    # orange + = qualstream's endorsement within the buy section: a GRADE
    # THRESHOLD, not a fixed top-N. A rank cut splits ties arbitrarily (grades
    # are coarse -- 11 names once tied on 62, so head(.., 20) marked 5 of them
    # and dropped 6 identical ones on alphabetical order), and a fixed N always
    # marks N names however weak the crop. The cap is only a backstop against a
    # future re-calibration inflating every grade.
    qs_min <- QS_MIN; qs_cap <- QS_CAP
    bg <- qs_grade[buys]
    qs_ok <- buys[!is.na(bg) & bg >= qs_min]
    qs_top <- utils::head(qs_ok[order(-qs_grade[qs_ok], qs_ok)], qs_cap)
    # buys_all feeds the grading-gap tripwire below (kanban display machinery
    # around it was retired 2026-08-26, stripped 2026-08-28)
    buys_all   <- buys
    # Grading cadence note. Continuous since 2026-08-13: the daily
    # qual_scorecards_on_entry DAG grades a name the day it enters the buy list
    # and re-grades standing buys AND holds past 90 days (--include-holds); the
    # Jan/May/Sep 4-monthly checkpoint calendar is retired. Latest as_of is read
    # from the loaded grades, never asserted from a calendar -- a silently dead
    # grader must not be announced as running.
    qs_ran <- if (!is.null(d$qs) && nrow(d$qs) > 0 && "as_of" %in% names(d$qs))
                suppressWarnings(max(as.Date(d$qs$as_of))) else as.Date(NA)
    # dead-grader tripwire: a buy that entered >= 2 days ago must have a grade
    # within 90d (entry day would have graded it; 90d staleness re-grades it).
    # One that doesn't means the entry grader is dead -- or the name's fresh
    # grade was a veto (d$qs is non-vetoed only), which deserves a look anyway.
    # Horizon-scoped: only assert a grading gap at the canonical 12-month horizon,
    # which is the universe the entry grader actually resolves. Other hold-lengths
    # replay a wider proven-slot set that includes names proven ONLY at that
    # horizon and therefore outside the grader's scope, so firing there is a false
    # alarm -- flip to 12mo and a genuine gap still shows. The DAG's own
    # verify_coverage task is the primary, horizon-independent tripwire.
    ungr <- if (!is.na(qs_ran) && !is.na(hz) && hz == 12L) {
      ent <- suppressWarnings(as.Date(dv$entry_of[buys_all]))
      # graded = ANY recorded verdict, vetoes included (d$qs_any_asof). Using
      # the non-vetoed d$qs here made freshly VETOED names look ungraded and
      # false-fired the banner (FTDR 18 / TSM 25, both vetoed mid-Aug, flagged
      # as a "grading gap" on 2026-09-01). A veto IS a grade for this check.
      aof <- if (!is.null(d$qs_any_asof)) d$qs_any_asof[toupper(buys_all)]
             else rep(as.Date(NA), length(buys_all))
      buys_all[!is.na(ent) & ent <= Sys.Date() - 2 &
               (is.na(aof) | aof < Sys.Date() - 90)]
    } else character(0)
    ck_note <- tagList(
      if (length(ungr)) div(
        style = "color:#f87171; font-size:0.78rem; font-weight:700; margin:0.15rem 0 0.6rem;",
        sprintf(paste("GRADING GAP - %d buy(s) entered 2+ days ago without a",
                      "grade in the last 90 days: %s. The entry grader",
                      "(qual_scorecards_on_entry) may be dead - check its last",
                      "run - or the name was vetoed on re-grade."),
                length(ungr), paste(ungr, collapse = ", "))),
      note_line(sprintf(paste(
        "Qualstream grading is continuous: a new buy is graded the day it",
        "enters (daily run after the ledger rebuild); standing buys and holds",
        "re-grade after 90 days, and a mark expires at %d days. Prune/adopt on",
        "your own rhythm - grades are always current. Gate flips, delistings",
        "and maturities in the sell section never wait.%s"),
        QS_MAX_AGE_DAYS,
        if (is.na(qs_ran)) " No grades recorded yet."
        else sprintf(" Latest recorded grades: %s.", format(qs_ran)))))
    # honesty notes: where the record is thin, say so instead of implying signal
    gap_lo <- if (length(dv$coh_dates)) max(dv$coh_dates) else NULL
    led_lo <- min(d$led$d)
    notes <- tagList(
      ck_note,
      if (dv$coh_n == 0) note_line(sprintf(paste(
        "No %d-month rank slot has ever passed the evidence gate (win rate >=",
        "55%% on >= 100 graded picks), so there are no historical entries at",
        "this horizon - the model's proven horizons are 12 months and shorter."),
        hz)),
      if (!is.null(gap_lo)) note_line(sprintf(paste(
        "Entry record: quarterly walk-forward cohorts to %s, then a gap with",
        "no recorded signals until the daily ledger, read from %s."),
        gap_lo, led_lo))
      else note_line(sprintf(
        "Entry record: daily ledger only, read from %s.", led_lo)),
      note_line(sprintf(paste(
        "The ledger physically starts 2026-06-16, but everything above is read",
        "from %s: the BUY gate was replaced on 2026-07-02 after the old one was",
        "measured anti-correlated with realized returns and had stopped emitting",
        "buys entirely. Pre-epoch rows are kept as history and excluded from run",
        "counts, entries and the ledger-vs-benchmark chart, so nothing here",
        "averages two different systems together."), led_lo)),
      if (length(qs_top)) note_line(sprintf(paste(
        "Orange + = qualstream grade >= %d among the buys (%d of %d graded).",
        "A threshold, not a top-N: it never splits a tie, and in a weak period",
        "fewer names qualify."), qs_min, length(qs_top), sum(!is.na(bg)))),
      if (length(qs_top)) note_line(tagList(
        span("+", style = "color:#fb923c; font-weight:800;"),
        local({
          nc_set <- lc_nc_only()
          qs_top_v <- if (!is.null(nc_set)) qs_top[toupper(qs_top) %in% nc_set] else qs_top
          sprintf(" passed qualstream%s: %s.",
            if (!is.null(nc_set)) paste0(" (\u25c6 disruptors + ", NC_GEAR, " enablers only)") else "",
            if (!length(qs_top_v)) "none"
            else paste(sprintf("%s %d", qs_top_v,
                               as.integer(round(qs_grade[qs_top_v]))), collapse = ", "))
        }))))
    closed_foot <- div(style = "color:#64748b; font-weight:600; font-size:0.85rem; margin:0.4rem 0 0.5rem;",
          sprintf("closed - exited over a month ago (%d) · detail in the table below",
                  length(closed)))
    # The kanban columns were retired 2026-08-26; the realized track-record strip
    # moved to its own output (output$lcTrackStripLC) right ABOVE the sketch
    # (Kevin 2026-10-06: "should be above graph, the aggregate of all").
    tagList(notes, closed_foot)
  })

  # Realized track-record strip (Kevin 2026-08-18): at-a-glance stats for every
  # name the board has TRADED since the ledger epoch - a trade = one buy->sell
  # episode (buy<->hold flips are held-through, NOT closes). Sold trades banked,
  # open marked to today; pre-epoch walk-forward cohort picks excluded (this is the
  # LIVE following record, not the backtest). Numbers come from lc_track_record(),
  # the same per-ticker episode logic as the flip-history modal, so a clicked chip
  # reconciles with the aggregate. Duration-fair by design: vs SPY spans each
  # trade's own window and win rate is one vote per trade - no per-day/annualising.
  # Universe follows the qualstream+picks filter (default on = qs-passed names only,
  # "only the qs are trades") AND, in the strip, the board's cluster-id view filter
  # (re-aggregated downstream) so the numbers match the master list / sketch you are
  # viewing. The compounded "how much money" view is the portfolio panel below.
  output$lcTrackStripLC <- renderUI({
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    d  <- tryCatch(app_dataLC(), error = function(e) NULL)
    if (is.null(dv) || is.null(d)) return(NULL)
    track_strip <- local({
      tr <- tryCatch(lc_track_record(), error = function(e) NULL)
      signcol <- function(v) if (is.na(v)) "#94a3b8" else if (v >= 0) "#10b981" else "#dc2626"
      pctcol  <- function(v) if (is.na(v)) "#94a3b8" else if (v >= 50) "#10b981" else "#dc2626"
      fmtpp   <- function(v) if (is.na(v)) "n/a" else sprintf("%+.1fpp", v)
      if (is.null(tr))
        return(div(style = "color:#64748b; font-size:0.72rem; margin:0.2rem 0 0.7rem;",
                   "No board trades priced yet since the epoch - Generate, or wait for the first close."))
      # Cluster-aware: the same cluster boxes as the sketch and master list
      # (lc_in_sel, by live id), applied to the whole-board record's episode tables
      # and re-aggregated. Filtering here (not in lc_track_record) keeps the price
      # query cached across cluster toggles.
      insel <- function(tk) unname(lc_in_sel(tk))
      A <- tr$table; B <- tr$wtable
      if (!is.null(A) && nrow(A)) A <- A[insel(A$ticker), , drop = FALSE]
      if (!is.null(B) && nrow(B)) B <- B[insel(B$ticker), , drop = FALSE]
      # Top-performers filter: when the box is on, the strip reflects the hindsight
      # top-performer set so its numbers update with the selection. These are
      # outcome-selected winners, so avg/win read high - flagged with a note below.
      tp_lc <- tryCatch(lc_top_perf(), error = function(e) NULL)
      tp_on <- !is.null(tp_lc) && length(tp_lc$tickers) > 0
      if (tp_on) {
        tset <- toupper(tp_lc$tickers)
        if (!is.null(A) && nrow(A)) A <- A[toupper(A$ticker) %in% tset, , drop = FALSE]
        if (!is.null(B) && nrow(B)) B <- B[toupper(B$ticker) %in% tset, , drop = FALSE]
      }
      tr <- lc_track_agg(A, B)
      if (!isTRUE(tr$n > 0))
        return(div(style = "color:#64748b; font-size:0.72rem; margin:0.2rem 0 0.7rem;",
                   if (tp_on)
                     "No board-traded names among the top performers (they may be pre-window holds or names never on the board)."
                   else if (!is.null(lc_nc_only()))
                     paste0("No board trades among the \u25c6 disruptors and ", NC_GEAR, " enablers (every cluster).")
                   else "No board trades in the selected clusters - adjust the cluster-id filter."))
      pill <- function(lab, big, sub, accent, valcol = "#e2e8f0")
        div(style = paste0("flex:1 1 0; min-width:120px; background:rgba(148,163,184,0.04);",
              " border:1px solid #1e293b; border-left:3px solid ", accent,
              "; border-radius:7px; padding:0.4rem 0.7rem;"),
          div(style = paste0("color:#94a3b8; font-size:0.66rem; font-weight:600;",
                " text-transform:uppercase; letter-spacing:0.03em;"), lab),
          div(style = sprintf(paste0("color:%s; font-size:1.15rem; font-weight:800;",
                " font-variant-numeric:tabular-nums;"), valcol), big),
          if (nzchar(sub)) div(style = "color:#64748b; font-size:0.64rem; margin-top:0.1rem;", sub))
      div(style = "margin:0.2rem 0 0.7rem;",
        if (tp_on) div(style = paste0("color:#e879f9; font-size:0.66rem; font-weight:600;",
              " margin-bottom:0.35rem;"),
          sprintf("↳ top performers (%d traded) · hindsight-selected winners, descriptive", tr$n)),
        # the view narrows the record to marked names from every cluster: say so,
        # and that the tag is today's model-knowledge label laid over past trades
        if (!is.null(lc_nc_only())) div(style = paste0("color:#c084fc; font-size:0.66rem;",
              " font-weight:600; margin-bottom:0.35rem;"),
          paste0("\u21b3 \u25c6 disruptors + ", NC_GEAR, " enablers, every cluster \u00b7 today's model-knowledge tag laid over past trades, descriptive, not a test of the tag")),
        # PRIMARY: the full buy->sell round-trip record (one economic position each)
        div(style = "display:flex; gap:0.5rem; flex-wrap:wrap; align-items:stretch;",
          pill("trades", as.character(tr$n),
               sprintf("%d closed · %d open", tr$closed, tr$open), "#64748b"),
          pill("avg return", sprintf("%+.1f%%", tr$avg_ret),
               "own move, per round trip", "#10b981", signcol(tr$avg_ret)),
          pill("vs spy", fmtpp(tr$avg_vs),
               "per round trip, own window", "#3b82f6", signcol(tr$avg_vs)),
          pill("win rate", if (is.na(tr$win)) "n/a" else sprintf("%.0f%%", tr$win),
               sprintf("beat SPY on %d of %d", tr$n_win, tr$n_vs), "#a78bfa", pctcol(tr$win))),
        # DIAGNOSTIC: each active-buy RUN vs SPY, duration-weighted. "windows · names"
        # exposes flip churn - a flippy name spawns many windows but few names.
        div(style = paste0("color:#94a3b8; font-size:0.62rem; margin:0.5rem 0 0.2rem;",
              " text-transform:uppercase; letter-spacing:0.03em;"),
          "vs SPY by active-buy window · duration-weighted"),
        div(style = "display:flex; gap:0.5rem; flex-wrap:wrap; align-items:stretch;",
          pill("to hold", fmtpp(tr$vs_hold),
               sprintf("%d windows · %d names", tr$n_hold, tr$nm_hold), "#3b82f6", signcol(tr$vs_hold)),
          pill("to sell", fmtpp(tr$vs_sell),
               sprintf("%d windows · %d names", tr$n_sell, tr$nm_sell), "#3b82f6", signcol(tr$vs_sell)),
          pill("still buying", fmtpp(tr$vs_now),
               sprintf("%d windows · %d names", tr$n_now, tr$nm_now), "#3b82f6", signcol(tr$vs_now))),
        div(style = "color:#64748b; font-size:0.66rem; margin-top:0.35rem;",
          sprintf(paste0("every %s %s since the %s epoch - small sample, read as directional not proof.",
            " Top row = full buy->sell round trips (one vote per position, holds held through). Bottom =",
            " each active-buy RUN vs SPY over its own dates, DURATION-WEIGHTED so a brief buy<->hold flip",
            " barely counts; a re-buy opens a new run, and 'windows vs names' shows the flip churn. buy->hold",
            " ends at the downgrade; to-sell is a direct buy->sell (a sell from a hold sits in the round-trip",
            " number). Compounded $ view: the portfolio panel below."),
            if (isTRUE(input$lcBuyQSonly)) "qualstream+ name" else "board name",
            if (is.null(lc_nc_only())) "in the selected clusters"
            else paste0("among the \u25c6 disruptors and ", NC_GEAR, " enablers (every cluster)"),
            format(as.Date(LEDGER_EPOCH), "%b %d"))))
    })
    # The buy/hold/sell kanban columns were retired 2026-08-26: they derived state
    # at the selected Hold-length (a 7mo lifecycle view) and were qualstream-hidden,
    # so their counts disagreed with the live-gate master list AND the portfolio
    # table (Kevin: "why column and table not same counts"). The master list is
    # now the single name board; the board keeps the honesty notes + closed-count
    # footer, and this strip the realized-trade metrics.
    track_strip
  })

  # ── State view helper (engine rule vs raw gate) ──────────────────────────
  lc_state_for_display <- function(dv, d, pp) {
    view <- if (!is.null(input$lcStateView)) input$lcStateView else "engine"
    st <- if (view == "engine") {
      if (!is.null(dv) && !is.null(dv$state_now))
        setNames(as.character(dv$state_now), names(dv$state_now))
      else setNames(character(0), character(0))
    } else if (view == "nested") {
      # Nested shadow's decisive state (monitoring.nested_board_shadow): BUY
      # while it holds the name, SELL for 30 days after its exit, otherwise the
      # engine state with an unconfirmed engine BUY shown as HOLD. The shadow
      # never adopts or sells anything; the book's own overrides still win below.
      eng <- if (!is.null(dv) && !is.null(dv$state_now))
        setNames(as.character(dv$state_now), names(dv$state_now))
        else setNames(character(0), character(0))
      nz <- if (!is.null(d) && !is.null(d$nested) && nrow(d$nested)) d$nested else NULL
      if (is.null(nz)) eng else {
        last_run <- suppressWarnings(max(as.Date(nz$run_date), na.rm = TRUE))
        open_now <- toupper(nz$ticker[nz$state == "BUY" & as.Date(nz$run_date) == last_run])
        ex <- nz[nz$state == "EXIT", , drop = FALSE]
        recent_exit <- setdiff(toupper(ex$ticker[as.Date(ex$run_date) >= last_run - 30]), open_now)
        out <- eng
        for (u in names(out)) {
          out[[u]] <- if (toupper(u) %in% open_now) "buy"
                      else if (toupper(u) %in% recent_exit) "sell"
                      else if (identical(out[[u]], "buy")) "hold"
                      else out[[u]]
        }
        out
      }
    } else {
      gate_now <- if (!is.null(d) && !is.null(d$gate) && nrow(d$gate) &&
                      all(c("ticker", "gate_today") %in% names(d$gate)))
        setNames(toupper(as.character(d$gate$gate_today)), toupper(d$gate$ticker))
        else setNames(character(0), character(0))
      raw <- setNames(vapply(names(gate_now), function(u) {
        a <- gate_now[[u]]
        if (identical(a, "BUY")) "buy" else if (identical(a, "SELL")) "sell" else "hold"
      }, character(1)), names(gate_now))
      # a tracked name can be absent from today's gate read (delisted, dropped
      # universe); fall back to its engine state instead of a blank STATE cell
      if (!is.null(dv) && !is.null(dv$state_now)) {
        eng <- setNames(as.character(dv$state_now), names(dv$state_now))
        miss <- setdiff(names(eng), names(raw))
        if (length(miss)) raw[miss] <- eng[miss]
      }
      raw
    }
    if (!is.null(pp) && nrow(pp)) {
      pe <- as.character(pp$end_date); ps <- as.character(pp$sold_date)
      for (t in toupper(pp$ticker[!is.na(pe) & nzchar(pe) & (is.na(ps) | !nzchar(ps))])) st[t] <- "hold"
      for (t in toupper(pp$ticker[!is.na(ps) & nzchar(ps)])) st[t] <- "sell"
    }
    st
  }

  # ── Best bets by horizon (4 / 7 / 12mo) ──────────────────────────────────
  # ONE table across horizons so a bet is picked BY the hold length it is
  # strongest at, instead of flipping the Hold-length selector and diffing in
  # your head (Kevin 2026-08-26). Reads the per-(ticker, fut_lag) shortlist
  # evidence already loaded in app_dataLC()$sl: for each qualstream passer it
  # shows, at 4/7/12mo, whether the name is a TOP-SLOT buy (wf_bin 1 of a long
  # id) and that slot's realized win rate. Display-only; changes no rule.
  output$bestBetsLC <- renderUI({
    d  <- tryCatch(app_dataLC(), error = function(e) NULL)
    dv <- tryCatch(derivedLC(),  error = function(e) NULL)
    if (is.null(d) || is.null(d$sl) || !nrow(d$sl) || is.null(dv)) return(NULL)
    sl <- d$sl
    g  <- setNames(suppressWarnings(as.numeric(dv$qs_grade)), toupper(names(dv$qs_grade)))
    cand <- sort(unique(names(g)[!is.na(g) & g >= QS_MIN]))
    # OWN book: open positions (sold_date empty) drive the Holding column AND join
    # the universe, so a name you hold that has since dropped off the board / below
    # qualstream (adopted as a buy, now SKIP - e.g. CVNA/HWM) still shows here as an
    # owned hold instead of vanishing. Per ticker: entry count + latest add date, so
    # a re-fired name shows its most recent hold clock and how many entries it has.
    pp <- tryCatch(lc_pos(), error = function(e) NULL)
    own_cnt <- integer(0); own_last <- numeric(0)
    if (!is.null(pp) && nrow(pp)) {
      op <- pp[is.na(pp$sold_date) | !nzchar(as.character(pp$sold_date)), , drop = FALSE]
      if (nrow(op)) {
        otk <- toupper(op$ticker)
        own_cnt  <- table(otk)
        own_last <- tapply(as.Date(op$start_date), otk, function(x) max(x, na.rm = TRUE))
      }
    }
    owned_ticks <- names(own_cnt)
    cand <- sort(unique(c(cand, owned_ticks)))
    if (!length(cand)) return(NULL)
    # cluster boxes narrow the master list too, same rule as the sketch
    # (Kevin 2026-10-07: "only the specific tickers to that cluster ... on graph
    # and table") - owned names in other clusters included
    # market-creators view: marked names that are not master-list names go on
    # the chart only; list them under the table (and in the empty state)
    nc_now <- lc_nc_only()
    off_list <- if (!is.null(nc_now)) sort(setdiff(nc_now, toupper(cand))) else character(0)
    off_note <- if (length(off_list)) tags$div(
      style = "color:#94a3b8;font-size:0.72rem;margin:0.4rem 0 0.2rem;",
      tags$span(paste0("\u25c6", NC_GEAR), style = "color:#c084fc;"),
      paste0(" off the master list (chart only): ",
             paste(sprintf("%s (%s)", off_list, lc_offlist_why(off_list)), collapse = "; "), "."))
    cand <- cand[lc_in_sel(cand)]
    if (!length(cand))
      return(tagList(div(style = "color:#64748b; font-size:0.8rem; margin:0.4rem 0 0.8rem;",
                 if (!is.null(nc_now))
                   paste0("No \u25c6 disruptors or ", NC_GEAR, " enablers on the master list. ",
                          "Untick \"Disruptors + enablers only\" to list every name.")
                 else "No cluster ticked - tick a cluster box in the sidebar to list its names."),
                 off_note))
    live <- lc_live_ids()
    # market-shift marker: hover = kind, category, before, stage, rank, vote
    nc_tags <- lc_nc_tags()
    nc_tip <- if (!is.null(nc_tags) && nrow(nc_tags)) setNames(lc_nc_tip(nc_tags), nc_tags$ticker)
              else character(0)
    nc_mark <- if (!is.null(nc_tags) && nrow(nc_tags)) setNames(nc_tags$marker, nc_tags$ticker)
               else character(0)
    nc_gear <- if (!is.null(nc_tags) && nrow(nc_tags)) setNames(nc_tags$enabler %in% TRUE, nc_tags$ticker)
               else logical(0)
    HZS <- c(4L, 7L, 12L)
    sl_ix <- if (!is.null(sl) && nrow(sl))
               split(seq_len(nrow(sl)), paste(toupper(sl$ticker), sl$fut_lag, sep = "|")) else list()
    cell_for <- function(tku, fl) {
      ii <- sl_ix[[paste(tku, fl, sep = "|")]]
      r <- if (is.null(ii)) sl[0, , drop = FALSE] else sl[ii, , drop = FALSE]
      if (!nrow(r)) return(list(top = FALSE, bin = NA, win = NA_real_, eid = NA))
      r <- r[1, ]
      list(top = (!is.na(r$rank_bin) && r$rank_bin == 1L &&
                  !is.na(r$eid) && r$eid <= 12),
           bin = r$rank_bin, win = r$bin_win_pct, eid = r$eid)
    }
    st <- lc_state_for_display(dv, d, pp)
    # move over the sketch window (slider) = the end of the name's line on the sketch
    wr <- tryCatch(lc_ml_winret(), error = function(e) setNames(numeric(0), character(0)))
    recs <- lapply(cand, function(tk) {
      tku <- toupper(tk)
      cells <- setNames(lapply(HZS, function(fl) cell_for(tku, fl)), as.character(HZS))
      eid <- cells[["7"]]$eid; if (is.na(eid)) eid <- cells[["12"]]$eid
      ntop <- sum(vapply(cells, function(cc) isTRUE(cc$top), logical(1)))
      win7 <- cells[["7"]]$win
      tops <- HZS[vapply(HZS, function(fl) isTRUE(cells[[as.character(fl)]]$top), logical(1))]
      best <- if (length(tops))
        tops[which.max(vapply(tops, function(fl) {
          w <- cells[[as.character(fl)]]$win; if (is.na(w)) -1 else w }, numeric(1)))]
        else NA_integer_
      state <- if (tku %in% names(st)) st[[tku]] else NA_character_
      # grade guarded: owned-off-board names are not in the qualstream grade map.
      gr <- if (tku %in% names(g)) g[[tku]] else NA_real_
      # Holding clock: the hold period is the name's BEST horizon (fallback 7mo for
      # an owned name with no top-slot evidence, e.g. an off-board hold); "left" =
      # that many months from the LATEST entry, minus elapsed.
      owned <- tku %in% owned_ticks
      n_ent <- if (owned) as.integer(own_cnt[[tku]]) else NA_integer_
      add_num <- if (owned && tku %in% names(own_last)) own_last[[tku]] else NA_real_
      add_d <- if (!is.na(add_num) && is.finite(add_num)) as.Date(add_num, origin = "1970-01-01") else as.Date(NA)
      hold_mo <- if (!is.na(best)) as.numeric(best) else 7
      left_mo <- if (owned && !is.na(add_d)) hold_mo - as.numeric(Sys.Date() - add_d) / 30.44 else NA_real_
      list(tk = tku, grade = gr, eid = eid, lid = unname(live[tku]), cells = cells, best = best,
           state = state, ntop = ntop, win7 = if (is.na(win7)) -1 else win7,
           owned = owned, n_ent = n_ent, add_d = add_d, left_mo = left_mo,
           wret = if (tku %in% names(wr)) unname(wr[[tku]]) else NA_real_)
    })
    # inside each buy/hold/sell group: window return high to low (the order of the
    # line ends on the sketch, Kevin 2026-10-15), unpriced names last; owned rows
    # keep their tint but no longer float (that would break the high-to-low order)
    sord <- c(buy = 0, hold = 1, sell = 2, closed = 3)
    wret_v <- vapply(recs, function(r) as.numeric(r$wret), numeric(1))
    ord  <- order(vapply(recs, function(r) {
                    v <- sord[r$state]; if (length(v) != 1 || is.na(v)) 4 else v }, numeric(1)),
                  is.na(wret_v),
                  -ifelse(is.na(wret_v), 0, wret_v),
                  -vapply(recs, `[[`, numeric(1), "ntop"),
                  -vapply(recs, `[[`, numeric(1), "win7"))
    recs <- recs[ord]
    scol <- c(buy = "#10b981", hold = "#eab308", sell = "#dc2626", closed = "#64748b")
    state_td <- function(s) {
      if (is.na(s)) return(tags$td(style = "text-align:center;color:#334155;", "—"))
      cc <- scol[[s]]; if (is.null(cc)) cc <- "#94a3b8"
      tags$td(style = "text-align:center;",
        tags$span(toupper(s), style = sprintf(
          "color:%s;font-weight:700;font-size:0.68rem;border:1px solid %s66;border-radius:4px;padding:1px 6px;", cc, cc)))
    }
    best_td <- function(h) if (is.na(h))
      tags$td(style = "text-align:center;color:#334155;", "—")
      else tags$td(style = "text-align:center;color:#38bdf8;font-weight:700;", sprintf("%dmo", h))
    fmt_cell <- function(cc) {
      if (isTRUE(cc$top))
        tags$td(style = "text-align:center;color:#34d399;font-weight:600;white-space:nowrap;",
                sprintf("✓ %s%%", if (is.na(cc$win)) "?" else format(round(cc$win))))
      else if (!is.na(cc$bin))
        tags$td(style = "text-align:center;color:#64748b;", sprintf("b%d", cc$bin))
      else
        tags$td(style = "text-align:center;color:#334155;", "—")
    }
    # Holding cell: "N× · <latest add> · <time left>" for an owned name, else a dash.
    # N = open entry count for the ticker, latest add = most recent entry date, time
    # left = months remaining on the (BEST-horizon) hold from that latest entry.
    hold_td <- function(r) {
      if (!isTRUE(r$owned))
        return(tags$td(style = "text-align:center;color:#334155;", "—"))
      add <- if (is.na(r$add_d)) "?" else format(r$add_d, "%b %d")
      lft <- if (is.na(r$left_mo)) ""
             else if (r$left_mo <= 0) " · matured"
             else sprintf(" · %.1fmo left", r$left_mo)
      tags$td(style = "text-align:center;color:#cbd5e1;white-space:nowrap;font-size:0.72rem;",
              sprintf("%d× · %s%s", r$n_ent, add, lft))
    }
    # Return cell: the name's move over the sketch window, the sort key above
    ret_td <- function(x) if (is.na(x))
      tags$td(style = "text-align:right;color:#334155;padding:2px 8px;", "—")
      else tags$td(style = sprintf("text-align:right;padding:2px 8px;font-weight:600;color:%s;white-space:nowrap;",
                                   if (x >= 0) "#34d399" else "#f87171"),
                   sprintf("%+.1f%%", x))
    # persisted checked set (isolate: ticking a box must not re-render the whole
    # table - only the sketch reacts, via lc_sketch_ticks / lc_checked_lines)
    chk_ticks <- toupper(isolate(lc_sketch_ticks()))
    row_tr <- function(r) tags$tr(
      style = if (isTRUE(r$owned)) "background:rgba(16,185,129,0.06);" else "",
      tags$td(style = "text-align:center;padding:2px 4px;",
        tags$input(type = "checkbox", class = "sketchrow", `data-tk` = r$tk,
                   `aria-label` = paste("plot", r$tk, "on the sketch"),
                   checked = if (r$tk %in% chk_ticks) NA else NULL)),
      # the diamond rides the ticker (nowrap only on marked rows, so an untagged
      # board renders byte-identical to before)
      tags$td(style = paste0("color:#e2e8f0;font-weight:600;padding:2px 8px;",
                             if (r$tk %in% names(nc_tip)) "white-space:nowrap;" else ""),
              r$tk,
              if (r$tk %in% names(nc_tip)) {
                mk1 <- nc_mark[[r$tk]]; gear <- isTRUE(nc_gear[[r$tk]])
                tags$span(lc_nc_glyph(mk1, gear), title = nc_tip[[r$tk]],
                          `aria-label` = paste(c(
                            if (identical(mk1, "hollow")) "building a new market, not selling yet",
                            if (identical(mk1, "filled")) "creating or redefining a market",
                            if (gear) "enabler: a key supplier to a large, fast-growing market"),
                            collapse = "; "),
                          style = "color:#c084fc;margin-left:4px;font-size:0.82rem;cursor:help;")
              }),
      ret_td(r$wret),
      state_td(r$state),
      hold_td(r),
      best_td(r$best),
      # live gate id = the number on the cluster boxes (was the walk-forward eid,
      # a different numbering: CASY showed 10 here while sitting in box 8)
      tags$td(style = "text-align:center;color:#94a3b8;",
              if (is.null(r$lid) || is.na(r$lid)) "—" else as.character(r$lid)),
      tags$td(style = "text-align:center;color:#94a3b8;",
              if (is.na(r$grade)) "—" else as.character(round(r$grade))),
      fmt_cell(r$cells[["4"]]), fmt_cell(r$cells[["7"]]), fmt_cell(r$cells[["12"]])
    )
    # Group the ONE table by buy / hold / sell with a full-width section header, so
    # the board's kanban and the horizon table are the same surface: buy/hold/sell
    # is the grouping, and every holding period (4/7/12mo) stays on each row. Rows
    # sort by the sketch-window return inside their group; owned names are tinted.
    scol_hdr <- c(buy = "#10b981", hold = "#eab308", sell = "#dc2626", other = "#64748b")
    slab_hdr <- c(buy = "BUY", hold = "HOLD", sell = "SELL", other = "UNRANKED")
    skey <- function(r) if (is.na(r$state)) "other" else r$state
    scount <- table(vapply(recs, skey, character(1)))
    grp_hdr <- function(s) tags$tr(tags$td(colspan = 11, style = sprintf(paste0(
      "padding:9px 8px 3px;color:%s;font-weight:800;font-size:0.76rem;",
      "text-transform:uppercase;letter-spacing:0.05em;border-bottom:2px solid %s55;"),
      scol_hdr[[s]], scol_hdr[[s]]),
      sprintf("%s — %d", slab_hdr[[s]], as.integer(scount[[s]]))))
    body <- list(); prev <- NULL
    for (r in recs) {
      s <- skey(r)
      if (!identical(s, prev)) { body[[length(body) + 1L]] <- grp_hdr(s); prev <- s }
      body[[length(body) + 1L]] <- row_tr(r)
    }
    th <- function(x, al = "center") tags$th(style = sprintf(
      "text-align:%s;color:#cbd5e1;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.04em;padding:3px 8px;border-bottom:1px solid #1e293b;", al), x)
    tagList(
      tags$div(style = "color:#f1f5f9;font-weight:700;font-size:0.95rem;margin:0.3rem 0 0.1rem;",
               "Master list — every best bet, all horizons"),
      tags$div(style = "color:#64748b;font-size:0.72rem;margin-bottom:0.4rem;",
        HTML("One table, grouped by BUY / HOLD / SELL, every holding period on each row. Tick a row's box to draw that name's own line on the sketch below. RETURN = the name's move over the sketch window (the slider), the same number its line ends on; rows sort on it, high to low, inside each group · STATE = current buy/hold/sell (filter-independent) · HOLDING = your open entries (count&times; · latest add · time left on the hold), owned rows tinted green · BEST = strongest hold length · &#10003; = top-slot buy at that horizon with its realized win% · b# = ranked but not top slot · &mdash; = not ranked. New adopts appear here automatically.")),
      if (!is.null(d$nc) && nrow(d$nc)) tags$div(
        style = "color:#64748b;font-size:0.72rem;margin:-0.2rem 0 0.4rem;",
        tags$span("\u25c6", style = "color:#c084fc;"),
        " = disruptor: leads a market shift still under way (creating a category or redefining a market); ",
        tags$span("\u25c7", style = "color:#c084fc;"),
        " = building one, not selling yet; ",
        tags$span(NC_GEAR, style = "color:#c084fc;"),
        paste(" = enabler: a key supplier to someone else's large, fast-growing market. A",
              "model-knowledge tag, display only (it never feeds the grade or any rule).",
              "Hover a mark for the market and why.")),
      tags$div(style = "overflow-x:auto;",
        tags$table(style = "width:100%;border-collapse:collapse;font-size:0.8rem;",
          tags$thead(tags$tr(th("Plot"), th("Ticker", "left"), th("Return", "right"), th("State"),
                             th("Holding"), th("Best"),
                             th("Cluster"), th("Grade"),
                             th("4 mo"), th("7 mo"), th("12 mo"))),
          tags$tbody(body))),
      off_note,
      # Delegated, bound once (survives the hourly re-render): any checkbox change
      # collects every ticked row into lcSketchChecked, which drives lc_sketch_ticks
      # -> lc_checked_lines -> the per-name lines on the sketch.
      tags$script(HTML(
        "if(!window.__lcSketchWired){window.__lcSketchWired=true;",
        "$(document).on('change','input.sketchrow',function(){",
        "var sel=[];$('input.sketchrow:checked').each(function(){sel.push(this.getAttribute('data-tk'));});",
        "Shiny.setInputValue('lcSketchChecked',sel,{priority:'event'});});}"))
    )
  })

