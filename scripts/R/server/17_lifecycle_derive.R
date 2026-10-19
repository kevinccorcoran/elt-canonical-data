# AlphaStream dashboard - Lifecycle: the board (derivedLC), filters, master-list pricing. Part of app.R, split 2026-10-19: files load in name order (see app.R).
  # State machine + summary, re-derived live on horizon changes without
  # re-querying. The snapshot stream at horizon hz is the model's own record:
  # quarterly walk-forward cohort picks (fut_lag == hz) followed by the daily
  # live ledger. The walk is unchanged; only the inputs got deeper.
  derivedLC <- reactive({
    d <- app_dataLC(); req(d)
    # shared across sessions per load and day (see LC_MEMO_TTL_SEC); M carries
    # the load stamp so lc_board_trail can memo the trails of this walk
    mk <- if (!is.null(d$memo_stamp)) paste(d$memo_stamp, format(Sys.Date()), sep = "|")
    if (!is.null(mk) && identical(.lc_memo$dv_key, mk)) return(.lc_memo$dv)
    dv <- if (!is.null(d$board)) lc_dv_from_board(d$board, d) else (function() {
    hz <- 12L   # the board derives at the canonical 12-month gate horizon (holdLC retired 2026-08-26)
    led <- d$led
    ch  <- if (!is.null(d$coh) && nrow(d$coh) > 0)
             d$coh[d$coh$fut_lag == hz, , drop = FALSE]
           else NULL
    coh_dates <- if (!is.null(ch) && nrow(ch) > 0) sort(unique(ch$d)) else character(0)
    dates <- sort(unique(c(coh_dates, led$d)))
    tickers <- sort(unique(c(led$ticker[led$global_action == "BUY"],
                             if (!is.null(ch)) ch$ticker else character(0))))
    if (length(tickers) == 0) return(NULL)
    # ledger actions first; cohort picks add a BUY at their cutoff date unless
    # the ledger already recorded that (ticker, date) - the recorded call wins.
    # Integer-indexed (name x day) matrix, not a named-vector lookup: the old
    # act[paste(t, D)] re-matched ~40k names on every one of ~100k (name, day)
    # steps - most of a 60s+ first page load (profiled 2026-10-12). Same
    # actions, same walk below.
    nd <- length(dates)
    A  <- matrix(NA_character_, nrow = length(tickers), ncol = nd)
    ai <- match(led$ticker, tickers); aj <- match(led$d, dates)
    ok <- rev(which(!is.na(ai) & !is.na(aj)))     # first row per key wins, as act[] did
    A[cbind(ai[ok], aj[ok])] <- led$global_action[ok]
    if (!is.null(ch) && nrow(ch) > 0) {
      ci <- match(ch$ticker, tickers); cj <- match(ch$d, dates)
      cidx <- cbind(ci, cj)[!is.na(ci) & !is.na(cj), , drop = FALSE]
      fill <- is.na(A[cidx])
      if (any(fill)) A[cidx[fill, , drop = FALSE]] <- "BUY"
    }
    dd_all <- as.Date(dates)
    dn_all <- as.numeric(dd_all)      # day numbers: a Date compare per step dispatched S3 ~200k times
    is_coh <- dates %in% coh_dates
    # maturity per calendar day, computed once instead of per (name, day)
    mat_by_day <- do.call(c, lapply(dd_all, function(x)
      seq(x, by = paste(hz, "months"), length.out = 2)[2]))
    mat_n_by_day <- as.numeric(mat_by_day)
    dl   <- if (!is.null(d$meta) && nrow(d$meta) > 0)
              setNames(suppressWarnings(as.Date(d$meta$delisted_date)), d$meta$ticker)
            else setNames(as.Date(character(0)), character(0))
    # proven-at-hz evidence (today's rank bin at the latest cutoff, this lag):
    # the SAME standard as the cohort record - top vingtile (bin 1) of a long
    # id (eid <= 12) whose bin-1 cell wins >= 55% on >= 100 obs. Any-bin 55/100
    # passes are NOT proven: 28 (id,bin) cells clear that pooled bar and the
    # mid-table ones are multiple-comparisons noise, not validated edge.
    slh <- if (!is.null(d$sl) && nrow(d$sl) > 0 && "fut_lag" %in% names(d$sl))
             d$sl[d$sl$fut_lag == hz, , drop = FALSE]
           else NULL
    prov_of <- if (!is.null(slh) && nrow(slh) > 0)
                 setNames(!is.na(slh$bin_win_pct) & slh$bin_win_pct >= 55 &
                          !is.na(slh$bin_n)       & slh$bin_n >= 100 &
                          !is.na(slh$rank_bin)    & slh$rank_bin == 1 &
                          !is.na(slh$eid)         & slh$eid <= 12, slh$ticker)
               else setNames(logical(0), character(0))
    wpct <- if (!is.null(slh) && nrow(slh) > 0)
              setNames(as.numeric(slh$bin_win_pct), slh$ticker) else numeric(0)
    provf <- function(t) t %in% names(prov_of) && isTRUE(prov_of[[t]])
    M <- matrix("", nrow = length(tickers), ncol = length(dates),
                dimnames = list(tickers, dates))
    entry_of <- setNames(rep(NA_character_, length(tickers)), tickers)
    exit_of  <- setNames(rep(NA_character_, length(tickers)), tickers)
    reason_of <- setNames(rep("", length(tickers)), tickers)
    src_of  <- setNames(rep(NA_character_, length(tickers)), tickers)
    mat_of  <- setNames(rep(as.Date(NA), length(tickers)), tickers)
    for (ti in seq_along(tickers)) {
      t <- tickers[ti]
      open <- FALSE; sold <- FALSE; entry_d <- NA_character_; mat_d <- as.Date(NA)
      dl_d <- if (t %in% names(dl)) dl[[t]] else as.Date(NA)
      has_dl <- !is.na(dl_d); dl_n <- as.numeric(dl_d); mat_n <- NA_real_
      for (j in seq_len(nd)) {
        D <- dates[j]; Dn <- dn_all[j]; a <- A[ti, j]
        if (!open && !sold) {                       # flat: wait for an entry
          if (!is.na(a) && a == "BUY") {
            open <- TRUE; entry_d <- D
            mat_d <- mat_by_day[j]; mat_n <- mat_n_by_day[j]
            M[ti, j] <- "buy"; reason_of[ti] <- "new signal"
            src_of[ti] <- if (is_coh[j]) "cohort" else "ledger"
          }
        } else if (open && !sold) {                 # held: check exit triggers
          if (!is.na(a) && a == "SELL") {
            sold <- TRUE; open <- FALSE; M[ti, j] <- "sell"
            reason_of[ti] <- "gate flipped"; exit_of[ti] <- D
          } else if ((has_dl && dl_n <= Dn) || Dn >= mat_n) {
            # both can have elapsed between sparse snapshots: earlier event wins
            sold <- TRUE; open <- FALSE; M[ti, j] <- "sell"
            reason_of[ti] <- if (has_dl && dl_n <= Dn && dl_n <= mat_n)
              "delisted" else "matured"
            exit_of[ti] <- D
          } else if (!is.na(a) && a == "BUY") {
            M[ti, j] <- "buy"; reason_of[ti] <- "signal continuing"   # gate still says BUY
          } else {
            M[ti, j] <- "hold"; reason_of[ti] <- "maturing"          # washed to SKIP, still held
          }
        } else {                                    # sold: sticky until re-entry
          if (!is.na(a) && a == "BUY") {
            open <- TRUE; sold <- FALSE; entry_d <- D
            mat_d <- mat_by_day[j]; mat_n <- mat_n_by_day[j]
            M[ti, j] <- "buy"; reason_of[ti] <- "re-entry"; exit_of[ti] <- NA_character_
            src_of[ti] <- if (is_coh[j]) "cohort" else "ledger"
          } else M[ti, j] <- "sell"
        }
      }
      entry_of[ti] <- entry_d
      mat_of[ti]   <- mat_d
    }
    # current-decision bucket (the board's truth), 4 states as of TODAY:
    #   sell   - action item: matured/delisted/gate-flipped within ~a month
    #   buy    - proven rank slot backed by the MAJORITY of the last month's
    #            recorded runs (rolling monthly evaluation: one off-day cannot
    #            demote a durable rec, one on-day cannot promote a flicker)
    #   hold   - open position below the monthly majority
    #   closed - the exit is > 30 days old; history, not an action item
    today <- Sys.Date()
    led_dd <- if (!is.null(led$dd)) led$dd else as.Date(led$d)
    d30 <- sort(unique(led_dd[led_dd >= today - 30]))
    b30 <- led[led_dd >= today - 30 & led$global_action == "BUY", , drop = FALSE]
    share_of <- if (nrow(b30) > 0) {
      fb <- tapply(as.Date(b30$d), b30$ticker, min)   # first BUY inside window
      nb <- tapply(b30$d, b30$ticker, length)
      # denominator starts at the name's first window appearance so a fresh
      # entrant (3/3 runs) qualifies while a long flicker (8/21) does not
      setNames(mapply(function(n, f) n / max(1L, sum(d30 >= f)), nb, fb),
               names(nb))
    } else setNames(numeric(0), character(0))
    majf <- function(t) t %in% names(share_of) && share_of[[t]] >= 0.5
    gate_now <- setNames(as.character(d$gate$gate_today), d$gate$ticker)
    fin <- M[, ncol(M)]
    state_now <- setNames(rep("hold", length(tickers)), tickers)
    why_now   <- setNames(rep("washed out to SKIP", length(tickers)), tickers)
    for (t in tickers) {
      e <- as.Date(entry_of[[t]])
      mat_d <- mat_of[[t]]
      dl_d <- if (t %in% names(dl)) dl[[t]] else as.Date(NA)
      g <- if (t %in% names(gate_now)) gate_now[[t]] else NA_character_
      if (fin[[t]] == "sell") {                     # recorded exit in the walk
        r <- reason_of[[t]]
        # date the exit really happened: true maturity/delist date, not the
        # (possibly much later) snapshot that first observed it
        ref_d <- if (r == "matured") mat_d
                 else if (r == "delisted" && !is.na(dl_d)) dl_d
                 else suppressWarnings(as.Date(exit_of[[t]]))
        stale <- !is.na(ref_d) && as.numeric(today - ref_d) > 30
        state_now[[t]] <- if (stale) "closed" else "sell"
        why_now[[t]]   <- if (is.na(ref_d)) r else sprintf("%s %s", r, format(ref_d))
        next
      }
      if (!is.na(dl_d) && dl_d <= today) {          # open but delisted by now
        state_now[[t]] <- if (as.numeric(today - dl_d) > 30) "closed" else "sell"
        why_now[[t]]   <- sprintf("delisted %s", format(dl_d))
      } else if (!is.na(mat_d) && today >= mat_d) { # open but horizon elapsed
        state_now[[t]] <- if (as.numeric(today - mat_d) > 30) "closed" else "sell"
        why_now[[t]]   <- sprintf("matured %s", format(mat_d))
      } else if (!is.na(g) && g == "SELL") {
        state_now[[t]] <- "sell"; why_now[[t]] <- "gate flipped (today)"
      } else if (majf(t) && provf(t)) {
        state_now[[t]] <- "buy"
        why_now[[t]] <- if (!is.na(e) && as.numeric(today - e) <= 7) "new entry"
                        else sprintf("held since %s", entry_of[[t]])
      } else {                                      # hold: open, below majority
        d2m <- if (is.na(mat_d)) NA_real_ else as.numeric(mat_d - today)
        why_now[[t]] <- if (!is.na(d2m) && d2m <= 30)
            sprintf("matures %s", format(mat_d))
          else if (majf(t)) "signal live (unproven slot)"
          else if (!is.na(g) && g == "BUY") "flickering (below monthly majority)"
          else "washed out to SKIP"
      }
    }
    # board membership: cohort entries earned their slot at entry; ledger names
    # must be proven at this horizon today. Everything else is table-only.
    board_ok <- setNames(
      vapply(tickers, function(t)
        identical(src_of[[t]], "cohort") || provf(t), logical(1)),
      tickers)
    # signal persistence over the review window (trailing 4 months of daily
    # runs, floored at the regime epoch): on how many recorded runs was this name
    # a BUY? The period's real buy recs are the durable ones - this ranks the buy
    # section and rides on every chip, so a day-one flicker can't outrank a name
    # endorsed all period. The floor matters HERE most of all: 686 names held
    # pre-epoch BUY runs, and unfloored they bank persistence the current model
    # never awarded them. It cut both ways - CARR/HWM/NXT read 25% (10/40) while
    # the live gate has not bought them once (0/24), and genuinely durable names
    # were held down (ICHR 88% -> 100%, PENG 83% -> 96%).
    win_lo <- max(today - 122, as.Date(LEDGER_EPOCH))
    in_win <- led_dd >= win_lo
    runs_tot <- length(unique(led$d[in_win]))
    pb <- led[in_win & led$global_action == "BUY", ]
    runs_of <- table(factor(pb$ticker, levels = tickers))
    runs_of <- setNames(as.integer(runs_of), names(runs_of))
    # qualstream grade per ticker + the orange-+ set (board-universe BUYs graded
    # >= QS_MIN, top QS_CAP by grade), resolved HERE so the board mark and the
    # portfolio default-seed share one definition. Deliberately independent of the
    # board's transient cluster-id view filter, so narrowing the view never
    # shrinks the seeded portfolio.
    qs_grade <- if (!is.null(d$qs) && nrow(d$qs) > 0 &&
                    all(c("ticker", "grade") %in% names(d$qs)))
                  setNames(suppressWarnings(as.numeric(d$qs$grade)), d$qs$ticker)
                else setNames(numeric(0), character(0))
    buy_univ <- tickers[board_ok[tickers] & state_now[tickers] == "buy"]
    # seed candidates (policy 2026-08-23, Kevin's call): validated-board BUYs
    # that ALSO pass qualstream - grade >= QS_MIN, no veto. Ungraded and
    # below-bar names stay visible on the board but never enter the adopt
    # queue. History: the gate was removed 2026-08-1x because the 68 cut is
    # one retroactive vintage (unvalidated); Kevin chose the stricter book
    # anyway - fewer, higher-graded names. Mirrors compute_board in
    # airflow/dags/portfolio_sync.py (QS_MIN_GRADE).
    qs_ok    <- setdiff(buy_univ, if (!is.null(d$qs_veto)) d$qs_veto else character(0))
    gq       <- suppressWarnings(as.numeric(qs_grade[qs_ok]))
    qs_ok    <- qs_ok[!is.na(gq) & gq >= QS_MIN]
    gr_rank  <- as.numeric(qs_grade[qs_ok]); gr_rank[is.na(gr_rank)] <- -Inf
    pr_rank  <- as.numeric(runs_of[qs_ok]);  pr_rank[is.na(pr_rank)] <- 0
    # concentration guard: within the ranked queue, keep at most
    # QS_CLUSTER_CAP names per cluster id before the overall QS_CAP. One cluster
    # model flipping its gate then moves at most that many seeds, not half the
    # board. Names without a cluster id share one bucket. Existing tracked
    # positions are never auto-pruned - the cap shapes what gets marked/seeded.
    qs_rank <- qs_ok[order(-gr_rank, -pr_rank, qs_ok)]
    id_seed <- if (!is.null(d$gate$id))
                 setNames(suppressWarnings(as.integer(d$gate$id)),
                          toupper(d$gate$ticker))
               else setNames(integer(0), character(0))
    if (!is.null(d$coh) && all(c("ticker", "id") %in% names(d$coh))) {
      ex <- d$coh[!(toupper(d$coh$ticker) %in% names(id_seed)), c("ticker", "id")]
      if (nrow(ex)) id_seed <- c(id_seed, setNames(
        suppressWarnings(as.integer(ex$id)), toupper(ex$ticker)))
    }
    cid_seed <- id_seed[qs_rank]; cid_seed[is.na(cid_seed)] <- -1L
    within_k <- stats::ave(seq_along(qs_rank), cid_seed, FUN = seq_along)
    qs_buys  <- utils::head(qs_rank[within_k <= QS_CLUSTER_CAP], QS_CAP)
    list(M = M, dates = dates, tickers = tickers,
         entry_of = entry_of, exit_of = exit_of, reason_of = reason_of, hz = hz,
         state_now = state_now, why_now = why_now,
         mat_of = mat_of, src_of = src_of, prov_of = prov_of, wpct = wpct,
         board_ok = board_ok, coh_dates = coh_dates,
         coh_n = if (is.null(ch)) 0L else nrow(ch),
         runs_of = runs_of, runs_tot = runs_tot,
         qs_grade = qs_grade, qs_buys = qs_buys)
    })()
    if (!is.null(mk)) {
      attr(dv$M, "lc_memo") <- d$memo_stamp
      .lc_memo$dv <- dv; .lc_memo$dv_key <- mk
    }
    dv
  })

  # The decision board: three name sections ordered by action (exit list first,
  # then today's endorsed buys, then the maturing holds). Every ticker is
  # readable directly; the table below adds detail, filters, and CSV.
  # cluster-id filter (mirrors the Predictions tab): checkboxes for the ids
  # present in the loaded universe, all on by default; uncheck to narrow. The
  # board and the table both read input$idsLC.
  # Cluster ids the board id filter offers: only clusters that actually hold a
  # current BUY - NOT the whole gate universe (d$gate$id U d$coh$id), where a
  # checkbox for a buy-less cluster just narrows the board to nothing. When
  # "show qualstream + picks only" is on, scope further to clusters whose buy
  # passed qualstream (>= QS_MIN), matching the board's orange-+ set. Buy-scoped
  # by design (Kevin's ask); holds/sells contribute no ids.
  lc_board_filter_ids <- reactive({
    d <- app_dataLC(); dv <- tryCatch(derivedLC(), error = function(e) NULL)
    if (is.null(d) || is.null(d$gate$id) || is.null(dv) || is.null(dv$state_now))
      return(integer(0))
    buys <- toupper(dv$tickers[dv$board_ok[dv$tickers] %in% TRUE &
                               dv$state_now[dv$tickers] %in% "buy"])
    g <- if (length(dv$qs_grade))
           setNames(suppressWarnings(as.numeric(dv$qs_grade)), toupper(names(dv$qs_grade)))
         else setNames(numeric(0), character(0))
    if (isTRUE(input$lcBuyQSonly) && length(g))
      buys <- buys[!is.na(g[buys]) & g[buys] >= QS_MIN]
    # + the clusters of every master-list name (graded >= QS_MIN or an open
    # position, in a live state), so the boxes survive a day with no BUYs. After
    # the 2026-10-01 re-mint the board had 0 buys and the whole filter vanished
    # (Kevin 2026-10-07: "what happened to the boxes where i could select and
    # view each cluster"). Ids are the LIVE gate id - the same id the sketch
    # lines, pins and track strip narrow by.
    pp <- tryCatch(lc_pos(), error = function(e) NULL)
    owned <- if (!is.null(pp) && nrow(pp)) {
      sd <- as.character(pp$sold_date)
      toupper(pp$ticker[is.na(sd) | !nzchar(sd)])
    } else character(0)
    tk <- toupper(dv$tickers)
    live <- dv$state_now[dv$tickers] %in% c("buy", "hold", "sell", "closed")
    ml <- tk[live & ((!is.na(g[tk]) & g[tk] >= QS_MIN) | tk %in% owned)]
    id_of <- setNames(suppressWarnings(as.integer(d$gate$id)), toupper(d$gate$ticker))
    sort(unique(stats::na.omit(id_of[unique(c(buys, ml))])))
  })

  # The cluster selection every reader applies. The stored selection can outlive
  # the filter it came from: after the 2026-10-01 re-mint the board had 0 buys, the
  # checkboxes vanished, and a stale pick (or a deselect) kept narrowing the board,
  # sketch and pins to nothing with no visible control left to undo it. Rule: no
  # filter on screen -> no filter applied; picks that no longer exist -> ignored.
  # NULL = no narrowing; character(0) = explicit deselect-all; else = narrow to these.
  lc_ids_effective <- reactive({
    sel <- lc_board_ids_sel()
    if (is.null(sel)) return(NULL)
    avail <- as.character(tryCatch(lc_board_filter_ids(), error = function(e) integer(0)))
    if (!length(avail)) return(NULL)
    if (!length(sel)) return(character(0))
    kept <- intersect(sel, avail)
    if (!length(kept)) return(NULL)
    # every box ticked = nothing narrowed (same as untouched), so a "narrowed"
    # reader below never has to special-case the full set
    if (setequal(kept, avail)) NULL else kept
  })

  # ticker -> LIVE gate cluster id (serving.return_cluster_ticker_global_action_current
  # .id): the one numbering the cluster boxes, sketch lines, pins, track strip and
  # master list all share. Smallest id per ticker, matching the DISTINCT ON in
  # LC_QS_COMPARE_DETAIL_SQL that feeds the per-cluster sketch lines.
  lc_live_ids <- reactive({
    d <- app_dataLC()
    if (is.null(d) || is.null(d$gate) || !nrow(d$gate) || is.null(d$gate$id))
      return(setNames(integer(0), character(0)))
    g <- data.frame(tk = toupper(d$gate$ticker),
                    id = suppressWarnings(as.integer(d$gate$id)), stringsAsFactors = FALSE)
    g <- g[!is.na(g$id), , drop = FALSE]
    g <- g[order(g$tk, g$id), , drop = FALSE]
    g <- g[!duplicated(g$tk), , drop = FALSE]
    setNames(g$id, g$tk)
  })

  # Market-shift tags (display-only, see NC_TAG_VERSION): the marked rows (filled
  # ◆ or hollow ◇), upper-case tickers. NULL = no tags loaded at all.
  lc_nc_tags <- reactive({
    d <- app_dataLC()
    x <- if (!is.null(d)) d$nc else NULL
    if (is.null(x) || !nrow(x)) return(NULL)
    x[x$marked %in% TRUE, , drop = FALSE]
  })
  # The market-creators view's ticker set; NULL = no narrowing (box off, or no
  # tags loaded: a missing table must never blank the board). character(0) = tags
  # loaded but no name is marked, so the view is honestly empty. While it is
  # non-NULL the view shows these names from EVERY cluster (Kevin 2026-10-13:
  # "select all regardless of their cluster"): lc_in_sel ignores the boxes.
  lc_nc_only <- reactive({
    if (!isTRUE(input$lcNewCatOnly)) return(NULL)
    x <- lc_nc_tags()
    if (is.null(x)) NULL else x$ticker
  })
  # The master-list universe (latest non-vetoed grade >= QS_MIN, or an open
  # position), the same set bestBetsLC lists: marked names outside it are drawn on
  # the chart only.
  lc_ml_names <- reactive({
    dv <- tryCatch(derivedLC(), error = function(e) NULL)
    g <- if (!is.null(dv) && length(dv$qs_grade))
           setNames(suppressWarnings(as.numeric(dv$qs_grade)), toupper(names(dv$qs_grade)))
         else setNames(numeric(0), character(0))
    pp <- tryCatch(lc_pos(), error = function(e) NULL)
    owned <- if (!is.null(pp) && nrow(pp)) {
      sdv <- as.character(pp$sold_date)
      toupper(pp$ticker[is.na(sdv) | !nzchar(sdv)])
    } else character(0)
    unique(c(names(g)[!is.na(g) & g >= QS_MIN], owned))
  })
  # Master-list window return (Kevin 2026-10-15: "sort table high to low, based on
  # the current graph extent, take into consideration the slide range"). One price
  # pull per data load for every master-list name from the full sketch anchor; the
  # return over the slider window is then computed in R on every drag. Same
  # arithmetic as lc_price_lines + lc_win_cut, so a name's number equals the end of
  # its line on the sketch: base = first close on/after the window start, end =
  # last close on/before the window end (SPY's last date when the window is open).
  lc_ml_px <- reactive({
    d <- app_dataLC()
    if (is.null(d) || is.null(d$qscmp_anchor)) return(NULL)
    if (is.null(input$db_pass) || input$db_pass == "") return(NULL)
    tks <- lc_ml_names(); if (!length(tks)) return(NULL)
    con <- tryCatch(get_con(input), error = function(e) NULL); if (is.null(con)) return(NULL)
    on.exit({ if (DBI::dbIsValid(con)) dbDisconnect(con) }, add = TRUE)
    px <- tryCatch(lc_px_get(con, tks, as.Date(d$qscmp_anchor)), error = function(e) NULL)
    if (is.null(px) || !nrow(px)) return(NULL)
    px$ticker <- toupper(px$ticker); px$d <- as.Date(px$d); px$px <- as.numeric(px$px)
    px[!is.na(px$px) & px$px > 0, , drop = FALSE]
  })
  lc_ml_winret <- reactive({
    none <- setNames(numeric(0), character(0))
    px <- lc_ml_px(); if (is.null(px) || !nrow(px)) return(none)
    d  <- lc_sketch_data()
    ws <- if (!is.null(d) && !is.null(d$qscmp_anchor)) as.Date(d$qscmp_anchor) else min(px$d)
    we <- if (!is.null(d) && !is.null(d$qscmp_to)) as.Date(d$qscmp_to) else max(px$d)
    w  <- px[px$d >= ws & px$d <= we, , drop = FALSE]
    if (!nrow(w)) return(none)
    w  <- w[order(w$ticker, w$d), , drop = FALSE]
    b  <- w[!duplicated(w$ticker), , drop = FALSE]
    e  <- w[!duplicated(w$ticker, fromLast = TRUE), , drop = FALSE]
    setNames((e$px[match(b$ticker, e$ticker)] / b$px - 1) * 100, b$ticker)
  })
  # Why a marked name is not on the master list (chart hover, note under the
  # table): vetoed, graded under QS_MIN, or no grade inside QS_MAX_AGE_DAYS.
  lc_offlist_why <- function(tks) {
    d <- app_dataLC()
    qs <- if (!is.null(d)) d$qs else NULL
    g  <- if (!is.null(qs) && nrow(qs)) setNames(suppressWarnings(as.numeric(qs$grade)), toupper(qs$ticker))
          else setNames(numeric(0), character(0))
    gd <- if (!is.null(qs) && nrow(qs)) setNames(as.Date(qs$as_of), toupper(qs$ticker)) else NULL
    anyd <- if (!is.null(d)) d$qs_any_asof else NULL
    veto <- if (!is.null(d) && length(d$qs_veto)) d$qs_veto else character(0)
    on <- function(v, tk) {
      x <- if (!is.null(v) && tk %in% names(v)) v[[tk]] else NA
      if (is.na(x)) "" else paste0(", ", format(as.Date(x), "%b %d"))
    }
    vapply(toupper(tks), function(tk) {
      if (tk %in% veto) return(paste0("vetoed by qualstream", on(anyd, tk)))
      if (tk %in% names(g) && !is.na(g[[tk]]))
        return(sprintf("graded %d, under %d%s", as.integer(round(g[[tk]])), QS_MIN, on(gd, tk)))
      if (!is.null(anyd) && tk %in% names(anyd)) return(paste0("grade expired, last graded", on(anyd, tk)))
      "no grade in the last 150 days"
    }, character(1), USE.NAMES = FALSE)
  }
  output$lcNewCatUI <- renderUI({
    d <- app_dataLC()
    if (is.null(d) || is.null(d$nc) || !nrow(d$nc)) return(NULL)
    ml <- lc_ml_names()
    mk <- d$nc[d$nc$marked %in% TRUE, , drop = FALSE]
    n_hollow <- sum(mk$marker == "hollow")
    n_gear <- sum(mk$enabler %in% TRUE)
    n_untagged <- sum(!(ml %in% d$nc$ticker))
    div(style = "margin-bottom:0.9rem;",
      # isolate: keep the box's state across the hourly data refresh re-render
      checkboxInput("lcNewCatOnly", paste0("\u25c6 Disruptors + ", NC_GEAR, " enablers only (every cluster)"),
                    value = isTRUE(isolate(input$lcNewCatOnly))),
      div(style = "color:#64748b; font-size:0.68rem; margin-top:-0.55rem;",
          sprintf("%d \u25c6%s%s (%d on the master list%s); hover a mark for why. Display only.",
                  sum(mk$marker == "filled"),
                  if (n_hollow > 0) sprintf(" + %d \u25c7", n_hollow) else "",
                  sprintf(" + %d %s", n_gear, NC_GEAR),
                  sum(mk$ticker %in% ml),
                  if (n_untagged > 0) sprintf("; %d master-list names not tagged yet", n_untagged)
                  else "")))
  })
  # Under the cluster boxes while the market-creators view is on: they are ignored.
  output$lcNcPausedUI <- renderUI({
    if (is.null(lc_nc_only())) return(NULL)
    div(style = "color:#c084fc; font-size:0.68rem; margin:-0.3rem 0 0.8rem;",
        paste0("Clusters paused: the \u25c6 ", NC_GEAR, " view shows every cluster."))
  })

  # TRUE for tickers inside the ticked clusters (all TRUE when nothing is narrowed).
  # Strictly by live id: when a cluster is ticked, names you own in OTHER clusters
  # and names with no live id drop out too. Kevin 2026-10-07: "when a cluster is
  # selected only the specific tickers to that cluster should be showed on graph and
  # table" - this replaces the 2026-09-01 owned-names exemption on the pins.
  # While the market-creators view is on, the boxes are ignored and only marked
  # names pass, from every cluster (Kevin 2026-10-13). That covers the master
  # list, pins, pick and checked lines and the track strip; lc_top_perf, boardLC
  # and qsCompareLC read lc_nc_only() directly.
  lc_in_sel <- function(tks, sel = lc_ids_effective()) {
    nc <- lc_nc_only()
    if (!is.null(nc)) return(toupper(tks) %in% nc)
    if (is.null(sel)) rep(TRUE, length(tks))
    else if (!length(sel)) rep(FALSE, length(tks))
    else {
      ids <- lc_live_ids()[toupper(tks)]
      !is.na(ids) & ids %in% as.integer(sel)
    }
  }
  # Exactly one cluster ticked (Kevin 2026-10-12: "light grey lines for each
  # ticker when and only when a specific cluster id is selected"): the sketch then
  # draws every name of that cluster as its own line without the pick-lines box.
  # Off while the market-creators view is on: that view draws its own lines.
  lc_single_cluster <- reactive({
    if (!is.null(lc_nc_only())) return(NA_integer_)
    sel <- lc_ids_effective()
    if (!is.null(sel) && length(sel) == 1) as.integer(sel) else NA_integer_
  })

  output$idFilterLC <- renderUI({
    ids <- lc_board_filter_ids()
    if (!length(ids)) return(div(
      style = "color:#64748b; font-size:0.72rem; margin-bottom:0.6rem;",
      if (isTRUE(input$lcBuyQSonly))
        "No qualstream-passing buys - no clusters to filter by."
      else "No current buys - no clusters to filter by."))
    # survive data refreshes: rebuild with the user's stored selection (isolate,
    # same pattern as lcChartFilter) instead of resetting to all-on
    sel_now <- isolate(lc_board_ids_sel())
    sel_use <- if (is.null(sel_now)) as.character(ids)
               else intersect(sel_now, as.character(ids))
    div(style = "margin-bottom:0.6rem;",
      tags$label("Cluster id filter (uncheck to narrow)",
                 style = paste0("color:#94a3b8; font-size:0.72rem; font-weight:600;",
                                " display:block; margin-bottom:0.3rem;")),
      checkboxGroupInput("idsLC", NULL, choices = ids, selected = sel_use, inline = TRUE),
      # buttons, not links: same control pair as the Predictions/Forecast tabs
      actionButton("idsAllLC", "Select all",
                   style = "padding:2px 10px; font-size:0.72rem; margin-right:0.35rem;"),
      actionButton("idsNoneLC", "Deselect all",
                   style = "padding:2px 10px; font-size:0.72rem;"))
  })
  observeEvent(input$idsAllLC, {
    # select-all = every buy cluster the filter offers (same scope as the list),
    # not the whole gate universe. as.character: checkbox values are strings.
    updateCheckboxGroupInput(session, "idsLC",
      selected = as.character(lc_board_filter_ids()))
  })
  observeEvent(input$idsNoneLC, {
    updateCheckboxGroupInput(session, "idsLC", selected = character(0))
  })

