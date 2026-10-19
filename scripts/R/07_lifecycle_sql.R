# AlphaStream dashboard - Lifecycle SQL and portfolio/sell-ladder series. Part of app.R, split 2026-10-19: files load in name order (see app.R).
# Lifecycle qualstream comparison: equal-weight return of the CURRENT BUYs that
# qualstream graded (latest non-vetoed scorecard) vs the >= 68 subset vs SPY,
# over a rolling 120-day lookback (__ANCHOR__ = today - 120). DESCRIPTIVE, not a
# walk-forward test: qualstream's grades are a single as-of snapshot applied
# across the whole window, and it only covers the curated top-picks (a fraction
# of the gate's BUYs). Same 68 cut and 150d freshness as the board's orange +.
LC_QS_COMPARE_SQL <- "
WITH params AS (SELECT DATE '__ANCHOR__' AS start_d),
scored AS (
    -- latest NON-VETOED scorecard per ticker: veto filtered BEFORE DISTINCT ON,
    -- the same rule the board's orange + uses, so chart and board populations
    -- never diverge once multiple grade runs exist
    SELECT DISTINCT ON (ticker) ticker, overall
    FROM qual.ticker_scorecards
    WHERE NOT veto
      AND rubric_version = 'buy_decision_v1' AND as_of >= CURRENT_DATE - __QS_AGE__
    ORDER BY ticker, as_of DESC, graded_at DESC
),
graded_buys AS (
    -- basket = EVERY non-vetoed name qualstream graded in the age window, NOT the
    -- current-day gate snapshot and NOT the board's buy list. This is deliberately
    -- broader than the board's ~15 standing buys: the descriptive question here is
    -- whether, across everything qualstream graded, the >= 68 names outran the rest,
    -- so a passer that is on hold / matured / off the board still belongs in the
    -- basket. The board's orange + is the intersection of THIS >= 68 set with the
    -- live buy column (7 of the 15 passers, at time of writing) - a strict subset,
    -- not an equality.
    SELECT s.ticker, (s.overall >= 68) AS passed FROM scored s
),
maxd AS (SELECT MAX(date) AS d FROM cdm.ingest_combined WHERE ticker = 'SPY'),
-- one indexed range scan of the basket tickers' prices over the window, reused
-- for the entry price, the per-day return, and (via SPY) the date grid. The old
-- shape did a correlated as-of subquery per ticker per day (617ms); this is a
-- plain join on SPY's trading calendar (~40ms), numerically identical.
slice AS (
    SELECT i.ticker, i.date AS d, i.adj_close AS px
    FROM cdm.ingest_combined i
    JOIN graded_buys g ON g.ticker = i.ticker
    WHERE i.date >= (SELECT start_d FROM params) AND i.date <= (SELECT d FROM maxd)
),
entry AS (SELECT DISTINCT ON (ticker) ticker, px AS entry_px FROM slice ORDER BY ticker, d),
basket AS (
    SELECT s.d, g.passed, s.px / NULLIF(e.entry_px, 0) - 1 AS ret
    FROM slice s
    JOIN graded_buys g ON g.ticker = s.ticker
    JOIN entry e ON e.ticker = s.ticker
    WHERE e.entry_px > 0
),
spy AS (
    SELECT i.date AS d, i.adj_close AS px,
      (SELECT adj_close FROM cdm.ingest_combined WHERE ticker = 'SPY'
       AND date >= (SELECT start_d FROM params) ORDER BY date LIMIT 1) AS px0
    FROM cdm.ingest_combined i
    WHERE i.ticker = 'SPY' AND i.date >= (SELECT start_d FROM params) AND i.date <= (SELECT d FROM maxd)
)
SELECT sp.d::text AS d,
       ROUND((AVG(b.ret) * 100)::numeric, 2) AS graded_pct,
       ROUND((AVG(b.ret) FILTER (WHERE b.passed) * 100)::numeric, 2) AS passed_pct,
       ROUND((((sp.px / sp.px0) - 1) * 100)::numeric, 2) AS spy_pct
FROM spy sp
LEFT JOIN basket b ON b.d = sp.d
GROUP BY sp.d, sp.px, sp.px0
ORDER BY sp.d;"

# Per-ticker daily detail behind LC_QS_COMPARE_SQL, used ONLY to re-aggregate the
# HINDSIGHT sketch's graded/passed lines for a selected cluster without re-querying.
# Same basket/entry/return shape (same optimized SPY-calendar slice) but WITHOUT the
# per-date aggregate, plus the ticker's current cluster id. The id LEFT JOIN is
# DISTINCT ON (ticker) so a name in >1 cluster can't double-count; a future off-board
# name simply gets a NULL id and drops out only when a specific cluster is selected
# (in the all-clusters/default view the sketch keeps reading the pre-aggregated
# LC_QS_COMPARE_SQL, so those names still count there).
LC_QS_COMPARE_DETAIL_SQL <- "
WITH params AS (SELECT DATE '__ANCHOR__' AS start_d),
scored AS (
    SELECT DISTINCT ON (ticker) ticker, overall
    FROM qual.ticker_scorecards
    WHERE NOT veto
      AND rubric_version = 'buy_decision_v1' AND as_of >= CURRENT_DATE - __QS_AGE__
    ORDER BY ticker, as_of DESC, graded_at DESC
),
graded_buys AS (SELECT s.ticker, (s.overall >= 68) AS passed FROM scored s),
maxd AS (SELECT MAX(date) AS d FROM cdm.ingest_combined WHERE ticker = 'SPY'),
slice AS (
    SELECT i.ticker, i.date AS d, i.adj_close AS px
    FROM cdm.ingest_combined i
    JOIN graded_buys g ON g.ticker = i.ticker
    WHERE i.date >= (SELECT start_d FROM params) AND i.date <= (SELECT d FROM maxd)
),
entry AS (SELECT DISTINCT ON (ticker) ticker, px AS entry_px FROM slice ORDER BY ticker, d)
SELECT s.d::text AS d, s.ticker, gate.id, g.passed,
       s.px / NULLIF(e.entry_px, 0) - 1 AS ret
FROM slice s
JOIN graded_buys g ON g.ticker = s.ticker
JOIN entry e ON e.ticker = s.ticker
LEFT JOIN (SELECT DISTINCT ON (ticker) ticker, id
           FROM serving.return_cluster_ticker_global_action_current
           ORDER BY ticker, id) gate ON gate.ticker = s.ticker
WHERE e.entry_px > 0
ORDER BY s.ticker, s.d;"

# Sketch window (Kevin 2026-10-14: "a ruler I could drag, either way, and have the
# viz re-rendered from the mark"). w = c(start, end) Dates, NA = that end left at
# its edge. Re-anchors the sketch at the window's first trading day and trims it
# at the end: SPY restarts at 0% there, and the graded/passed baskets are rebuilt
# from the per-ticker detail with each name entering at its first bar on/after the
# start, so they equal LC_QS_COMPARE_SQL run with __ANCHOR__ = start (equal weight
# from that day, not the full window's drifted weights). qscmp_anchor moves to the
# start, so the overlay, pins and name lines re-anchor there by their own rules.
# NULL when the window holds under two trading days.
lc_window_sketch <- function(d, w) {
  cmp <- d$qscmp; dd <- as.Date(cmp$d)
  ws <- if (is.na(w[1])) min(dd, na.rm = TRUE) else w[1]
  we <- if (is.na(w[2])) max(dd, na.rm = TRUE) else w[2]
  cmp <- cmp[!is.na(dd) & dd >= ws & dd <= we, , drop = FALSE]
  if (nrow(cmp) < 2) return(NULL)
  ws <- min(as.Date(cmp$d))                       # snap to the first trading day
  rb <- function(x) {                             # % series, restarted at its first value
    b <- x[!is.na(x)][1]
    if (is.na(b)) x else round(((1 + x / 100) / (1 + b / 100) - 1) * 100, 2)
  }
  cmp$spy_pct <- rb(cmp$spy_pct)
  det <- d$qscmp_detail
  if (!is.null(det) && nrow(det)) {
    ddt <- as.Date(det$d)
    det <- det[!is.na(ddt) & ddt >= ws & ddt <= we, , drop = FALSE]
    det <- det[order(det$ticker, det$d), , drop = FALSE]
    if (nrow(det)) {                              # each name from its first in-window bar
      base <- ave(det$ret, det$ticker, FUN = function(x) x[!is.na(x)][1])
      det$ret <- (1 + det$ret) / (1 + base) - 1
    }
    key <- as.character(cmp$d)
    avg <- function(sub) if (!nrow(sub)) rep(NA_real_, length(key)) else
      round(as.numeric(tapply(sub$ret, sub$d, function(x) mean(x, na.rm = TRUE) * 100)[key]), 2)
    cmp$graded_pct <- avg(det)
    cmp$passed_pct <- avg(det[det$passed %in% TRUE, , drop = FALSE])
  } else {                                        # no detail loaded: restart the lines as drawn
    cmp$graded_pct <- rb(cmp$graded_pct)
    cmp$passed_pct <- rb(cmp$passed_pct)
  }
  d$qscmp <- cmp; d$qscmp_detail <- det; d$qscmp_anchor <- ws
  d$qscmp_to <- if (is.na(w[2])) NULL else we
  d
}

# rows of df dated on/before we (the sketch window's end); NULL we = unchanged
lc_win_cut <- function(df, we, col = "d") {
  if (is.null(we) || is.null(df) || !nrow(df)) return(df)
  df[as.Date(df[[col]]) <= we, , drop = FALSE]
}

# Personal DCA portfolio value + return vs a SAME-CASH-FLOW SPY benchmark.
# __BUYS__ is a VALUES list of (ticker, buy_date, dollars) expanded in R from the
# saved positions. Each buy fills at the first adj_close on/after its date; the
# SAME dollars also buy SPY (spy_shares) so the benchmark runs the identical DCA
# (this is the FORECAST_CURVE_SQL spy_sh trick, not a lump SPY index). Holdings
# are marked to market monthly (+ today) with the as-of close; return =
# value/invested - 1. Reuses the FORECAST_LIVE_SQL share-accumulation shape.
LC_PORTFOLIO_SQL <- "
WITH buys(ticker, d, amt) AS (VALUES __BUYS__),
sells(ticker, d, frac, trig_d) AS (__SELLS__),
maxd AS (SELECT MAX(date) AS d FROM cdm.ingest_combined WHERE ticker = 'SPY'),
fills AS (
    SELECT b.ticker, b.d::date AS buy_d, b.amt::numeric AS amt,
      (SELECT i.adj_close FROM cdm.ingest_combined i
       WHERE i.ticker = b.ticker AND i.date >= b.d::date ORDER BY i.date LIMIT 1) AS px,
      (SELECT i.adj_close FROM cdm.ingest_combined i
       WHERE i.ticker = 'SPY' AND i.date >= b.d::date ORDER BY i.date LIMIT 1) AS spy_px
    FROM buys b
),
lots AS (
    SELECT ticker, buy_d, amt, amt / NULLIF(px, 0) AS shares,
           amt / NULLIF(spy_px, 0) AS spy_shares
    FROM fills WHERE px > 0 AND spy_px > 0
),
-- ladder legs: every episode fully exits (backstops + supersede), so shares
-- held at trigger k = cum bought by k minus cum bought by k-1 (telescoping LAG)
trig_leg AS (
    SELECT c.ticker, c.trig_d,
           c.cum_sh  - COALESCE(LAG(c.cum_sh)  OVER (PARTITION BY c.ticker ORDER BY c.trig_d), 0) AS leg_sh,
           c.cum_spy - COALESCE(LAG(c.cum_spy) OVER (PARTITION BY c.ticker ORDER BY c.trig_d), 0) AS leg_spy
    FROM (
        SELECT sv.ticker, sv.trig_d::date AS trig_d,
               COALESCE(SUM(l.shares), 0) AS cum_sh, COALESCE(SUM(l.spy_shares), 0) AS cum_spy
        FROM (SELECT DISTINCT ticker, trig_d FROM sells) sv
        LEFT JOIN lots l ON l.ticker = sv.ticker AND l.buy_d <= sv.trig_d::date
        GROUP BY sv.ticker, sv.trig_d::date
    ) c
),
sell_ev AS (
    SELECT s.ticker, s.d::date AS sell_d,
           tl.leg_sh  * s.frac::numeric AS sh_sold,
           tl.leg_spy * s.frac::numeric AS spy_sold,
           (SELECT i.adj_close FROM cdm.ingest_combined i
            WHERE i.ticker = s.ticker AND i.date <= s.d::date ORDER BY i.date DESC LIMIT 1) AS px,
           (SELECT i.adj_close FROM cdm.ingest_combined i
            WHERE i.ticker = 'SPY' AND i.date <= s.d::date ORDER BY i.date DESC LIMIT 1) AS spy_px
    FROM sells s JOIN trig_leg tl
      ON tl.ticker = s.ticker AND tl.trig_d = s.trig_d::date
),
vdates AS (
    SELECT d FROM generate_series(
        (SELECT MIN(buy_d) FROM lots),
        (SELECT d FROM maxd), INTERVAL '1 month') AS g(d)
    UNION SELECT (SELECT d FROM maxd)
    UNION SELECT sell_d FROM sell_ev
),
holdings AS (
    SELECT v.d AS vdate, l.ticker,
           SUM(l.shares) AS shares, SUM(l.amt) AS invested,
           SUM(l.spy_shares) AS spy_shares
    FROM vdates v JOIN lots l ON l.buy_d <= v.d
    GROUP BY v.d, l.ticker
),
sold_cum AS (
    SELECT v.d AS vdate, e.ticker,
           SUM(e.sh_sold) AS sh_sold, SUM(e.spy_sold) AS spy_sold,
           SUM(e.sh_sold * e.px) AS cash, SUM(e.spy_sold * e.spy_px) AS spy_cash
    FROM vdates v JOIN sell_ev e ON e.sell_d <= v.d
    GROUP BY v.d, e.ticker
),
valued AS (
    SELECT h.vdate, SUM(h.invested) AS invested,
           SUM(GREATEST(h.shares - COALESCE(sc.sh_sold, 0), 0) *
             (SELECT i.adj_close FROM cdm.ingest_combined i
              WHERE i.ticker = h.ticker AND i.date <= h.vdate
              ORDER BY i.date DESC LIMIT 1)) AS mkt,
           SUM(COALESCE(sc.cash, 0)) AS cash,
           SUM(GREATEST(h.spy_shares - COALESCE(sc.spy_sold, 0), 0)) AS spy_sh,
           SUM(COALESCE(sc.spy_cash, 0)) AS spy_cash
    FROM holdings h
    LEFT JOIN sold_cum sc ON sc.vdate = h.vdate AND sc.ticker = h.ticker
    GROUP BY h.vdate
)
SELECT vdate::date::text AS d,
       ROUND(invested::numeric, 2) AS invested,
       ROUND((mkt + cash)::numeric, 2) AS value,
       ROUND((100 * ((mkt + cash) / NULLIF(invested, 0) - 1))::numeric, 2) AS ret_pct,
       ROUND((spy_sh * (SELECT adj_close FROM cdm.ingest_combined
         WHERE ticker = 'SPY' AND date <= vdate ORDER BY date DESC LIMIT 1)
         + spy_cash)::numeric, 2) AS spy_value,
       ROUND((100 * ((spy_sh * (SELECT adj_close FROM cdm.ingest_combined
         WHERE ticker = 'SPY' AND date <= vdate ORDER BY date DESC LIMIT 1)
         + spy_cash) / NULLIF(invested, 0) - 1))::numeric, 2) AS spy_ret_pct
FROM valued ORDER BY vdate;"

# Per-TICKER return series for the holdings chart: one weekly cumulative-return
# line per ticker (100 * value/invested - 1) plus a single same-cash SPY line
# labelled '__SPY__'. __BUYS__ = VALUES (id, ticker, 'YYYY-MM-DD', amt), one per
# gated buy. Accumulation view (no sell wind-down); each ticker's
# line starts at 0% on its first buy. Weekly vdates keep it light but smooth; a
# brand-new position yields a single point (rendered as a marker in R).
LC_PORTFOLIO_TICKER_SQL <- "
WITH buys(id, ticker, d, amt) AS (VALUES __BUYS__),
maxd AS (SELECT MAX(date) AS d FROM cdm.ingest_combined WHERE ticker = 'SPY'),
fills AS (
    -- fill_d = the day the buy actually prices (first close >= plan date):
    -- anchoring the series on the PLAN date valued Friday's close against
    -- Monday-priced shares broke the starts-at-0% invariant for weekend
    -- entries (audit 2026-08-23)
    SELECT b.ticker, b.d::date AS buy_d, b.amt::numeric AS amt,
      (SELECT i.date FROM cdm.ingest_combined i
       WHERE i.ticker = b.ticker AND i.date >= b.d::date ORDER BY i.date LIMIT 1) AS fill_d,
      (SELECT i.adj_close FROM cdm.ingest_combined i
       WHERE i.ticker = b.ticker AND i.date >= b.d::date ORDER BY i.date LIMIT 1) AS px,
      (SELECT i.adj_close FROM cdm.ingest_combined i
       WHERE i.ticker = 'SPY' AND i.date >= b.d::date ORDER BY i.date LIMIT 1) AS spy_px
    FROM buys b
),
lots AS (
    SELECT ticker, buy_d, fill_d, amt, amt / NULLIF(px, 0) AS shares,
           amt / NULLIF(spy_px, 0) AS spy_shares
    FROM fills WHERE px > 0 AND spy_px > 0
),
vdates AS (
    SELECT g::date AS d FROM generate_series(
        (SELECT MIN(fill_d) FROM lots), (SELECT d FROM maxd), INTERVAL '1 week') AS g
    UNION SELECT (SELECT d FROM maxd)
    UNION SELECT fill_d FROM lots   -- each line starts at 0% on its own first fill
),
tk AS (
    SELECT v.d AS vdate, l.ticker, SUM(l.shares) AS shares, SUM(l.amt) AS invested,
           SUM(l.spy_shares) AS spy_shares
    FROM vdates v JOIN lots l ON l.fill_d <= v.d
    GROUP BY v.d, l.ticker
),
tk_val AS (
    SELECT vdate, ticker, invested,
           shares * (SELECT i.adj_close FROM cdm.ingest_combined i
             WHERE i.ticker = tk.ticker AND i.date <= tk.vdate
             ORDER BY i.date DESC LIMIT 1) AS value,
           spy_shares * (SELECT adj_close FROM cdm.ingest_combined
             WHERE ticker = 'SPY' AND date <= tk.vdate
             ORDER BY date DESC LIMIT 1) AS spy_value
    FROM tk
),
spy AS (
    SELECT v.d AS vdate, SUM(l.amt) AS invested, SUM(l.spy_shares) AS spy_sh
    FROM vdates v JOIN lots l ON l.fill_d <= v.d
    GROUP BY v.d
),
-- lump benchmark: the whole stake placed at the row's FIRST fill price - what
-- the entry-policy audit calls HODL/lump-at-entry. One px per ticker.
firstpx AS (
    SELECT DISTINCT ON (ticker) ticker, fill_d AS first_d,
           px AS first_px, spy_px AS first_spy
    FROM fills WHERE px > 0 AND spy_px > 0 ORDER BY ticker, fill_d
),
-- daily peak/trough of the vs-SPY excess since first fill (lump basis): the
-- highest and lowest the position's edge has BEEN, not just where it is now
spydaily AS (
    SELECT DISTINCT ON (date) date, adj_close FROM cdm.ingest_combined
    WHERE ticker = 'SPY' ORDER BY date, adj_close
),
extremes AS (
    SELECT fp.ticker,
           ROUND(MAX(100 * (c.adj_close / fp.first_px - s.adj_close / fp.first_spy))::numeric, 1) AS peak_vs,
           ROUND(MIN(100 * (c.adj_close / fp.first_px - s.adj_close / fp.first_spy))::numeric, 1) AS trough_vs
    FROM firstpx fp
    JOIN cdm.ingest_combined c ON c.ticker = fp.ticker AND c.date >= fp.first_d
    JOIN spydaily s ON s.date = c.date
    GROUP BY fp.ticker
)
-- per-ticker rows carry ret_pct (chart) AND invested/value/spy_value (the table
-- reads the latest row per ticker); the '__SPY__' row is the chart benchmark.
SELECT tk_val.vdate::text AS d, tk_val.ticker,
       ROUND((100 * (value / NULLIF(invested, 0) - 1))::numeric, 2) AS ret_pct,
       ROUND(invested::numeric, 2)  AS invested,
       ROUND(value::numeric, 2)     AS value,
       ROUND(spy_value::numeric, 2) AS spy_value,
       ROUND((100 * ((SELECT i.adj_close FROM cdm.ingest_combined i
                      WHERE i.ticker = tk_val.ticker AND i.date <= tk_val.vdate
                      ORDER BY i.date DESC LIMIT 1)
                     / NULLIF(f.first_px, 0) - 1))::numeric, 2) AS lump_ret_pct,
       e.peak_vs, e.trough_vs
FROM tk_val
JOIN firstpx f ON f.ticker = tk_val.ticker
LEFT JOIN extremes e ON e.ticker = tk_val.ticker
UNION ALL
SELECT vdate::text AS d, '__SPY__' AS ticker,
       ROUND((100 * ((spy_sh * (SELECT adj_close FROM cdm.ingest_combined
                WHERE ticker = 'SPY' AND date <= spy.vdate ORDER BY date DESC LIMIT 1))
              / NULLIF(invested, 0) - 1))::numeric, 2) AS ret_pct,
       NULL::numeric AS invested, NULL::numeric AS value, NULL::numeric AS spy_value,
       NULL::numeric AS lump_ret_pct, NULL::numeric AS peak_vs, NULL::numeric AS trough_vs
FROM spy
ORDER BY ticker, d;"

# Dynamic per-stock sell ladder. __TRIGGERS__ = VALUES ('TICK','YYYY-MM-DD',20)
# (ticker, trigger date, n_bars in {20,40}). Delisted episodes never reach here.
# One output row per tranche (stage 1..4): 25% at trigger, then 25% at the first
# close >= safe/good/best (p40/p60/p80 of the stock's own 3y max-runup over
# n_bars windows), with deadline + stop-loss backstops. has_hist=FALSE (<60
# windows) falls back to time-thirds. kind in trigger/level/sched/stop/deadline/pending.
LC_SELL_LADDER_SQL <- "
WITH trig(ticker, trig_d, n_bars) AS (VALUES __TRIGGERS__),
trig2 AS (
    SELECT ticker, trig_d::date AS trig_d, n_bars::int AS n_bars,
           (n_bars::int * 7 / 5) AS w_days,
           LEAD(trig_d::date) OVER (PARTITION BY ticker ORDER BY trig_d::date) AS next_trig_d
    FROM trig
),
px AS (
    SELECT i.ticker, i.date, i.adj_close,
           MAX(i.adj_close) OVER (PARTITION BY i.ticker ORDER BY i.date
             ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) / NULLIF(i.adj_close,0) - 1 AS runup20,
           MAX(i.adj_close) OVER (PARTITION BY i.ticker ORDER BY i.date
             ROWS BETWEEN 1 FOLLOWING AND 40 FOLLOWING) / NULLIF(i.adj_close,0) - 1 AS runup40,
           LEAD(i.date, 20) OVER (PARTITION BY i.ticker ORDER BY i.date) AS lead20_d,
           LEAD(i.date, 40) OVER (PARTITION BY i.ticker ORDER BY i.date) AS lead40_d
    FROM cdm.ingest_combined i
    WHERE i.ticker IN (SELECT DISTINCT ticker FROM trig2)
      AND i.date >= (SELECT MIN(trig_d) FROM trig2) - INTERVAL '3 years'
),
samples AS (
    SELECT t.ticker, t.trig_d,
           CASE WHEN t.n_bars = 20 THEN p.runup20 ELSE p.runup40 END AS runup
    FROM trig2 t JOIN px p ON p.ticker = t.ticker
    WHERE p.date >= t.trig_d - INTERVAL '3 years'
      AND CASE WHEN t.n_bars = 20 THEN p.lead20_d ELSE p.lead40_d END <= t.trig_d
),
lv AS (
    SELECT ticker, trig_d, COUNT(*) AS n_win,
           percentile_cont(0.40) WITHIN GROUP (ORDER BY runup) AS p40,
           percentile_cont(0.60) WITHIN GROUP (ORDER BY runup) AS p60,
           percentile_cont(0.80) WITHIN GROUP (ORDER BY runup) AS p80
    FROM samples GROUP BY ticker, trig_d
),
base AS (
    SELECT t.*, l.n_win, l.p40, l.p60, l.p80,
           COALESCE(l.n_win, 0) >= 60 AS has_hist,
           (SELECT i.adj_close FROM cdm.ingest_combined i
            WHERE i.ticker = t.ticker AND i.date <= t.trig_d
            ORDER BY i.date DESC LIMIT 1) AS p0_px,
           (SELECT MAX(i.date) FROM cdm.ingest_combined i WHERE i.ticker = t.ticker) AS max_d,
           LEAST(t.trig_d + (t.n_bars * 7 / 5), COALESCE(t.next_trig_d, DATE '9999-01-01')) AS win_end
    FROM trig2 t LEFT JOIN lv l ON l.ticker = t.ticker AND l.trig_d = t.trig_d
),
stops AS (
    SELECT b.*, sd.stop_detect_d, se.stop_exec_d
    FROM base b
    LEFT JOIN LATERAL (
        SELECT MIN(i.date) AS stop_detect_d FROM cdm.ingest_combined i
        WHERE i.ticker = b.ticker AND i.date > b.trig_d AND i.date <= b.win_end
          AND i.adj_close <= b.p0_px * 0.90) sd ON TRUE
    LEFT JOIN LATERAL (
        SELECT MIN(i.date) AS stop_exec_d FROM cdm.ingest_combined i
        WHERE i.ticker = b.ticker AND i.date > sd.stop_detect_d AND i.date <= b.win_end) se ON TRUE
    WHERE b.p0_px IS NOT NULL
),
tspec AS (
    SELECT s.*, st.stage,
           CASE WHEN s.has_hist THEN s.p0_px * (1 + CASE st.stage
                WHEN 2 THEN s.p40 WHEN 3 THEN s.p60 ELSE s.p80 END) END AS level_px,
           CASE WHEN NOT s.has_hist THEN s.trig_d + (s.w_days * (st.stage - 1) / 3) END AS sched_d
    FROM stops s CROSS JOIN (VALUES (2), (3), (4)) st(stage)
),
tfill AS (
    SELECT ts.*,
           CASE WHEN ts.level_px IS NOT NULL THEN
             (SELECT MIN(i.date) FROM cdm.ingest_combined i
              WHERE i.ticker = ts.ticker AND i.date > ts.trig_d
                AND i.date <= ts.win_end AND i.adj_close >= ts.level_px)
           WHEN ts.max_d >= LEAST(ts.sched_d, ts.win_end) THEN
             (SELECT MAX(i.date) FROM cdm.ingest_combined i
              WHERE i.ticker = ts.ticker AND i.date > ts.trig_d
                AND i.date <= LEAST(ts.sched_d, ts.win_end))
           END AS raw_fill_d
    FROM tspec ts
),
resolved AS (
    SELECT ticker, trig_d, n_bars, w_days, win_end, n_win, has_hist,
           p0_px, p40, p60, p80, stop_detect_d, stop_exec_d, max_d,
           1 AS stage, 0.25 AS frac, 'trigger' AS kind, trig_d AS sell_d,
           NULL::numeric AS level_px, NULL::date AS sched_d
    FROM stops
    UNION ALL
    SELECT tf.ticker, tf.trig_d, tf.n_bars, tf.w_days, tf.win_end, tf.n_win, tf.has_hist,
           tf.p0_px, tf.p40, tf.p60, tf.p80, tf.stop_detect_d, tf.stop_exec_d, tf.max_d,
           tf.stage, 0.25,
           CASE
             WHEN tf.raw_fill_d IS NOT NULL
                  AND (tf.stop_exec_d IS NULL OR tf.raw_fill_d < tf.stop_exec_d)
               THEN CASE WHEN tf.level_px IS NOT NULL THEN 'level' ELSE 'sched' END
             WHEN tf.stop_exec_d IS NOT NULL THEN 'stop'
             WHEN tf.max_d >= tf.win_end THEN 'deadline'
             ELSE 'pending' END,
           CASE
             WHEN tf.raw_fill_d IS NOT NULL
                  AND (tf.stop_exec_d IS NULL OR tf.raw_fill_d < tf.stop_exec_d)
               THEN tf.raw_fill_d
             WHEN tf.stop_exec_d IS NOT NULL THEN tf.stop_exec_d
             WHEN tf.max_d >= tf.win_end THEN
               (SELECT MAX(i.date) FROM cdm.ingest_combined i
                WHERE i.ticker = tf.ticker AND i.date <= tf.win_end)
             END,
           tf.level_px, tf.sched_d
    FROM tfill tf
)
SELECT r.ticker, r.trig_d::text AS trig_d, r.n_bars, r.w_days,
       r.win_end::text AS deadline_d, COALESCE(r.n_win, 0) AS n_win, r.has_hist,
       ROUND(r.p0_px::numeric, 4)                 AS p0,
       ROUND((r.p0_px * (1 + r.p40))::numeric, 4) AS level_safe,
       ROUND((r.p0_px * (1 + r.p60))::numeric, 4) AS level_good,
       ROUND((r.p0_px * (1 + r.p80))::numeric, 4) AS level_best,
       r.stage, r.frac, r.kind,
       r.sell_d::text AS sell_d, r.sched_d::text AS sched_d,
       ROUND((SELECT i.adj_close FROM cdm.ingest_combined i
              WHERE i.ticker = r.ticker AND i.date <= r.sell_d
              ORDER BY i.date DESC LIMIT 1)::numeric, 4) AS fill_px,
       r.stop_detect_d::text AS stop_detect_d, r.stop_exec_d::text AS stop_exec_d
FROM resolved r
ORDER BY r.ticker, r.trig_d, r.stage;"

# Run LC_PORTFOLIO_SQL for the saved positions on an open connection, honoring
# signal-gated model buys and (optional) ladder sell events. Returns a tidy
# series (d/invested/value/ret_pct/spy_value/spy_ret_pct) or NULL. Injection-safe:
# tickers ^[A-Za-z.-]+$, dates ISO, dollars/fractions numeric.
compute_portfolio_series <- function(con, positions, led = NULL,
                                     sell_events = NULL, today = Sys.Date(),
                                     model_start = "epoch") {
  sched <- gated_expand_schedule(positions, led, today, model_start = model_start)
  skipped <- attr(sched, "model_skipped")
  if (nrow(sched) == 0) return(NULL)
  ok <- grepl("^[A-Za-z.-]+$", sched$ticker) &
        grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", sched$d) & is.finite(sched$amount)
  sched <- sched[ok, , drop = FALSE]
  if (nrow(sched) == 0) return(NULL)
  vals <- paste(sprintf("('%s','%s',%s)", toupper(sched$ticker), sched$d,
                        format(sched$amount, scientific = FALSE, trim = TRUE)),
                collapse = ", ")
  sell_sql <- "SELECT NULL::text, NULL::text, NULL::numeric, NULL::text WHERE FALSE"
  if (!is.null(sell_events) && nrow(sell_events)) {
    se <- sell_events[toupper(sell_events$ticker) %in% toupper(sched$ticker), , drop = FALSE]
    if (nrow(se)) {
      se$frac <- suppressWarnings(as.numeric(se$frac))
      ok2 <- grepl("^[A-Za-z.-]+$", se$ticker) &
             grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", se$sell_d) &
             grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", se$trig_d) &
             is.finite(se$frac) & se$frac > 0 & se$frac <= 1
      se <- se[ok2, , drop = FALSE]
      if (nrow(se))
        sell_sql <- paste0("VALUES ", paste(sprintf("('%s','%s',%s,'%s')",
          toupper(se$ticker), se$sell_d,
          format(se$frac, scientific = FALSE, trim = TRUE), se$trig_d), collapse = ", "))
    }
  }
  q <- gsub("__BUYS__", vals, LC_PORTFOLIO_SQL, fixed = TRUE)
  q <- gsub("__SELLS__", sell_sql, q, fixed = TRUE)
  df <- coerce_numeric_cols(dbGetQuery(con, q),
    c("invested", "value", "ret_pct", "spy_value", "spy_ret_pct"))
  if (is.null(df) || nrow(df) == 0) return(NULL)
  df$d <- as.Date(df$d)
  attr(df, "model_skipped") <- skipped
  df
}

# Turn the board's current sell/closed model tickers into ladder triggers.
# Uses derivedLC's already-computed state/reason/exit (one episode per name in
# the current ledger window). reason in gate/matured/delisted -> window sizing.
sell_triggers_from_dv <- function(dv, led, meta, model_tickers, today = Sys.Date()) {
  if (is.null(dv) || is.null(dv$state_now)) return(NULL)
  st <- dv$state_now
  cand <- intersect(toupper(model_tickers), names(st)[st %in% c("sell", "closed")])
  if (!length(cand)) return(NULL)
  dl <- NULL
  if (!is.null(meta) && nrow(meta))
    dl <- setNames(suppressWarnings(as.Date(meta$delisted_date)), toupper(meta$ticker))
  g1 <- function(x, t) if (!is.null(x) && t %in% names(x)) x[[t]] else NA
  rows <- list()
  for (t in cand) {
    rsn  <- tolower(paste(g1(dv$reason_of, t), g1(dv$why_now, t)))
    exd  <- suppressWarnings(as.Date(g1(dv$exit_of, t)))
    matd <- suppressWarnings(as.Date(g1(dv$mat_of, t)))
    is_del <- grepl("delist", rsn) || (!is.null(dl) && t %in% names(dl) && !is.na(dl[[t]]))
    if (is_del) {
      reason <- "delisted"
      trig <- if (!is.null(dl) && t %in% names(dl) && !is.na(dl[[t]])) dl[[t]] else exd
    } else if (grepl("matur", rsn)) {
      reason <- "matured"; trig <- if (!is.na(matd)) matd else exd
    } else { reason <- "gate"; trig <- exd }
    if (is.na(trig)) trig <- today
    if (trig > today) trig <- today
    wd <- switch(reason, gate = LC_WINDDOWN$gate, matured = LC_WINDDOWN$matured, NULL)
    rows[[length(rows) + 1]] <- data.frame(
      ticker = t, trig_d = trig, reason = reason,
      n_bars = if (is.null(wd)) NA_integer_ else wd$n_bars,
      w_days = if (is.null(wd)) NA_integer_ else wd$w_days, stringsAsFactors = FALSE)
  }
  if (!length(rows)) return(NULL)
  out <- do.call(rbind, rows)
  out[!duplicated(paste(out$ticker, out$trig_d)), , drop = FALSE]
}

# Run the ladder query for laddered triggers (delisted -> 100% local event).
# Returns list(events, meta) or NULL. Capped at 25 triggers/run.
build_sell_events <- function(con, triggers) {
  if (is.null(triggers) || !nrow(triggers)) return(NULL)
  triggers <- triggers[order(triggers$ticker, triggers$trig_d), , drop = FALSE]
  if (nrow(triggers) > 25L) triggers <- triggers[seq_len(25L), , drop = FALSE]
  mcols <- c("ticker", "trig_d", "reason", "p0", "level_safe", "level_good",
             "level_best", "n_win", "has_hist", "deadline_d", "stop_detect_d", "no_price")
  events <- list(); meta <- list()
  del <- triggers[triggers$reason == "delisted", , drop = FALSE]
  lad <- triggers[triggers$reason != "delisted", , drop = FALSE]
  if (nrow(del)) {
    events[[length(events) + 1]] <- data.frame(
      ticker = del$ticker, sell_d = format(del$trig_d), frac = 1.0,
      trig_d = format(del$trig_d), stage = 1L, kind = "delisted",
      fill_px = NA_real_, stringsAsFactors = FALSE)
    meta[[length(meta) + 1]] <- data.frame(
      ticker = del$ticker, trig_d = format(del$trig_d), reason = "delisted",
      p0 = NA_real_, level_safe = NA_real_, level_good = NA_real_, level_best = NA_real_,
      n_win = 0L, has_hist = FALSE, deadline_d = format(del$trig_d),
      stop_detect_d = NA_character_, no_price = FALSE, stringsAsFactors = FALSE)[, mcols]
  }
  if (nrow(lad)) {
    ok <- grepl("^[A-Za-z.-]+$", lad$ticker) &
          grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", format(lad$trig_d)) &
          lad$n_bars %in% c(20L, 40L)
    lad <- lad[ok, , drop = FALSE]
    if (nrow(lad)) {
      vals <- paste(sprintf("('%s','%s',%d)", toupper(lad$ticker),
                            format(lad$trig_d), as.integer(lad$n_bars)), collapse = ", ")
      res <- tryCatch(coerce_numeric_cols(
        dbGetQuery(con, gsub("__TRIGGERS__", vals, LC_SELL_LADDER_SQL, fixed = TRUE)),
        c("p0", "level_safe", "level_good", "level_best", "frac", "fill_px", "n_win")),
        error = function(e) NULL)
      if (!is.null(res) && nrow(res)) {
        ev <- res[res$kind != "pending" & !is.na(res$sell_d),
                  c("ticker", "sell_d", "frac", "trig_d", "stage", "kind", "fill_px")]
        if (nrow(ev)) events[[length(events) + 1]] <- ev
        m <- res[!duplicated(paste(res$ticker, res$trig_d)),
                 c("ticker", "trig_d", "p0", "level_safe", "level_good", "level_best",
                   "n_win", "has_hist", "deadline_d", "stop_detect_d")]
        m$reason <- lad$reason[match(paste(m$ticker, m$trig_d),
                                     paste(toupper(lad$ticker), format(lad$trig_d)))]
        m$no_price <- is.na(m$p0)
        meta[[length(meta) + 1]] <- m[, mcols]
      }
    }
  }
  ev_df   <- if (length(events)) do.call(rbind, events) else NULL
  meta_df <- if (length(meta))   do.call(rbind, meta)   else NULL
  if (is.null(ev_df) && is.null(meta_df)) return(NULL)
  list(events = ev_df, meta = meta_df)
}

