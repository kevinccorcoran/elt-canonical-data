# AlphaStream dashboard - Forecast tab SQL. Part of app.R, split 2026-10-19: files load in name order (see app.R).
# ─── Forecast tab SQL ───
# Backtest growth curve: the 8-strategy x 12-vintage $10/mo DCA backtest of the
# trust-gated top-picks basket, averaged to ONE cumulative return per horizon
# (12/18/24/30/36 mo) plus the same money's SPY leg. ~10s on prod. The (0,0)
# anchor and the ledger projection are added in R, not here.
FORECAST_CURVE_SQL <- "
WITH vintages AS (
    SELECT unnest(ARRAY[
        DATE '2013-07-01', DATE '2014-07-01', DATE '2015-07-01', DATE '2016-07-01',
        DATE '2017-07-01', DATE '2018-07-01', DATE '2019-07-01', DATE '2020-07-01',
        DATE '2021-07-01', DATE '2022-07-01', DATE '2023-07-01', DATE '2024-07-01'
    ]) AS start_d),
mkt AS (SELECT MAX(date) AS max_d FROM cdm.ingest_combined WHERE ticker='SPY'),
horizons AS (SELECT unnest(ARRAY[12,18,24,30,36]) AS h),
vcut AS (SELECT v.start_d, (SELECT MAX(t.train_cutoff_date) FROM validation.walk_forward_top_picks t WHERE t.train_cutoff_date <= v.start_d) AS c FROM vintages v),
picks AS (SELECT vc.start_d, vc.c, t.ticker FROM vcut vc JOIN validation.walk_forward_top_picks t ON t.train_cutoff_date=vc.c),
mons AS (SELECT generate_series(0,11) AS m),
spy_hist AS (SELECT date, adj_close, adj_close/lag(adj_close) OVER (ORDER BY date) - 1 AS ret,
               CASE WHEN adj_close >= AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS above200
             FROM cdm.ingest_combined WHERE ticker='SPY'
               AND date >= (SELECT MIN(start_d) FROM vintages) - INTERVAL '400 days'
               AND date <= (SELECT MAX(start_d) FROM vintages) + INTERVAL '13 months'),
spy_drops AS (SELECT start_d, date FROM (
      SELECT v.start_d, s.date, ROW_NUMBER() OVER (PARTITION BY v.start_d ORDER BY s.ret ASC) AS rn
      FROM vintages v JOIN spy_hist s ON s.ret IS NOT NULL AND s.date BETWEEN v.start_d AND (v.start_d + INTERVAL '12 months')) z WHERE rn <= 10),
month_regime AS (SELECT md.start_d, md.d, (SELECT s.above200 FROM spy_hist s WHERE s.date >= md.d ORDER BY s.date LIMIT 1) AS above
      FROM (SELECT DISTINCT v.start_d, (v.start_d + (mo.m||' months')::interval)::date AS d FROM vintages v CROSS JOIN mons mo) md),
sched AS (
    SELECT 'A) MONTHLY'::text AS s, p.start_d, p.ticker, (p.start_d + (mo.m||' months')::interval)::date AS d, 10.0 AS amt FROM picks p CROSS JOIN mons mo
    UNION ALL SELECT 'B) SPY DIP', p.start_d, p.ticker, (sd.date + 10)::date, 10.0 FROM picks p JOIN spy_drops sd ON sd.start_d=p.start_d
    UNION ALL SELECT 'C) LUMP', p.start_d, p.ticker, p.start_d, 120.0 FROM picks p
    UNION ALL SELECT 'D) FRONT 6MO', p.start_d, p.ticker, (p.start_d + (mo.m||' months')::interval)::date, 20.0 FROM picks p CROSS JOIN mons mo WHERE mo.m < 6
    UNION ALL SELECT 'E) QUARTERLY', p.start_d, p.ticker, (p.start_d + (mo.m||' months')::interval)::date, 30.0 FROM picks p CROSS JOIN mons mo WHERE mo.m % 3 = 0
    UNION ALL SELECT 'F) MON+200DMA', p.start_d, p.ticker, (p.start_d + (mo.m||' months')::interval)::date, 10.0
       FROM picks p CROSS JOIN mons mo
       WHERE EXISTS (SELECT 1 FROM month_regime mr WHERE mr.start_d=p.start_d AND mr.d=(p.start_d + (mo.m||' months')::interval)::date AND mr.above=1)),
buy_prices AS (SELECT bn.ticker, bn.d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=bn.ticker AND i.date>=bn.d ORDER BY i.date LIMIT 1) AS px FROM (SELECT DISTINCT ticker, d FROM sched) bn),
spy_buy AS (SELECT x.d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker='SPY' AND i.date>=x.d ORDER BY i.date LIMIT 1) AS spy_px FROM (SELECT DISTINCT d FROM sched) x),
priced AS (SELECT b.s, b.start_d, b.ticker, b.amt, bp.px AS buy_px, sb.spy_px FROM sched b JOIN buy_prices bp ON bp.ticker=b.ticker AND bp.d=b.d JOIN spy_buy sb ON sb.d=b.d),
acc AS (SELECT s, start_d, ticker,
      SUM(CASE WHEN buy_px>0 THEN amt/buy_px ELSE 0 END) AS sh, SUM(CASE WHEN buy_px>0 THEN amt ELSE 0 END) AS inv,
      SUM(CASE WHEN buy_px>0 AND spy_px>0 THEN amt/spy_px ELSE 0 END) AS spy_sh FROM priced GROUP BY s, start_d, ticker),
ax AS (SELECT a.*, hz.h, (a.start_d + (hz.h||' months')::interval)::date AS exit_d,
              ((a.start_d + (hz.h||' months')::interval)::date <= (SELECT max_d FROM mkt)) AS valid FROM acc a CROSS JOIN horizons hz),
final_prices AS (SELECT fn.ticker, fn.exit_d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=fn.ticker AND i.date<=fn.exit_d ORDER BY i.date DESC LIMIT 1) AS px FROM (SELECT DISTINCT ticker, exit_d FROM ax WHERE valid) fn),
spy_finals AS (SELECT y.exit_d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker='SPY' AND i.date<=y.exit_d ORDER BY i.date DESC LIMIT 1) AS px FROM (SELECT DISTINCT exit_d FROM ax WHERE valid) y),
per AS (
    SELECT ax.s, ax.start_d, ax.h,
      100*(SUM(ax.sh*fp.px)/NULLIF(SUM(ax.inv),0)-1)     AS port_ret,
      100*(SUM(ax.spy_sh*sf.px)/NULLIF(SUM(ax.inv),0)-1) AS spy_ret
    FROM ax JOIN final_prices fp ON fp.ticker=ax.ticker AND fp.exit_d=ax.exit_d
            JOIN spy_finals sf ON sf.exit_d=ax.exit_d
    WHERE ax.valid GROUP BY ax.s, ax.start_d, ax.h)
SELECT p.h AS horizon_months,
       ROUND(AVG(p.port_ret)::numeric,2)             AS all_strategies_ret_pct,
       ROUND(AVG(p.spy_ret)::numeric,2)              AS spy_ret_pct,
       ROUND(AVG(p.port_ret - p.spy_ret)::numeric,2) AS beat_pct,
       COUNT(*)                                      AS n_cells
FROM per p GROUP BY p.h ORDER BY p.h;"

# LEDGER REGIME EPOCH. monitoring.prediction_ledger rows before this date were
# produced by a BUY gate that was replaced on 2026-07-02 (elt-inference-models
# b650cb2) because it emitted ZERO BUYs and its in-sample screen was measured
# ANTI-correlated with realized IC (CORR -0.72). The record either side of that
# boundary is two different systems: BUYs run 63 -> 19 -> 0 through Jul 1, then
# 0 -> 572 overnight, while SELLs stay flat (~170-186) because only the long gate
# changed that night. Jul 2's batch was hand-run at 00:30 UTC, 54 minutes BEFORE
# the commit landed, so Jul 3 07:46 is the first scheduled run on committed code
# and is the epoch. Nothing that averages runs or grades forward performance may
# span this date. Smaller boundaries are NOT floored (they would leave 14 usable
# days): Jul 5 IC tuning settles, Jul 18 the SELL gate becomes performance-based,
# Jul 23 evidence_status/buy_weight_mature begin.
# Deliberately NOT applied to: the 30-day majority window (already clears it and
# always will) and the as-of replay (time travel: it must show what was actually
# said on the date the user picks).
LEDGER_EPOCH <- "2026-07-03"

# Live ledger basket = the earliest recorded BUY snapshot at/after the regime
# epoch (2026-07-03; pre-epoch picks came from the retired gate), equal-weighted,
# graded to the latest bar off the CURRENT series so a post-entry split can't
# re-base it (same split-safe rule the Buy List ledger replay uses).
# Expect entry_d to read EARLIER than the epoch (2026-06-23 at the time of
# writing) and the per-name entry prices to be non-uniform. That is the ledger's
# entry_date, i.e. the last price bar available when the call was logged, and
# prediction_ledger admits names whose last bar is up to 10 days old. The Jun 16
# anchor hid this because all 63 of its names happened to price at Jun 15; the
# Jul 3 anchor spreads Jun 23 - Jul 2. Pre-existing and out of scope here (Kevin:
# the price dating is by design) - but it means the basket banks a few days of
# drift before the call, so read the first days of this chart loosely.
FORECAST_LEDGER_SQL <- gsub("__EPOCH__", LEDGER_EPOCH, "
WITH first_snap AS (
    SELECT ticker, entry_date
    FROM monitoring.prediction_ledger
    WHERE prediction_date = (SELECT MIN(prediction_date)
                             FROM monitoring.prediction_ledger
                             WHERE prediction_date >= '__EPOCH__')
      AND global_action = 'BUY'
),
entry_now AS (
    SELECT ic.ticker, ic.adj_close AS entry_px
    FROM cdm.ingest_combined ic
    JOIN first_snap k ON k.ticker = ic.ticker AND k.entry_date = ic.date
),
now_px AS (
    SELECT DISTINCT ON (ticker) ticker, adj_close AS px_now
    FROM cdm.ingest_combined
    WHERE ticker IN (SELECT ticker FROM first_snap)
      AND date >= (SELECT MIN(entry_date) FROM first_snap) - INTERVAL '10 days'
    ORDER BY ticker, date DESC
),
spy AS (
    SELECT
      (SELECT adj_close FROM cdm.ingest_combined WHERE ticker='SPY'
         AND date = (SELECT MIN(entry_date) FROM first_snap)) AS spy_entry,
      (SELECT adj_close FROM cdm.ingest_combined WHERE ticker='SPY'
         ORDER BY date DESC LIMIT 1) AS spy_now
)
SELECT
  COUNT(*) AS n_names,
  ROUND((AVG((n.px_now/NULLIF(e.entry_px,0))-1)*100)::numeric, 2) AS basket_ret_pct,
  ROUND(((SELECT spy_now FROM spy)/(SELECT spy_entry FROM spy)-1)::numeric*100, 2) AS spy_ret_pct,
  (SELECT MIN(entry_date) FROM first_snap)::text AS entry_d,
  ROUND((( (SELECT MAX(date) FROM cdm.ingest_combined WHERE ticker='SPY')
          - (SELECT MIN(entry_date) FROM first_snap) )/30.0)::numeric, 2) AS months_held
FROM first_snap f
JOIN entry_now e ON e.ticker = f.ticker
JOIN now_px n ON n.ticker = f.ticker;", fixed = TRUE)

# Live all-strategy portfolio, REAL calendar monthly series. Buys the CURRENT
# picks (latest cutoff) starting at __ANCHOR__ via the 6-strategy DCA, values the
# accumulated shares at each month-end, averages the strategies, and returns the
# same money's SPY leg beside it. __ANCHOR__ is replaced (gsub) with the lookback
# start date in R. ~7s on prod. Latest cutoff (2024-12-31) is <= any anchor we
# use, so buying its picks in the past is knowledge the model already had.
FORECAST_LIVE_SQL <- "
WITH params AS (SELECT DATE '__ANCHOR__' AS start_d),
cutoffp AS (SELECT MAX(train_cutoff_date) AS cutoff FROM validation.walk_forward_top_picks, params p WHERE train_cutoff_date <= p.start_d),
maxd AS (SELECT MAX(date) AS md FROM cdm.ingest_combined WHERE ticker='SPY'),
picks AS (SELECT t.ticker FROM validation.walk_forward_top_picks t, cutoffp c WHERE t.train_cutoff_date = c.cutoff __IDFILTER__),
mons AS (SELECT generate_series(0,11) AS m),
spy_hist AS (SELECT date, adj_close, adj_close/lag(adj_close) OVER (ORDER BY date) - 1 AS ret,
               CASE WHEN adj_close >= AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS above200
             FROM cdm.ingest_combined WHERE ticker='SPY'
               AND date >= (SELECT start_d FROM params) - INTERVAL '400 days'
               AND date <= (SELECT start_d FROM params) + INTERVAL '13 months'),
spy_drops AS (SELECT date FROM (
      SELECT s.date, ROW_NUMBER() OVER (ORDER BY s.ret ASC) AS rn
      FROM spy_hist s, params p WHERE s.ret IS NOT NULL AND s.date BETWEEN p.start_d AND (p.start_d + INTERVAL '12 months')) z WHERE rn <= 10),
month_regime AS (SELECT md.d, (SELECT s.above200 FROM spy_hist s WHERE s.date >= md.d ORDER BY s.date LIMIT 1) AS above
      FROM (SELECT (p.start_d + (mo.m||' months')::interval)::date AS d FROM params p CROSS JOIN mons mo) md),
sched AS (
    SELECT 'A) MONTHLY'::text AS s, pk.ticker, (p.start_d + (mo.m||' months')::interval)::date AS d, 10.0 AS amt FROM picks pk CROSS JOIN mons mo, params p
    UNION ALL SELECT 'B) SPY DIP', pk.ticker, (sd.date + 10)::date, 10.0 FROM picks pk CROSS JOIN spy_drops sd
    UNION ALL SELECT 'C) LUMP', pk.ticker, p.start_d, 120.0 FROM picks pk, params p
    UNION ALL SELECT 'D) FRONT 6MO', pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 20.0 FROM picks pk CROSS JOIN mons mo, params p WHERE mo.m < 6
    UNION ALL SELECT 'E) QUARTERLY', pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 30.0 FROM picks pk CROSS JOIN mons mo, params p WHERE mo.m % 3 = 0
    UNION ALL SELECT 'F) MON+200DMA', pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 10.0
       FROM picks pk CROSS JOIN mons mo, params p
       WHERE EXISTS (SELECT 1 FROM month_regime mr WHERE mr.d=(p.start_d + (mo.m||' months')::interval)::date AND mr.above=1)),
buy_prices AS (SELECT bn.ticker, bn.d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=bn.ticker AND i.date>=bn.d ORDER BY i.date LIMIT 1) AS px FROM (SELECT DISTINCT ticker, d FROM sched) bn),
priced_buys AS (SELECT sc.s, sc.ticker, sc.d AS buy_d, sc.amt, sc.amt/NULLIF(bp.px,0) AS shares FROM sched sc JOIN buy_prices bp ON bp.ticker=sc.ticker AND bp.d=sc.d WHERE bp.px>0),
vdates AS (SELECT generate_series(p.start_d, LEAST((p.start_d + INTERVAL '__HOLD__ months'), (SELECT md FROM maxd)), INTERVAL '1 month')::date AS d FROM params p),
val_prices AS (SELECT k.ticker, k.vdate, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=k.ticker AND i.date<=k.vdate ORDER BY i.date DESC LIMIT 1) AS px
      FROM (SELECT DISTINCT pb.ticker, vd.d AS vdate FROM priced_buys pb CROSS JOIN vdates vd) k),
holdings AS (SELECT pb.s, pb.ticker, vd.d AS vdate, SUM(pb.shares) AS shares, SUM(pb.amt) AS invested
      FROM priced_buys pb CROSS JOIN vdates vd WHERE pb.buy_d <= vd.d GROUP BY pb.s, pb.ticker, vd.d),
valued AS (SELECT h.s, h.vdate, SUM(h.shares*vp.px) AS value, SUM(h.invested) AS invested
      FROM holdings h JOIN val_prices vp ON vp.ticker=h.ticker AND vp.vdate=h.vdate GROUP BY h.s, h.vdate),
per_strat AS (SELECT s, vdate, 100*(value/NULLIF(invested,0)-1) AS ret FROM valued),
spy_base AS (SELECT adj_close AS px0 FROM cdm.ingest_combined, params p WHERE ticker='SPY' AND date>=p.start_d ORDER BY date LIMIT 1),
spy_val AS (SELECT vd.d AS vdate, (SELECT adj_close FROM cdm.ingest_combined WHERE ticker='SPY' AND date<=vd.d ORDER BY date DESC LIMIT 1) AS px FROM vdates vd)
SELECT ps.vdate::text AS vdate,
       ROUND(AVG(ps.ret)::numeric,2) AS portfolio_pct,
       ROUND(((sv.px/(SELECT px0 FROM spy_base))-1)::numeric*100,2) AS spy_pct
FROM per_strat ps JOIN spy_val sv ON sv.vdate=ps.vdate
GROUP BY ps.vdate, sv.px ORDER BY ps.vdate;"

# Per-id decomposition: the SAME 6-strategy DCA as the portfolio line, but grouped
# by cluster id (all ids, no filter) -> one monthly return path per id. Drawn as
# faint grey dashed lines (one per id, id labelled at the right end) when EVERY id
# is selected. Same anchor/hold placeholders as the live query. ~0.7s.
FORECAST_PERID_SQL <- "
WITH params AS (SELECT DATE '__ANCHOR__' AS start_d),
cutoffp AS (SELECT MAX(train_cutoff_date) AS cutoff FROM validation.walk_forward_top_picks, params p WHERE train_cutoff_date <= p.start_d),
maxd AS (SELECT MAX(date) AS md FROM cdm.ingest_combined WHERE ticker='SPY'),
picks AS (SELECT t.ticker, t.id FROM validation.walk_forward_top_picks t, cutoffp c WHERE t.train_cutoff_date = c.cutoff),
mons AS (SELECT generate_series(0,11) AS m),
spy_hist AS (SELECT date, adj_close, adj_close/lag(adj_close) OVER (ORDER BY date) - 1 AS ret,
               CASE WHEN adj_close >= AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS above200
             FROM cdm.ingest_combined WHERE ticker='SPY'
               AND date >= (SELECT start_d FROM params) - INTERVAL '400 days'
               AND date <= (SELECT start_d FROM params) + INTERVAL '13 months'),
spy_drops AS (SELECT date FROM (
      SELECT s.date, ROW_NUMBER() OVER (ORDER BY s.ret ASC) AS rn
      FROM spy_hist s, params p WHERE s.ret IS NOT NULL AND s.date BETWEEN p.start_d AND (p.start_d + INTERVAL '12 months')) z WHERE rn <= 10),
month_regime AS (SELECT md.d, (SELECT s.above200 FROM spy_hist s WHERE s.date >= md.d ORDER BY s.date LIMIT 1) AS above
      FROM (SELECT (p.start_d + (mo.m||' months')::interval)::date AS d FROM params p CROSS JOIN mons mo) md),
sched AS (
    SELECT 'A) MONTHLY'::text AS s, pk.id, pk.ticker, (p.start_d + (mo.m||' months')::interval)::date AS d, 10.0 AS amt FROM picks pk CROSS JOIN mons mo, params p
    UNION ALL SELECT 'B) SPY DIP', pk.id, pk.ticker, (sd.date + 10)::date, 10.0 FROM picks pk CROSS JOIN spy_drops sd
    UNION ALL SELECT 'C) LUMP', pk.id, pk.ticker, p.start_d, 120.0 FROM picks pk, params p
    UNION ALL SELECT 'D) FRONT 6MO', pk.id, pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 20.0 FROM picks pk CROSS JOIN mons mo, params p WHERE mo.m < 6
    UNION ALL SELECT 'E) QUARTERLY', pk.id, pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 30.0 FROM picks pk CROSS JOIN mons mo, params p WHERE mo.m % 3 = 0
    UNION ALL SELECT 'F) MON+200DMA', pk.id, pk.ticker, (p.start_d + (mo.m||' months')::interval)::date, 10.0
       FROM picks pk CROSS JOIN mons mo, params p
       WHERE EXISTS (SELECT 1 FROM month_regime mr WHERE mr.d=(p.start_d + (mo.m||' months')::interval)::date AND mr.above=1)),
buy_prices AS (SELECT bn.ticker, bn.d, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=bn.ticker AND i.date>=bn.d ORDER BY i.date LIMIT 1) AS px FROM (SELECT DISTINCT ticker, d FROM sched) bn),
priced_buys AS (SELECT sc.s, sc.id, sc.ticker, sc.d AS buy_d, sc.amt, sc.amt/NULLIF(bp.px,0) AS shares FROM sched sc JOIN buy_prices bp ON bp.ticker=sc.ticker AND bp.d=sc.d WHERE bp.px>0),
vdates AS (SELECT generate_series(p.start_d, LEAST((p.start_d + INTERVAL '__HOLD__ months'), (SELECT md FROM maxd)), INTERVAL '1 month')::date AS d FROM params p),
val_prices AS (SELECT k.ticker, k.vdate, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=k.ticker AND i.date<=k.vdate ORDER BY i.date DESC LIMIT 1) AS px
      FROM (SELECT DISTINCT pb.ticker, vd.d AS vdate FROM priced_buys pb CROSS JOIN vdates vd) k),
holdings AS (SELECT pb.s, pb.id, pb.ticker, vd.d AS vdate, SUM(pb.shares) AS shares, SUM(pb.amt) AS invested
      FROM priced_buys pb CROSS JOIN vdates vd WHERE pb.buy_d <= vd.d GROUP BY pb.s, pb.id, pb.ticker, vd.d),
valued AS (SELECT h.s, h.id, h.vdate, SUM(h.shares*vp.px) AS value, SUM(h.invested) AS invested
      FROM holdings h JOIN val_prices vp ON vp.ticker=h.ticker AND vp.vdate=h.vdate GROUP BY h.s, h.id, h.vdate),
per_strat AS (SELECT s, id, vdate, 100*(value/NULLIF(invested,0)-1) AS ret FROM valued)
SELECT ps.vdate::text AS vdate, ps.id::int AS id, ROUND(AVG(ps.ret)::numeric,2) AS ret_pct
FROM per_strat ps GROUP BY ps.id, ps.vdate ORDER BY ps.id, ps.vdate;"

# Live ledger vs Benchmark on the ledger's OWN clock: daily cumulative return of the
# epoch BUY basket (equal-weight, split-safe current-series entry) and the same-day
# SPY, both rebased to 0 at the basket's entry date. Fast (~0.1s). This is the fair
# out-of-sample view - the ledger is weeks old and cannot share the 2-year calendar
# axis with a portfolio that has compounded since 2024. Anchored at LEDGER_EPOCH,
# not the ledger's first row: the pre-epoch snapshots were picked by the retired
# gate, so charting them would grade a system that no longer exists.
FORECAST_LEDGER_SERIES_SQL <- gsub("__EPOCH__", LEDGER_EPOCH, "
WITH first_snap AS (
    SELECT ticker, entry_date FROM monitoring.prediction_ledger
    WHERE prediction_date = (SELECT MIN(prediction_date)
                             FROM monitoring.prediction_ledger
                             WHERE prediction_date >= '__EPOCH__')
      AND global_action = 'BUY'
),
maxd AS (SELECT MAX(date) AS d FROM cdm.ingest_combined WHERE ticker = 'SPY'),
mind AS (SELECT MIN(entry_date) AS d FROM first_snap),
spy AS (
    SELECT DISTINCT ON (i.date) i.date AS d, i.adj_close AS px,
      (SELECT adj_close FROM cdm.ingest_combined WHERE ticker = 'SPY'
       AND date >= (SELECT d FROM mind) ORDER BY date LIMIT 1) AS px0
    FROM cdm.ingest_combined i
    WHERE i.ticker = 'SPY' AND i.date >= (SELECT d FROM mind) AND i.date <= (SELECT d FROM maxd)
    ORDER BY i.date, i.adj_close
),
grid AS (SELECT d FROM spy),
-- One indexed range scan of the basket tickers' prices, then gap-fill onto the
-- SPY calendar with last-observation-carried-forward (the COUNT-partition
-- FIRST_VALUE trick). Same reshape that took LC_QS_COMPARE_SQL 617ms -> 40ms;
-- the old per-(ticker,day) correlated as-of subqueries here measured 729ms vs
-- 232ms for this shape, output verified row-identical. The LOCF matters: 7
-- basket names have stopped trading (delisted/acquired) and must stay in the
-- average at their last mark -- a plain price join would silently drop them
-- from the day they go quiet (measured +0.07pp flattery by Aug 2026).
led_slice AS (
    SELECT DISTINCT ON (i.ticker, i.date) i.ticker, i.date AS d, i.adj_close AS px
    FROM cdm.ingest_combined i
    JOIN (SELECT DISTINCT ticker FROM first_snap) t ON t.ticker = i.ticker
    WHERE i.date >= (SELECT d FROM mind) AND i.date <= (SELECT d FROM maxd)
    ORDER BY i.ticker, i.date, i.adj_close
),
entry_now AS (
    SELECT DISTINCT ON (s.ticker) s.ticker, s.px AS entry_px
    FROM led_slice s JOIN first_snap k ON k.ticker = s.ticker AND k.entry_date = s.d
    ORDER BY s.ticker
),
led_fill AS (
    SELECT ticker, d, entry_px,
           FIRST_VALUE(px) OVER (PARTITION BY ticker, grp ORDER BY d) AS px
    FROM (
        SELECT en.ticker, g.d, en.entry_px, sl.px,
               COUNT(sl.px) OVER (PARTITION BY en.ticker ORDER BY g.d) AS grp
        FROM entry_now en
        CROSS JOIN grid g
        LEFT JOIN led_slice sl ON sl.ticker = en.ticker AND sl.d = g.d
    ) z
),
led AS (
    SELECT d, AVG(px / NULLIF(entry_px, 0) - 1) AS ret
    FROM led_fill WHERE entry_px > 0
    GROUP BY d
),
qs_proven AS (
    -- Board-parity gate for the qs line (frozen_gate_2026-08-13 instrument fix):
    -- top rank slot (bin 1) of a LONG cluster (eid <= 12) whose bin-1 cell wins
    -- >= 55% on >= 100 obs at fut_lag 12 -- the resolver/board's exact proven
    -- rule, evaluated at the latest cutoff. Without this the line carried 7
    -- names graded 2026-08-05 by the pre-parity-fix resolver (best-of-any-slot
    -- drift; bins 2-7) that the board rule never selected and the fixed
    -- resolver will never re-grade.
    SELECT DISTINCT w.ticker FROM (
        SELECT m.id AS eid, r.ticker,
               NTILE(20) OVER (PARTITION BY m.id
                 ORDER BY r.ticker_score DESC, r.tiebreak_score DESC NULLS LAST,
                          r.n_weighted DESC, r.ticker
               )::int AS wf_bin
        FROM validation.walk_forward_ticker_rank r
        JOIN (SELECT MAX(train_cutoff_date) AS cut
              FROM validation.walk_forward_ticker_rank WHERE fut_lag = 12) mx
          ON mx.cut = r.train_cutoff_date
        JOIN validation.walk_forward_cluster_id_map m
          ON m.train_cutoff_date = r.train_cutoff_date
         AND m.cluster_id       = r.cluster_id
        WHERE r.fut_lag = 12 AND r.ticker_score IS NOT NULL AND r.ticker_score <> 0
    ) w
    JOIN validation.walk_forward_pctile_summary ps
      ON ps.id = w.eid AND ps.fut_lag = 12 AND ps.pctile_bin = w.wf_bin
    WHERE w.wf_bin = 1 AND w.eid <= 12
      AND ROUND(ps.hit_rate::numeric * 100, 1) >= 55 AND ps.n_obs >= 100
),
qs_pick AS (
    -- The qs >= 68 PICKS universe: every BOARD-RULE ticker qualstream has scored
    -- >= 68 (non-vetoed), with the date it FIRST did so. Proven-gated so the
    -- line tests the frozen gate's graded output, not the drift-era loose
    -- universe; membership follows the CURRENT evidence vintage by design.
    SELECT s.ticker, MIN(s.as_of) AS first_pass
    FROM qual.ticker_scorecards s
    JOIN qs_proven p ON p.ticker = s.ticker
    WHERE s.rubric_version = 'buy_decision_v1' AND s.overall >= 68 AND NOT s.veto
    GROUP BY s.ticker),
qs_slice AS (
    SELECT DISTINCT ON (i.ticker, i.date) i.ticker, i.date AS d, i.adj_close AS px
    FROM cdm.ingest_combined i JOIN qs_pick qp ON qp.ticker = i.ticker
    WHERE i.date >= qp.first_pass AND i.date <= (SELECT d FROM maxd)
    ORDER BY i.ticker, i.date, i.adj_close
),
qs_fill AS (
    SELECT ticker, d, FIRST_VALUE(px) OVER (PARTITION BY ticker, grp ORDER BY d) AS px
    FROM (
        SELECT qp.ticker, g.d, sl.px,
               COUNT(sl.px) OVER (PARTITION BY qp.ticker ORDER BY g.d) AS grp
        FROM qs_pick qp
        JOIN grid g ON g.d >= qp.first_pass
        LEFT JOIN qs_slice sl ON sl.ticker = qp.ticker AND sl.d = g.d
    ) z
),
qs_entry AS (
    -- Entry = first close ON/AFTER the first-pass date: you buy when the signal
    -- fires, never before. No backdating to Jun (the flaw in the first cut, which
    -- subset the frozen basket from its Jul entry and credited pre-grade gains).
    SELECT DISTINCT ON (ticker) ticker, px AS entry_px
    FROM qs_fill WHERE px IS NOT NULL
    ORDER BY ticker, d
),
qs_grades AS (
    -- point-in-time validity interval per grade row: a grade applies from its
    -- as_of until the next grade's as_of (same-day regrades leave the superseded
    -- row a zero-length interval). Joining a day into its interval = 'the latest
    -- grade known by d', the rule the old per-cell subquery applied.
    SELECT ticker, as_of, (overall >= 68 AND NOT veto) AS passed,
           -- validity ends at the NEXT grade or 150d staleness, whichever comes
           -- first (mirrors the board's QS freshness window): a name the grader
           -- stops re-grading ages out instead of passing forever
           LEAST(COALESCE(LEAD(as_of) OVER (PARTITION BY ticker
                            ORDER BY as_of, graded_at), DATE '9999-12-31'),
                 as_of + 150) AS valid_to
    FROM qual.ticker_scorecards
    WHERE rubric_version = 'buy_decision_v1'
),
qs_px AS (
    -- Per (pick, date) from its first-pass date on: held = the latest grade known
    -- by d still passes; a veto/<68 flip prunes it that day. A re-pass re-enters
    -- at the ORIGINAL entry_px (rare; kept simple).
    SELECT f.ticker, f.d, e.entry_px, f.px, g.passed AS held
    FROM qs_fill f
    JOIN qs_entry e ON e.ticker = f.ticker
    LEFT JOIN qs_grades g ON g.ticker = f.ticker AND f.d >= g.as_of
                         AND f.d < g.valid_to
    WHERE e.entry_px > 0
),
qs_series AS (
    -- Equal-weight return of the held qs picks, each since ITS OWN grade-date entry.
    -- NULL before the first grade date, so the line starts ~Aug 5 near 0% and grows
    -- as new names pass. Mechanical artifact (intended): each new joiner enters at
    -- 0%, nudging the average down the day it joins.
    SELECT d, ROUND((AVG(px/NULLIF(entry_px,0)-1) FILTER (WHERE held) * 100)::numeric, 2) AS qs_pct
    FROM qs_px GROUP BY d)
SELECT sp.d::text AS d,
       ROUND((l.ret * 100)::numeric, 2) AS ledger_pct,
       qs.qs_pct,
       ROUND(((sp.px / sp.px0) - 1)::numeric * 100, 2) AS spy_pct
FROM spy sp
LEFT JOIN led l ON l.d = sp.d
LEFT JOIN qs_series qs ON qs.d = sp.d
ORDER BY sp.d;", fixed = TRUE)

