# AlphaStream dashboard - Forecast risk SQL. Part of app.R, split 2026-10-19: files load in name order (see app.R).
# Risk-adjustment series: the EQUAL-WEIGHT buy-and-hold basket value (=$1 -> avg
# of price/entry over the picks) and SPY value, monthly, over the hold window.
# Equal-weight-hold (not DCA) so month-over-month ratios are pure time-weighted
# returns with no cash-flow distortion -> clean beta/alpha/vol regression in R.
FORECAST_RISK_SQL <- "
WITH params AS (SELECT DATE '__ANCHOR__' AS start_d),
cutoffp AS (SELECT MAX(train_cutoff_date) AS cutoff FROM validation.walk_forward_top_picks, params p WHERE train_cutoff_date <= p.start_d),
maxd AS (SELECT MAX(date) AS md FROM cdm.ingest_combined WHERE ticker='SPY'),
enddate AS (SELECT LEAST((SELECT start_d FROM params) + INTERVAL '__HOLD__ months', (SELECT md FROM maxd)) AS ed),
picks AS (SELECT t.ticker FROM validation.walk_forward_top_picks t, cutoffp c WHERE t.train_cutoff_date = c.cutoff __IDFILTER__),
entry AS (SELECT pk.ticker, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=pk.ticker AND i.date>=(SELECT start_d FROM params) ORDER BY i.date LIMIT 1) AS e_px FROM picks pk),
vdates AS (SELECT generate_series((SELECT start_d FROM params), (SELECT ed FROM enddate), INTERVAL '1 month')::date AS d),
bv AS (SELECT v.d, e.ticker, e.e_px, (SELECT i.adj_close FROM cdm.ingest_combined i WHERE i.ticker=e.ticker AND i.date<=v.d ORDER BY i.date DESC LIMIT 1) AS px FROM vdates v CROSS JOIN entry e WHERE e.e_px>0),
spy_e AS (SELECT adj_close AS px0 FROM cdm.ingest_combined, params p WHERE ticker='SPY' AND date>=p.start_d ORDER BY date LIMIT 1)
SELECT v.d::text AS d,
   AVG(bv.px/NULLIF(bv.e_px,0))::double precision AS basket_val,
   ((SELECT adj_close FROM cdm.ingest_combined WHERE ticker='SPY' AND date<=v.d ORDER BY date DESC LIMIT 1)/(SELECT px0 FROM spy_e))::double precision AS spy_val
FROM vdates v JOIN bv ON bv.d=v.d GROUP BY v.d ORDER BY v.d;"

