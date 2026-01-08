-- return_likelihood_ground_zero_all_columns
-- FIXED: adds locally correct percent_of_bucket_local

WITH run_time AS (
    SELECT now() AS processed_at
),

raw_base AS (
    SELECT
        /* ─────────────────────────────
           1. Identity & structural keys
           ───────────────────────────── */
        r.id,
        r.cluster_id,

        r.fibonacci_lag_value,
        r.future_fibonacci_lag_value,

        /* ─────────────────────────────
           2. Regime / state descriptors
           ───────────────────────────── */
        r.past_excess_return_z_bucket,
        r.past_excess_return_z_bucket_num,

        r.monthly_growth_vol_z_bucket,
        r.monthly_growth_vol_z_bucket_num,

        /* ─────────────────────────────
           3. Sample size & composition
           ───────────────────────────── */
        r.ticker_count,
        r.record_count,

        r.avg_months_count,
        r.min_months_count,
        r.max_months_count,

        /* ─────────────────────────────
           4. Frequency / weighting
           ───────────────────────────── */
        r.percent_of_bucket,       -- global (kept as-is)
        r.percent_of_total,

        r.record_pct_total,
        r.record_pct_per_id,

        /* ─────────────────────────────
           5. Past distribution context
           ───────────────────────────── */
        r.past_range_width,
        r.p05_past_excess_return_vs_spy,
        r.p95_past_excess_return_vs_spy,

        /* ─────────────────────────────
           6. Future outcome distribution
           ───────────────────────────── */
        r.median_future_excess_return_vs_spy,
        r.p05_future_excess_return_vs_spy,
        r.p95_future_excess_return_vs_spy,
        r.future_range_width,

        /* ─────────────────────────────
           7. Growth dynamics (tails only)
           ───────────────────────────── */
        r.min_growth_pct_per_month,
        r.max_growth_pct_per_month

        /* ─────────────────────────────
           8. FIXED: Local percent of bucket
           ───────────────────────────── */
   

    FROM metrics.return_likelihood_matrix r
)

SELECT
    rb.*,
    rt.processed_at
FROM raw_base rb
CROSS JOIN run_time rt
WHERE (rb.fibonacci_lag_value, rb.future_fibonacci_lag_value) = (33, 54)
AND rb.id in (3)
ORDER BY
    rb.id,
    rb.fibonacci_lag_value,
    rb.future_fibonacci_lag_value,
    rb.monthly_growth_vol_z_bucket_num,
    rb.past_excess_return_z_bucket_num


--past_excess_return_z_bucket_num

--33	54
--12	33
--12	20
--4	12
--4	7
--2	7
--1	4
--1	2
