-- Open issues in the EDGAR raw layer that the indicator layer must handle.
-- Read-only. Run: psql "$DATABASE_URL" -f tools/edgar_open_issues.sql
-- Doc: docs/edgar_fundamentals.md ("Open issues").

-- Every ticker <-> CIK pair any source has seen. SEC's map is current only;
-- raw.ticker_metadata (Polygon) also carries delisted tickers, whose symbols
-- are often reused later by a different company. Tickers are compared as
-- stored: Polygon marks preferreds and when-issued lines with a lowercase
-- suffix ('AAp' is an Alcoa preferred, not 'AAP'), so no case folding.
CREATE TEMP VIEW ticker_cik_pairs AS
SELECT ticker, cik, 'sec_company_tickers' AS src FROM edgar.company_tickers
UNION
SELECT t, cik, 'sec_submissions' FROM edgar.companies, unnest(tickers) AS t
UNION
SELECT ticker, cik, 'polygon_ticker_metadata' FROM raw.ticker_metadata WHERE cik IS NOT NULL;

-- 1. Tickers that map to more than one CIK.
\echo '== 1. tickers mapped to more than one CIK (count, then a sample)'
SELECT count(*) AS tickers_multi_cik
FROM (SELECT ticker FROM ticker_cik_pairs GROUP BY ticker HAVING count(DISTINCT cik) > 1) x;

SELECT p.ticker,
       array_agg(DISTINCT p.cik ORDER BY p.cik)  AS ciks,
       array_agg(DISTINCT p.src ORDER BY p.src)  AS sources
FROM ticker_cik_pairs p
GROUP BY p.ticker
HAVING count(DISTINCT p.cik) > 1
ORDER BY p.ticker
LIMIT 25;

-- 2. CIKs with facts but no ticker in any source. Split by whether the
--    company still files (filed in the last 400 days) or has gone quiet.
\echo '== 2. CIKs with facts and no ticker anywhere'
WITH no_ticker AS (
    SELECT c.cik, c.entity_type
    FROM edgar.companies c
    WHERE NOT EXISTS (SELECT 1 FROM ticker_cik_pairs p WHERE p.cik = c.cik)
),
last_filed AS (
    SELECT cik, max(filing_date) AS last_filing FROM edgar.filings GROUP BY cik
)
SELECT coalesce(n.entity_type, '?') AS entity_type,
       count(*)                                                        AS ciks,
       count(*) FILTER (WHERE l.last_filing >= current_date - 400)     AS still_filing,
       count(*) FILTER (WHERE l.last_filing <  current_date - 400
                           OR l.last_filing IS NULL)                   AS quiet
FROM no_ticker n
LEFT JOIN last_filed l USING (cik)
GROUP BY 1
ORDER BY 2 DESC;

-- 3. Companies that report in a currency other than USD. Uses Assets (in
--    both us-gaap and ifrs-full) because every filer tags it.
\echo '== 3. companies reporting Assets in a non-USD currency'
SELECT unit, taxonomy, count(DISTINCT cik) AS companies
FROM edgar.facts
WHERE tag = 'Assets' AND unit <> 'USD'
GROUP BY unit, taxonomy
ORDER BY companies DESC
LIMIT 25;

SELECT count(DISTINCT cik) AS companies_non_usd_assets
FROM edgar.facts
WHERE tag = 'Assets' AND unit <> 'USD';
