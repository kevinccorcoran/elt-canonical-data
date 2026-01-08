 -- sqlfluff lint --dialect postgres sqlfluffing.sql

-- sqlfluff fix --dialect postgres  sqlfluffing.sql
 
 with cte as ( 
	 SELECT 
        PROCESSED_AT
    FROM
        CDM.HISTORICAL_DAILY_MAIN_CLEAN
    WHERE
        (OPEN <= 0 OR OPEN IS NULL)
        OR (HIGH <= 0 OR HIGH IS NULL)
        OR (LOW <= 0 OR LOW IS NULL) 
	 
 
 )
select PROCESSED_AT, 
	count(*), 
	'cdm_historical_daily_main_clean_check_nulls' as test_id,
	'The test checks how many NULL and <= 0 values for OPEN, HIGH, and LOW exist per "processed_at" timestamp.'  as check,
	'' as severity,
	'' as priority
from cte
group by PROCESSED_AT, test_id
