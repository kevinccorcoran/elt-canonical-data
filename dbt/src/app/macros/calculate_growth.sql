CREATE OR REPLACE FUNCTION calculate_growth(open NUMERIC, close NUMERIC)
RETURNS NUMERIC AS $$
BEGIN
    RETURN CASE
        WHEN open = 0 THEN NULL  -- Avoids division by zero, returning NULL
        ELSE ((close - open) / open) * 100  -- Calculates percentage growth
    END;
END;
$$ LANGUAGE plpgsql;
