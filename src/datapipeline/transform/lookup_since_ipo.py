import os
import sys
import logging
import psycopg2
import pandas as pd
from pandas.tseries.offsets import DateOffset
from datetime import datetime
from decimal import Decimal
import io

# ---------------------------------------------------------------------------
# Configure Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------------------------------------------------------------------------
# Retrieve DATABASE_URL (or DB_CONNECTION_STRING) from environment variables
# ---------------------------------------------------------------------------
connection_string = os.getenv('DATABASE_URL')
if not connection_string:
    logging.error("DATABASE_URL environment variable not set.")
    sys.exit(1)

# Adjust the connection string for psycopg2 compatibility if needed
if connection_string.startswith("postgresql+psycopg2://"):
    connection_string = connection_string.replace("postgresql+psycopg2://", "postgresql://", 1)

# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------
def get_next_trading_day(date):
    return pd.bdate_range(date, periods=1)[0].date()

def cumulative_fibonacci(n):
    fib_set = set()
    a, b = 0, 1
    while a <= n:
        fib_set.add(a)
        a, b = b, a + b
    return fib_set

def process_stock_data(df, years_to_add=3, months_to_add=36):
    df['date'] = pd.to_datetime(df['date']).dt.date
    min_dates = df.groupby('ticker')['date'].min().reset_index()
    all_dfs = []

    for year in range(0, years_to_add + 1):
        min_dates_shifted = min_dates.copy()
        min_dates_shifted['date'] = min_dates_shifted['date'] + DateOffset(years=year)
        min_dates_shifted['date'] = min_dates_shifted['date'].apply(get_next_trading_day)
        min_dates_shifted['date'] = pd.to_datetime(min_dates_shifted['date']).dt.date
        shifted_df = pd.merge(min_dates_shifted, df, on=['ticker', 'date'], how='left')[['ticker', 'date']]
        shifted_df['offset_type'] = 'year'
        all_dfs.append(shifted_df)

    for month in range(1, months_to_add + 1):
        min_dates_shifted = min_dates.copy()
        min_dates_shifted['date'] = min_dates_shifted['date'] + DateOffset(months=month)
        min_dates_shifted['date'] = min_dates_shifted['date'].apply(get_next_trading_day)
        min_dates_shifted['date'] = pd.to_datetime(min_dates_shifted['date']).dt.date
        shifted_df = pd.merge(min_dates_shifted, df, on=['ticker', 'date'], how='left')[['ticker', 'date']]
        shifted_df['offset_type'] = 'month'
        all_dfs.append(shifted_df)

    result_df = pd.concat(all_dfs, ignore_index=True)
    result_df.dropna(inplace=True)

    result_df['row_number'] = (
        result_df.sort_values(['ticker', 'offset_type', 'date'])
                 .groupby(['ticker', 'offset_type'])
                 .cumcount()
    )

    return result_df

# ---------------------------------------------------------------------------
# Main ETL Logic
# ---------------------------------------------------------------------------
try:
    with psycopg2.connect(connection_string) as conn:
        schema_name = 'raw'
        table_name = 'api_data_ingestion'
        target_schema = 'cdm'
        target_table = 'lookup_since_ipo'

        query = f"SELECT * FROM {schema_name}.{table_name}"
        logging.info("Fetching data from %s.%s ...", schema_name, table_name)

        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]

        if not rows:
            logging.warning("No rows returned from the database.")
            sys.exit(0)

        df = pd.DataFrame(rows, columns=colnames)
        logging.info("Data fetched. Shape of DataFrame: %s", df.shape)

        unique_tickers = df['ticker'].unique()
        ticker_batches = [unique_tickers[i:i + 10] for i in range(0, len(unique_tickers), 10)]
        logging.info("Number of batches: %s", len(ticker_batches))

        for batch_num, ticker_batch in enumerate(ticker_batches, start=1):
            logging.info("Processing batch %d with %d tickers...", batch_num, len(ticker_batch))
            batch_df = df[df['ticker'].isin(ticker_batch)]
            result_df = process_stock_data(batch_df, years_to_add=120, months_to_add=1700)

            fib_sequence = cumulative_fibonacci(result_df['row_number'].max())
            matching_rows = result_df[result_df['row_number'].isin(fib_sequence)].copy()
            matching_rows.sort_values(by=['ticker', 'offset_type', 'date', 'row_number'], inplace=True)

            if matching_rows.empty:
                logging.info("Batch %d has no matching rows after Fibonacci filtering.", batch_num)
                continue

            matching_rows['ticker_date_id'] = matching_rows['ticker'] + '_' + matching_rows['date'].astype(str)
            matching_rows = matching_rows[['ticker_date_id', 'ticker', 'date', 'offset_type', 'row_number']]
            matching_rows.drop_duplicates(inplace=True)

            # Step 5: Filter out existing ticker_date_id
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT ticker_date_id FROM {target_schema}.{target_table}
                    WHERE ticker_date_id = ANY(%s)
                """, (list(matching_rows['ticker_date_id'].unique()),))
                existing_ids = {row[0] for row in cursor.fetchall()}

            new_rows = matching_rows[~matching_rows['ticker_date_id'].isin(existing_ids)]

            if new_rows.empty:
                logging.info("Batch %d: all rows already exist. Skipping insert.", batch_num)
                continue

            logging.info("Batch %d: inserting %d new rows.", batch_num, new_rows.shape[0])

            csv_buffer = io.StringIO()
            new_rows.to_csv(csv_buffer, index=False, header=True)
            csv_buffer.seek(0)

            columns_to_copy = ['ticker_date_id', 'ticker', 'date', 'offset_type', 'row_number']
            copy_sql = f"""
                COPY {target_schema}.{target_table} ({', '.join(columns_to_copy)})
                FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
            """

            with conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, csv_buffer)
            conn.commit()

            logging.info("Batch %d saved successfully.", batch_num)

        logging.info("All batches processed and saved successfully.")

except psycopg2.Error as db_err:
    logging.error("Database error: %s", db_err)
    sys.exit(1)
except Exception as e:
    logging.error("Unexpected error: %s", e)
    sys.exit(1)
