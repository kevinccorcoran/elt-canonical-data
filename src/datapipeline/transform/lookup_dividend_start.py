# polars works with Fibonacci from every dividend date
import os
import sys
import logging
import psycopg2
import polars as pl
import io
from datetime import datetime, timedelta
from typing import Union

# ---------------------------------------------------------------------------
# Configure Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Retrieve DATABASE_URL from environment variables
# ---------------------------------------------------------------------------
connection_string = os.getenv("DATABASE_URL")
if not connection_string:
    logging.error("DATABASE_URL environment variable not set.")
    sys.exit(1)

if connection_string.startswith("postgresql+psycopg2://"):
    connection_string = connection_string.replace("postgresql+psycopg2://", "postgresql://", 1)

# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------
def get_next_trading_day(date: Union[datetime, datetime.date]) -> datetime.date:
    if isinstance(date, datetime):
        date = date.date()
    while date.weekday() >= 5:  # Skip Saturday and Sunday
        date += timedelta(days=1)
    return date

def cumulative_fibonacci(n: int) -> set:
    fib_set = set()
    a, b = 0, 1
    while a <= n:
        fib_set.add(a)
        a, b = b, a + b
    return fib_set

def process_stock_data(div_dates_df: pl.DataFrame, months_to_add=36) -> pl.DataFrame:
    all_dfs = [div_dates_df]

    for month in range(1, months_to_add + 1):
        shifted = div_dates_df.with_columns([
            pl.col("date").map_elements(
                lambda d: get_next_trading_day(
                    (datetime(d.year, d.month, 1) + timedelta(days=32 * month)).replace(day=1)
                ),
                return_dtype=pl.Date
            )
        ])
        all_dfs.append(shifted)

    result_df = pl.concat(all_dfs).drop_nulls()

    result_df = result_df.sort(["ticker", "group_number", "date"]).with_columns(
        pl.arange(0, pl.len()).over(["ticker", "group_number"]).alias("row_number")
    )

    return result_df

# ---------------------------------------------------------------------------
# Main ETL Logic
# ---------------------------------------------------------------------------
source_schema = "cdm"
source_table = "api_data_ingestion"
target_schema = "cdm"
target_table = "lookup_dividend_start"

try:
    with psycopg2.connect(connection_string) as conn:
        # Step 0a: Pull dividend data
        dividend_query = f"""
            SELECT ticker, CAST(date AS DATE) AS date
            FROM {source_schema}.{source_table}
            WHERE dividends IS NOT NULL AND dividends <> 0
        """
        logging.info("Fetching dividend date data ...")
        df_div = pl.read_database(dividend_query, connection=conn)
        logging.info("Fetched %d dividend rows", df_div.height)

        if df_div.is_empty():
            logging.info("No dividend data found. Exiting.")
            sys.exit(0)

        # Step 0b: Pull all adj_close
        adj_close_query = f"""
            SELECT ticker, CAST(date AS DATE) AS date, adj_close
            FROM {source_schema}.{source_table}
            WHERE adj_close IS NOT NULL
        """
        logging.info("Fetching adj_close values for all dates ...")
        df_adj = pl.read_database(adj_close_query, connection=conn)
        logging.info("Fetched %d adj_close rows", df_adj.height)

        # Step 1: Assign group_number for each dividend date per ticker
        df_div = df_div.sort(["ticker", "date"]).with_columns(
            pl.arange(0, pl.len()).over("ticker").alias("group_number")
        )

        # Step 2: Generate Fibonacci offset dates
        result_df = process_stock_data(df_div, months_to_add=1700)

        max_row_number = result_df["row_number"].max()
        fib_sequence = cumulative_fibonacci(int(max_row_number))
        matching_rows = result_df.filter(pl.col("row_number").is_in(fib_sequence))
        matching_rows = matching_rows.sort(["ticker", "date", "group_number", "row_number"])

        if matching_rows.is_empty():
            logging.info("No Fibonacci-matching rows found. Exiting.")
            sys.exit(0)

        # Step 2b: Merge in adj_close
        df_adj = df_adj.with_columns([
            (pl.col("ticker") + "_" + pl.col("date").cast(pl.Utf8)).alias("ticker_date_id")
        ])
        matching_rows = matching_rows.with_columns([
            (pl.col("ticker") + "_" + pl.col("date").cast(pl.Utf8)).alias("ticker_date_id")
        ])
        matching_rows = matching_rows.unique(subset=["ticker_date_id", "ticker", "date", "group_number", "row_number"])

        merged = matching_rows.join(
            df_adj.select(["ticker_date_id", "adj_close"]),
            on="ticker_date_id",
            how="left"
        )

        # Step 3: Filter out existing ticker_date_ids
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT ticker_date_id FROM {target_schema}.{target_table}
                WHERE ticker_date_id = ANY(%s)
            """, (merged["ticker_date_id"].unique().to_list(),))
            existing_ids = {row[0] for row in cursor.fetchall()}

        new_rows = merged.filter(~pl.col("ticker_date_id").is_in(existing_ids))

        if new_rows.is_empty():
            logging.info("All ticker_date_ids already exist in target table. Skipping insert.")
            sys.exit(0)

        logging.info("Before deduplication, inserting %d new rows into %s.%s", new_rows.height, target_schema, target_table)

        # Optional: Detect duplicates before deduplication
        dupes = (
            new_rows.group_by("ticker_date_id")
            .count()
            .filter(pl.col("count") > 1)
        )
        if dupes.height > 0:
            logging.warning("Found %d duplicate ticker_date_ids in new_rows", dupes.height)
            logging.info(dupes)

        # Step 3b: Remove duplicate ticker_date_ids before insert
        new_rows = new_rows.unique(subset=["ticker_date_id"])

        # Step 4: Ensure correct column order and date format
        new_rows = new_rows.select([
            "ticker_date_id",
            "ticker",
            pl.col("date").cast(pl.Utf8),
            "group_number",
            "row_number",
            "adj_close"
        ])

        logging.info("Preview of new_rows to be inserted:")
        logging.info(new_rows.head(5))

        csv_buffer = io.StringIO()
        new_rows.write_csv(csv_buffer, include_header=True)
        csv_buffer.seek(0)

        copy_sql = f"""
            COPY {target_schema}.{target_table}
            (ticker_date_id, ticker, date, group_number, row_number, adj_close)
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
        """
        with conn.cursor() as cursor:
            cursor.copy_expert(copy_sql, csv_buffer)
        conn.commit()
        logging.info("Insert complete.")

except psycopg2.Error as db_err:
    logging.error("Database error: %s", db_err)
    sys.exit(1)
except Exception as e:
    logging.error("Unexpected error: %s", e)
    sys.exit(1)
