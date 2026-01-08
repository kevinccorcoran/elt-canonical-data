#!/usr/bin/env python3
import os
import sys
import gc
import io
import logging
import argparse
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal
from urllib.parse import urlparse

import psycopg2
import polars as pl

# ----------------------------------------------------------------------
# LOGGING
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ----------------------------------------------------------------------
# Airflow Variable helper (works both inside and outside Airflow)
# ----------------------------------------------------------------------
try:
    from airflow.models import Variable

    def get_var(key: str, default=None):
        try:
            return Variable.get(key)
        except Exception:
            return os.getenv(key, default)
except ModuleNotFoundError:
    def get_var(key: str, default=None):
        return os.getenv(key, default)


def _safe_dsn(dsn: str) -> str:
    """Mask password for logging."""
    try:
        p = urlparse(dsn)
        user = p.username or ""
        host = p.hostname or ""
        port = f":{p.port}" if p.port else ""
        path = p.path.lstrip("/")
        return f"{p.scheme}://{user}:***@{host}{port}/{path}"
    except Exception:
        return "***"


# ----------------------------------------------------------------------
# DB CONNECTION (env-aware: dev vs staging)
# ----------------------------------------------------------------------
# DB_DATABASE can come from Airflow Variable or env; default to dev
ENV_DB = (get_var("DB_DATABASE", default="dev") or "dev").lower()

connection_string = None

# Prefer explicit per-env URLs
if ENV_DB == "staging":
    connection_string = get_var("DATABASE_URL_STAGING")
elif ENV_DB == "dev":
    connection_string = get_var("DATABASE_URL_DEV")
else:
    # Allow custom env names like "prod" → DATABASE_URL_PROD
    connection_string = get_var(f"DATABASE_URL_{ENV_DB.upper()}")

# Fallbacks
if not connection_string:
    # Generic DATABASE_URL if set
    connection_string = get_var("DATABASE_URL")

if not connection_string:
    logging.error(
        "No connection string found. Tried DATABASE_URL_%s, DATABASE_URL, and env vars.",
        ENV_DB.upper(),
    )
    sys.exit(1)

# Convert SQLAlchemy-style URL → psycopg2 URL if needed
if connection_string.startswith("postgresql+psycopg2://"):
    connection_string = connection_string.replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
elif connection_string.startswith("postgres://"):
    # Just in case
    connection_string = connection_string.replace("postgres://", "postgresql://", 1)

logging.info("ENV_DB=%s; using connection string %s", ENV_DB, _safe_dsn(connection_string))

# ----------------------------------------------------------------------
# CONSTANTS
# ----------------------------------------------------------------------
SOURCE_SCHEMA = "raw"
SOURCE_TABLE = "api_data_ingestion_polygon"

TARGET_SCHEMA = "cdm"
TARGET_TABLE = "api_data_ingestion_massive"

# how many tickers per in-process sub-batch (for memory)
SUB_BATCH_TICKERS = 50
# rows per COPY call into temp table
INSERT_BATCH_ROWS = 400_000

COPY_COLUMN_ORDER = [
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "dividends",
    "stock_splits",
    "processed_at",
    "adj_close",
    "capital_gains",
    "date_type",
    "ticker_date_id",
    "source",
]

# ----------------------------------------------------------------------
# CLEANING
# ----------------------------------------------------------------------
def clean_row(row: tuple, colnames: list) -> dict:
    """Normalize row values coming from psycopg2 → safe for Polars."""
    out = {}
    for col, val in zip(colnames, row):
        # Normalize Decimal -> float
        if isinstance(val, Decimal):
            out[col] = float(val)
            continue

        # Normalize 'date' if DB returns timestamp
        if col == "date" and isinstance(val, datetime):
            out[col] = val.date()
            continue

        # Force raw processed_at to string (we overwrite later anyway)
        if col == "processed_at":
            if isinstance(val, datetime):
                out[col] = val.replace(microsecond=0).isoformat()
            elif val is None:
                out[col] = None
            else:
                out[col] = str(val)
            continue

        # capital_gains: keep as string for consistent Utf8 schema
        if col == "capital_gains" and val is not None and not isinstance(val, str):
            out[col] = str(val)
            continue

        out[col] = val

    return out


# ----------------------------------------------------------------------
# GENERATE SYNTHETIC DATES PER TICKER
#   - from ticker's min(date) to max(date)
#   - missing rows → synthetic
# ----------------------------------------------------------------------
def generate_full_date_range(df: pl.DataFrame) -> pl.DataFrame:
    """
    For each ticker:
        - build continuous date range [min(date), max(date)]
        - left join original rows
        - mark missing as date_type = 'synthetic', existing as 'natural'
    """
    if df.is_empty():
        return pl.DataFrame()

    results = []
    tickers = df.select("ticker").unique().to_series().to_list()

    for ticker in tickers:
        tdf = df.filter(pl.col("ticker") == ticker)

        min_date = tdf.select(pl.col("date").min()).item()
        max_date = tdf.select(pl.col("date").max()).item()

        # continuous calendar range per ticker
        full_range = pl.DataFrame(
            {
                "date": pl.date_range(
                    start=min_date,
                    end=max_date,
                    interval="1d",
                    eager=True,
                ),
                "ticker": [ticker] * ((max_date - min_date).days + 1),
            }
        )

        joined = full_range.join(tdf, on=["date", "ticker"], how="left")

        # Synthetic if missing, else natural
        joined = joined.with_columns(
            pl.when(pl.col("open").is_null())
            .then(pl.lit("synthetic"))
            .otherwise(pl.lit("natural"))
            .alias("date_type")
        )

        results.append(joined)

    return pl.concat(results)


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, required=True)
    parser.add_argument("--num_batches", type=int, required=True)
    parser.add_argument("--start_date", type=str, required=True)
    parser.add_argument("--end_date", type=str, required=True)
    args = parser.parse_args()

    batch = args.batch
    num_batches = args.num_batches
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    process_ts = datetime.now(timezone.utc).replace(microsecond=0)
    processed_at = process_ts + timedelta(hours=2)

    logging.info(
        "Run ts: %s | processed_at: %s | batch %d/%d | date range: %s .. %s",
        process_ts.isoformat(),
        processed_at.isoformat(),
        batch,
        num_batches,
        start_date.isoformat(),
        end_date.isoformat(),
    )

    try:
        with psycopg2.connect(connection_string) as conn:
            conn.autocommit = False

            # Debug: where are we connected?
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user")
                db_name, db_user = cur.fetchone()
            logging.info("Connected to DB=%s as user=%s", db_name, db_user)

            # TEMP TABLE
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TEMP TABLE temp_{TARGET_TABLE}
                    (LIKE {TARGET_SCHEMA}.{TARGET_TABLE} INCLUDING DEFAULTS)
                    """
                )
            conn.commit()
            logging.info("Created temp table temp_%s", TARGET_TABLE)

            # ------------------------------------------------------------------
            # Load ALL tickers from SOURCE
            # ------------------------------------------------------------------
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT ticker
                    FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
                    ORDER BY ticker
                    """
                )
                all_tickers = [row[0] for row in cur.fetchall()]

            total_tickers = len(all_tickers)
            if total_tickers == 0:
                logging.info("No tickers found in source. Exiting.")
                return

            logging.info("Total tickers in source: %d", total_tickers)

            # ------------------------------------------------------------------
            # Determine which tickers this batch is responsible for
            # ------------------------------------------------------------------
            if batch < 1 or batch > num_batches:
                raise ValueError(
                    f"Invalid batch {batch}, must be between 1 and {num_batches}"
                )

            base = total_tickers // num_batches
            remainder = total_tickers % num_batches

            # fair distribution across batches
            start_idx = (batch - 1) * base + min(batch - 1, remainder)
            end_idx = start_idx + base + (1 if batch <= remainder else 0)

            batch_tickers = all_tickers[start_idx:end_idx]
            logging.info(
                "Batch %d/%d handles %d tickers (indices %d..%d)",
                batch,
                num_batches,
                len(batch_tickers),
                start_idx,
                end_idx - 1 if end_idx > start_idx else start_idx,
            )

            if not batch_tickers:
                logging.info("No tickers assigned to this batch. Exiting.")
                return

            # ------------------------------------------------------------------
            # Process tickers in sub-batches for memory control
            # ------------------------------------------------------------------
            for sub_start in range(0, len(batch_tickers), SUB_BATCH_TICKERS):
                sub_tickers = batch_tickers[sub_start : sub_start + SUB_BATCH_TICKERS]
                logging.info(
                    "Sub-batch: tickers %d–%d (size=%d)",
                    sub_start + 1,
                    sub_start + len(sub_tickers),
                    len(sub_tickers),
                )

                placeholders = ",".join(["%s"] * len(sub_tickers))

                # Fetch raw rows for this sub-batch
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT *
                        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
                        WHERE ticker IN ({placeholders})
                        """,
                        sub_tickers,
                    )
                    raw_rows = cur.fetchall()
                    col_names = [desc[0] for desc in cur.description]

                if not raw_rows:
                    logging.info("No raw rows found for this sub-batch.")
                    continue

                # Clean & build Polars DataFrame with explicit schema
                cleaned = [clean_row(r, col_names) for r in raw_rows]

                df_schema = {
                    "date": pl.Date,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Int64,
                    "dividends": pl.Float64,
                    "stock_splits": pl.Float64,
                    "ticker": pl.Utf8,
                    "processed_at": pl.Utf8,
                    "adj_close": pl.Float64,
                    "capital_gains": pl.Utf8,
                    "ticker_date_id": pl.Utf8,
                }

                raw_df = pl.DataFrame(cleaned, schema=df_schema)

                # --------------------------------------------------------------
                # Expand dates → synthetic rows per ticker (min_date..max_date)
                # --------------------------------------------------------------
                full_df = generate_full_date_range(raw_df)
                if full_df.is_empty():
                    logging.info("Full DF empty after synthetic expansion.")
                    continue

                # --------------------------------------------------------------
                # Backfill synthetic rows from FUTURE natural rows
                # --------------------------------------------------------------
                cols_to_fill = [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "dividends",
                    "stock_splits",
                    "adj_close",
                    "capital_gains",
                ]

                next_vals = {
                    c: (
                        pl.when(pl.col("date_type") == "natural")
                        .then(pl.col(c))
                        .otherwise(None)
                        .backward_fill()
                        .over("ticker")
                    )
                    for c in cols_to_fill
                }

                full_df = full_df.with_columns(
                    [
                        pl.when((pl.col("date_type") == "synthetic") & pl.col(c).is_null())
                        .then(next_vals[c])
                        .otherwise(pl.col(c))
                        .alias(c)
                        for c in cols_to_fill
                    ]
                )

                # volume back to Int64 (after fill)
                full_df = full_df.with_columns(
                    pl.col("volume").cast(pl.Int64, strict=False)
                )

                # --------------------------------------------------------------
                # Metadata columns: processed_at, ticker_date_id, source
                # --------------------------------------------------------------
                full_df = full_df.with_columns(
                    [
                        # adj_close smart rounding
                        pl.when(pl.col("adj_close").is_null())
                        .then(None)
                        .when(pl.col("adj_close") < 10)
                        .then(pl.col("adj_close").round(3))
                        .otherwise(pl.col("adj_close").round(2))
                        .alias("adj_close"),
                        (pl.col("ticker") + "_" + pl.col("date").cast(pl.Utf8)).alias(
                            "ticker_date_id"
                        ),
                        pl.lit(processed_at.isoformat()).alias("processed_at"),
                        pl.lit("massive").alias("source"),
                    ]
                )

                # Ensure uniqueness within this sub-batch
                full_df = full_df.unique(subset=["ticker_date_id"])

                rows_to_insert = full_df.height
                if rows_to_insert == 0:
                    logging.info("No rows to insert for this sub-batch.")
                    continue

                logging.info(
                    "Rows to insert into temp table for this sub-batch: %d",
                    rows_to_insert,
                )

                # --------------------------------------------------------------
                # COPY into TEMP TABLE in chunks
                # --------------------------------------------------------------
                for start in range(0, full_df.height, INSERT_BATCH_ROWS):
                    batch_df = full_df[start : start + INSERT_BATCH_ROWS]
                    batch_df = batch_df.select(COPY_COLUMN_ORDER)

                    csv_buf = io.StringIO()
                    batch_df.write_csv(csv_buf)
                    csv_buf.seek(0)

                    with conn.cursor() as cur:
                        cur.copy_expert(
                            f"""
                            COPY temp_{TARGET_TABLE} (
                                date, ticker, open, high, low, close, volume,
                                dividends, stock_splits, processed_at, adj_close,
                                capital_gains, date_type, ticker_date_id, source
                            )
                            FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
                            """,
                            csv_buf,
                        )

                    conn.commit()

                gc.collect()

            # ------------------------------------------------------------------
            # After all sub-batches: move from TEMP → TARGET with ON CONFLICT
            # ------------------------------------------------------------------
            # Inspect temp contents
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*), MIN(ticker_date_id), MAX(ticker_date_id) "
                    f"FROM temp_{TARGET_TABLE}"
                )
                temp_count, min_id, max_id = cur.fetchone()

            logging.info(
                "Temp table temp_%s: count=%d, min(ticker_date_id)=%s, max(ticker_date_id)=%s",
                TARGET_TABLE,
                temp_count,
                min_id,
                max_id,
            )

            if temp_count == 0:
                logging.info("No rows in temp table; nothing to merge into target.")
                conn.commit()
                return

            # Count rows in target BEFORE insert
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
                before_count = cur.fetchone()[0]
            logging.info(
                "Target %s.%s rows BEFORE insert: %d",
                TARGET_SCHEMA,
                TARGET_TABLE,
                before_count,
            )

            logging.info(
                "Inserting from temp table into target with "
                "ON CONFLICT (ticker_date_id) DO NOTHING."
            )

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} (
                        date, ticker, open, high, low, close, volume,
                        dividends, stock_splits, processed_at, adj_close,
                        capital_gains, date_type, ticker_date_id, source
                    )
                    SELECT
                        date, ticker, open, high, low, close, volume,
                        dividends, stock_splits, processed_at, adj_close,
                        capital_gains, date_type, ticker_date_id, source
                    FROM temp_{TARGET_TABLE}
                    ON CONFLICT (ticker_date_id) DO NOTHING
                    """
                )
                inserted = cur.rowcount

            conn.commit()
            logging.info("Rows inserted into target (rowcount): %d", inserted)

            # Count rows in target AFTER insert
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
                after_count = cur.fetchone()[0]

            logging.info(
                "Target %s.%s rows AFTER insert: %d (delta = %d)",
                TARGET_SCHEMA,
                TARGET_TABLE,
                after_count,
                after_count - before_count,
            )

            logging.info("Batch %d/%d finished successfully.", batch, num_batches)

    except Exception as e:
        logging.error("Fatal error: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()



    
# import os
# import psycopg2
# import logging
# import polars as pl
# from decimal import Decimal
# from datetime import timedelta, datetime, timezone
# import io
# import sys
# import gc

# # Configure logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# # Get and sanitize connection string
# connection_string = os.getenv("DATABASE_URL")
# if not connection_string:
#     logging.error("DATABASE_URL environment variable not set.")
#     sys.exit(1)
# if connection_string.startswith("postgresql+psycopg2://"):
#     connection_string = connection_string.replace("postgresql+psycopg2://", "postgresql://", 1)

# # Capture a single process timestamp (UTC) for this run and +2h
# PROCESS_TS = datetime.now(timezone.utc).replace(microsecond=0)
# PROCESSED_AT_PLUS_2H = PROCESS_TS + timedelta(hours=2)
# logging.info(
#     "Process timestamp (UTC): %s | processed_at (+2h): %s",
#     PROCESS_TS.isoformat(), PROCESSED_AT_PLUS_2H.isoformat()
# )

# # Clean individual row values (fix mixed types coming from psycopg2)
# def clean_row(row, colnames):
#     out = {}
#     for col, val in zip(colnames, row):
#         # Normalize Decimal -> float
#         if isinstance(val, Decimal):
#             out[col] = float(val)
#             continue

#         # Normalize 'date' if database returns timestamp instead of date
#         if col == "date" and isinstance(val, datetime):
#             out[col] = val.date()
#             continue

#         # Force raw processed_at to string to avoid Polars datetime builder conflicts
#         if col == "processed_at":
#             if isinstance(val, datetime):
#                 out[col] = val.replace(microsecond=0).isoformat()
#             elif val is None:
#                 out[col] = None
#             else:
#                 out[col] = str(val)
#             continue

#         # If capital_gains is heterogeneous, make it stringy (schema expects Utf8)
#         if col == "capital_gains" and val is not None and not isinstance(val, str):
#             out[col] = str(val)
#             continue

#         out[col] = val
#     return out

# # Generate full date range per ticker
# def generate_full_date_range(df: pl.DataFrame) -> pl.DataFrame:
#     if df.is_empty():
#         return pl.DataFrame()
#     full_data = []
#     tickers = df.select("ticker").unique().to_series().to_list()
#     for ticker in tickers:
#         ticker_df = df.filter(pl.col("ticker") == ticker)
#         min_date = ticker_df.select(pl.col("date").min())[0, 0]
#         max_date = ticker_df.select(pl.col("date").max())[0, 0]
#         days = (max_date - min_date).days + 1
#         full_range = pl.DataFrame(
#             {
#                 "date": [min_date + timedelta(days=i) for i in range(days)],
#                 "ticker": [ticker] * days,
#             }
#         )
#         joined = full_range.join(ticker_df, on=["date", "ticker"], how="left")
#         joined = joined.with_columns(
#             pl.when(pl.col("open").is_null())
#             .then(pl.lit("synthetic"))
#             .otherwise(pl.lit("natural"))
#             .alias("date_type")
#         )
#         full_data.append(joined)
#     return pl.concat(full_data) if full_data else pl.DataFrame()

# # Main script
# try:
#     with psycopg2.connect(connection_string) as conn:
#         schema_name = "raw"
#         table_name = "api_data_ingestion_polygon"
#         target_schema = "cdm"
#         target_table = "api_data_ingestion_massive"
#         batch_size = 50
#         insert_batch_size = 400_000

#         # Get all distinct tickers
#         with conn.cursor() as cursor:
#             cursor.execute(f"""
#                 SELECT DISTINCT ticker
#                 FROM {schema_name}.{table_name}
#                 ORDER BY ticker
#             """)
#             tickers = [row[0] for row in cursor.fetchall()]
#             logging.info("Total tickers: %d", len(tickers))

#         # Load all existing ticker_date_ids once
#         with conn.cursor() as cursor:
#             cursor.execute(f"""
#                 SELECT ticker_date_id
#                 FROM {target_schema}.{target_table}
#             """)
#             existing_ids = {row[0] for row in cursor.fetchall()}

#         # Process in batches
#         for i in range(0, len(tickers), batch_size):
#             ticker_batch = tickers[i:i + batch_size]
#             logging.info("Processing batch %d–%d: %s", i + 1, i + len(ticker_batch), ticker_batch)

#             # Fetch raw data
#             placeholders = ",".join(["%s"] * len(ticker_batch))
#             with conn.cursor() as cursor:
#                 cursor.execute(
#                     f"""
#                     SELECT * FROM {schema_name}.{table_name}
#                     WHERE ticker IN ({placeholders})
#                     """,
#                     ticker_batch,
#                 )
#                 raw_data = cursor.fetchall()
#                 colnames = [desc[0] for desc in cursor.description]

#             if not raw_data:
#                 logging.info("No data found for this batch.")
#                 continue

#             # Clean and to Polars (avoid dtype conflicts by normalizing first)
#             cleaned = [clean_row(row, colnames) for row in raw_data]

#             # Build DataFrame with explicit schema
#             schema = {
#                 "date": pl.Date,
#                 "open": pl.Float64,
#                 "high": pl.Float64,
#                 "low": pl.Float64,
#                 "close": pl.Float64,
#                 "volume": pl.Int64,
#                 "dividends": pl.Float64,
#                 "stock_splits": pl.Float64,
#                 "ticker": pl.Utf8,
#                 "processed_at": pl.Utf8,  # ISO 8601 string for COPY → timestamptz
#                 "adj_close": pl.Float64,
#                 "capital_gains": pl.Utf8,
#                 "ticker_date_id": pl.Utf8,
#             }
#             pl_df = pl.DataFrame(cleaned, schema=schema)

#             # Fill missing dates and label
#             full_df = generate_full_date_range(pl_df)
#             if full_df.is_empty():
#                 logging.info("No data after date expansion for this batch.")
#                 del pl_df
#                 gc.collect()
#                 continue

#             # Apply +2h processed_at, format adj_close, and add identifiers
#             # full_df = full_df.with_columns(
#             #     [
#             #         pl.when(pl.col("adj_close").is_null())
#             #         .then(None)
#             #         .when(pl.col("adj_close") < 10)
#             #         .then(pl.col("adj_close"))                 # keep as-is when < 10
#             #         .otherwise(pl.col("adj_close").round(0))   # >= 10 → round to 0 decimals
#             #         .alias("adj_close"),
#             #         (pl.col("ticker") + "_" + pl.col("date").cast(pl.Utf8)).alias("ticker_date_id"),
#             #         pl.lit(PROCESSED_AT_PLUS_2H.isoformat()).alias("processed_at"),  # +2 hours from run start (UTC)
#             #     ]
#             # )

#             full_df = full_df.with_columns(
#                 [
#                     # SMART ROUNDING: 3 decimals if < 10, 2 decimals if ≥ 10
#                     pl.when(pl.col("adj_close").is_null())
#                     .then(None)
#                     .when(pl.col("adj_close") < 10)
#                     .then(pl.col("adj_close").round(3))
#                     .otherwise(pl.col("adj_close").round(2))
#                     .alias("adj_close"),

#                     # ticker_date_id and processed_at
#                     (pl.col("ticker") + "_" + pl.col("date").cast(pl.Utf8)).alias("ticker_date_id"),
#                     pl.lit(PROCESSED_AT_PLUS_2H.isoformat()).alias("processed_at"),
#                 ]
#             )

#             # Ensure chronological order
#             full_df = full_df.sort(["ticker", "date"])

#             # Backward fill ONLY synthetic rows from the next *natural* row per ticker
#             cols_to_bfill = [
#                 "open",
#                 "high",
#                 "low",
#                 "close",
#                 "volume",
#                 "dividends",
#                 "stock_splits",
#                 "adj_close",
#                 "capital_gains",
#             ]
#             next_vals = {
#                 c: (
#                     pl.when(pl.col("date_type") == "natural")
#                     .then(pl.col(c))
#                     .otherwise(None)
#                     .backward_fill()
#                     .over("ticker")
#                 )
#                 for c in cols_to_bfill
#             }
#             full_df = full_df.with_columns(
#                 [
#                     pl.when((pl.col("date_type") == "synthetic") & pl.col(c).is_null())
#                     .then(next_vals[c])
#                     .otherwise(pl.col(c))
#                     .alias(c)
#                     for c in cols_to_bfill
#                 ]
#             )

#             # (Optional) cast back volume to Int64 if desired after fill
#             full_df = full_df.with_columns(pl.col("volume").cast(pl.Int64, strict=False))

#             # Final cleanup + deduplication
#             full_df = (
#                 full_df.select(
#                     [
#                         "date",
#                         "ticker",
#                         "open",
#                         "high",
#                         "low",
#                         "close",
#                         "volume",
#                         "dividends",
#                         "stock_splits",
#                         "processed_at",
#                         "adj_close",
#                         "capital_gains",
#                         "date_type",
#                         "ticker_date_id",
#                     ]
#                 )
#                 .unique(subset=["ticker_date_id"])
#             )

#             # Filter out existing IDs
#             full_df = full_df.filter(~pl.col("ticker_date_id").is_in(existing_ids))
#             if full_df.is_empty():
#                 logging.info("No new rows to insert for batch.")
#                 del pl_df, full_df
#                 gc.collect()
#                 continue

#             logging.info("New rows to insert: %d", full_df.shape[0])

#             # Insert in chunks
#             for start in range(0, full_df.shape[0], insert_batch_size):
#                 end = min(start + insert_batch_size, full_df.shape[0])
#                 insert_batch = full_df[start:end]

#                 csv_buffer = io.StringIO()
#                 insert_batch.write_csv(csv_buffer)
#                 csv_buffer.seek(0)

#                 try:
#                     with conn.cursor() as cursor:
#                         cursor.copy_expert(
#                             f"""COPY {target_schema}.{target_table} (
#                                 "date", ticker, "open", high, low, "close", volume,
#                                 dividends, "stock_splits", processed_at, adj_close,
#                                 "capital_gains", date_type, ticker_date_id
#                             ) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)""",
#                             csv_buffer,
#                         )
#                     conn.commit()
#                     logging.info("Inserted %d rows.", insert_batch.shape[0])
#                 except Exception as e:
#                     logging.error("Failed to insert rows %d–%d: %s", start, end, e)

#             # Clean up memory
#             del pl_df, full_df
#             gc.collect()

# except psycopg2.Error as db_err:
#     logging.error("Database error: %s", db_err)
# except Exception as e:
#     logging.error("Unexpected error: %s", e)
# work with tynthtics
# #!/usr/bin/env python3
# import argparse
# import os
# import io
# import gc
# from datetime import datetime, timezone, timedelta
# from decimal import Decimal
# import psycopg2
# import polars as pl
# import logging

# # ---------------------------------------------------------
# # CONFIG
# # ---------------------------------------------------------
# logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# SOURCE_SCHEMA = "raw"
# SOURCE_TABLE = "api_data_ingestion_polygon"
# TARGET_SCHEMA = "cdm"
# TARGET_TABLE = "api_data_ingestion_massive"
# FETCH_SIZE = 50000
# INSERT_BATCH_SIZE = 600000

# # ---------------------------------------------------------
# # ARGS
# # ---------------------------------------------------------
# def parse_args():
#     p = argparse.ArgumentParser()
#     p.add_argument("--batch", type=int, required=True)
#     p.add_argument("--num_batches", type=int, required=True)
#     p.add_argument("--start_date", type=str, default=None, help="Ignored – min date per ticker used")
#     p.add_argument("--end_date", type=str, required=True)
#     return p.parse_args()

# # ---------------------------------------------------------
# # CLEAN ROW
# # ---------------------------------------------------------
# def clean_row(row, colnames):
#     out = {}
#     for col, val in zip(colnames, row):
#         if isinstance(val, Decimal):
#             out[col] = float(val)
#             continue
#         if col == "date" and isinstance(val, datetime):
#             out[col] = val.date()
#             continue
#         if col == "processed_at":
#             out[col] = val.replace(microsecond=0).isoformat() if isinstance(val, datetime) else (None if val is None else str(val))
#             continue
#         if col == "capital_gains" and val is not None and not isinstance(val, str):
#             out[col] = str(val)
#             continue
#         out[col] = val
#     return out

# # ---------------------------------------------------------
# # INSERT CHUNK
# # ---------------------------------------------------------
# def insert_chunk(conn, df, processed_at, date_type="natural"):
#     if df.is_empty():
#         return

#     required = ["date","ticker","open","high","low","close","volume","dividends","stock_splits","processed_at","adj_close","capital_gains"]
#     for col in required:
#         if col not in df.columns:
#             df = df.with_columns(pl.lit(None).alias(col))

#     df = df.with_columns([
#         pl.col("date").cast(pl.Date),
#         pl.col("ticker").cast(pl.Utf8),
#         pl.col("open").cast(pl.Float64),
#         pl.col("high").cast(pl.Float64),
#         pl.col("low").cast(pl.Float64),
#         pl.col("close").cast(pl.Float64),
#         pl.col("volume").cast(pl.Float64),
#         pl.col("dividends").cast(pl.Float64),
#         pl.col("stock_splits").cast(pl.Float64),
#         pl.col("adj_close").cast(pl.Float64),
#         pl.col("capital_gains").cast(pl.Utf8),
#         (pl.col("ticker") + "_" + pl.col("date").cast(pl.Utf8)).alias("ticker_date_id"),
#         pl.lit(processed_at.isoformat()).alias("processed_at"),
#         pl.lit(date_type).alias("date_type"),
#         pl.lit("massive").alias("source"),
#     ]).select([
#         "date","ticker","open","high","low","close","volume",
#         "dividends","stock_splits","processed_at","adj_close",
#         "capital_gains","date_type","ticker_date_id","source"
#     ])

#     buf = io.StringIO()
#     df.write_csv(buf)
#     buf.seek(0)

#     with conn.cursor() as cur:
#         cur.copy_expert(f"""
#             COPY {TARGET_SCHEMA}.{TARGET_TABLE} (
#                 date, ticker, open, high, low, close, volume,
#                 dividends, stock_splits, processed_at, adj_close,
#                 capital_gains, date_type, ticker_date_id, source
#             )
#             FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
#         """, buf)
#     conn.commit()

# # ---------------------------------------------------------
# # GET MIN DATES PER TICKER
# # ---------------------------------------------------------
# def get_min_dates_per_ticker(conn, batch_tickers):
#     if not batch_tickers:
#         return {}
#     placeholders = ",".join(["%s"] * len(batch_tickers))
#     sql = f"SELECT ticker, MIN(date) FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} WHERE ticker IN ({placeholders}) GROUP BY ticker"
#     with conn.cursor() as cur:
#         cur.execute(sql, batch_tickers)
#         return {r[0]: r[1] for r in cur.fetchall()}

# # ---------------------------------------------------------
# # GENERATE CALENDAR GAPS (synthetic rows)
# # ---------------------------------------------------------
# def generate_calendar_gaps(conn, tickers, start_date, end_date):
#     if not tickers:
#         return pl.DataFrame()

#     start_str = start_date.isoformat() if hasattr(start_date, 'isoformat') else str(start_date)
#     end_str = str(end_date)
#     placeholders = ",".join(["%s"] * len(tickers))
#     sql = f"""
#     WITH RECURSIVE cal AS (
#         SELECT %s::date AS dt UNION ALL SELECT dt + 1 FROM cal WHERE dt < %s::date
#     ),
#     ticker_cal AS (
#         SELECT t.ticker, c.dt AS date
#         FROM (SELECT unnest(ARRAY[{','.join(['%s']*len(tickers))}]) AS ticker) t
#         CROSS JOIN cal c
#     ),
#     existing AS (
#         SELECT ticker, date FROM {TARGET_SCHEMA}.{TARGET_TABLE}
#         WHERE ticker = ANY(%s) AND date BETWEEN %s AND %s
#     )
#     SELECT tc.ticker, tc.date
#     FROM ticker_cal tc
#     LEFT JOIN existing e ON tc.ticker = e.ticker AND tc.date = e.date
#     WHERE e.ticker IS NULL
#     ORDER BY tc.ticker, tc.date;
#     """
#     params = [start_str, end_str, *tickers, tickers, start_str, end_str]
#     with conn.cursor() as cur:
#         cur.execute(sql, params)
#         rows = cur.fetchall()
#     if not rows:
#         return pl.DataFrame()

#     df = pl.DataFrame(rows, schema=["ticker", "date"], orient="row")
#     return df.with_columns([
#         pl.lit(None).cast(pl.Float64).alias(c) for c in
#         ["open","high","low","close","volume","dividends","stock_splits","adj_close"]
#     ]).with_columns([
#         pl.lit(None).cast(pl.Utf8).alias("capital_gains"),
#         pl.lit("synthetic").alias("date_type"),
#         (pl.col("ticker") + "_" + pl.col("date").cast(pl.Utf8)).alias("ticker_date_id"),
#         pl.lit("massive").alias("source"),
#     ])

# # ---------------------------------------------------------
# # FORWARD FILL IN POLARS (NO TYPE ERRORS)
# # ---------------------------------------------------------
# def forward_fill_in_memory(conn, batch_tickers, processed_at):
#     placeholders = ",".join(["%s"] * len(batch_tickers))
#     sql = f"""
#     SELECT date, ticker, open, high, low, close, volume, dividends, stock_splits, adj_close
#     FROM {TARGET_SCHEMA}.{TARGET_TABLE}
#     WHERE ticker IN ({placeholders})
#       AND date_type = 'synthetic'
#       AND (open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
#            OR volume IS NULL OR dividends IS NULL OR stock_splits IS NULL OR adj_close IS NULL)
#     ORDER BY ticker, date
#     """
#     with conn.cursor() as cur:
#         cur.execute(sql, batch_tickers)
#         cols = [desc[0] for desc in cur.description]
#         rows = cur.fetchall()
#     if not rows:
#         logging.info("No NULLs to forward-fill in synthetic rows.")
#         return

#     df = pl.DataFrame(rows, schema=cols, orient="row")
#     fill_cols = ["open", "high", "low", "close", "volume", "dividends", "stock_splits", "adj_close"]
#     df = df.sort(["ticker", "date"]).with_columns([
#         pl.col(col).forward_fill().over("ticker") for col in fill_cols
#     ])

#     # Build VALUES: pass processed_at as datetime
#     values = []
#     params = []
#     for row in df.iter_rows(named=True):
#         values.append("(" + ",".join(["%s"] * 11) + ")")
#         params.extend([
#             row["date"],
#             row["ticker"],
#             row["open"] if row["open"] is not None else None,
#             row["high"] if row["high"] is not None else None,
#             row["low"] if row["low"] is not None else None,
#             row["close"] if row["close"] is not None else None,
#             row["volume"] if row["volume"] is not None else None,
#             row["dividends"] if row["dividends"] is not None else None,
#             row["stock_splits"] if row["stock_splits"] is not None else None,
#             row["adj_close"] if row["adj_close"] is not None else None,
#             processed_at  # datetime object
#         ])

#     if not values:
#         return

#     placeholders = ",".join(values)
#     update_sql = f"""
#     UPDATE {TARGET_SCHEMA}.{TARGET_TABLE} t
#     SET
#         open = v.open::double precision,
#         high = v.high::double precision,
#         low = v.low::double precision,
#         close = v.close::double precision,
#         volume = v.volume::double precision,
#         dividends = v.dividends::double precision,
#         stock_splits = v.stock_splits::double precision,
#         adj_close = v.adj_close::double precision,
#         processed_at = v.processed_at::timestamp
#     FROM (VALUES {placeholders}) AS v(
#         date, ticker, open, high, low, close, volume,
#         dividends, stock_splits, adj_close, processed_at
#     )
#     WHERE t.ticker = v.ticker
#       AND t.date = v.date
#       AND t.date_type = 'synthetic'
#     """

#     try:
#         with conn.cursor() as cur:
#             cur.execute(update_sql, params)
#             updated = cur.rowcount
#             logging.info(f"Forward-filled {updated} NULL cells (UPDATE FROM VALUES).")
#         conn.commit()
#     except Exception as e:
#         logging.error(f"Forward fill failed: {e}")
#         conn.rollback()
#         raise

# # ---------------------------------------------------------
# # MAIN
# # ---------------------------------------------------------
# def main():
#     args = parse_args()
#     logging.info(f"=== START BATCH {args.batch}/{args.num_batches} ===")

#     conn_str = os.getenv("DATABASE_URL")
#     if not conn_str:
#         raise ValueError("DATABASE_URL not set")
#     if conn_str.startswith("postgresql+psycopg2://"):
#         conn_str = conn_str.replace("postgresql+psycopg2://", "postgresql://", 1)

#     process_ts = datetime.now(timezone.utc).replace(microsecond=0)
#     processed_at = process_ts + timedelta(hours=2)

#     with psycopg2.connect(conn_str) as conn:
#         conn.autocommit = False
#         with conn.cursor() as cur:
#             cur.execute("SET synchronous_commit = off;")
#             cur.execute("SET work_mem = '256MB';")

#         # 1. Get tickers
#         with conn.cursor() as cur:
#             cur.execute(f"SELECT DISTINCT ticker FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} ORDER BY ticker")
#             all_tickers = [r[0] for r in cur.fetchall()]
#         batch_tickers = [t for i, t in enumerate(all_tickers) if (i % args.num_batches) + 1 == args.batch]
#         if not batch_tickers:
#             logging.info("No tickers.")
#             return

#         # 2. Insert missing real rows
#         placeholders = ",".join(["%s"] * len(batch_tickers))
#         sql = f"""
#             SELECT t.* FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} t
#             LEFT JOIN {TARGET_SCHEMA}.{TARGET_TABLE} c ON t.ticker = c.ticker AND t.date = c.date
#             WHERE t.ticker IN ({placeholders}) AND c.ticker IS NULL
#             ORDER BY t.ticker, t.date
#         """
#         cur = conn.cursor()
#         cur.execute(sql, batch_tickers)
#         buffer = []
#         first = cur.fetchmany(FETCH_SIZE)
#         if first:
#             colnames = [d[0] for d in cur.description]
#             for row in first:
#                 buffer.append(clean_row(row, colnames))
#             while True:
#                 rows = cur.fetchmany(FETCH_SIZE)
#                 if not rows: break
#                 for row in rows:
#                     buffer.append(clean_row(row, colnames))
#                 if len(buffer) >= INSERT_BATCH_SIZE:
#                     insert_chunk(conn, pl.DataFrame(buffer), processed_at, date_type="natural")
#                     buffer.clear()
#                     gc.collect()
#             if buffer:
#                 insert_chunk(conn, pl.DataFrame(buffer), processed_at, date_type="natural")

#         # 3. Insert synthetic gaps
#         logging.info("Generating synthetic rows…")
#         min_dates = get_min_dates_per_ticker(conn, batch_tickers)
#         if min_dates:
#             synth_frames = []
#             for ticker, min_date in min_dates.items():
#                 df = generate_calendar_gaps(conn, [ticker], min_date, args.end_date)
#                 if not df.is_empty():
#                     synth_frames.append(df)
#             if synth_frames:
#                 synth_df = pl.concat(synth_frames)
#                 synth_df = synth_df.with_columns(pl.lit(processed_at.isoformat()).alias("processed_at"))
#                 insert_chunk(conn, synth_df, processed_at, date_type="synthetic")
#                 logging.info(f"Inserted {len(synth_df)} synthetic rows.")

#         # 4. Forward fill
#         logging.info("Forward-filling NULLs in memory…")
#         forward_fill_in_memory(conn, batch_tickers, processed_at)

#     logging.info("=== FINISHED BATCH ===")

# if __name__ == "__main__":
#     main()

# import argparse
# import os
# import sys
# import io
# import gc
# from datetime import datetime, timezone, timedelta
# from decimal import Decimal

# import psycopg2
# import polars as pl
# import logging


# # ---------------------------------------------------------
# # CONFIG
# # ---------------------------------------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s",
# )

# SOURCE_SCHEMA = "raw"
# SOURCE_TABLE = "api_data_ingestion_polygon"

# TARGET_SCHEMA = "cdm"
# TARGET_TABLE = "api_data_ingestion_massive"

# FETCH_SIZE = 50000         # rows pulled from DB at a time
# INSERT_BATCH_SIZE = 600000  # rows inserted via COPY at a time


# # ---------------------------------------------------------
# # ARGS
# # ---------------------------------------------------------
# def parse_args():
#     p = argparse.ArgumentParser()
#     p.add_argument("--batch", type=int, required=True)
#     p.add_argument("--num_batches", type=int, required=True)
#     p.add_argument("--start_date", type=str, default="1950-01-01")
#     p.add_argument("--end_date", type=str, required=True)
#     return p.parse_args()


# # ---------------------------------------------------------
# # CLEAN ROW
# # ---------------------------------------------------------
# def clean_row(row, colnames):
#     out = {}

#     for col, val in zip(colnames, row):

#         if isinstance(val, Decimal):
#             out[col] = float(val)
#             continue

#         if col == "date" and isinstance(val, datetime):
#             out[col] = val.date()
#             continue

#         if col == "processed_at":
#             if isinstance(val, datetime):
#                 out[col] = val.replace(microsecond=0).isoformat()
#             elif val is None:
#                 out[col] = None
#             else:
#                 out[col] = str(val)
#             continue

#         if col == "capital_gains" and val is not None and not isinstance(val, str):
#             out[col] = str(val)
#             continue

#         out[col] = val

#     return out


# # ---------------------------------------------------------
# # INSERT CHUNK
# # ---------------------------------------------------------
# def insert_chunk(conn, rows, processed_at):
#     if not rows:
#         return

#     df = pl.DataFrame(rows)

#     required = [
#         "date","ticker","open","high","low","close","volume",
#         "dividends","stock_splits","processed_at","adj_close","capital_gains"
#     ]
#     for col in required:
#         if col not in df.columns:
#             df = df.with_columns(pl.lit(None).alias(col))

#     df = df.with_columns([
#         pl.col("date").cast(pl.Date),
#         pl.col("ticker").cast(pl.Utf8),
#         pl.col("open").cast(pl.Float64),
#         pl.col("high").cast(pl.Float64),
#         pl.col("low").cast(pl.Float64),
#         pl.col("close").cast(pl.Float64),
#         pl.col("volume").cast(pl.Float64),
#         pl.col("dividends").cast(pl.Float64),
#         pl.col("stock_splits").cast(pl.Float64),
#         pl.col("adj_close").cast(pl.Float64),
#         pl.col("capital_gains").cast(pl.Utf8),
#     ])

#     df = df.with_columns([
#         (pl.col("ticker") + "_" + pl.col("date").cast(pl.Utf8)).alias("ticker_date_id"),
#         pl.lit(processed_at.isoformat()).alias("processed_at"),
#         pl.lit("natural").alias("date_type"),
#         pl.lit("massive").alias("source"),
#     ])

#     df = df.select([
#         "date","ticker","open","high","low","close","volume",
#         "dividends","stock_splits","processed_at","adj_close",
#         "capital_gains","date_type","ticker_date_id","source"
#     ])

#     buf = io.StringIO()
#     df.write_csv(buf)
#     buf.seek(0)

#     with conn.cursor() as cur:
#         cur.copy_expert(
#             f"""
#             COPY {TARGET_SCHEMA}.{TARGET_TABLE} (
#                 date, ticker, open, high, low, close, volume,
#                 dividends, stock_splits, processed_at, adj_close,
#                 capital_gains, date_type, ticker_date_id, source
#             )
#             FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
#             """,
#             buf,
#         )

#     conn.commit()


# # ---------------------------------------------------------
# # MAIN
# # ---------------------------------------------------------
# def main():
#     args = parse_args()

#     logging.info(f"=== START BATCH {args.batch}/{args.num_batches} ===")

#     conn_str = os.getenv("DATABASE_URL")
#     if conn_str.startswith("postgresql+psycopg2://"):
#         conn_str = conn_str.replace("postgresql+psycopg2://", "postgresql://")

#     process_ts = datetime.now(timezone.utc).replace(microsecond=0)
#     processed_at = process_ts + timedelta(hours=2)

#     with psycopg2.connect(conn_str) as conn:
#         conn.autocommit = False

#         with conn.cursor() as cur:
#             cur.execute("SET synchronous_commit = off;")
#             cur.execute("SET work_mem = '128MB';")

#         with conn.cursor() as cur:
#             cur.execute(
#                 f"SELECT DISTINCT ticker FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} ORDER BY ticker"
#             )
#             all_tickers = [r[0] for r in cur.fetchall()]

#         total = len(all_tickers)
#         batch_tickers = [
#             t for i, t in enumerate(all_tickers)
#             if (i % args.num_batches) + 1 == args.batch
#         ]

#         if not batch_tickers:
#             logging.info("No tickers for this batch.")
#             return

#         placeholders = ",".join(["%s"] * len(batch_tickers))

#         sql = f"""
#             SELECT t.*
#             FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} t
#             LEFT JOIN {TARGET_SCHEMA}.{TARGET_TABLE} c
#               ON t.ticker = c.ticker AND t.date = c.date
#             WHERE t.ticker IN ({placeholders})
#               AND c.ticker IS NULL
#             ORDER BY t.ticker, t.date
#         """

#         cur = conn.cursor()
#         cur.execute(sql, batch_tickers)

#         buffer = []
#         first = cur.fetchmany(FETCH_SIZE)
#         if not first:
#             logging.info("Nothing missing.")
#             return

#         colnames = [d[0] for d in cur.description]

#         for row in first:
#             buffer.append(clean_row(row, colnames))

#         while True:
#             rows = cur.fetchmany(FETCH_SIZE)
#             if not rows:
#                 break

#             for row in rows:
#                 buffer.append(clean_row(row, colnames))

#             if len(buffer) >= INSERT_BATCH_SIZE:
#                 insert_chunk(conn, buffer, processed_at)
#                 buffer.clear()
#                 gc.collect()

#         if buffer:
#             insert_chunk(conn, buffer, processed_at)
#             buffer.clear()

#         logging.info("=== FINISHED BATCH ===")


# if __name__ == "__main__":
#     main()
