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


# Logging for batch ETL visibility
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Unified Airflow Variable / env-var accessor
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


# Mask credentials for safe connection logging
def _safe_dsn(dsn: str) -> str:
    try:
        p = urlparse(dsn)
        user = p.username or ""
        host = p.hostname or ""
        port = f":{p.port}" if p.port else ""
        path = p.path.lstrip("/")
        return f"{p.scheme}://{user}:***@{host}{port}/{path}"
    except Exception:
        return "***"


# Resolve DB connection string based on environment
ENV_DB = (get_var("DB_DATABASE", default="dev") or "dev").lower()

if ENV_DB == "staging":
    connection_string = get_var("DATABASE_URL_STAGING")
elif ENV_DB == "dev":
    connection_string = get_var("DATABASE_URL_DEV")
else:
    connection_string = get_var(f"DATABASE_URL_{ENV_DB.upper()}")

if not connection_string:
    connection_string = get_var("DATABASE_URL")

if not connection_string:
    logging.error("No connection string found for ENV_DB=%s", ENV_DB)
    sys.exit(1)

# Normalize DSN for psycopg2
if connection_string.startswith("postgresql+psycopg2://"):
    connection_string = connection_string.replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
elif connection_string.startswith("postgres://"):
    connection_string = connection_string.replace("postgres://", "postgresql://", 1)

logging.info("ENV_DB=%s; using connection string %s", ENV_DB, _safe_dsn(connection_string))


# ETL constants: source → target (raw → cdm)
SOURCE_SCHEMA = "raw"
SOURCE_TABLE = "api_data_ingestion_massive"
TARGET_SCHEMA = "cdm"
TARGET_TABLE = "api_data_ingestion_massive"

# Batch sizes tuned for memory + COPY throughput
SUB_BATCH_TICKERS = 50
INSERT_BATCH_ROWS = 400_000

# Explicit column order for COPY consistency
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


# Normalize psycopg2 row values into a stable Polars schema
def clean_row(row: tuple, colnames: list) -> dict:
    out = {}
    for col, val in zip(colnames, row):
        if isinstance(val, Decimal):
            out[col] = float(val)
            continue

        if col == "date" and isinstance(val, datetime):
            out[col] = val.date()
            continue

        # Keep processed_at stringy to avoid dtype conflicts; overwritten later anyway
        if col == "processed_at":
            if isinstance(val, datetime):
                out[col] = val.replace(microsecond=0).isoformat()
            elif val is None:
                out[col] = None
            else:
                out[col] = str(val)
            continue

        # Keep capital_gains as Utf8 to match target schema and avoid mixed types
        if col == "capital_gains" and val is not None and not isinstance(val, str):
            out[col] = str(val)
            continue

        out[col] = val

    return out


# Expand each ticker into a continuous calendar and label gaps as synthetic
def generate_full_date_range(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame()

    results = []
    tickers = df.select("ticker").unique().to_series().to_list()

    for ticker in tickers:
        tdf = df.filter(pl.col("ticker") == ticker)

        min_date = tdf.select(pl.col("date").min()).item()
        max_date = tdf.select(pl.col("date").max()).item()

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

        joined = joined.with_columns(
            pl.when(pl.col("open").is_null())
            .then(pl.lit("synthetic"))
            .otherwise(pl.lit("natural"))
            .alias("date_type")
        )

        results.append(joined)

    return pl.concat(results)


# Batch ETL entrypoint: assign tickers → build synthetic rows → COPY to temp → merge to target
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

    # Single run timestamp used for consistent processed_at across all inserted rows
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

            # Sanity log: confirm database/user context
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user")
                db_name, db_user = cur.fetchone()
            logging.info("Connected to DB=%s as user=%s", db_name, db_user)

            # Temp staging table for this run (enables COPY + single merge step)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TEMP TABLE temp_{TARGET_TABLE}
                    (LIKE {TARGET_SCHEMA}.{TARGET_TABLE} INCLUDING DEFAULTS)
                    """
                )
            conn.commit()
            logging.info("Created temp table temp_%s", TARGET_TABLE)

            # Load all tickers and assign the slice this batch is responsible for
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

            if batch < 1 or batch > num_batches:
                raise ValueError(
                    f"Invalid batch {batch}, must be between 1 and {num_batches}"
                )

            base = total_tickers // num_batches
            remainder = total_tickers % num_batches

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

            # Process tickers in sub-batches to cap memory usage
            for sub_start in range(0, len(batch_tickers), SUB_BATCH_TICKERS):
                sub_tickers = batch_tickers[sub_start : sub_start + SUB_BATCH_TICKERS]
                logging.info(
                    "Sub-batch: tickers %d–%d (size=%d)",
                    sub_start + 1,
                    sub_start + len(sub_tickers),
                    len(sub_tickers),
                )

                placeholders = ",".join(["%s"] * len(sub_tickers))

                # Pull raw rows for this sub-batch
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

                cleaned = [clean_row(r, col_names) for r in raw_rows]

                # Explicit schema avoids dtype inference surprises during joins/fills
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

                # Expand to continuous calendar and label missing days as synthetic
                full_df = generate_full_date_range(raw_df)
                if full_df.is_empty():
                    logging.info("Full DF empty after synthetic expansion.")
                    continue

                # Backfill synthetic rows from the next available natural observation per ticker
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

                # Recast after fill to restore expected numeric types
                full_df = full_df.with_columns(
                    pl.col("volume").cast(pl.Int64, strict=False)
                )

                # Add metadata + stable identifiers for merge/dedupe
                full_df = full_df.with_columns(
                    [
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

                full_df = full_df.unique(subset=["ticker_date_id"])
                if full_df.height == 0:
                    logging.info("No rows to insert for this sub-batch.")
                    continue

                # COPY into temp table in row chunks to avoid oversized buffers
                for start in range(0, full_df.height, INSERT_BATCH_ROWS):
                    batch_df = full_df[start : start + INSERT_BATCH_ROWS].select(COPY_COLUMN_ORDER)

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

            # Merge temp → target with conflict-safe insert
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

            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
                before_count = cur.fetchone()[0]

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

            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")
                after_count = cur.fetchone()[0]

            logging.info(
                "Target %s.%s rows: before=%d after=%d delta=%d (rowcount=%d)",
                TARGET_SCHEMA,
                TARGET_TABLE,
                before_count,
                after_count,
                after_count - before_count,
                inserted,
            )

            logging.info("Batch %d/%d finished successfully.", batch, num_batches)

    except Exception as e:
        logging.error("Fatal error: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
