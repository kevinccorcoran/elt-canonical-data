import os
import psycopg2
import logging
import polars as pl
from decimal import Decimal
from datetime import timedelta, datetime, timezone
import io
import sys
import gc


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


connection_string = os.getenv("DATABASE_URL")
if not connection_string:
    logging.error("DATABASE_URL environment variable not set.")
    sys.exit(1)
if connection_string.startswith("postgresql+psycopg2://"):
    connection_string = connection_string.replace("postgresql+psycopg2://", "postgresql://", 1)

# Run-level timestamps
PROCESS_TS = datetime.now(timezone.utc).replace(microsecond=0)
PROCESSED_AT_PLUS_2H = PROCESS_TS + timedelta(hours=2)
logging.info(
    "Process timestamp (UTC): %s | processed_at (+2h): %s",
    PROCESS_TS.isoformat(), PROCESSED_AT_PLUS_2H.isoformat()
)

# Normalize psycopg2 row to a Polars-safe dict
def clean_row(row, colnames):
    out = {}
    for col, val in zip(colnames, row):
        if isinstance(val, Decimal):
            out[col] = float(val)
            continue
        if col == "date" and isinstance(val, datetime):
            out[col] = val.date()
            continue
        if col == "processed_at":
            if isinstance(val, datetime):
                out[col] = val.replace(microsecond=0).isoformat()
            elif val is None:
                out[col] = None
            else:
                out[col] = str(val)
            continue
        if col == "capital_gains" and val is not None and not isinstance(val, str):
            out[col] = str(val)
            continue
        out[col] = val
    return out

# Expand each ticker into a full date range, labelling gaps as synthetic
def generate_full_date_range(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame()
    full_data = []
    tickers = df.select("ticker").unique().to_series().to_list()
    for ticker in tickers:
        ticker_df = df.filter(pl.col("ticker") == ticker)
        min_date = ticker_df.select(pl.col("date").min())[0, 0]
        max_date = ticker_df.select(pl.col("date").max())[0, 0]
        days = (max_date - min_date).days + 1
        full_range = pl.DataFrame(
            {
                "date": [min_date + timedelta(days=i) for i in range(days)],
                "ticker": [ticker] * days,
            }
        )
        joined = full_range.join(ticker_df, on=["date", "ticker"], how="left")
        joined = joined.with_columns(
            pl.when(pl.col("open").is_null())
            .then(pl.lit("synthetic"))
            .otherwise(pl.lit("natural"))
            .alias("date_type")
        )
        full_data.append(joined)
    return pl.concat(full_data) if full_data else pl.DataFrame()

# ETL: backfill calendar gaps and insert into CDM
try:
    with psycopg2.connect(connection_string) as conn:
        schema_name = "raw"
        table_name = "api_data_ingestion_yfinance"
        target_schema = "cdm"
        target_table = "api_data_ingestion_yfinance"
        batch_size = 50
        insert_batch_size = 400_000

        # Load distinct tickers from source
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT DISTINCT ticker
                FROM {schema_name}.{table_name}
                ORDER BY ticker
            """)
            tickers = [row[0] for row in cursor.fetchall()]
            logging.info("Total tickers found: %d", len(tickers))

        # Override: restrict to predefined ticker universe
        tickers = [t for t in tickers if t in TICKERS_FULL]
        logging.info("Filtered tickers (override): %s", tickers)

        # Load existing IDs to avoid duplicates
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT ticker_date_id
                FROM {target_schema}.{target_table}
            """)
            existing_ids = {row[0] for row in cursor.fetchall()}
            logging.info("Existing IDs in target table: %d", len(existing_ids))

        # Process in batches
        for i in range(0, len(tickers), batch_size):
            ticker_batch = tickers[i:i + batch_size]
            logging.info("Processing batch %d–%d: %s", i + 1, i + len(ticker_batch), ticker_batch)

            # Fetch raw rows
            placeholders = ",".join(["%s"] * len(ticker_batch))
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT * FROM {schema_name}.{table_name}
                    WHERE ticker IN ({placeholders})
                    """,
                    ticker_batch,
                )
                raw_data = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]

            if not raw_data:
                logging.info("No data found for this batch.")
                continue

            # Clean and build DataFrame
            cleaned = [clean_row(row, colnames) for row in raw_data]
            pl_df = pl.DataFrame(cleaned, schema={
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
            })

            # Fill missing dates with synthetic rows
            full_df = generate_full_date_range(pl_df)
            if full_df.is_empty():
                del pl_df
                gc.collect()
                continue

            # Add metadata and identifiers
            full_df = full_df.with_columns(
                [
                    pl.when(pl.col("adj_close").is_null())
                    .then(None)
                    .when(pl.col("adj_close") < 10)
                    .then(pl.col("adj_close").round(3))
                    .otherwise(pl.col("adj_close").round(2))
                    .alias("adj_close"),
                    (pl.col("ticker") + "_" + pl.col("date").cast(pl.Utf8)).alias("ticker_date_id"),
                    pl.lit(PROCESSED_AT_PLUS_2H.isoformat()).alias("processed_at"),
                    pl.lit("yfinance").alias("source"),
                ]
            ).sort(["ticker", "date"])

            # Backfill synthetic rows
            cols_to_bfill = [
                "open", "high", "low", "close", "volume",
                "dividends", "stock_splits", "adj_close", "capital_gains",
            ]
            next_vals = {
                c: (
                    pl.when(pl.col("date_type") == "natural")
                    .then(pl.col(c))
                    .otherwise(None)
                    .backward_fill()
                    .over("ticker")
                )
                for c in cols_to_bfill
            }
            full_df = full_df.with_columns(
                [
                    pl.when((pl.col("date_type") == "synthetic") & pl.col(c).is_null())
                    .then(next_vals[c])
                    .otherwise(pl.col(c))
                    .alias(c)
                    for c in cols_to_bfill
                ]
            ).with_columns(pl.col("volume").cast(pl.Int64, strict=False))

            # Deduplicate and exclude existing rows
            full_df = (
                full_df.select(
                    [
                        "date", "ticker", "open", "high", "low", "close", "volume",
                        "dividends", "stock_splits", "processed_at", "adj_close",
                        "capital_gains", "date_type", "ticker_date_id", "source"
                    ]
                )
                .unique(subset=["ticker_date_id"])
                .filter(~pl.col("ticker_date_id").is_in(existing_ids))
            )

            if full_df.is_empty():
                del pl_df, full_df
                gc.collect()
                continue

            # Bulk insert via COPY
            for start in range(0, full_df.shape[0], insert_batch_size):
                insert_batch = full_df[start:start + insert_batch_size]
                csv_buffer = io.StringIO()
                insert_batch.write_csv(csv_buffer)
                csv_buffer.seek(0)

                with conn.cursor() as cursor:
                    cursor.copy_expert(
                        f"""COPY {target_schema}.{target_table} (
                            "date", ticker, "open", high, low, "close", volume,
                            dividends, "stock_splits", processed_at, adj_close,
                            "capital_gains", date_type, ticker_date_id, source
                        ) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)""",
                        csv_buffer,
                    )
                conn.commit()

            del pl_df, full_df
            gc.collect()

except psycopg2.Error as db_err:
    logging.error("Database error: %s", db_err)
except Exception as e:
    logging.error("Unexpected error: %s", e)
