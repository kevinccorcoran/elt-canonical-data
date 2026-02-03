#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import warnings
import argparse
import time
from datetime import datetime, date
from typing import Optional
from urllib.parse import quote_plus, urlparse
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

import polars as pl
from polygon import RESTClient


# Single source of truth for timezone handling
AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")


# Logging + warning hygiene for ETL runtime
warnings.filterwarnings("ignore", category=FutureWarning)
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


# Normalized environment resolution
from datapipeline.config.env import ENV


# Database DSN construction with env-specific overrides
def _env_override(key_base: str, env: str, default: Optional[str] = None):
    env_key = f"{key_base}_{env.upper()}"
    return get_var(env_key, get_var(key_base, default))

def _build_dsn(env: str) -> str:
    user = _env_override("DB_USER", env, "postgres")
    password = _env_override("DB_PASSWORD", env, "")
    host = _env_override("DB_HOST", env, "postgres")
    port = _env_override("DB_PORT", env, "5432")
    name = _env_override("DB_NAME", env, "dev")
    return (
        f"postgresql+psycopg2://{user}:{quote_plus(password or '')}"
        f"@{host}:{port}/{name}"
    )

# Mask secrets for safe logging
def _safe_dsn(dsn: str) -> str:
    p = urlparse(dsn)
    return (
        f"{p.scheme}://{p.username or ''}:***@{p.hostname or ''}"
        f"{f':{p.port}' if p.port else ''}/{p.path.lstrip('/')}"
    )


# Resolve DATABASE_URL based on environment
# Resolve DATABASE_URL based on environment
DATABASE_URL = get_var("DATABASE_URL")
if not DATABASE_URL:
    if ENV == "heroku_postgres":
        pass  # Should have been caught above
    else:
        key = "DATABASE_URL_STAGING" if ENV == "staging" else "DATABASE_URL_DEV"
        DATABASE_URL = get_var(key) or _build_dsn(ENV)

# Normalize legacy postgres scheme
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace(
        "postgres://", "postgresql+psycopg2://", 1
    )

# psycopg2-compatible DSN
def get_psycopg_dsn(dsn: str) -> str:
    return dsn.replace("postgresql+psycopg2://", "postgresql://", 1)

PSYCOPG_DSN = get_psycopg_dsn(DATABASE_URL)

logging.info("ENV=%s; Using DATABASE_URL=%s", ENV, _safe_dsn(DATABASE_URL))


# Massive API client configuration
MASSIVE_API_KEY = get_var("MASSIVE_API_KEY")
if not MASSIVE_API_KEY:
    raise RuntimeError("MASSIVE_API_KEY not set")

MAX_WORKERS = int(os.getenv("MASSIVE_MAX_WORKERS", "32"))

client = RESTClient(
    MASSIVE_API_KEY,
    num_pools=max(64, MAX_WORKERS * 2),
)


# Environment-aware ingestion targets
from datapipeline.config.ingestion_targets import (
    TICKERS_SUB,
    TICKERS_FULL,
)

if ENV == "dev":
    TICKERS = list(TICKERS_SUB)
else:
    TICKERS = list(TICKERS_FULL)

logging.info(
    "Resolved tickers in ETL: ENV=%s → %s (count=%d)",
    ENV,
    TICKERS,
    len(TICKERS),
)

# Guardrail: prevent futures ingestion in dev
if ENV == "dev":
    forbidden = [t for t in TICKERS if "=F" in t]
    if forbidden:
        raise RuntimeError(
            f"DEV environment attempted futures ingestion: {forbidden}"
        )


# Project helpers
from datapipeline.config.helpers import save_to_database


# Ingestion configuration
TABLE_SCHEMA = "raw"
TABLE_NAME = "api_data_ingestion_massive"
DEFAULT_BATCH_SIZE = int(os.getenv("MASSIVE_BATCH_SIZE", "20"))
EPOCH_START = date(1970, 1, 1)


# Yahoo → Massive ticker normalization
YAHOO_TO_MASSIVE = {
    "^GSPC": "I:SPX",
    "^DJI": "I:DJI",
    "^IXIC": "I:COMP",
}

def to_massive(ticker: str) -> str:
    return YAHOO_TO_MASSIVE.get(ticker, ticker)


# Utility helpers
def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

def clamp_range(start, end):
    today = date.today()
    s = datetime.strptime(start, "%Y-%m-%d").date() if start else EPOCH_START
    e = datetime.strptime(end, "%Y-%m-%d").date() if end else today
    if s < EPOCH_START:
        s = EPOCH_START
    if e < s:
        e = s
    return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")


# Cached lookup to avoid repeated metadata calls
@lru_cache(maxsize=None)
def get_list_date(ticker: str):
    logging.info("[CACHE MISS] Getting list_date for %s", ticker)
    try:
        details = client.get_ticker_details(ticker)
        if details and details.list_date:
            return datetime.strptime(details.list_date, "%Y-%m-%d").date()
    except Exception as exc:
        logging.warning("%s: list_date fetch failed: %s", ticker, exc)
    return None


# Fetch daily bars from Massive and return as Polars DataFrame
def fetch_massive_df(ticker: str, start, end) -> pl.DataFrame:
    tkr = to_massive(ticker)
    s, e = clamp_range(start, end)
    t0 = time.perf_counter()

    processed_at = datetime.now(AMSTERDAM_TZ).replace(microsecond=0)

    try:
        aggs = client.get_aggs(
            ticker=tkr,
            multiplier=1,
            timespan="day",
            from_=s,
            to=e,
            adjusted=True,
            sort="asc",
            limit=50_000,
        )
    except Exception as exc:
        logging.error("%s fetch failed: %s", tkr, exc)
        return pl.DataFrame()

    rows = []
    for bar in aggs or []:
        d = datetime.fromtimestamp(bar.timestamp / 1000, tz=ZoneInfo("UTC")).date()
        rows.append(
            {
                "ticker_date_id": f"{tkr}_{d}",
                "ticker": tkr,
                "date": d,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": int(bar.volume),
                "adj_close": float(bar.close),
                "processed_at": processed_at,
            }
        )

    if not rows:
        logging.info("%s: No data found in range", tkr)
        return pl.DataFrame()

    df = pl.DataFrame(rows)

    # Enforce post-listing data only
    ld = get_list_date(tkr)
    if ld:
        df = df.filter(pl.col("date") >= ld)

    logging.info(
        "%s: fetched %s rows in %.3fs",
        tkr,
        df.height,
        time.perf_counter() - t0,
    )
    return df


# Batch-oriented, parallelized ETL entrypoint
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date")
    parser.add_argument("--end_date")
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    selected = [t for t in TICKERS if isinstance(t, str)]
    logging.info("Total tickers: %s", len(selected))

    # Explicit column order to match target table schema
    EXPECTED_ORDER = [
        "ticker_date_id",
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adj_close",
        "processed_at",
    ]

    total_rows = 0
    written_rows = 0

    for i, batch in enumerate(chunk_list(selected, args.batch_size), 1):
        logging.info("────────── BATCH %s (%s tickers) ──────────", i, len(batch))
        t0 = time.perf_counter()

        dfs = []
        workers = min(MAX_WORKERS, len(batch))

        # Parallel fetch per batch
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    fetch_massive_df, t, args.start_date, args.end_date
                ): t
                for t in batch
            }
            for fut in as_completed(futures):
                df = fut.result()
                if df.height > 0:
                    dfs.append(df)

        if dfs:
            combined = pl.concat(dfs, rechunk=True).select(EXPECTED_ORDER)
            total_rows += combined.height

            t_write = time.perf_counter()
            save_to_database(
                combined,
                table_name=TABLE_NAME,
                schema_name=TABLE_SCHEMA,
                connection_string=PSYCOPG_DSN,
            )
            written_rows += combined.height

            logging.info(
                "DB write completed in %.2fs",
                time.perf_counter() - t_write,
            )

        logging.info(
            "BATCH %s completed in %.2fs",
            i,
            time.perf_counter() - t0,
        )

    # Final run summary
    logging.info("━━━━━━━━━━━━━━ SUMMARY ━━━━━━━━━━━━━━")
    logging.info("Total rows fetched: %s", total_rows)
    logging.info("Total rows written*: %s", written_rows)
    logging.info("*Actual DB writes may be smaller due to dedupe.")
    logging.info("Done.")


if __name__ == "__main__":
    main()
