#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import logging
import warnings
import argparse
import time
from datetime import datetime, date
from typing import Optional
from urllib.parse import quote_plus, urlparse
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
from polygon import RESTClient

# ───────────────────────────── Logging / warnings ─────────────────────────────
warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ───────────────────────── Airflow Variables helper ──────────────────────────
try:
    from airflow.models import Variable

    def get_var(key: str, default_var=None):
        try:
            return Variable.get(key)
        except Exception:
            return os.getenv(key, default_var)
except ModuleNotFoundError:
    def get_var(key: str, default_var=None):
        return os.getenv(key, default_var)

# ─────────────────────── Environment & DB URL resolution ──────────────────────
ENV = (get_var("DB_DATABASE", default_var=os.getenv("DB_DATABASE", "dev")) or "dev").lower()

def _env_override(key_base: str, env: str, default: Optional[str] = None):
    env_key = f"{key_base}_{env.upper()}"
    return get_var(env_key, default_var=get_var(key_base, default_var=default))

def _build_dsn(env: str) -> str:
    user = _env_override("DB_USER", env, default="postgres")
    password = _env_override("DB_PASSWORD", env, default="")
    host = _env_override("DB_HOST", env, default="localhost")
    port = _env_override("DB_PORT", env, default="5432")
    name = _env_override("DB_NAME", env, default="dev")
    return f"postgresql+psycopg2://{user}:{quote_plus(password or '')}@{host}:{port}/{name}"

def _safe_dsn(dsn: str) -> str:
    p = urlparse(dsn)
    return f"{p.scheme}://{p.username or ''}:***@{p.hostname or ''}{f':{p.port}' if p.port else ''}/{p.path.lstrip('/')}"

if ENV == "heroku_postgres":
    DATABASE_URL = get_var("DATABASE_URL")
else:
    key = "DATABASE_URL_STAGING" if ENV == "staging" else "DATABASE_URL_DEV"
    DATABASE_URL = get_var(key, default_var=None) or _build_dsn(ENV)

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)

# ←←← FIX: Convert SQLAlchemy → pure libpq DSN for psycopg2
def get_psycopg_dsn(dsn: str) -> str:
    """Convert SQLAlchemy-style DSN to one that psycopg2 accepts."""
    return dsn.replace("postgresql+psycopg2://", "postgresql://", 1)

PSYCOPG_DSN = get_psycopg_dsn(DATABASE_URL)

logging.info("ENV=%s; Using DATABASE_URL=%s", ENV, _safe_dsn(DATABASE_URL))

# ───────────────────────── Polygon API Key / client ─────────────────────────
POLYGON_API_KEY = get_var("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY not set")

# Increase connection pool size to match our concurrency (correct param: num_pools)
MAX_WORKERS = int(os.getenv("POLYGON_MAX_WORKERS", "32"))
client = RESTClient(POLYGON_API_KEY, num_pools=MAX_WORKERS + 8)

# ───────────────────────── Project imports ─────────────────────────
from datapipeline.config.helpers import save_to_database
from datapipeline.config.ingestion_targets import TICKERS, TICKERS_FULL

# ───────────────────────────── Config constants ─────────────────────────────
TABLE_SCHEMA = "raw"
TABLE_NAME = "api_data_ingestion_polygon"
DEFAULT_BATCH_SIZE = int(os.getenv("POLYGON_BATCH_SIZE", "20"))
EPOCH_START = date(1970, 1, 1)

YAHOO_TO_POLYGON = {"^GSPC": "I:SPX", "^DJI": "I:DJI", "^IXIC": "I:COMP"}
def to_polygon(t): return YAHOO_TO_POLYGON.get(t, t)

# ───────────────────────────── Helpers ─────────────────────────────
def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def clamp_range(s, e):
    today = date.today()
    start = datetime.strptime(s, "%Y-%m-%d").date() if s else EPOCH_START
    end = datetime.strptime(e, "%Y-%m-%d").date() if e else today
    if start < EPOCH_START:
        start = EPOCH_START
    if end < start:
        end = start
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

@lru_cache(maxsize=None)
def get_list_date(tkr):
    logging.info(f"[CACHE MISS] Getting list_date for {tkr}")
    try:
        details = client.get_ticker_details(tkr)
        if details and details.list_date:
            return datetime.strptime(details.list_date, "%Y-%m-%d").date()
    except Exception as e:
        logging.warning(f"{tkr}: list_date fetch failed: {e}")
    return None

# ───────────────────────────── Fetch polygon df ─────────────────────────────
def fetch_polygon_df(ticker, start, end, full=True):
    if not isinstance(ticker, str):
        logging.warning(f"Invalid ticker {ticker}")
        return pl.DataFrame()

    tkr = to_polygon(ticker)
    s, e = clamp_range(start, end)

    t0 = time.perf_counter()

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
        logging.error(f"[ERROR] {tkr} fetch failed: {exc}")
        return pl.DataFrame()

    rows = []
    for bar in aggs or []:
        d = datetime.utcfromtimestamp(bar.timestamp / 1000).date()
        rows.append({
            "date": d,
            "open": float(bar.open),
            "high": float(bar.high),
            "low": float(bar.low),
            "close": float(bar.close),
            "adj_close": float(bar.close),
            "volume": int(bar.volume or 0),
            "dividends": None,
            "stock_splits": None,
            "capital_gains": None,
            "ticker": tkr,
            "ticker_date_id": f"{tkr}_{d}",
            "processed_at": datetime.utcnow(),
        })

    df = pl.DataFrame(rows)

    ld = get_list_date(tkr)
    if ld:
        df = df.filter(pl.col("date") >= ld)

    logging.info(f"{tkr}: fetched {df.height} rows in {time.perf_counter() - t0:.3f}s")
    return df

# ───────────────────────────── Main ─────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date")
    parser.add_argument("--end_date")
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    selected = TICKERS if ENV == "dev" else TICKERS_FULL
    selected = [t for t in selected if isinstance(t, str)]

    logging.info(f"Total tickers: {len(selected)}")

    total_rows = 0
    written_rows = 0

    EXPECTED_ORDER = [
        "ticker_date_id",
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "dividends",
        "stock_splits",
        "capital_gains",
        "processed_at",
    ]

    for i, batch in enumerate(chunk_list(selected, args.batch_size), 1):
        logging.info(f"────────── BATCH {i} ({len(batch)} tickers) ──────────")
        t0 = time.perf_counter()

        dfs = []
        workers = min(MAX_WORKERS, len(batch))

        with ThreadPoolExecutor(max_workers=workers) as ex:
            tasks = {ex.submit(fetch_polygon_df, t, args.start_date, args.end_date, True): t for t in batch}
            for fut in as_completed(tasks):
                df = fut.result()
                if df.height > 0:
                    dfs.append(df)

        if dfs:
            combined = pl.concat(dfs, rechunk=True)
            combined = combined.select(EXPECTED_ORDER)

            total_rows += combined.height

            t_write = time.perf_counter()
            save_to_database(
                combined,
                table_name=TABLE_NAME,
                schema_name=TABLE_SCHEMA,
                connection_string=PSYCOPG_DSN,   # ← Fixed DSN
            )
            logging.info(f"DB write completed in {time.perf_counter() - t_write:.2f}s")
            written_rows += combined.height

        logging.info(f"BATCH {i} completed in {time.perf_counter() - t0:.2f}s")

    logging.info("━━━━━━━━━━━━━━ SUMMARY ━━━━━━━━━━━━━━")
    logging.info(f"Total rows fetched: {total_rows}")
    logging.info(f"Total rows written*: {written_rows}")
    logging.info("*Actual DB writes may be smaller due to dedupe (ON CONFLICT DO NOTHING).")
    logging.info("Done.")

if __name__ == "__main__":
    main()