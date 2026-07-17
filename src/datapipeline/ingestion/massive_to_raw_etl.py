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


AMSTERDAM_TZ = ZoneInfo("Europe/Amsterdam")


warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


from datapipeline.config.env import ENV, get_var


def _env_override(key_base: str, env: str, default: Optional[str] = None):
    env_key = f"{key_base}_{env.upper()}"
    return get_var(env_key, get_var(key_base, default))

def _build_dsn(env: str) -> str:
    user = _env_override("DB_USER", env, "postgres")
    password = _env_override("DB_PASSWORD", env, "")
    host = _env_override("DB_HOST", env, "postgres")
    port = _env_override("DB_PORT", env, "5432")
    sslmode = _env_override("DB_SSLMODE", env, "prefer")

    default_db = get_var("DB_DATABASE", "dev")
    name = _env_override("DB_NAME", env, default_db)
    
    dsn = (
        f"postgresql+psycopg2://{user}:{quote_plus(password or '')}"
        f"@{host}:{port}/{name}"
    )
    if sslmode:
        dsn += f"?sslmode={sslmode}"
    return dsn


def _safe_dsn(dsn: str) -> str:
    if not dsn:
        return "None"
    p = urlparse(dsn)
    return (
        f"{p.scheme}://{p.username or ''}:***@{p.hostname or ''}"
        f"{f':{p.port}' if p.port else ''}/{p.path.lstrip('/')}"
    )


DATABASE_URL = get_var("DATABASE_URL")

if not DATABASE_URL:
    if ENV == "prod":
        key = "DATABASE_URL_PROD"
    elif ENV == "staging":
        key = "DATABASE_URL_STAGING"
    else:
        key = "DATABASE_URL_DEV"

    DATABASE_URL = get_var(key) or _build_dsn(ENV)

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace(
        "postgres://", "postgresql+psycopg2://", 1
    )

def get_psycopg_dsn(dsn: str) -> str:
    return dsn.replace("postgresql+psycopg2://", "postgresql://", 1)

PSYCOPG_DSN = get_psycopg_dsn(DATABASE_URL)

logging.info("ENV=%s; Using DATABASE_URL=%s", ENV, _safe_dsn(DATABASE_URL))


MASSIVE_API_KEY = get_var("MASSIVE_API_KEY")
if not MASSIVE_API_KEY:
    raise RuntimeError("MASSIVE_API_KEY not set")

MAX_WORKERS = int(os.getenv("MASSIVE_MAX_WORKERS", "32"))

client = RESTClient(
    MASSIVE_API_KEY,
    num_pools=max(64, MAX_WORKERS * 2),
)


from datapipeline.config.ingestion_targets import (
    TICKERS_SUB,
    TICKERS_FULL,
    TICKERS as _TICKERS,
)

TICKERS = list(_TICKERS)

logging.info(
    "Resolved tickers in ETL: ENV=%s → %s (count=%d)",
    ENV,
    TICKERS,
    len(TICKERS),
)

if ENV == "dev":
    forbidden = [t for t in TICKERS if "=F" in t]
    if forbidden:
        raise RuntimeError(
            f"DEV environment attempted futures ingestion: {forbidden}"
        )


from datapipeline.config.helpers import save_to_database


TABLE_SCHEMA = "raw"
TABLE_NAME = "api_data_ingestion_massive"
DEFAULT_BATCH_SIZE = int(os.getenv("MASSIVE_BATCH_SIZE", "15"))
EPOCH_START = date(1970, 1, 1)


YAHOO_TO_MASSIVE = {
    "^GSPC": "I:SPX",
    "^DJI": "I:DJI",
    "^IXIC": "I:COMP",
}

def to_massive(ticker: str) -> str:
    return YAHOO_TO_MASSIVE.get(ticker, ticker)


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


BASIS_SENTINEL_OFFSET = 40   # trading rows back from the newest fetched bar
BASIS_TOLERANCE = 0.01       # >1% divergence = adjustment-basis break


def heal_basis_breaks(combined: pl.DataFrame, dsn: str) -> None:
    """Detect and heal split-adjustment basis breaks before writing.

    Full-history runs fetch every ticker's complete adjusted series, but the
    writers insert with ON CONFLICT DO NOTHING, so a re-adjusted history (stock
    split / provider restatement) is silently discarded and stored rows stay on
    the old basis - permanent fake one-day cliffs in cdm.ingest_combined (2026-07
    incident: 27 tickers incl. a 4:1 CRWD split; see
    data_quality.split_basis_break_repair_20260717).

    Compare the freshly fetched adj_close at a sentinel date ~40 trading days
    back against the stored row. On divergence, delete the ticker's rows from
    raw + cdm copies so this batch's insert rewrites the whole history on the
    new basis. Single-day runs (raw_ingest_massive_daily) fetch too few rows
    per ticker and are skipped.
    """
    import psycopg2

    checks = []  # (ticker, sentinel ticker_date_id, fetched adj_close)
    for tdf in combined.partition_by("ticker"):
        tdf = tdf.sort("date")
        n = tdf.height
        if n < 5:
            continue   # single-day / tiny fetch: nothing to compare against
        row = tdf.row(max(0, n - BASIS_SENTINEL_OFFSET), named=True)
        if row["adj_close"] and row["adj_close"] > 0:
            checks.append((row["ticker"], row["ticker_date_id"], row["adj_close"]))
    if not checks:
        return

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker_date_id, adj_close"
                " FROM raw.api_data_ingestion_massive"
                " WHERE ticker_date_id = ANY(%s)",
                ([c[1] for c in checks],),
            )
            stored = dict(cur.fetchall())
        broken = sorted({
            t for (t, tid, px) in checks
            if tid in stored and stored[tid] and float(stored[tid]) > 0
            and abs(px / float(stored[tid]) - 1.0) > BASIS_TOLERANCE
        })
        if not broken:
            return
        logging.warning(
            "BASIS BREAK detected for %s - deleting stored history so this "
            "run rewrites it on the new adjustment basis", broken,
        )
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM raw.api_data_ingestion_massive WHERE ticker = ANY(%s)",
                (broken,),
            )
            cur.execute(
                "DELETE FROM cdm.api_data_ingestion_massive WHERE ticker = ANY(%s)",
                (broken,),
            )
        conn.commit()
        logging.warning("BASIS BREAK healed: %s rewritten from this fetch", broken)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_date")
    parser.add_argument("--end_date")
    parser.add_argument("--batch", type=int, default=1)
    parser.add_argument("--num_batches", type=int, default=1)
    parser.add_argument("--batch_size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    selected = [t for t in TICKERS if isinstance(t, str)]
    total_tickers = len(selected)
    
    batch = args.batch
    num_batches = args.num_batches

    if num_batches > 1:
        base = total_tickers // num_batches
        remainder = total_tickers % num_batches

        start_idx = (batch - 1) * base + min(batch - 1, remainder)
        end_idx = start_idx + base + (1 if batch <= remainder else 0)
        selected = selected[start_idx:end_idx]

    logging.info("Batch %d/%d handles %d tickers", batch, num_batches, len(selected))

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
            heal_basis_breaks(combined, PSYCOPG_DSN)
            save_to_database(
                combined,
                table_name=TABLE_NAME,
                schema_name=TABLE_SCHEMA,
                connection_string=PSYCOPG_DSN,
            )
            written_rows += combined.height

            del combined
            del dfs
            import gc
            gc.collect()

            logging.info(
                "DB write completed in %.2fs",
                time.perf_counter() - t_write,
            )

        logging.info(
            "BATCH %s completed in %.2fs",
            i,
            time.perf_counter() - t0,
        )

    logging.info("━━━━━━━━━━━━━━ SUMMARY ━━━━━━━━━━━━━━")
    logging.info("Total rows fetched: %s", total_rows)
    logging.info("Total rows written*: %s", written_rows)
    logging.info("*Actual DB writes may be smaller due to dedupe.")
    logging.info("Done.")


if __name__ == "__main__":
    main()
