#!/usr/bin/env python3
"""
Phase 1.5: enrich raw.ticker_metadata with fields only available from
Polygon's per-ticker detail endpoint (/v3/reference/tickers/{ticker}):

    sic_code   — Standard Industrial Classification. '6770' = Blank
                 Checks, i.e. SPACs. Useful for filtering synthetic
                 vehicles out of a survivorship-bias backtest.
    list_date  — Date the ticker first listed. Combined with
                 delisted_utc gives listing duration; short durations
                 (< 2 years) are a strong SPAC/shell proxy.

Costs ~1 API call per ticker, so this is expensive (~2–3k calls for the
current in-plan delisted equity universe). The list endpoint hit by
massive_fetch_ticker_metadata does NOT return these fields.

Idempotent — updates rows in place; can be re-run freely.

Typical invocations:
    # Enrich all delisted-and-fetched tickers (fast practical default)
    python -m datapipeline.ingestion.massive_enrich_ticker_details \\
        --where "delisted_utc IS NOT NULL AND aggregates_fetched = TRUE"

    # Enrich only rows still missing sic_code
    python -m datapipeline.ingestion.massive_enrich_ticker_details \\
        --where "sic_code IS NULL AND aggregates_fetched = TRUE"
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from datetime import timedelta
from typing import Any, Optional
from urllib.parse import urlparse

import psycopg2
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

from datapipeline.config.env import ENV, get_var  # noqa: E402


DETAIL_URL = "https://api.polygon.io/v3/reference/tickers/{ticker}"
HTTP_TIMEOUT_SECONDS = 30
MAX_BACKOFF_SECONDS = 120
MAX_RETRIES = 8
PROGRESS_EVERY = 200


def _safe_dsn(dsn: str) -> str:
    p = urlparse(dsn)
    port = f":{p.port}" if p.port else ""
    return f"{p.scheme}://{p.username or ''}:***@{p.hostname or ''}{port}/{p.path.lstrip('/')}"


def _resolve_dsn() -> str:
    dsn = get_var("DATABASE_URL")
    if not dsn:
        key = {"prod": "DATABASE_URL_PROD", "staging": "DATABASE_URL_STAGING"}.get(
            ENV, "DATABASE_URL_DEV"
        )
        dsn = get_var(key)
    if not dsn:
        raise RuntimeError("No DATABASE_URL resolved for ENV=%s" % ENV)
    if dsn.startswith("postgresql+psycopg2://"):
        dsn = dsn.replace("postgresql+psycopg2://", "postgresql://", 1)
    elif dsn.startswith("postgres://"):
        dsn = dsn.replace("postgres://", "postgresql://", 1)
    return dsn


def _get_api_key() -> str:
    key = get_var("MASSIVE_API_KEY")
    if not key:
        raise RuntimeError("MASSIVE_API_KEY not set")
    return key


def _request_detail(
    ticker: str,
    api_key: str,
    as_of: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    # Polygon's /v3/reference/tickers/{ticker} endpoint returns the
    # *current* entity for a symbol by default. Delisted tickers 404 or
    # (worse) return a different company now using the same symbol.
    # Passing ?date=YYYY-MM-DD (point-in-time) resolves to the historical
    # entity, which is what we want for survivorship-bias backfill.
    url = DETAIL_URL.format(ticker=ticker)
    params = {"apiKey": api_key}
    if as_of:
        params["date"] = as_of
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            if attempt >= MAX_RETRIES:
                raise
            delay = min(MAX_BACKOFF_SECONDS, 2 ** attempt) + random.uniform(0, 1)
            logging.warning("Network error for %s: %s; retry %d in %.1fs",
                            ticker, exc, attempt, delay)
            time.sleep(delay)
            continue

        if resp.status_code == 200:
            return resp.json().get("results") or None

        if resp.status_code in (404, 403):
            # 404 = ticker unknown; 403 = plan-level restriction. Treat as
            # "no data" so we can move on and not block.
            return None

        if resp.status_code == 429 or resp.status_code >= 500:
            if attempt >= MAX_RETRIES:
                resp.raise_for_status()
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = min(MAX_BACKOFF_SECONDS, int(retry_after))
            else:
                delay = min(MAX_BACKOFF_SECONDS, 2 ** attempt) + random.uniform(0, 1)
            logging.warning("HTTP %s on %s; retry %d in %.1fs",
                            resp.status_code, ticker, attempt, delay)
            time.sleep(delay)
            continue

        resp.raise_for_status()
        return None


UPDATE_SQL = """
UPDATE raw.ticker_metadata SET
    sic_code          = %s,
    list_date         = %s,
    last_refreshed_at = NOW()
WHERE ticker = %s;
"""


def run(where_clause: str, limit: Optional[int], dry_run: bool) -> None:
    api_key = _get_api_key()
    dsn = _resolve_dsn()
    logging.info("ENV=%s; DSN=%s; where=%s; limit=%s",
                 ENV, _safe_dsn(dsn), where_clause, limit or "(none)")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    try:
        # Pull delisted_utc so we can use it as the point-in-time `date`
        # param to resolve to the historical entity.
        sql = (
            "SELECT ticker, delisted_utc::date FROM raw.ticker_metadata "
            f"WHERE {where_clause} ORDER BY ticker"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        with conn.cursor() as cur:
            cur.execute(sql)
            work = cur.fetchall()

        total = len(work)
        logging.info("Queued %d tickers for detail enrichment", total)
        if total == 0:
            return

        t0 = time.perf_counter()
        updated = 0
        missing = 0
        errors = 0

        for idx, (ticker, delisted_date) in enumerate(work, start=1):
            # Use delisted_utc - 1 day as the point-in-time anchor so we
            # hit the entity's final pre-delisting state.
            as_of = None
            if delisted_date:
                as_of = (delisted_date - timedelta(days=1)).isoformat()

            try:
                details = _request_detail(ticker, api_key, as_of=as_of)
            except Exception as exc:
                errors += 1
                logging.error("%s: detail fetch failed: %s", ticker, exc)
                continue

            if details is None:
                missing += 1
                continue

            sic_code = details.get("sic_code")
            list_date_str = details.get("list_date")

            if dry_run:
                updated += 1
                continue

            with conn.cursor() as cur:
                cur.execute(UPDATE_SQL, (sic_code, list_date_str, ticker))
            conn.commit()
            updated += 1

            if idx % PROGRESS_EVERY == 0 or idx == total:
                elapsed = time.perf_counter() - t0
                rate = idx / elapsed if elapsed > 0 else 0.0
                logging.info(
                    "progress %d/%d updated=%d missing=%d err=%d (%.2f/s)",
                    idx, total, updated, missing, errors, rate,
                )

        logging.info(
            "Done. updated=%d missing=%d errors=%d dry_run=%s",
            updated, missing, errors, dry_run,
        )
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--where",
        default="delisted_utc IS NOT NULL AND aggregates_fetched = TRUE",
        help="SQL WHERE clause against raw.ticker_metadata (no 'WHERE'). "
             "Defaults to all delisted-and-fetched rows.",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    run(where_clause=args.where, limit=args.limit, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
