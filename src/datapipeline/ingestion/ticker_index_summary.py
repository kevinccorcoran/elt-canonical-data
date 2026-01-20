#!/usr/bin/env python3
"""
ticker_index_summary.py

Aggregate index / ETF / crypto memberships into:
    ticker | indices | index_count

Output:
    raw.ticker_index_summary
"""

import os
import time
import logging
from typing import List

import requests
from bs4 import BeautifulSoup
import polars as pl

from datapipeline.config.helpers import save_to_database


# ───────────────────────── Logging ─────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# ───────────────────────── Environment ─────────────────────────

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise EnvironmentError("DATABASE_URL not set")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}

ARK_ETF_SYMBOLS = ["ARKK", "ARKG", "ARKF", "ARKQ", "ARKW", "ARKX", "ARKY"]


# ───────────────────────── Network helper ─────────────────────────

def fetch_with_retry(url: str, max_retries: int = 3, params=None) -> str:
    for i in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.warning(f"{url} failed attempt {i+1}: {e}")
            if i == max_retries - 1:
                raise
            time.sleep(2 ** i)


# ───────────────────────── Ticker validation ─────────────────────────

def _is_valid_ticker(t: str) -> bool:
    if not t:
        return False

    t = t.strip().upper()

    if len(t) == 0 or len(t) > 10:
        return False
    if " " in t:
        return False
    if not any(ch.isalpha() for ch in t):
        return False

    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-")
    return all(ch in allowed for ch in t)


# ───────────────────────── Wikipedia extraction ─────────────────────────

def extract_table_tickers_from_html(html: str) -> List[str]:
    """
    Extract tickers only from tables with an explicit Symbol/Ticker column.
    """
    soup = BeautifulSoup(html, "html.parser")
    tickers: set[str] = set()

    for table in soup.find_all("table"):
        caption = table.find("caption")
        if caption and "former" in caption.get_text(strip=True).lower():
            continue

        rows = table.find_all("tr")
        if not rows:
            continue

        headers = [c.get_text(strip=True).lower() for c in rows[0].find_all(["th", "td"])]

        symbol_col = next(
            (i for i, h in enumerate(headers) if "symbol" in h or "ticker" in h),
            None,
        )
        if symbol_col is None:
            continue

        for row in rows[1:]:
            cols = row.find_all(["td", "th"])
            if len(cols) <= symbol_col:
                continue

            text = cols[symbol_col].get_text(strip=True)
            text = text.split()[0].replace("[", "").replace("]", "")

            if _is_valid_ticker(text):
                tickers.add(text.upper())

    return sorted(tickers)


def extract_from_wikipedia(url: str, index_name: str) -> pl.DataFrame:
    try:
        logger.info(f"Fetching {index_name} ← Wikipedia")
        html = fetch_with_retry(url)
        symbols = extract_table_tickers_from_html(html)

        return pl.DataFrame(
            {"ticker": symbols, "index": [index_name] * len(symbols)}
        )
    except Exception as e:
        logger.warning(f"{index_name} unavailable ({e})")
        return pl.DataFrame({"ticker": [], "index": []})


# ───────────────────────── DJIA (special-case table) ─────────────────────────

def get_djia() -> pl.DataFrame:
    url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
    index_name = "DJIA"

    try:
        logger.info(f"Fetching {index_name} ← Wikipedia")
        html = fetch_with_retry(url)
        soup = BeautifulSoup(html, "html.parser")

        tickers: list[str] = []

        for table in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not any("symbol" in h for h in headers):
                continue

            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue

                candidate = cols[1].get_text(strip=True).split()[0]
                candidate = candidate.replace("[", "").replace("]", "")

                if _is_valid_ticker(candidate):
                    tickers.append(candidate.upper())

        tickers = sorted(set(tickers))
        return pl.DataFrame({"ticker": tickers, "index": [index_name] * len(tickers)})

    except Exception as e:
        logger.warning(f"{index_name} unavailable ({e})")
        return pl.DataFrame({"ticker": [], "index": []})


# ───────────────────────── ARK ETFs ─────────────────────────

def get_ark_etf(symbol: str) -> pl.DataFrame:
    url = "https://arkfunds.io/api/v1/etf/holdings"
    logger.info(f"Fetching {symbol} ← arkfunds.io")

    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, params={"symbol": symbol}, timeout=20)
            r.raise_for_status()
            holdings = r.json().get("holdings", [])

            tickers = sorted(
                {
                    h["ticker"].strip().upper()
                    for h in holdings
                    if h.get("ticker")
                }
            )

            return pl.DataFrame({"ticker": tickers, "index": [symbol] * len(tickers)})
        except Exception as e:
            logger.warning(f"{symbol}: attempt {attempt+1} failed ({e})")
            time.sleep(2 ** attempt)

    return pl.DataFrame({"ticker": [], "index": []})


# ───────────────────────── Crypto ─────────────────────────

def get_major_us_crypto() -> pl.DataFrame:
    tickers = [
        "X:BTCUSD", "X:ETHUSD", "X:SOLUSD", "X:DOGEUSD", "X:ADAUSD",
        "X:LTCUSD", "X:BCHUSD", "X:ETCUSD", "X:MATICUSD", "X:AVAXUSD",
        "X:DOTUSD", "X:ATOMUSD", "X:SHIBUSD", "X:XLMUSD", "X:LINKUSD",
        "X:UNIUSD",
    ]
    return pl.DataFrame({"ticker": tickers, "index": ["crypto"] * len(tickers)})


# ───────────────────────── Other indices ─────────────────────────

def get_sp500():       return extract_from_wikipedia("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "SP500")
def get_sp400():       return extract_from_wikipedia("https://en.wikipedia.org/wiki/List_of_S%26P_400_companies", "SP400")
def get_sp600():       return extract_from_wikipedia("https://en.wikipedia.org/wiki/List_of_S%26P_600_companies", "SP600")
def get_nasdaq100():   return extract_from_wikipedia("https://en.wikipedia.org/wiki/NASDAQ-100", "NASDAQ100")
def get_russell1000(): return extract_from_wikipedia("https://en.wikipedia.org/wiki/Russell_1000_Index", "RUSSELL1000")


# ───────────────────────── Aggregation ─────────────────────────

def aggregate(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("ticker")
        .agg(
            [
                pl.col("index").unique().sort().alias("indices"),
                pl.col("index").unique().count().alias("index_count"),
            ]
        )
    )


# ───────────────────────── Collection pipeline ─────────────────────────

def get_all_index_memberships() -> pl.DataFrame:
    dfs: list[pl.DataFrame] = []

    for s in ARK_ETF_SYMBOLS:
        dfs.append(get_ark_etf(s))

    dfs.append(get_major_us_crypto())

    dfs.extend(
        [
            get_djia(),
            get_nasdaq100(),
            get_russell1000(),
            get_sp400(),
            get_sp500(),
            get_sp600(),
        ]
    )

    return pl.concat(dfs, how="vertical_relaxed").drop_nulls("ticker").unique()


# ───────────────────────── Main ─────────────────────────

if __name__ == "__main__":
    try:
        df_raw = get_all_index_memberships()
        df_grouped = aggregate(df_raw)

        # Convert index list to Postgres array literal
        df_grouped = df_grouped.with_columns(
            pl.col("indices")
            .list.join(",")
            .map_elements(lambda s: "{" + s + "}")
            .alias("indices")
        )

        save_to_database(
            df=df_grouped,
            table_name="ticker_index_summary",
            schema_name="raw",
            connection_string=DATABASE_URL,
        )

        print(f"SUCCESS → raw.ticker_index_summary updated ({df_grouped.height} rows)")

    except Exception:
        logger.exception("Script failed")
        raise SystemExit(1)
