import requests
import string
import os
import re

MASSIVE_API_KEY = os.getenv('MASSIVE_API_KEY')
polygon_base_url = os.getenv('POLYGON_BASE_URL')  # e.g. https://api.polygon.io/v3/reference/tickers
if not MASSIVE_API_KEY or not polygon_base_url:
    raise EnvironmentError("Please set the MASSIVE_API_KEY and POLYGON_BASE_URL environment variables.")


def is_clean_common(result: dict) -> bool:
    """
    Keep only clean common stock tickers.
    Allow share classes (e.g. BRK.A).
    Exclude: Preferred Stock, REITs, ADRs, LPs, Trusts, Hybrids.
    """
    tkr = result.get("ticker", "")
    typ = result.get("type", "")
    name = (result.get("name") or "").lower()
    sic_desc = (result.get("sic_description") or "").lower()

    # Must be Common Stock
    if typ != "CS":
        return False

    # Exclude REITs
    if "reit" in name or "real estate investment trust" in sic_desc:
        return False

    # Exclude ADRs
    if "american depositary" in name or "adr" in name or "depositary" in sic_desc:
        return False

    # Exclude LPs, Trusts, Units (handle variations like "L.P.", "L P")
    if re.search(r"\blp\b", name) or "l.p" in name or "trust" in name or "unit" in name:
        return False

    return True


def fetch_tickers(exchange_code):
    tickers = []
    rejected = []
    base_params = {
        "market": "stocks",
        "exchange": exchange_code,
        "active": "true",
        "type": "CS",          # common stock only
        "limit": 1000,
        "apiKey": MASSIVE_API_KEY,
    }

    for letter in string.ascii_uppercase:
        cursor = None
        while True:
            params = dict(base_params)
            params["ticker.gte"] = letter
            params["ticker.lt"] = chr(ord(letter) + 1)
            if cursor:
                params["cursor"] = cursor

            resp = requests.get(polygon_base_url, params=params, timeout=30)
            if resp.status_code != 200:
                print(f"Error: {resp.status_code} - {resp.text}")
                break

            data = resp.json()
            results = data.get("results", []) or []

            for r in results:
                if is_clean_common(r):
                    tickers.append(r["ticker"])
                else:
                    rejected.append((r["ticker"], r.get("name", ""), r.get("type", "")))

            print(
                f"{exchange_code} {letter}: got {len(results)} results, "
                f"kept {len(tickers)}, rejected {len(rejected)}"
            )

            cursor = data.get("next_cursor")
            if not cursor:
                break

    return sorted(set(tickers)), rejected


# Fetch tickers
nyse_tickers, nyse_rejected = fetch_tickers("XNYS")
nasdaq_tickers, nasdaq_rejected = fetch_tickers("XNAS")

dupes = set(nyse_tickers) & set(nasdaq_tickers)
print("Duplicate tickers across NYSE/NASDAQ:", sorted(dupes) if dupes else "None")

# Write results
output_path = "/Users/kevin/repos/ELT_private/python/src/dev/config/polygon/output.py"
with open(output_path, "w") as f:
    f.write("NYSE = [\n")
    for i, t in enumerate(nyse_tickers):
        if i and i % 10 == 0:
            f.write("\n")
        f.write(f"'{t}', ")
    f.write("\n]\n\n")

    f.write("NASDAQ = [\n")
    for i, t in enumerate(nasdaq_tickers):
        if i and i % 10 == 0:
            f.write("\n")
        f.write(f"'{t}', ")
    f.write("\n]\n")

# Also dump rejected for debugging
rejects_path = "/Users/kevin/repos/ELT_private/python/src/dev/config/polygon/rejected.log"
with open(rejects_path, "w") as f:
    f.write("=== NYSE REJECTED ===\n")
    for tkr, name, typ in nyse_rejected:
        f.write(f"{tkr} | {name} | {typ}\n")

    f.write("\n=== NASDAQ REJECTED ===\n")
    for tkr, name, typ in nasdaq_rejected:
        f.write(f"{tkr} | {name} | {typ}\n")

print(f"Total NYSE clean CS: {len(nyse_tickers)}")
print(f"Total NASDAQ clean CS: {len(nasdaq_tickers)}")
print(f"Tickers saved to {output_path}")
print(f"Rejected tickers logged to {rejects_path}")

