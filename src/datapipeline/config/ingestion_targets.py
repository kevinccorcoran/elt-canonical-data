import logging
from datapipeline.config.env import ENV

TICKERS_SUB = ["SPY", "AAPL", "NET"]
TICKERS_FULL = ["NVDA", "SPY"]

TICKERS = TICKERS_SUB if ENV == "dev" else TICKERS_FULL

logging.error(
    "INGESTION_TARGETS: ENV=%r | TICKERS_SUB=%s | TICKERS_FULL=%s | TICKERS=%s",
    ENV, TICKERS_SUB, TICKERS_FULL, TICKERS
)
