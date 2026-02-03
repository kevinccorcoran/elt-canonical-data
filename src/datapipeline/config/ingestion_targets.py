import logging
from datapipeline.config.env import ENV


# Small ticker subset for local development
TICKERS_SUB = ["SPY", "AAPL", "NET"]

# Full ticker universe for staging / production ingestion
TICKERS_FULL = [
    "CL=F", "BZ=F","NET"
    # … rest of your universe
]


# Environment switch
if ENV == "dev":
    TICKERS = TICKERS_SUB
elif ENV in ("staging", "prod"):
    TICKERS = TICKERS_FULL
else:
    raise RuntimeError(f"Unknown ENV: {ENV}")


# Explicitly log resolved ingestion targets
logging.error(
    "INGESTION_TARGETS: ENV=%r | TICKERS_SUB=%s | TICKERS_FULL=%s | TICKERS=%s",
    ENV, TICKERS_SUB, TICKERS_FULL, TICKERS
)
