import sys
import os
import warnings
import logging
import argparse
from datetime import datetime
import time

import yfinance as yf
import pandas as pd

# Safe Airflow import
try:
    from airflow.models import Variable
    ENV = Variable.get("ENV", default_var="dev")
except ModuleNotFoundError:
    import os
    ENV = os.getenv("ENV", "dev")

# Project imports
sys.path.append('/Users/kevin/repos/ELT_private/python/src/')
from dev.config.ingestion_targets import TICKERS, TICKERS_FULL
from src.datapipeline.config.helpers import save_to_database

# Logging & warnings
warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Column alias mapping
COLUMN_ALIASES = {
    'Date': 'date',
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Close': 'close',
    'Adj Close': 'adj_close',
    'Volume': 'volume',
    'Dividends': 'dividends',
    'Stock Splits': 'stock_splits',
    'Capital Gains': 'capital_gains'
}

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def fetch_stock_data(ticker, start_date=None, end_date=None, retries=3):
    for attempt in range(retries):
        try:
            stock_data = yf.Ticker(ticker)
            dx = stock_data.history(start=start_date, end=end_date) if start_date and end_date else stock_data.history(period="max")
            if dx.empty:
                logging.warning(f"No data found for ticker {ticker}")
                return None
            logging.info(f"Raw columns fetched for {ticker}: {dx.columns.tolist()}")
            if 'Adj Close' not in dx.columns and 'Close' in dx.columns:
                dx['Adj Close'] = dx['Close']
                logging.info(f"'Adj Close' missing for {ticker}. Fallback to 'Close'.")
            return dx
        except Exception as e:
            logging.warning(f"Error fetching data for {ticker}: {e}. Retrying ({attempt + 1}/{retries})...")
            time.sleep(2 ** attempt)
    logging.error(f"Failed to fetch data for {ticker} after {retries} attempts.")
    return None

def build_df(tickers, start_date=None, end_date=None):
    df = pd.DataFrame()
    for ticker in tickers:
        dx = fetch_stock_data(ticker, start_date, end_date)
        if dx is not None:
            dx.rename(columns={k: v for k, v in COLUMN_ALIASES.items() if k in dx.columns}, inplace=True)

            for col in ['open', 'high', 'low', 'close', 'adj_close', 'volume']:
                if col in dx.columns:
                    dx[col] = pd.to_numeric(dx[col], errors='coerce').round(2)

            dx['ticker'] = ticker
            df = pd.concat([df, dx], sort=False)

        time.sleep(0.5)

    if not df.empty:
        df.reset_index(inplace=True)

        if 'Date' in df.columns:
            df.rename(columns={'Date': 'date'}, inplace=True)
        elif df.columns[0].lower() == 'date':
            df.rename(columns={df.columns[0]: 'date'}, inplace=True)
        elif 'date' not in df.columns:
            logging.error(f"Columns in DataFrame: {df.columns.tolist()}")
            raise ValueError("No 'date' column found after resetting index.")

        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None).dt.date
        df.dropna(subset=["date"], inplace=True)

        df['processed_at'] = datetime.now()
        df['ticker_date_id'] = df['ticker'].astype(str) + "_" + df['date'].astype(str)

        for col in df.select_dtypes(include=['object', 'string']).columns:
            if col != 'date':
                df[col] = df[col].astype(str).str.replace("\x00", "", regex=False)

        if 'capital_gains' in df.columns:
            df['capital_gains'] = df['capital_gains'].astype(str).str.replace("\x00", "", regex=False)

        before = df.shape[0]
        df.drop_duplicates(subset=["ticker_date_id"], inplace=True)
        after = df.shape[0]
        logging.info(f"Removed {before - after} duplicate rows based on ticker_date_id.")

        final_columns = [
            'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume',
            'dividends', 'stock_splits', 'capital_gains',
            'processed_at', 'ticker', 'ticker_date_id'
        ]
        df = df[[col for col in final_columns if col in df.columns]]

    return df

if __name__ == "__main__":
    SELECTED_TICKERS = TICKERS if ENV == "dev" else TICKERS_FULL

    parser = argparse.ArgumentParser(description="Fetch and store stock data from Yahoo Finance.")
    parser.add_argument("--start_date", help="Start date (YYYY-MM-DD)", required=False)
    parser.add_argument("--end_date", help="End date (YYYY-MM-DD)", required=False)
    args = parser.parse_args()

    today = datetime.now().strftime('%Y-%m-%d')
    start_date = args.start_date or today
    end_date = args.end_date or today

    table_name = 'api_data_ingestion'
    key_columns = ['ticker_date_id']

    connection_string = os.getenv('DATABASE_URL')
    if not connection_string:
        raise ValueError("DATABASE_URL environment variable not set")

    if connection_string.startswith("postgres://"):
        connection_string = connection_string.replace("postgres://", "postgresql+psycopg2://", 1)

    try:
        for batch in chunk_list(SELECTED_TICKERS, 100):
            df = build_df(batch, start_date=start_date, end_date=end_date)

            if not df.empty:
                logging.info(f"Columns in DataFrame: {df.columns.tolist()}")
                logging.info(f"Fetched data for {df['ticker'].nunique()} tickers.")
                print(df.head())
                save_to_database(df, table_name, connection_string, schema_name='raw', key_columns=key_columns)
            else:
                logging.info("No data fetched in this batch.")

    except Exception as e:
        logging.exception("Unexpected error occurred during ETL run.")


# import sys
# import os
# import warnings
# import logging
# import argparse
# from datetime import datetime
# import time

# import yfinance as yf
# import pandas as pd
# from airflow.models import Variable

# # Project imports
# sys.path.append('/Users/kevin/repos/ELT_private/python/src/')
# from dev.config.config import TICKERS, TICKERS_FULL
# from dev.config.helpers import save_to_database

# # Logging & warnings
# warnings.filterwarnings("ignore", category=FutureWarning)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # Column alias mapping
# COLUMN_ALIASES = {
#     'Date': 'date',
#     'Open': 'open',
#     'High': 'high',
#     'Low': 'low',
#     'Close': 'close',
#     'Adj Close': 'adj_close',
#     'Volume': 'volume',
#     'Dividends': 'dividends',
#     'Stock Splits': 'stock_splits',
#     'Capital Gains': 'capital_gains'
# }

# def chunk_list(lst, n):
#     for i in range(0, len(lst), n):
#         yield lst[i:i + n]

# def fetch_stock_data(ticker, start_date=None, end_date=None, retries=3):
#     for attempt in range(retries):
#         try:
#             stock_data = yf.Ticker(ticker)
#             dx = stock_data.history(start=start_date, end=end_date) if start_date and end_date else stock_data.history(period="max")
#             if dx.empty:
#                 logging.warning(f"No data found for ticker {ticker}")
#                 return None
#             logging.info(f"Raw columns fetched for {ticker}: {dx.columns.tolist()}")
#             return dx
#         except Exception as e:
#             logging.warning(f"Error fetching data for {ticker}: {e}. Retrying ({attempt + 1}/{retries})...")
#             time.sleep(2 ** attempt)  # exponential backoff: 1s, 2s, 4s
#     logging.error(f"Failed to fetch data for {ticker} after {retries} attempts.")
#     return None

# def build_df(tickers, start_date=None, end_date=None):
#     df = pd.DataFrame()
#     for ticker in tickers:
#         dx = fetch_stock_data(ticker, start_date, end_date)
#         if dx is not None:
#             dx.rename(columns={k: v for k, v in COLUMN_ALIASES.items() if k in dx.columns}, inplace=True)

#             for col in ['open', 'high', 'low', 'close', 'adj_close', 'volume']:
#                 if col in dx.columns:
#                     dx[col] = pd.to_numeric(dx[col], errors='coerce').round(2)

#             dx['ticker'] = ticker
#             df = pd.concat([df, dx], sort=False)

#         time.sleep(0.5)

#     if not df.empty:
#         df.reset_index(inplace=True)

#         # Ensure 'date' column is present
#         if 'Date' in df.columns:
#             df.rename(columns={'Date': 'date'}, inplace=True)
#         elif df.columns[0].lower() == 'date':
#             df.rename(columns={df.columns[0]: 'date'}, inplace=True)
#         elif 'date' not in df.columns:
#             logging.error(f"Columns in DataFrame: {df.columns.tolist()}")
#             raise ValueError("No 'date' column found after resetting index.")

#         df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None).dt.date
#         df.dropna(subset=["date"], inplace=True)

#         df['processed_at'] = datetime.now()
#         df['ticker_date_id'] = df['ticker'].astype(str) + "_" + df['date'].astype(str)

#         # Clean null bytes
#         for col in df.select_dtypes(include=['object', 'string']).columns:
#             if col != 'date':
#                 df[col] = df[col].astype(str).str.replace("\x00", "", regex=False)

#         if 'capital_gains' in df.columns:
#             df['capital_gains'] = df['capital_gains'].astype(str).str.replace("\x00", "", regex=False)

#         # Deduplication
#         before = df.shape[0]
#         df.drop_duplicates(subset=["ticker_date_id"], inplace=True)
#         after = df.shape[0]
#         logging.info(f"Removed {before - after} duplicate rows based on ticker_date_id.")

#         # Final column order
#         final_columns = [
#             'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume',
#             'dividends', 'stock_splits', 'capital_gains',
#             'processed_at', 'ticker', 'ticker_date_id'
#         ]
#         df = df[[col for col in final_columns if col in df.columns]]

#     return df

# if __name__ == "__main__":
#     ENV = Variable.get("ENV", default_var="dev")
#     SELECTED_TICKERS = TICKERS if ENV == "dev" else TICKERS_FULL

#     parser = argparse.ArgumentParser(description="Fetch and store stock data from Yahoo Finance.")
#     parser.add_argument("--start_date", help="Start date (YYYY-MM-DD)", required=False)
#     parser.add_argument("--end_date", help="End date (YYYY-MM-DD)", required=False)
#     args = parser.parse_args()

#     today = datetime.now().strftime('%Y-%m-%d')
#     start_date = args.start_date or today
#     end_date = args.end_date or today

#     table_name = 'api_data_ingestion'
#     key_columns = ['ticker_date_id']

#     connection_string = os.getenv('DATABASE_URL')
#     if not connection_string:
#         raise ValueError("DATABASE_URL environment variable not set")

#     if connection_string.startswith("postgres://"):
#         connection_string = connection_string.replace("postgres://", "postgresql+psycopg2://", 1)

#     try:
#         for batch in chunk_list(SELECTED_TICKERS, 100):
#             df = build_df(batch, start_date=start_date, end_date=end_date)

#             if not df.empty:
#                 logging.info(f"Columns in DataFrame: {df.columns.tolist()}")
#                 logging.info(f"Fetched data for {df['ticker'].nunique()} tickers.")
#                 print(df.head())
#                 save_to_database(df, table_name, connection_string, schema_name='raw', key_columns=key_columns)
#             else:
#                 logging.info("No data fetched in this batch.")

#     except Exception as e:
#         logging.exception("Unexpected error occurred during ETL run.")