import sys
import os
import pandas as pd
from pandas.tseries.offsets import DateOffset
import adbc_driver_postgresql.dbapi as pg_dbapi
from datetime import datetime

# Add your application directory to the system path
sys.path.append('/Users/kevin/Dropbox/applications/ELT/python/src/')
from dev.config.config import TICKERS  # Import TICKERS from config.py

# Ensure the utils folder is in the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../utils'))

# Import the cumulative_fibonacci function
from dev.config.fibonacci import cumulative_fibonacci

def get_next_trading_day(date):
    """Returns the next available trading day."""
    return pd.bdate_range(date, periods=1)[0].date()

def process_stock_data(df, years_to_add=3):
    """Processes the stock data to find and extend minimum dates for each ticker."""
    
    # Group by 'ticker' and select the minimum date for each group
    min_dates = df.groupby('ticker')['date'].min().reset_index()
    
    # Initialize a list to store all the dataframes
    all_years_dfs = []

    # Loop through the range to add multiple years
    for year in range(1, years_to_add + 3):
        min_dates_shifted = min_dates.copy()
        min_dates_shifted['date'] = min_dates_shifted['date'] + DateOffset(years=year)
        min_dates_shifted['date'] = min_dates_shifted['date'].apply(get_next_trading_day)
        
        # Merge with the original DataFrame to get other columns for the shifted dates
        shifted_df = pd.merge(min_dates_shifted, df, on=['ticker', 'date'], how='left')[['ticker', 'date', 'open', 'high', 'low', 'close']]
        
        # Add the shifted DataFrame to the list
        all_years_dfs.append(shifted_df)

    # Also include the original minimum dates DataFrame
    min_result_df = pd.merge(min_dates, df, on=['ticker', 'date'], how='left')[['ticker', 'date', 'open', 'high', 'low', 'close']]
    all_years_dfs.insert(0, min_result_df)
    
    # Concatenate all DataFrames to have min date, min_date + n years in separate rows
    result_df = pd.concat(all_years_dfs, ignore_index=True)
    
    # Ensure all dates are in datetime.date format
    result_df['date'] = pd.to_datetime(result_df['date']).dt.date
    
    # Filter out any rows with NaN values (if any remain)
    result_df = result_df.dropna()
    
    # Add a row number partitioned by 'ticker' and ordered by 'date'
    result_df['row_number'] = result_df.sort_values('date').groupby('ticker').cumcount()  # Start at 0 instead of 1
    
    return result_df  # Return the processed DataFrame

def fetch_data_from_database(table_name, connection_string):
    """Fetches the stock data from the specified table in the database."""
    try:
        with pg_dbapi.connect(connection_string) as conn:
            query = f"SELECT * FROM raw.{table_name};"
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print("Error fetching data from the database:", e)
        return None

def save_to_database(df, table_name, connection_string):
    """Saves the DataFrame to the specified table in the 'cdm' schema of the database."""
    try:
        with pg_dbapi.connect(connection_string) as conn:
            with conn.cursor() as cur:  # Use a cursor object
                cur.execute(f"SET search_path TO cdm;")
                
                # Fetch existing ticker and date combinations
                existing_query = f"SELECT ticker, date FROM {table_name};"
                existing_data = pd.read_sql(existing_query, conn)
                
                # Ensure date column is in the correct format for comparison
                existing_data['date'] = pd.to_datetime(existing_data['date']).dt.date
                
                # Merge to find rows that are not already in the database
                df_to_insert = pd.merge(df, existing_data, on=['ticker', 'date'], how='left', indicator=True)
                df_to_insert = df_to_insert[df_to_insert['_merge'] == 'left_only'].drop(columns=['_merge'])
                
                if not df_to_insert.empty:
                    # Insert the new rows into the database
                    df_to_insert.to_sql(table_name, conn, if_exists='append', index=False)
                    print(f"{len(df_to_insert)} new rows successfully saved to {table_name} in schema 'cdm'")
                else:
                    print("No new rows to insert. All data already exists in the database.")
                
    except Exception as e:
        print("Failed to save data to database:", e)
        return None

if __name__ == "__main__":
    table_name = 'history_data_fetcher'  # Name of the table to fetch data from
    new_table_name = 'fibonacci_data'  # Name of the table to save the processed data
    
    # Retrieve connection string from environment variables
    connection_string = os.getenv('DB_CONNECTION_STRING')
    
    if connection_string is None:
        print("DB_CONNECTION_STRING environment variable not set")
    else:
        try:
            # Fetch the data from the database
            df = fetch_data_from_database(table_name, connection_string)
            
            if df is not None:
                # Ensure correct data types for numeric columns
                df['open'] = df['open'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                df['close'] = df['close'].astype(float)
                
                # Process the fetched data and specify the number of years to add
                result_df = process_stock_data(df, years_to_add=100)
                
                # Generate the cumulative Fibonacci series for filtering
                cumulative_fib_sequence = cumulative_fibonacci(len(result_df))
                
                # Filter the result_df to only include rows with 'row_number' in the Fibonacci series
                #matching_rows = result_df[result_df['row_number'].isin(cumulative_fib_sequence)]
                matching_rows = result_df[result_df['row_number'].isin(cumulative_fib_sequence)].copy()
                matching_rows.loc[:, 'processed_at'] = datetime.now()
                # Add the processed_at column with the current timestamp
                #matching_rows['processed_at'] = datetime.now()
                matching_rows.loc[:, 'processed_at'] = datetime.now()

                # Save the filtered DataFrame to the specified table in the 'cdm' schema in the database
                save_to_database(matching_rows, new_table_name, connection_string)
                
                # Display the resulting filtered DataFrame
                print(matching_rows)
            else:
                print("No data fetched from the database.")
        except Exception as e:
            print("Unexpected error occurred:", e)
print(result_df)
