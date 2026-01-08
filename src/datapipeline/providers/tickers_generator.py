import requests
import string
import os

# Retrieve API key and base URL from environment variables
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
polygon_base_url = os.getenv('POLYGON_BASE_URL')

# Ensure the API key and URL are set
if not POLYGON_API_KEY or not polygon_base_url:
    raise EnvironmentError("Please set the POLYGON_API_KEY and POLYGON_BASE_URL environment variables.")

# Function to fetch tickers for a given exchange
def fetch_tickers(exchange_code):
    tickers = []
    params = {
        'market': 'stocks',    # Limits to stock tickers
        'exchange': exchange_code,  # Specifies the exchange code
        'active': 'true',      # Fetches only active tickers
        'limit': 1000,         # Maximum limit per request
        'apiKey': POLYGON_API_KEY
    }

    # Loop through each letter in the alphabet to fetch batches
    for letter in string.ascii_uppercase:
        cursor = None  # Reset cursor for each letter batch

        while True:
            # Add starting ticker symbol letter filter
            params['ticker.gte'] = letter  # Start with the current letter
            params['ticker.lt'] = chr(ord(letter) + 1)  # End with the next letter

            # Add cursor if present for pagination
            if cursor:
                params['cursor'] = cursor

            # API request
            response = requests.get(polygon_base_url, params=params)
            
            # Check for request errors
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                break

            # Process data
            data = response.json()
            
            # Check if the data has results
            if 'results' in data and data['results']:
                tickers.extend([ticker['ticker'] for ticker in data['results']])
                print(f"Fetched {len(data['results'])} tickers starting with {letter} for {exchange_code}, total so far: {len(tickers)}")
            else:
                print(f"No more tickers found for {letter} or an error in fetching data.")
                break

            # Update cursor for pagination if available
            cursor = data.get('next_cursor')
            
            # Exit loop if no more pages
            if not cursor:
                break
    return tickers

# Fetch tickers for both NYSE and NASDAQ
nyse_tickers = fetch_tickers('XNYS')
nasdaq_tickers = fetch_tickers('XNAS')

# Check for duplicates between NYSE and NASDAQ lists
nyse_set = set(nyse_tickers)
nasdaq_set = set(nasdaq_tickers)
duplicates = nyse_set.intersection(nasdaq_set)

# Print duplicates, if any
if duplicates:
    print(f"Duplicate tickers found between NYSE and NASDAQ: {duplicates}")
else:
    print("No duplicate tickers found between NYSE and NASDAQ.")

# Write tickers to config file in a readable format with line breaks and minimal spacing
output_path = '/Users/kevin/Dropbox/applications/ELT/python/src/dev/config/polygon/output.py'
with open(output_path, 'w') as file:
    # Write NYSE tickers
    file.write("NYSE = [\n")
    for i, ticker in enumerate(nyse_tickers):
        if i % 10 == 0 and i != 0:
            file.write("\n")
        file.write(f"'{ticker}', ")
    file.write("\n]\n\n")

    # Write NASDAQ tickers
    file.write("NASDAQ = [\n")
    for i, ticker in enumerate(nasdaq_tickers):
        if i % 10 == 0 and i != 0:
            file.write("\n")
        file.write(f"'{ticker}', ")
    file.write("\n]\n")

print(f"Total NYSE tickers fetched: {len(nyse_tickers)}")
print(f"Total NASDAQ tickers fetched: {len(nasdaq_tickers)}")
print(f"Tickers saved to {output_path}")
