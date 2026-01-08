import importlib.util
import logging
from pytest_tests.utilities.database_utils import Database
from tests.utilities.send_telegram_message import send_telegram_message

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to load environment variables from a specified Python file
def load_env_variables(path):
    spec = importlib.util.spec_from_file_location("env", path)
    env = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(env)
    return env

# Load environment variables from .env.py
env_path = '/Users/kevin/Dropbox/applications/ELT/.env.py'
env = load_env_variables(env_path)

def get_ticker_count():
    """Retrieve the count of tickers where open, high, or low prices are invalid."""
    # Initialize the database connection pool
    Database.initialize()
    
    # Get a connection from the pool and create a cursor
    connection = Database.get_connection()
    cursor = connection.cursor()
    
    # Execute query to count invalid ticker entries
    cursor.execute("""
        SELECT COUNT(*)
        FROM CDM.HISTORICAL_DAILY_MAIN_CLEAN
        WHERE (OPEN <= 0 OR OPEN IS NULL)
          AND (HIGH <= 0 OR HIGH IS NULL)
          AND (LOW <= 0 OR LOW IS NULL)
    """)
    
    # Fetch and handle result
    result = cursor.fetchone()
    count = result[0] if result else 0  # Default to 0 if result is None
    
    # Close resources
    cursor.close()
    Database.return_connection(connection)
    
    return count

def test_ticker_count():
    """Test that the ticker count is zero and send a Telegram message if the test fails."""
    # Retrieve Telegram credentials from environment variables
    chat_id = getattr(env, 'TELEGRAM_CHAT_ID', None)
    bot_token = getattr(env, 'TELEGRAM_BOT_TOKEN', None)

    logging.info(f"Loaded chat ID: {chat_id}, and bot token: {bot_token[:10]}...")  # Partial token for security
    
    try:
        count = get_ticker_count()
        assert count == 0, "Expected count to be 18."
    except Exception as e:
        # Prepare the failure message
        test_name = "test_ticker_count"
        error_message = str(e)
        custom_message = f"Test failed - {test_name}: {error_message}"
        
        # Send notification to Telegram
        send_telegram_message(chat_id, custom_message, bot_token)
        
        # Log the error
        logging.error(custom_message)
        
        # Raise the exception to ensure Pytest recognizes the failure
        raise
