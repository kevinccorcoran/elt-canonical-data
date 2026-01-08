import sys
import pytest

# Add project directory to sys.path for module imports
sys.path.append('/Users/kevin/Dropbox/applications/ELT')

from pytest_tests.tests.stats.crypo_main_clean.stats_test_01 import test_ticker_count as test_ticker_count_1
from tests.utilities.send_telegram_message import send_telegram_message

def run_all_tests():
    """Run all test functions and handle any assertion errors."""
    try:
        # Call the test function directly
        test_ticker_count_1()
        print("All tests passed.")
    except AssertionError as e:
        # Log test failure
        print(f"Test failed: {e}")
        # Send notification in case of failure
        send_telegram_message(
            chat_id=getattr(env, 'TELEGRAM_CHAT_ID', None),
            message=f"Test failure: {str(e)}",
            token=getattr(env, 'TELEGRAM_BOT_TOKEN', None)
        )

if __name__ == "__main__":
    run_all_tests()
