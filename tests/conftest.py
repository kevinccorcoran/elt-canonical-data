import pytest
from .utilities.send_telegram_message import send_telegram_message

# Define constants for the Telegram bot
CHAT_ID = "TELEGRAM_CHAT_ID"
BOT_TOKEN = "TELEGRAM_BOT_TOKEN"

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    Hook to send a Telegram message when a test fails.
    """
    # Yield to get the test result
    outcome = yield
    report = outcome.get_result()

    # Check if the test has failed
    if report.when == 'call' and report.failed:
        message = f"Test failed: {item.nodeid}"
        try:
            send_telegram_message(CHAT_ID, message, BOT_TOKEN)
        except Exception as e:
            # Log an error message if Telegram notification fails
            print(f"Failed to send Telegram notification: {e}")