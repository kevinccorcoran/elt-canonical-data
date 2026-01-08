import requests
import logging

# Configure logging with a basic setup
# Adjust or move this line to the main script/module initialization as needed
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_telegram_message(chat_id, message, bot_token):
    """
    Send a message to a Telegram chat using a bot.

    Parameters:
        chat_id (str): ID of the Telegram chat to send the message to.
        message (str): Text message to send.
        bot_token (str): Token of the Telegram bot for authentication.

    Returns:
        response (dict): Response from the Telegram API, or None on failure.
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"  # Telegram API URL

    data = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",  # Optional: Allows Markdown formatting in the message
    }

    try:
        # Send the request to the Telegram API
        response = requests.post(url, data=data)
        response.raise_for_status()  # Raise exception for HTTP errors

        if response.status_code == 200:
            logging.info("Message sent successfully")
            return response.json()  # Return the API response as JSON
        else:
            logging.error("Failed to send message, status code: %s", response.status_code)

    except Exception as e:
        logging.error("An error occurred while sending the message: %s", e)
        return None