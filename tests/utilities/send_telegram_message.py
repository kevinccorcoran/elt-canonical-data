import requests
import logging



def send_telegram_message(chat_id, message, bot_token):
    """Send a message to a Telegram chat."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    data = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }

    try:
        response = requests.post(url, data=data)
        response.raise_for_status()

        if response.status_code == 200:
            return response.json()
        else:
            logging.error("Failed to send message, status code: %s", response.status_code)

    except Exception as e:
        logging.error("An error occurred while sending the message: %s", e)
        return None