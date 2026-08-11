"""WhatsApp delivery via CallMeBot (a free relay).

To change transport (e.g. to Meta's official WhatsApp Cloud API), swap this one
file — callers only ever use send_whatsapp(text). Setup for CallMeBot:
  1. add +34 644 51 95 23 to your phone contacts
  2. WhatsApp it: "I allow callmebot to send me messages"
  3. it replies with your apikey -> put it (and your number) in tests/.env
"""
from __future__ import annotations

import logging
import os

import requests

CALLMEBOT_URL = "https://api.callmebot.com/whatsapp.php"


def send_whatsapp(text: str, *, phone: str | None = None,
                  apikey: str | None = None, timeout: int = 20) -> bool:
    """Send one WhatsApp message. Returns True if sent, False if unconfigured
    (logs a warning instead of raising, so an unconfigured cron run is harmless)."""
    phone = phone or os.getenv("WHATSAPP_PHONE")
    apikey = apikey or os.getenv("CALLMEBOT_APIKEY")
    if not phone or not apikey:
        logging.warning(
            "WhatsApp not configured (set WHATSAPP_PHONE + CALLMEBOT_APIKEY in "
            "tests/.env); message NOT sent:\n%s", text)
        return False
    resp = requests.get(
        CALLMEBOT_URL,
        params={"phone": phone, "text": text, "apikey": apikey},
        timeout=timeout,
    )
    resp.raise_for_status()
    return True
