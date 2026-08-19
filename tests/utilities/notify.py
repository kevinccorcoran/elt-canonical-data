"""Notification delivery via a Telegram bot.

Callers only ever use send_notification(text). One-time setup (~2 min, no account
signup, no template approval, no phone verification):
  1. In Telegram, message @BotFather -> /newbot -> pick a name + a username ->
     it replies with a bot token.
  2. Open your new bot and tap Start (send it any message) so a chat exists.
  3. put the token in tests/.env as TELEGRAM_BOT_TOKEN. The chat id is discovered
     from getUpdates on first send; set TELEGRAM_CHAT_ID to pin it (recommended,
     and required for the Airflow failure alert which does not auto-discover).

Telegram bots send free-form text to any user who has started them -- no template,
no 24h customer-service window. Only trade-off vs WhatsApp: it's Telegram.
"""
from __future__ import annotations

import logging
import os

import requests

_API = "https://api.telegram.org/bot{token}/{method}"


def _resolve_chat_id(token: str, timeout: int) -> str | None:
    """Best-effort chat id from the most recent update the bot received
    (getUpdates). Works once you've tapped Start. Pin TELEGRAM_CHAT_ID to skip
    this -- getUpdates only returns the last ~24h and clashes with a webhook."""
    try:
        resp = requests.get(_API.format(token=token, method="getUpdates"),
                            timeout=timeout)
        data = resp.json()
    except (requests.RequestException, ValueError):
        return None
    for upd in reversed(data.get("result", []) or []):
        chat = ((upd.get("message") or upd.get("my_chat_member") or {})
                .get("chat") or {})
        if chat.get("id") is not None:
            return str(chat["id"])
    return None


def send_notification(text: str, *, chat_id: str | None = None,
                      token: str | None = None, timeout: int = 20) -> bool:
    """Send one Telegram message. Returns True if delivered, False if unconfigured
    or rejected (logs a warning instead of raising, so an unconfigured cron run is
    harmless)."""
    token = token or os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not token:
        logging.warning(
            "Telegram not configured (set TELEGRAM_BOT_TOKEN in tests/.env); "
            "message NOT sent:\n%s", text)
        return False
    if not chat_id:
        chat_id = _resolve_chat_id(token, timeout)
        if not chat_id:
            logging.warning(
                "Telegram chat id unknown -- message the bot once (tap Start), or "
                "set TELEGRAM_CHAT_ID in tests/.env; message NOT sent:\n%s", text)
            return False

    resp = requests.post(
        _API.format(token=token, method="sendMessage"),
        json={"chat_id": chat_id, "text": text},
        timeout=timeout,
    )
    # Bot API: HTTP 200 + {"ok": true, ...} on success; otherwise
    # {"ok": false, "description": ...}. Don't raise -- log & return.
    try:
        body = resp.json()
    except ValueError:
        body = {}
    if resp.status_code == 200 and body.get("ok"):
        return True
    logging.warning("Telegram did not send (HTTP %s): %s",
                    resp.status_code, body.get("description") or resp.text[:200])
    return False
