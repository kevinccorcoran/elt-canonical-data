"""send_notification posts to the Telegram Bot API and reports delivery from the
response: HTTP 200 + {"ok": true} means sent; anything else means it was not."""
from __future__ import annotations

import json as _json

import tests.utilities.notify as notify


class _Resp:
    def __init__(self, payload, status: int = 200):
        self._payload = payload
        self.status_code = status
        self.text = _json.dumps(payload)

    def json(self):
        return self._payload


def _capture_post(monkeypatch, resp):
    sent = {}

    def _post(url, *, json=None, timeout=None):
        sent.update(url=url, json=json)
        return resp

    monkeypatch.setattr(notify.requests, "post", _post)
    return sent


def test_success_returns_true(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:ABC")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "555")
    sent = _capture_post(monkeypatch, _Resp({"ok": True, "result": {"message_id": 1}}))
    assert notify.send_notification("hi") is True
    assert sent["json"] == {"chat_id": "555", "text": "hi"}
    assert sent["url"].endswith("/sendMessage")


def test_api_error_is_failure(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:ABC")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "555")
    _capture_post(monkeypatch, _Resp({"ok": False, "description": "chat not found"}, status=400))
    assert notify.send_notification("hi") is False


def test_unconfigured_returns_false(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    assert notify.send_notification("hi") is False


def test_chat_id_autodiscovered_from_getupdates(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:ABC")
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    def _get(url, *, timeout=None):
        return _Resp({"ok": True, "result": [
            {"update_id": 1, "message": {"chat": {"id": 777}}}]})

    monkeypatch.setattr(notify.requests, "get", _get)
    sent = _capture_post(monkeypatch, _Resp({"ok": True, "result": {"message_id": 2}}))
    assert notify.send_notification("hi") is True
    assert sent["json"]["chat_id"] == "777"
