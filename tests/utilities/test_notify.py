"""send_whatsapp must read CallMeBot's body: the API answers HTTP 200 even for
failures (bad apikey, rate limit) — only "Message queued" means accepted."""
from __future__ import annotations

import tests.utilities.notify as notify


class _Resp:
    def __init__(self, text: str, status: int = 200):
        self.text = text
        self.status_code = status

    def raise_for_status(self):
        pass


def _patch(monkeypatch, body: str):
    monkeypatch.setattr(notify.requests, "get", lambda *a, **k: _Resp(body))
    monkeypatch.setenv("WHATSAPP_PHONE", "31600000000")
    monkeypatch.setenv("CALLMEBOT_APIKEY", "x")


def test_queued_body_is_success(monkeypatch):
    _patch(monkeypatch, "<b>Message queued.</b> You will receive it in a few seconds.")
    assert notify.send_whatsapp("hi") is True


def test_error_body_is_failure(monkeypatch):
    _patch(monkeypatch, "APIKey is invalid")
    assert notify.send_whatsapp("hi") is False


def test_unconfigured_returns_false(monkeypatch):
    monkeypatch.delenv("WHATSAPP_PHONE", raising=False)
    monkeypatch.delenv("CALLMEBOT_APIKEY", raising=False)
    assert notify.send_whatsapp("hi") is False
