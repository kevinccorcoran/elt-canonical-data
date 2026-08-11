"""Board transition notifier: diff each qualstream-graded ticker's board column
against the last run and WhatsApp every change.

Run it on a schedule (cron/Airflow) after the ledger + gate DAGs, and once daily
(maturities change the board with no DB write):

    python -m tests.monitoring.notifier

First run seeds the snapshot silently; later runs message only real transitions.
"""
from __future__ import annotations

import datetime as dt

from tests.monitoring.board_state import BoardName, DEFAULT_HZ, graded_state

DEFAULT_SNAPSHOT_TABLE = "monitoring.board_state_snapshot"

_ICON = {"buy": "\U0001F7E2", "hold": "\U0001F7E1", "sell": "\U0001F534", "closed": "⚫"}


def _ddl(table: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table} ("
        " ticker text PRIMARY KEY,"
        " state text NOT NULL,"
        " grade numeric,"
        " seen_at timestamptz NOT NULL DEFAULT now())"
    )


def diff_states(prev: dict[str, str], cur: dict[str, BoardName]) -> list[tuple]:
    """Pure diff. Returns (ticker, old_state | None, new_state, grade | None):
    new watch-list names carry old=None; names that left the board get 'off-board'."""
    out: list[tuple] = []
    for t in sorted(cur):
        old = prev.get(t)
        if old is None:
            out.append((t, None, cur[t].state, cur[t].grade))
        elif old != cur[t].state:
            out.append((t, old, cur[t].state, cur[t].grade))
    for t in sorted(set(prev) - set(cur)):
        out.append((t, prev[t], "off-board", None))
    return out


def format_message(ticker: str, old: str | None, new: str, grade: float | None) -> str:
    g = f" ({grade:.0f})" if grade is not None else ""
    if old is None:
        return f"\U0001F7E2 {ticker}{g} entered board → {new}"
    if new == "off-board":
        return f"⚪ {ticker} left the board (was {old})"
    return f"{_ICON.get(new, '•')} {ticker}{g}: {old} → {new}"


def run_once(conn, notify_fn, *, hz: int = DEFAULT_HZ, today: dt.date | None = None,
             snapshot_table: str = DEFAULT_SNAPSHOT_TABLE, commit: bool = True) -> list[tuple]:
    """Compute -> diff vs snapshot -> notify -> persist. Returns the transitions."""
    with conn.cursor() as cur:
        cur.execute(_ddl(snapshot_table))
        cur.execute(f"SELECT ticker, state FROM {snapshot_table}")
        prev = dict(cur.fetchall())

    current = graded_state(conn, hz, today)
    transitions = [] if not prev else diff_states(prev, current)  # empty snapshot -> silent baseline

    for ticker, old, new, grade in transitions:
        notify_fn(format_message(ticker, old, new, grade))

    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {snapshot_table}")
        for ticker, name in current.items():
            cur.execute(
                f"INSERT INTO {snapshot_table} (ticker, state, grade) VALUES (%s, %s, %s)",
                (ticker, name.state, name.grade),
            )
    if commit:
        conn.commit()
    return transitions


if __name__ == "__main__":
    import os
    import pathlib

    from dotenv import load_dotenv

    load_dotenv(pathlib.Path(__file__).resolve().parents[1] / ".env")
    from tests.utilities.db import get_conn
    from tests.utilities.notify import send_whatsapp

    # every message carries the watched environment ("[dev] ...", "[prod] ..."),
    # so one phone receiving from several notifiers stays unambiguous.
    # BOARD_ENV in tests/.env; falls back to DB_NAME.
    env_label = os.getenv("BOARD_ENV") or os.getenv("DB_NAME") or "db"

    def _notify(text: str) -> bool:
        return send_whatsapp(f"[{env_label}] {text}")

    connection = get_conn()
    try:
        result = run_once(connection, _notify)
        print(f"[{env_label}] {len(result)} transition(s):")
        for tk, ol, nw, gr in result:
            print("  ", format_message(tk, ol, nw, gr))
    finally:
        connection.close()
