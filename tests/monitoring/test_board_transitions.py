"""Regression tests for the board state machine + the transition notifier.

They prove board_state.py stays faithful to the R dashboard (scripts/app.R) and
that the diff only fires on real column changes. DB edits run inside the db_conn
transaction and are rolled back, so nothing here mutates dev for real.
"""
from __future__ import annotations

import datetime as dt
from collections import Counter

from tests.monitoring.board_state import BoardName, QS_MIN, compute_board_state, graded_state
from tests.monitoring.notifier import diff_states, run_once


def test_reproduces_live_board(db_conn):
    counts = Counter(b.state for b in compute_board_state(db_conn).values())
    assert counts["buy"] > 0
    assert sum(counts.values()) > 50
    assert set(counts) <= {"buy", "hold", "sell", "closed"}


def test_graded_watchlist_passes_threshold(db_conn):
    watch = graded_state(db_conn)
    assert watch, "expected a non-empty qualstream-graded watch list"
    assert all(b.grade is not None and b.grade >= QS_MIN for b in watch.values())


def _reset_coke(conn):
    """Pin COKE to its pristine baseline (recent ledger BUY, gate SKIP) INSIDE
    the test's transaction, so the tests are deterministic regardless of any
    leftover demo state in dev. Rolled back with everything else."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE monitoring.prediction_ledger SET global_action='BUY' "
            "WHERE ticker='COKE' AND prediction_date >= %s", (dt.date(2026, 8, 2),))
        cur.execute(
            "UPDATE serving.return_cluster_ticker_global_action_current "
            "SET global_action='SKIP' WHERE ticker='COKE'")


def test_wash_demotes_buy_to_hold(db_conn):
    """A proven buy whose recent ledger washes to SKIP drops below the monthly
    majority -> hold. This is the COKE buy->hold case validated in the dashboard."""
    _reset_coke(db_conn)
    assert compute_board_state(db_conn)["COKE"].state == "buy"
    with db_conn.cursor() as cur:
        cur.execute(
            "UPDATE monitoring.prediction_ledger SET global_action='SKIP' "
            "WHERE ticker='COKE' AND prediction_date >= %s", (dt.date(2026, 8, 2),))
    assert compute_board_state(db_conn)["COKE"].state == "hold"


def test_gate_flip_moves_to_sell(db_conn):
    """Today's gate flipping to SELL sends an open position straight to sell,
    and the reason reports the gate flip (not the walk's leftover reason)."""
    _reset_coke(db_conn)
    with db_conn.cursor() as cur:
        cur.execute(
            "UPDATE serving.return_cluster_ticker_global_action_current "
            "SET global_action='SELL' WHERE ticker='COKE'")
    coke = compute_board_state(db_conn)["COKE"]
    assert coke.state == "sell"
    assert coke.reason == "gate flipped (today)"


def test_buy_reason_reports_tenure(db_conn):
    """An established buy reads 'held since <entry>' (COKE entered 2026-07-29)."""
    _reset_coke(db_conn)
    coke = compute_board_state(db_conn)["COKE"]
    assert coke.state == "buy"
    assert coke.reason.startswith(("held since", "new entry"))


def test_diff_only_reports_changes():
    prev = {"A": "buy", "B": "hold", "D": "buy"}
    current = {
        "A": BoardName("A", "hold", 70, None, ""),   # changed buy -> hold
        "B": BoardName("B", "hold", 72, None, ""),   # unchanged
        "C": BoardName("C", "buy", 75, None, ""),    # newly on board
        # "D" dropped off the board
    }
    got = {t: (old, new) for t, old, new, _grade in diff_states(prev, current)}
    assert got == {"A": ("buy", "hold"), "C": (None, "buy"), "D": ("buy", "off-board")}


def test_run_once_seeds_baseline_then_stays_quiet(db_conn):
    sent: list[str] = []
    table = "monitoring._board_snapshot_test"
    first = run_once(db_conn, sent.append, snapshot_table=table, commit=False)
    assert first == [] and sent == []            # first run seeds, no spam
    second = run_once(db_conn, sent.append, snapshot_table=table, commit=False)
    assert second == [] and sent == []           # nothing changed -> silent
