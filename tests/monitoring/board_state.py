"""Lifecycle board state machine — Python port of scripts/app.R `derivedLC`.

Computes each board-universe ticker's current column (buy / hold / sell / closed)
straight from the database, so a notifier can watch for column changes without
the interactive R dashboard.

FAITHFULNESS: this mirrors app.R `derivedLC` — the sequential per-ticker walk
(entry, maturity, sticky sell, re-entry) and the current-decision cascade. The R
dashboard stays the source of truth; `tests/monitoring/test_board_transitions.py`
validates this reproduces the live board. If app.R's rules change, re-validate.
"""
from __future__ import annotations

import calendar
import datetime as dt
from dataclasses import dataclass

LEDGER_EPOCH = dt.date(2026, 7, 3)
QS_MIN = 68
QS_CAP = 25
DEFAULT_HZ = 12  # months; the board's default "hold length"

# --- the six inputs, same queries as app.R execute_LC (filtered to one horizon) ---
SQL_LED = """
    SELECT prediction_date AS d, ticker, global_action
    FROM monitoring.prediction_ledger
    WHERE prediction_date >= %(epoch)s
    ORDER BY ticker, prediction_date
"""
SQL_GATE = """
    SELECT ticker, global_action AS gate_today, id
    FROM serving.return_cluster_ticker_global_action_current
"""
SQL_META = """
    SELECT ticker, delisted_utc::date AS delisted_date
    FROM raw.ticker_metadata
"""
SQL_SL = """
    WITH mx AS (
        SELECT fut_lag, MAX(train_cutoff_date) AS cut
        FROM validation.walk_forward_ticker_rank
        WHERE fut_lag = %(hz)s GROUP BY fut_lag
    ), wfbin AS (
        SELECT r.fut_lag, m.id AS eid, r.ticker,
               NTILE(20) OVER (PARTITION BY r.fut_lag, m.id
                 ORDER BY r.ticker_score DESC, r.n_weighted DESC, r.ticker)::int AS wf_bin
        FROM validation.walk_forward_ticker_rank r
        JOIN mx ON mx.fut_lag = r.fut_lag AND mx.cut = r.train_cutoff_date
        JOIN validation.walk_forward_cluster_id_map m
          ON m.train_cutoff_date = r.train_cutoff_date AND m.cluster_id = r.cluster_id
        WHERE r.ticker_score IS NOT NULL AND r.ticker_score <> 0
    )
    SELECT DISTINCT ON (w.ticker) w.ticker,
           ROUND(ps.hit_rate::numeric * 100, 1) AS bin_win_pct, ps.n_obs::int AS bin_n
    FROM wfbin w
    LEFT JOIN validation.walk_forward_pctile_summary ps
      ON ps.id = w.eid AND ps.fut_lag = %(hz)s AND ps.pctile_bin = w.wf_bin
    ORDER BY w.ticker, ps.hit_rate DESC NULLS LAST
"""
SQL_COH = """
    WITH wfbin AS (
        SELECT r.train_cutoff_date, m.id AS eid, r.ticker,
               NTILE(20) OVER (PARTITION BY r.train_cutoff_date, m.id
                 ORDER BY r.ticker_score DESC, r.n_weighted DESC, r.ticker)::int AS wf_bin
        FROM validation.walk_forward_ticker_rank r
        JOIN validation.walk_forward_cluster_id_map m
          ON m.train_cutoff_date = r.train_cutoff_date AND m.cluster_id = r.cluster_id
        WHERE r.fut_lag = %(hz)s AND r.ticker_score IS NOT NULL AND r.ticker_score <> 0
          AND r.train_cutoff_date >= (CURRENT_DATE - INTERVAL '33 months')::date
    )
    SELECT w.train_cutoff_date AS d, w.ticker, w.eid AS id
    FROM wfbin w
    JOIN validation.walk_forward_pctile_summary ps
      ON ps.id = w.eid AND ps.fut_lag = %(hz)s AND ps.pctile_bin = w.wf_bin
    WHERE w.wf_bin = 1 AND w.eid <= 12 AND ps.hit_rate >= 0.55 AND ps.n_obs >= 100
      AND NOT EXISTS (SELECT 1 FROM raw.ticker_metadata tm
                      WHERE tm.ticker = w.ticker AND tm.delisted_utc::date <= w.train_cutoff_date)
"""
SQL_QS = """
    SELECT DISTINCT ON (ticker) ticker, overall AS grade
    FROM qual.ticker_scorecards
    WHERE NOT veto AND rubric_version = 'buy_decision_v1' AND as_of >= CURRENT_DATE - 150
    ORDER BY ticker, as_of DESC, graded_at DESC
"""


@dataclass(frozen=True)
class BoardName:
    ticker: str
    state: str          # buy | hold | sell | closed
    grade: float | None  # qualstream grade, None if ungraded
    entry: dt.date | None
    reason: str


def _add_months(d: dt.date, n: int) -> dt.date:
    """Calendar month add with end-of-month clamping (matches R seq(by='n months'))."""
    y = d.year + (d.month - 1 + n) // 12
    m = (d.month - 1 + n) % 12 + 1
    return dt.date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def _rows(cur, sql, params):
    cur.execute(sql, params)
    return cur.fetchall()


def compute_board_state(conn, hz: int = DEFAULT_HZ, today: dt.date | None = None) -> dict[str, BoardName]:
    """Return {ticker: BoardName} for every board-universe ticker (cohort-entered
    or proven-at-hz). Ungraded names are included; the notifier filters to graded."""
    today = today or dt.date.today()
    p = {"epoch": LEDGER_EPOCH, "hz": hz}
    with conn.cursor() as cur:
        led = _rows(cur, SQL_LED, p)                       # (d, ticker, action)
        gate = {t: g for t, g, _ in _rows(cur, SQL_GATE, p)}
        dl = {t: d for t, d in _rows(cur, SQL_META, p) if d is not None}
        prov = {t: (bw is not None and bw >= 55 and bn is not None and bn >= 100)
                for t, bw, bn in _rows(cur, SQL_SL, p)}
        coh = _rows(cur, SQL_COH, p)                        # (d, ticker, id)
        qs = {t: float(g) for t, g in _rows(cur, SQL_QS, p) if g is not None}

    coh_dates = {d for d, _t, _i in coh}
    # action per (ticker, date): ledger wins; cohort adds a BUY where the ledger is silent
    act: dict[tuple[str, dt.date], str] = {(t, d): a for d, t, a in led}
    for d, t, _i in coh:
        act.setdefault((t, d), "BUY")
    dates = sorted(coh_dates | {d for d, _t, _a in led})
    tickers = sorted({t for d, t, a in led if a == "BUY"} | {t for _d, t, _i in coh})

    def provf(t: str) -> bool:
        return prov.get(t, False)

    # --- majority share over the trailing 30 days (majf) ---
    d30 = sorted({d for d, _t, _a in led if d >= today - dt.timedelta(days=30)})
    b30: dict[str, list[dt.date]] = {}
    for d, t, a in led:
        if a == "BUY" and d >= today - dt.timedelta(days=30):
            b30.setdefault(t, []).append(d)
    share: dict[str, float] = {}
    for t, ds in b30.items():
        first = min(ds)
        denom = max(1, sum(1 for x in d30 if x >= first))
        share[t] = len(ds) / denom

    def majf(t: str) -> bool:
        return share.get(t, 0.0) >= 0.5

    # --- per-ticker walk: entry / maturity / sticky sell / re-entry ---
    entry_of: dict[str, dt.date | None] = {}
    mat_of: dict[str, dt.date | None] = {}
    reason_of: dict[str, str] = {}
    exit_of: dict[str, dt.date | None] = {}
    src_of: dict[str, str | None] = {}
    fin_of: dict[str, str] = {}
    for t in tickers:
        is_open = sold = False
        entry_d = mat_d = exit_d = None
        src = None
        reason = ""
        cur_m = ""
        dl_d = dl.get(t)
        for d in dates:
            a = act.get((t, d))
            cur_m = ""
            if not is_open and not sold:                       # flat: await entry
                if a == "BUY":
                    is_open, entry_d = True, d
                    mat_d = _add_months(d, hz)
                    cur_m, reason = "buy", "new signal"
                    src = "cohort" if d in coh_dates else "ledger"
            elif is_open and not sold:                         # held: exit triggers
                if a == "SELL":
                    sold, is_open, cur_m, reason, exit_d = True, False, "sell", "gate flipped", d
                elif (dl_d is not None and dl_d <= d) or d >= mat_d:
                    sold, is_open, cur_m = True, False, "sell"
                    reason = "delisted" if (dl_d is not None and dl_d <= d and dl_d <= mat_d) else "matured"
                    exit_d = d
                elif a == "BUY":
                    cur_m, reason = "buy", "signal continuing"
                else:
                    cur_m, reason = "hold", "maturing"
            else:                                              # sold: sticky until re-entry
                if a == "BUY":
                    is_open, sold, entry_d = True, False, d
                    mat_d = _add_months(d, hz)
                    cur_m, reason, exit_d = "buy", "re-entry", None
                    src = "cohort" if d in coh_dates else "ledger"
                else:
                    cur_m = "sell"
        entry_of[t], mat_of[t], reason_of[t], exit_of[t], src_of[t], fin_of[t] = \
            entry_d, mat_d, reason, exit_d, src, cur_m

    # --- current-decision cascade -> state_now + why (mirrors app.R why_now) ---
    out: dict[str, BoardName] = {}
    for t in tickers:
        mat_d, dl_d, g, fin = mat_of[t], dl.get(t), gate.get(t), fin_of[t]
        entry_d = entry_of[t]
        if fin == "sell":
            r = reason_of[t]
            ref = mat_d if r == "matured" else (dl_d if r == "delisted" and dl_d else exit_of[t])
            stale = ref is not None and (today - ref).days > 30
            state = "closed" if stale else "sell"
            why = f"{r} {ref}" if ref is not None else r
        elif dl_d is not None and dl_d <= today:
            state = "closed" if (today - dl_d).days > 30 else "sell"
            why = f"delisted {dl_d}"
        elif mat_d is not None and today >= mat_d:
            state = "closed" if (today - mat_d).days > 30 else "sell"
            why = f"matured {mat_d}"
        elif g == "SELL":
            state = "sell"
            why = "gate flipped (today)"
        elif majf(t) and provf(t):
            state = "buy"
            why = ("new entry" if entry_d is not None and (today - entry_d).days <= 7
                   else f"held since {entry_d}")
        else:
            state = "hold"
            d2m = (mat_d - today).days if mat_d is not None else None
            if d2m is not None and d2m <= 30:
                why = f"matures {mat_d}"
            elif majf(t):
                why = "signal live (unproven slot)"
            elif g == "BUY":
                why = "flickering (below monthly majority)"
            else:
                why = "washed out to SKIP"

        board_ok = src_of[t] == "cohort" or provf(t)
        if board_ok:                                           # table-only names never notify
            out[t] = BoardName(t, state, qs.get(t), entry_d, why)
    return out


SQL_USER_MOVES = """
    SELECT UPPER(ticker),
           CASE WHEN sold_date IS NOT NULL THEN 'sell' ELSE 'hold' END
    FROM portfolio.positions
    WHERE end_date IS NOT NULL OR sold_date IS NOT NULL
"""


def user_moves(conn) -> dict[str, str]:
    """Tracker rows the user moved by hand: end_date stamped (not sold) -> hold,
    sold_date stamped -> sell. The dashboard renders these overriding the model
    state; the notifier must watch the same board the user sees. DBs without
    the portfolio schema just contribute no overrides."""
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_USER_MOVES)
            return dict(cur.fetchall())
    except Exception:
        conn.rollback()
        return {}


def graded_state(conn, hz: int = DEFAULT_HZ, today: dt.date | None = None) -> dict[str, BoardName]:
    """Board-universe names that carry a qualstream grade >= threshold — the notifier's watch list."""
    out = {t: b for t, b in compute_board_state(conn, hz, today).items()
           if b.grade is not None and b.grade >= QS_MIN}
    moves = user_moves(conn)
    if moves:
        from dataclasses import replace
        for t, mv in moves.items():
            if t not in out:
                continue
            if mv == "hold" and out[t].state in ("sell", "closed"):
                continue   # a user pause never masks a model exit call
            if mv == "sell" and out[t].state == "closed":
                continue   # already closed is stronger than a manual sell
            out[t] = replace(out[t], state=mv,
                             reason="paused by user" if mv == "hold" else "sold by user")
    return out
