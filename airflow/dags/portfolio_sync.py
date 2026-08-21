"""Hands-off portfolio maintenance -> portfolio.* (dashboard-owned schema).

Daily, after the ledger + gate + grades rebuild (triggered by
qual_scorecards_on_entry), this task mirrors the dashboard's auto-seed and exit
flow so the strategy-follower book maintains itself with no Generate click:

  1. ADOPT   every current validated-board BUY (the dashboard's dv$qs_buys rule:
             open ledger position + proven slot at the 12-month canonical
             horizon + monthly majority; vetoes excluded; grade ranks; max 5
             per cluster, 25 overall) that is not tracked or dismissed, as a
             one-off plan at the user's auto_adopt_amount setting.
  2. SNAPSHOT every tracked ticker's bucket to portfolio.state_history
             (ticker, as_of=today, hz=12) - the Buy->Hold->Sell trail no longer
             depends on opening the dashboard.
  3. ARCHIVE positions whose ticker flipped to SELL (and rows the user hand
             -stamped sold): frozen realized numbers into
             portfolio.closed_positions, row leaves the active book, Telegram
             ping carries the P&L and profit/loss verdict. Losses archive like
             profits (follow-the-model decision, 2026-08-21). Sync archives do
             NOT dismiss the ticker, so a genuinely new BUY episode re-enters
             on a later run; dashboard manual archives still dismiss.
  4. PING    Telegram only when Kevin must act: new adopts ("buy in Robinhood")
             and archives ("sold - P&L"). Holds/flips stay silent.

Writes ONLY the portfolio schema (authorized 2026-08-21); every model table is
read-only here. Runs only when ENV resolves to prod. Idempotent per day.
Dry-run: `python3 /opt/airflow/dags/portfolio_sync.py` inside the scheduler
container prints what it would do, writes nothing, pings nothing.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta

import pendulum

log = logging.getLogger(__name__)

LEDGER_EPOCH = date(2026, 7, 3)
HZ = 12                    # canonical horizon: board rule + grader universe
QS_CAP = 25                # overall auto-seed cap (mirrors app.R QS_CAP)
QS_CLUSTER_CAP = 5         # per-cluster cap (mirrors app.R QS_CLUSTER_CAP)
QS_MAX_AGE_DAYS = 150      # grade freshness (mirrors app.R)
SELL_RECENT_DAYS = 30      # exits older than this are history, not action items


def _connect():
    import psycopg2
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _rows(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchall()


def compute_board(cur):
    """Replicate the dashboard's 12-month board buckets from the same tables.
    Returns (bucket: {ticker: 'buy'|'hold'|'sell'}, grade: {ticker: float},
    cluster: {ticker: int}, run_dates: [date])."""
    led = _rows(cur, """
        SELECT prediction_date, upper(ticker), global_action
        FROM monitoring.prediction_ledger
        WHERE prediction_date >= %s
        ORDER BY upper(ticker), prediction_date""", (LEDGER_EPOCH,))
    run_dates = sorted({r[0] for r in led})
    if not run_dates:
        return {}, {}, {}, []
    D = run_dates[-1]

    # sticky per-ticker walk: BUY opens/reopens, SELL (while open) closes.
    # At hz=12 no entry since the epoch can have matured, so ledger-only is the
    # full walk (mirrors derivedLC's M for the current regime).
    state, buys_of, exit_of = {}, {}, {}
    cur_tk, open_, sold, exit_d = None, False, None, None
    for d, tk, a in led + [(None, None, None)]:
        if tk != cur_tk:
            if cur_tk is not None:
                state[cur_tk] = "open" if open_ else ("exited" if sold else "")
                exit_of[cur_tk] = exit_d
            cur_tk, open_, sold, exit_d = tk, False, None, None
        if tk is None:
            break
        if a == "BUY":
            open_, sold = True, None
            buys_of.setdefault(tk, []).append(d)
        elif a == "SELL" and open_:
            open_, sold, exit_d = False, True, d

    # proven slot at hz (verbatim shortlist rule: latest cutoff, bin 1 of a
    # long id, >=55% hit on >=100 obs)
    proven = {r[0].upper() for r in _rows(cur, """
        WITH mx AS (
            SELECT MAX(train_cutoff_date) AS cut
            FROM validation.walk_forward_ticker_rank WHERE fut_lag = %s
        ), wfbin AS (
            SELECT m.id AS eid, r.ticker,
                   NTILE(20) OVER (PARTITION BY m.id
                     ORDER BY r.ticker_score DESC, r.n_weighted DESC, r.ticker
                   )::int AS wf_bin
            FROM validation.walk_forward_ticker_rank r
            JOIN mx ON mx.cut = r.train_cutoff_date
            JOIN validation.walk_forward_cluster_id_map m
              ON m.train_cutoff_date = r.train_cutoff_date
             AND m.cluster_id       = r.cluster_id
            WHERE r.fut_lag = %s
              AND r.ticker_score IS NOT NULL AND r.ticker_score <> 0
        )
        SELECT DISTINCT w.ticker
        FROM wfbin w
        JOIN validation.walk_forward_pctile_summary ps
          ON ps.id = w.eid AND ps.fut_lag = %s AND ps.pctile_bin = w.wf_bin
        WHERE w.wf_bin = 1 AND w.eid <= 12
          AND ps.hit_rate >= 0.55 AND ps.n_obs >= 100""", (HZ, HZ, HZ))}

    # monthly-majority test (mirrors lc_board_trail's share_of: denominator
    # starts at the first BUY inside the trailing 30d window)
    lo = D - timedelta(days=30)

    def majority(tk):
        bw = [d for d in buys_of.get(tk, []) if lo <= d <= D]
        if not bw:
            return False
        denom = sum(1 for d in run_dates if bw[0] <= d <= D)
        return len(bw) / max(1, denom) >= 0.5

    bucket = {}
    for tk, st in state.items():
        if st == "open":
            bucket[tk] = "buy" if (tk in proven and majority(tk)) else "hold"
        elif st == "exited" and exit_of.get(tk) and \
                (D - exit_of[tk]).days <= SELL_RECENT_DAYS:
            bucket[tk] = "sell"

    grade = {r[0].upper(): float(r[1]) for r in _rows(cur, """
        SELECT DISTINCT ON (ticker) ticker, overall
        FROM qual.ticker_scorecards
        WHERE NOT veto AND rubric_version = 'buy_decision_v1'
          AND as_of >= CURRENT_DATE - %s
        ORDER BY ticker, as_of DESC, graded_at DESC""", (QS_MAX_AGE_DAYS,))}
    vetoed = {r[0].upper() for r in _rows(cur, """
        SELECT DISTINCT ON (ticker) ticker, veto
        FROM qual.ticker_scorecards
        WHERE rubric_version = 'buy_decision_v1'
        ORDER BY ticker, as_of DESC, graded_at DESC""") if r[1]}
    cluster = {r[0].upper(): int(r[1]) for r in _rows(cur, """
        SELECT upper(ticker), id
        FROM serving.return_cluster_ticker_global_action_current
        WHERE id IS NOT NULL""")}

    # persistence rank input: BUY count over the trailing 4 months (epoch floor)
    per_lo = max(D - timedelta(days=122), LEDGER_EPOCH)
    runs_of = {tk: sum(1 for d in ds if d >= per_lo)
               for tk, ds in buys_of.items()}

    # dv$qs_buys: every buy-bucket name minus vetoes, ranked grade-first then
    # persistence, capped 5/cluster then 25 overall
    cand = [tk for tk, b in bucket.items() if b == "buy" and tk not in vetoed]
    cand.sort(key=lambda t: (-(grade.get(t, float("-inf"))
                               if t in grade else float("-inf")),
                             -runs_of.get(t, 0), t))
    per_cl, qs_buys = {}, []
    for tk in cand:
        cid = cluster.get(tk, -1)
        per_cl[cid] = per_cl.get(cid, 0) + 1
        if per_cl[cid] <= QS_CLUSTER_CAP:
            qs_buys.append(tk)
        if len(qs_buys) >= QS_CAP:
            break
    return bucket, grade, cluster, qs_buys


def _px_asof(cur, tk, d, forward=False):
    op = ">=" if forward else "<="
    order = "ASC" if forward else "DESC"
    r = _rows(cur, f"""
        SELECT date, adj_close FROM cdm.ingest_combined
        WHERE upper(ticker) = %s AND date {op} %s
        ORDER BY date {order} LIMIT 1""", (tk.upper(), d))
    return (r[0][0], float(r[0][1])) if r else (None, None)


def realized(cur, tk, amount, cadence, start_d, day1, sold_d):
    """Simulated-fill realized numbers for one archived row, valued at sold_d.
    'once': single fill at first close >= start. Recurring: one fill per
    scheduled day1-of-month from start to sold (ungated approximation)."""
    fills = []
    if cadence == "once" or not cadence:
        fills = [start_d]
    else:
        step = 1
        d = date(start_d.year, start_d.month, min(day1 or 1, 28))
        if d < start_d:
            d = (d.replace(day=1) + timedelta(days=32)).replace(day=min(day1 or 1, 28))
        while d <= sold_d:
            fills.append(d)
            nxt = (d.replace(day=1) + timedelta(days=32 * step)).replace(day=min(day1 or 1, 28))
            d = nxt
    inv = tk_val = spy_val = 0.0
    first_px = None
    _, exit_px = _px_asof(cur, tk, sold_d)
    _, spy_exit = _px_asof(cur, "SPY", sold_d)
    if exit_px is None or spy_exit is None:
        return None
    entry_seen = None
    for f in fills:
        fd, px = _px_asof(cur, tk, f, forward=True)
        _, spx = _px_asof(cur, "SPY", f, forward=True)
        if px is None or spx is None or fd is None or fd > sold_d:
            continue
        if entry_seen is None:
            entry_seen, first_px = fd, px
        inv += amount
        tk_val += amount * exit_px / px
        spy_val += amount * spy_exit / spx
    if inv <= 0 or entry_seen is None:
        return None
    ret = 100 * (tk_val / inv - 1)
    spy = 100 * (spy_val / inv - 1)
    lump = 100 * (exit_px / first_px - 1)
    return dict(entry=entry_seen, invested=round(inv, 2),
                value=round(tk_val, 2), pnl=round(tk_val - inv, 2),
                ret=round(ret, 2), spy=round(spy, 2),
                vs_spy=round(ret - spy, 2), vs_lump=round(ret - lump, 2))


def run(dry=False):
    # env guard: this maintains the PROD book only (local schedulers skip)
    env = os.environ.get("ENV")
    if not env:
        try:
            from airflow.models import Variable
            env = Variable.get("ENV", default_var="dev")
        except Exception:
            env = "dev"
    if env != "prod" and not dry:
        log.info("ENV=%s - portfolio_sync only runs on prod; skipping.", env)
        return

    today = date.today()
    con = _connect()
    con.autocommit = False
    try:
        cur = con.cursor()
        bucket, grade, cluster, qs_buys = compute_board(cur)
        amt_rows = _rows(cur, "SELECT value FROM portfolio.app_settings WHERE key='auto_adopt_amount'")
        try:
            amount = float(amt_rows[0][0]) if amt_rows else 100.0
        except Exception:
            amount = 100.0
        if not (amount >= 1):
            amount = 100.0
        pos = _rows(cur, """
            SELECT id, upper(ticker), amount_usd, cadence, day1,
                   start_date, end_date, sold_date, mode
            FROM portfolio.positions""")
        dismissed = {r[0].upper() for r in _rows(cur, "SELECT ticker FROM portfolio.dismissed")}
        tracked = {r[1] for r in pos}

        # 1. ADOPT
        to_add = [tk for tk in qs_buys if tk not in tracked and tk not in dismissed]
        adopts = []
        for i, tk in enumerate(to_add, 1):
            rid = f"{tk}-sync{i}-{today.strftime('%Y%m%d')}"
            adopts.append((rid, tk))
            if not dry:
                cur.execute("""
                    INSERT INTO portfolio.positions
                      (id, ticker, amount_usd, cadence, day1, start_date,
                       mode, adopted_at, created_at)
                    VALUES (%s,%s,%s,'once',%s,%s,'model',now(),now())
                    ON CONFLICT (id) DO NOTHING""",
                    (rid, tk, amount, today.day, today))

        # 2. SNAPSHOT tracked states
        snaps = 0
        for _, tk, *_ in pos:
            st = bucket.get(tk, "closed")
            if not dry:
                cur.execute("""
                    INSERT INTO portfolio.state_history (ticker, as_of, hz, state, why, grade)
                    VALUES (%s,%s,%s,%s,'daily sync',%s)
                    ON CONFLICT (ticker, as_of, hz) DO UPDATE
                      SET state = EXCLUDED.state, why = EXCLUDED.why,
                          grade = EXCLUDED.grade, captured_at = now()""",
                    (tk, today, HZ, st, grade.get(tk)))
            snaps += 1

        # 3. ARCHIVE: model flipped to sell, or the user hand-stamped sold
        archived = []
        for rid, tk, amt, cadence, day1, start_d, end_d, sold_d, mode in pos:
            model_sell = bucket.get(tk) == "sell" and mode == "model"
            hand_sold = sold_d is not None
            if not (model_sell or hand_sold):
                continue
            exit_day = sold_d or today
            rz = realized(cur, tk, float(amt), cadence, start_d or today,
                          day1, exit_day)
            if rz is None:
                log.warning("no priced fills for %s - left active", tk)
                continue
            outcome = "profit" if rz["pnl"] > 0 else "loss"
            archived.append((tk, rz, outcome))
            if not dry:
                cur.execute("""
                    INSERT INTO portfolio.closed_positions
                      (id, ticker, cluster_id, cadence, amount_usd, invested_usd,
                       value_usd, pnl_usd, ret_pct, spy_ret_pct, vs_spy_pp,
                       vs_lump_pp, entry_date, sold_date, outcome)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id, sold_date) DO NOTHING""",
                    (rid, tk, cluster.get(tk), cadence, float(amt),
                     rz["invested"], rz["value"], rz["pnl"], rz["ret"],
                     rz["spy"], rz["vs_spy"], rz["vs_lump"], rz["entry"],
                     exit_day, outcome))
                cur.execute("DELETE FROM portfolio.positions WHERE id = %s", (rid,))

        # 3b. RECOUP crossings (ping-only, no writes): a one-off position whose
        # own return crossed the never-lose-money threshold since the previous
        # close. Audit 2026-08-21: +100% is the performance-free level (24/226
        # picks in 10y); crossings are ~top-decile events, so pings stay rare.
        thr_rows = _rows(cur, "SELECT value FROM portfolio.app_settings WHERE key='recoup_threshold_pct'")
        try:
            thr = float(thr_rows[0][0]) if thr_rows else 100.0
        except Exception:
            thr = 100.0
        if not (thr >= 10):
            thr = 100.0
        arch_tks = {a[0] for a in archived}
        recoups = []
        for rid, tk, amt, cadence, day1, start_d, end_d, sold_d, mode in pos:
            if sold_d is not None or tk in arch_tks or cadence not in (None, "once"):
                continue
            fd, fpx = _px_asof(cur, tk, start_d or today, forward=True)
            if fpx is None or fd is None or fd >= today:
                continue
            last2 = _rows(cur, """
                SELECT date, adj_close FROM cdm.ingest_combined
                WHERE upper(ticker) = %s AND date >= %s
                ORDER BY date DESC LIMIT 2""", (tk, fd))
            if len(last2) < 2:
                continue
            r_now = 100 * (float(last2[0][1]) / fpx - 1)
            r_prev = 100 * (float(last2[1][1]) / fpx - 1)
            if r_prev < thr <= r_now:
                frac = 100.0 / (1 + r_now / 100.0)
                recoups.append((tk, r_now, frac, float(amt)))

        if dry:
            con.rollback()
        else:
            con.commit()

        # 4. PING (after commit so a failed write can't produce a false ping)
        msgs = []
        if adopts:
            msgs.append("\U0001F7E2 NEW BUY adopted (" + f"${amount:g} each" + "): "
                        + ", ".join(f"{tk} (gr {grade.get(tk, '?')}"
                                    f", cl {cluster.get(tk, '?')})"
                                    for _, tk in adopts)
                        + " - buy in Robinhood")
        for tk, rz, outcome in archived:
            flag = "PROFIT \U00002705" if outcome == "profit" else "LOSS \U0000274C"
            msgs.append(f"\U0001F534 SOLD & archived: {tk} "
                        f"{rz['pnl']:+.2f}$ ({rz['ret']:+.1f}%, "
                        f"vs SPY {rz['vs_spy']:+.1f}pp) - {flag}"
                        " - sell in Robinhood if you hold it")
        for tk, r_now, frac, amt in recoups:
            msgs.append(f"\U0001F3AF {tk} hit {r_now:+.0f}% - sell ~{frac:.0f}% "
                        f"(≈${amt:.2f}) to take your stake back; "
                        "the rest is house money")
        if dry:
            print(f"DRY RUN {today} (env={env})")
            print(f"board: {sum(1 for b in bucket.values() if b == 'buy')} buy / "
                  f"{sum(1 for b in bucket.values() if b == 'hold')} hold / "
                  f"{sum(1 for b in bucket.values() if b == 'sell')} sell")
            print(f"qs_buys ({len(qs_buys)}): {', '.join(qs_buys)}")
            print(f"would adopt ({len(adopts)}): {', '.join(tk for _, tk in adopts) or '-'}")
            print(f"would snapshot: {snaps} tracked rows")
            print(f"would archive ({len(archived)}): "
                  + (", ".join(f"{tk} {rz['pnl']:+.2f}$ {o}" for tk, rz, o in archived) or "-"))
            for m in msgs:
                print("would ping:", m)
        else:
            if msgs:
                try:
                    from utils.alerting import notify
                    for m in msgs:
                        notify(m)
                except Exception:
                    log.exception("ping failed (sync itself committed fine)")
            log.info("sync done: %d adopted, %d snapshots, %d archived",
                     len(adopts), snaps, len(archived))
    finally:
        con.close()


if __name__ == "__main__":
    run(dry=True)
else:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from utils.alerting import on_failure_telegram

    with DAG(
        dag_id="portfolio_sync",
        description="Hands-off strategy-follower book: adopt new buys, snapshot states, archive sells, Telegram pings.",
        schedule=None,   # triggered by qual_scorecards_on_entry after grading
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        catchup=False,
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": on_failure_telegram,
        },
        tags=["portfolio", "daily", "hands-off"],
    ) as dag:
        PythonOperator(task_id="sync_portfolio", python_callable=run)
