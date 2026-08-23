#!/usr/bin/env python3
"""DEV end-to-end lifecycle audit: one bogus company (ZZX) rides the REAL
pipeline - ledger emissions -> compute_board bucket math -> sync_core ->
outbox -> live Telegram pings - through BUY -> buy -> HOLD (after gain,
recoup ping) -> BUY again -> HOLD again -> SELL (archive ping).

Real pings arrive on the phone tagged [DEV E2E ... do NOT trade]. Model-table
fixtures are removed afterwards; the visible outcome (ZZX Realized row +
transition trail) stays in dev's portfolio schema for inspection.
"""
import os
import sys
from datetime import date

sys.path.insert(0, "/opt/airflow/dags")
import json

import psycopg2
import portfolio_sync as PS
from utils.alerting import notify

D = [date(2026, 8, 12), date(2026, 8, 13), date(2026, 8, 14),
     date(2026, 8, 17), date(2026, 8, 18), date(2026, 8, 19)]
PX = {D[0]: 10.0, D[1]: 13.0, D[2]: 16.0, D[3]: 14.5, D[4]: 14.5, D[5]: 15.0}
LEDGER = {D[0]: ("ZZX", "BUY"), D[1]: ("ZZXMARK", "MARK"), D[2]: ("ZZXMARK", "MARK"),
          D[3]: ("ZZX", "BUY"), D[4]: ("ZZXMARK", "MARK"), D[5]: ("ZZX", "SELL")}
EXPECT = {D[0]: "buy", D[1]: "buy", D[2]: "hold", D[3]: "buy", D[4]: "hold", D[5]: "sell"}
LABEL = {D[0]: "day 1/6: new company enters BUY", D[1]: "day 2/6",
         D[2]: "day 3/6: HOLD after gain", D[3]: "day 4/6: back to BUY",
         D[4]: "day 5/6: back to HOLD", D[5]: "day 6/6: model SELL"}
CUT = date(2024, 12, 31)   # dev walk-forward max cutoff at fut_lag 12
EID_CLUSTER = 9            # cluster 9 -> eid 9 (hit .76, n 910: passes the slot)

con = psycopg2.connect(host="host.docker.internal", port=5432, dbname="dev",
                       user=os.environ.get("LOCAL_DB_USER", "postgres"),
                       password=os.environ["DB_PASSWORD"])
con.autocommit = False
cur = con.cursor()


def q(sql, p=None):
    cur.execute(sql, p) if p is not None else cur.execute(sql)
    try:
        return cur.fetchall()
    except psycopg2.ProgrammingError:
        return []


results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))


def cleanup_model():
    for sql in (
        "DELETE FROM monitoring.prediction_ledger WHERE ticker IN ('ZZX','ZZXMARK')",
        "DELETE FROM validation.walk_forward_ticker_rank WHERE upper(ticker)='ZZX'",
        "DELETE FROM qual.ticker_scorecards WHERE upper(ticker)='ZZX'",
        "DELETE FROM serving.return_cluster_ticker_global_action_current WHERE upper(ticker)='ZZX'",
        "DELETE FROM cdm.ingest_combined WHERE ticker='ZZX'",
    ):
        cur.execute(sql)
    cur.execute("""DELETE FROM portfolio.state_history
                   WHERE as_of BETWEEN %s AND %s AND why='daily sync'
                     AND upper(ticker) <> 'ZZX'""", (D[0], D[-1]))
    cur.execute("DELETE FROM portfolio.sync_runs WHERE as_of BETWEEN %s AND %s", (D[0], D[-1]))
    cur.execute("""UPDATE portfolio.positions SET recoup_pinged_at = NULL
                   WHERE recoup_pinged_at = DATE '1900-01-01'""")


def full_reset():
    cleanup_model()
    for sql in (
        "DELETE FROM portfolio.positions WHERE upper(ticker) IN ('ZZX','ZZXMARK')",
        "DELETE FROM portfolio.closed_positions WHERE upper(ticker) IN ('ZZX','ZZXMARK')",
        "DELETE FROM portfolio.state_history WHERE upper(ticker) IN ('ZZX','ZZXMARK')",
        "DELETE FROM portfolio.dismissed WHERE upper(ticker) IN ('ZZX','ZZXMARK')",
    ):
        cur.execute(sql)


full_reset()

# ── guards: no dev row may side-fire during the sim ──────────────────────────
sold_priced = q("""
    SELECT upper(p.ticker) FROM portfolio.positions p
    WHERE p.sold_date IS NOT NULL
      AND EXISTS (SELECT 1 FROM cdm.ingest_combined c
                  WHERE upper(c.ticker)=upper(p.ticker))""")
assert not sold_priced, f"dev rows would auto-archive during the sim: {sold_priced}"
# freeze real rows out of the recoup scanner for the duration (restored in cleanup)
cur.execute("""UPDATE portfolio.positions SET recoup_pinged_at = DATE '1900-01-01'
               WHERE recoup_pinged_at IS NULL AND upper(ticker) <> 'ZZX'""")

# ── static fixtures: settings + the board-eligibility rows ───────────────────
notnull = {r[0] for r in q("""SELECT column_name FROM information_schema.columns
    WHERE table_schema='validation' AND table_name='walk_forward_ticker_rank'
      AND is_nullable='NO'""")}
assert notnull <= {"train_cutoff_date", "fut_lag", "ticker", "cluster_id",
                   "ticker_score", "n_weighted", "rank_within_cluster",
                   "cluster_size"}, f"unexpected NOT NULLs: {notnull}"
cur.execute("""INSERT INTO portfolio.app_settings (key, value) VALUES
    ('auto_adopt_amount','5'), ('recoup_threshold_pct','50')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()""")
cur.execute("""INSERT INTO validation.walk_forward_ticker_rank
    (train_cutoff_date, fut_lag, ticker, cluster_id, ticker_score, n_weighted,
     rank_within_cluster, cluster_size)
    VALUES (%s, 12, 'ZZX', %s, 99999, 1, 1, 1)""", (CUT, EID_CLUSTER))
cur.execute("""INSERT INTO qual.ticker_scorecards
    (ticker, as_of, rubric_version, veto, overall, confidence, flags,
     missing_data, model, graded_at, thesis)
    VALUES ('ZZX', %s, 'buy_decision_v1', false, 74, 3, '[]'::jsonb,
            '[]'::jsonb, 'e2e-fixture', now(), 'bogus e2e company')""",
    (date(2026, 8, 10),))
cur.execute("""INSERT INTO serving.return_cluster_ticker_global_action_current
    (id, ticker, cluster_id, global_action) VALUES (%s, 'ZZX', %s, 'BUY')""",
    (EID_CLUSTER, EID_CLUSTER))
con.commit()

# ── the six simulated days ───────────────────────────────────────────────────
day_log = []
for day in D:
    cur.execute("INSERT INTO cdm.ingest_combined (ticker, date, adj_close, source) "
                "VALUES ('ZZX', %s, %s, 'ZZXE2E')", (day, PX[day]))
    tk, act = LEDGER[day]
    cur.execute("INSERT INTO monitoring.prediction_ledger "
                "(prediction_date, ticker, global_action) VALUES (%s,%s,%s)",
                (day, tk, act))
    con.commit()

    bucket, grade, cluster, qs_buys, exit_of = PS.compute_board(cur)
    check(f"{day} board state = {EXPECT[day]}", bucket.get("ZZX") == EXPECT[day],
          f"got {bucket.get('ZZX')!r}")
    if day == D[0]:
        check(f"{day} ZZX in the real adopt queue", "ZZX" in qs_buys,
              str(qs_buys[:12]))

    # scope the drive to ZZX so the sim never archives/adopts real dev rows;
    # bucket math above stays the real, unscoped computation
    zb = {"ZZX": bucket["ZZX"]} if "ZZX" in bucket else {}
    zq = [t for t in qs_buys if t == "ZZX"]
    amt_rows = q("SELECT value FROM portfolio.app_settings WHERE key='auto_adopt_amount'")
    amount = float(amt_rows[0][0]) if amt_rows else None
    thr_rows = q("SELECT value FROM portfolio.app_settings WHERE key='recoup_threshold_pct'")
    thr = float(thr_rows[0][0]) if thr_rows else 100.0

    adopts, snaps, archived, recoups, skipped = PS.sync_core(
        cur, zb, grade, cluster, zq, amount, thr, day, dry=False)
    if day == D[0] and adopts:
        # same-day re-run (idempotent) so the fresh adopt gets its day-1 snapshot
        PS.sync_core(cur, zb, grade, cluster, zq, amount, thr, day, dry=False)

    msgs = PS.build_msgs(adopts, archived, recoups, amount, grade, cluster, skipped)
    tag = f"[DEV E2E {LABEL[day]} - simulation, do NOT trade] "
    msgs = [tag + m for m in msgs]
    cur.execute("""INSERT INTO portfolio.sync_runs
          (as_of, qs_buys, n_adopt, n_snap, n_arch, n_recoup, msgs, sent)
        VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
        ON CONFLICT (as_of) DO UPDATE SET msgs = EXCLUDED.msgs, sent = EXCLUDED.sent""",
        (day, zq, len(adopts), snaps, len(archived), len(recoups),
         json.dumps(msgs), not msgs))
    con.commit()
    PS.deliver_outbox(con)
    sent = q("SELECT sent FROM portfolio.sync_runs WHERE as_of=%s", (day,))[0][0]
    day_log.append((day, bucket.get("ZZX"), [m[len(tag):] for m in msgs], sent))

    if day == D[0]:
        check("day 1 adopted at the $5 setting",
              q("SELECT amount_usd FROM portfolio.positions WHERE upper(ticker)='ZZX'")[0][0] == 5)
        check("day 1 adopt pinged", bool(msgs) and "BUY in Robinhood" in msgs[0] and sent is True)
    if day == D[1]:
        check("day 2 silent (gain +30% below recoup 50)", not msgs)
    if day == D[2]:
        check("day 3 recoup pinged at +60% while flipping to hold",
              any("stake back" in m for m in msgs) and sent is True, str(msgs))
        check("day 3 recoup stamped once-per-position",
              q("SELECT recoup_pinged_at FROM portfolio.positions "
                "WHERE upper(ticker)='ZZX'")[0][0] == day)
    if day == D[3]:
        check("day 4 back-to-BUY is silent (held row, no re-adopt)", not msgs)
        check("day 4 no duplicate recoup after the fade", not recoups)
    if day == D[4]:
        check("day 5 back-to-HOLD is silent", not msgs)
    if day == D[5]:
        check("day 6 SELL pinged with the P&L verdict",
              any("sell all of it" in m and "PROFIT" in m for m in msgs) and sent is True,
              str(msgs))

# ── outcome asserts ──────────────────────────────────────────────────────────
row = q("""SELECT invested_usd, value_usd, pnl_usd, ret_pct, outcome, entry_date, sold_date
           FROM portfolio.closed_positions WHERE upper(ticker)='ZZX'""")
check("archived once with exact realized numbers ($5 -> $7.50, +50%)",
      len(row) == 1 and abs(float(row[0][0]) - 5) < 0.01
      and abs(float(row[0][1]) - 7.5) < 0.01 and abs(float(row[0][2]) - 2.5) < 0.01
      and abs(float(row[0][3]) - 50) < 0.1 and row[0][4] == "profit"
      and row[0][5] == D[0] and row[0][6] == D[5], str(row))
check("position left the active book",
      q("SELECT count(*) FROM portfolio.positions WHERE upper(ticker)='ZZX'")[0][0] == 0)
check("model sell NOT dismissed (re-entry stays possible)",
      q("SELECT count(*) FROM portfolio.dismissed WHERE upper(ticker)='ZZX'")[0][0] == 0)
trail = [(str(r[0]), r[1]) for r in q(
    "SELECT as_of, state FROM portfolio.state_history WHERE upper(ticker)='ZZX' ORDER BY as_of")]
check("full transition trail buy,buy,hold,buy,hold,sell",
      [s for _, s in trail] == ["buy", "buy", "hold", "buy", "hold", "sell"], str(trail))

# ── recap ping + report ──────────────────────────────────────────────────────
recap = ("[DEV E2E recap - no action needed] ZZX lifecycle audit done. "
         "d1 new BUY -> adopted @$5 (PINGED) | d2 buy, +30% (silent) | "
         "d3 HOLD after +60% gain -> recoup PINGED (sell ~62%, stake back) | "
         "d4 back to BUY (silent: already held, flips never re-adopt) | "
         "d5 back to HOLD (silent) | d6 SELL -> archived +$2.50 PROFIT (PINGED). "
         "Silent days are by design: a held name flipping buy<->hold needs no "
         "action from you. See Dev dashboard: Realized row + transition trail.")
recap_ok = notify(recap)
check("recap ping delivered", recap_ok)

cleanup_model()
con.commit()

print("=" * 74)
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL':4}  {name}" + (f"   [{detail}]" if detail and not ok else ""))
print("=" * 74)
print("\nDAY LOG (state | pinged?):")
for day, state, m, sent in day_log:
    print(f"  {day}  {state:>4}  " + ("; ".join(m) if m else "(silent)")
          + ("" if not m else f"   [delivered={sent}]"))
print("\nfailures:", sum(1 for _, ok, _ in results if not ok))
con.close()
sys.exit(1 if any(not ok for _, ok, _ in results) else 0)
