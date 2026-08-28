#!/usr/bin/env python3
"""Board-level gate tests against the DEV database (audit 2026-08-23).

The scenario matrix injects qs_buys by hand, so the grade >= QS_MIN_GRADE
adoption gate - the single most load-bearing rule of the 2026-08-23 policy -
had ZERO coverage, along with the delist/gate-today overrides and the
stale-exit archive. These run fixture tickers through the REAL compute_board.
No Telegram: messages are asserted as text, nothing is delivered.

  S22a grade 74 board BUY  -> in the queue, adopted
  S22b grade 68 exactly    -> in the queue (pins >=, not >)
  S22c grade 50 board BUY  -> excluded, silent
  S22d ungraded board BUY  -> excluded, silent
  S22e the same name graded 70 later -> adopts on the next pass
  S25  dismissed-while-BUY (dashboard drop) -> queued but never adopted, silent
  S28a serving gate says SELL today -> bucket sell, never queued
  S28b delisted name (raw.ticker_metadata) -> bucket sell -> tracked position
       archived + SELL ping (the ledger alone would hold it open forever)
  S28c ledger SELL older than 30d (bucket window) -> still archived via exit_of

Run inside the LOCAL airflow-scheduler container:
  docker exec -i -e DB_PASSWORD -e LOCAL_DB_USER airflow-scheduler \
      python3 - < tools/portfolio_tests/gate_board.py
"""
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, "/opt/airflow/dags")
import psycopg2
import portfolio_sync as PS

CUT = date(2024, 12, 31)      # dev walk-forward max cutoff at fut_lag 12
AMT, THR = 5.0, 50.0
FIX = {"ZGPASS", "ZGEDGE", "ZGLOW", "ZGNONE", "ZGDISS", "ZGGATE",
       "ZGDEAD", "ZGSTALE"}
# spread over the proven clusters so the 5-per-cluster cap never trims a fixture
CL = {"ZGPASS": 9, "ZGEDGE": 9, "ZGNONE": 9, "ZGLOW": 8, "ZGDISS": 8,
      "ZGGATE": 8, "ZGDEAD": 6, "ZGSTALE": 6}

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


def cleanup():
    cur.execute("""CREATE TABLE IF NOT EXISTS portfolio.reentry_pinged (
        ticker text NOT NULL, episode int NOT NULL,
        pinged_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (ticker, episode))""")
    for sql in (
        "DELETE FROM portfolio.reentry_pinged WHERE upper(ticker) LIKE 'ZG%'",
        "DELETE FROM monitoring.prediction_ledger WHERE upper(ticker) LIKE 'ZG%'",
        "DELETE FROM validation.walk_forward_ticker_rank WHERE upper(ticker) LIKE 'ZG%'",
        "DELETE FROM qual.ticker_scorecards WHERE upper(ticker) LIKE 'ZG%'",
        "DELETE FROM serving.return_cluster_ticker_global_action_current WHERE upper(ticker) LIKE 'ZG%'",
        "DELETE FROM raw.ticker_metadata WHERE upper(ticker) LIKE 'ZG%'",
        "DELETE FROM cdm.ingest_combined WHERE upper(ticker) LIKE 'ZG%'",
        "DELETE FROM portfolio.positions WHERE upper(ticker) LIKE 'ZG%'",
        "DELETE FROM portfolio.closed_positions WHERE upper(ticker) LIKE 'ZG%'",
        "DELETE FROM portfolio.state_history WHERE upper(ticker) LIKE 'ZG%'",
        "DELETE FROM portfolio.dismissed WHERE upper(ticker) LIKE 'ZG%'",
    ):
        cur.execute(sql)


cleanup()
con.commit()

# anchor on the ledger's existing latest run date so the board's D is unmoved
D0 = q("SELECT max(prediction_date) FROM monitoring.prediction_ledger")[0][0]
RUN_D = D0
# guard + freeze, same pattern as e2e_lifecycle: nothing real may side-fire
sold_priced = q("""
    SELECT upper(p.ticker) FROM portfolio.positions p
    WHERE p.sold_date IS NOT NULL
      AND EXISTS (SELECT 1 FROM cdm.ingest_combined c
                  WHERE upper(c.ticker)=upper(p.ticker))""")
assert not sold_priced, f"dev rows would auto-archive during the test: {sold_priced}"
cur.execute("""UPDATE portfolio.positions SET recoup_pinged_at = DATE '1900-01-01'
               WHERE recoup_pinged_at IS NULL AND upper(ticker) NOT LIKE 'ZG%'""")

# dev price calendar for the two archive fixtures
spy_dates = [r[0] for r in q("""SELECT DISTINCT date FROM cdm.ingest_combined
    WHERE ticker='SPY' AND date BETWEEN %s AND %s ORDER BY date""",
    (D0 - timedelta(days=30), D0))]
assert len(spy_dates) > 5, "dev SPY calendar too thin"
ENTRY = spy_dates[0]


def seed_prices(tk, start, end):
    n = len(spy_dates)
    cur.executemany(
        "INSERT INTO cdm.ingest_combined (ticker, date, adj_close, source) "
        "VALUES (%s,%s,%s,'ZGTEST')",
        [(tk, d, start + (end - start) * i / (n - 1))
         for i, d in enumerate(spy_dates)])


def seed_board(tk, action="BUY", ledger=True):
    """Proven slot + serving row (+ a D0 ledger BUY unless ledger=False)."""
    cur.execute("""INSERT INTO validation.walk_forward_ticker_rank
        (train_cutoff_date, fut_lag, ticker, cluster_id, ticker_score,
         n_weighted, rank_within_cluster, cluster_size)
        VALUES (%s, 12, %s, %s, 99999, 1, 1, 1)""", (CUT, tk, CL[tk]))
    cur.execute("""INSERT INTO serving.return_cluster_ticker_global_action_current
        (id, ticker, cluster_id, global_action) VALUES (%s, %s, %s, %s)""",
        (CL[tk], tk, CL[tk], action))
    if ledger:
        cur.execute("""INSERT INTO monitoring.prediction_ledger
            (prediction_date, ticker, global_action) VALUES (%s, %s, 'BUY')""",
            (D0, tk))


def grade_tk(tk, overall, as_of=None):
    cur.execute("""INSERT INTO qual.ticker_scorecards
        (ticker, as_of, rubric_version, veto, overall, confidence, flags,
         missing_data, model, graded_at, thesis)
        VALUES (%s, %s, 'buy_decision_v1', false, %s, 3, '[]'::jsonb,
                '[]'::jsonb, 'gate-fixture', now(), 'gate test fixture')""",
        (tk, as_of or D0 - timedelta(days=3), overall))


seed_board("ZGPASS"); grade_tk("ZGPASS", 74)
seed_board("ZGEDGE"); grade_tk("ZGEDGE", 68)          # exactly on the bar
seed_board("ZGLOW");  grade_tk("ZGLOW", 50)           # below the bar
seed_board("ZGNONE")                                  # no grade at all (yet)
seed_board("ZGDISS"); grade_tk("ZGDISS", 80)          # queued but pre-dismissed
cur.execute("INSERT INTO portfolio.dismissed (ticker) VALUES ('ZGDISS') "
            "ON CONFLICT (ticker) DO NOTHING")
seed_board("ZGGATE", action="SELL"); grade_tk("ZGGATE", 75)   # gate-today SELL
# delisted holding: open in the ledger, present in raw.ticker_metadata
seed_board("ZGDEAD"); grade_tk("ZGDEAD", 70)
seed_prices("ZGDEAD", 10.0, 13.0)
cur.execute("""INSERT INTO raw.ticker_metadata
    (ticker, name, delisting_category, delisted_utc)
    VALUES ('ZGDEAD', 'Gate Test Dead Co', 'delisted', %s)""",
    (D0 - timedelta(days=2),))
cur.execute("""INSERT INTO portfolio.positions
    (id, ticker, amount_usd, cadence, day1, start_date, mode, adopted_at, created_at)
    VALUES ('ZGDEAD-fix','ZGDEAD',%s,'once',1,%s,'model',now(),now())""",
    (AMT, ENTRY))
# stale exit: BUY then SELL > 30d before D0 - out of the bucket window.
# Both rows must sit INSIDE the ledger epoch or the walk never sees them;
# dev's ledger only reaches ~5 weeks past the epoch, so the exit goes at
# D0-31 (just past the window) and the BUY squeezes in before it.
stale_sell = D0 - timedelta(days=31)
stale_buy = max(PS.LEDGER_EPOCH + timedelta(days=1), D0 - timedelta(days=37))
assert stale_buy < stale_sell, (
    f"dev ledger too shallow for the stale-exit case "
    f"(epoch {PS.LEDGER_EPOCH}, D0 {D0})")
seed_board("ZGSTALE", ledger=False); grade_tk("ZGSTALE", 70)
seed_prices("ZGSTALE", 10.0, 8.0)
cur.execute("""INSERT INTO monitoring.prediction_ledger
    (prediction_date, ticker, global_action) VALUES (%s,'ZGSTALE','BUY')""",
    (stale_buy,))
cur.execute("""INSERT INTO monitoring.prediction_ledger
    (prediction_date, ticker, global_action) VALUES (%s,'ZGSTALE','SELL')""",
    (stale_sell,))
cur.execute("""INSERT INTO portfolio.positions
    (id, ticker, amount_usd, cadence, day1, start_date, mode, adopted_at, created_at)
    VALUES ('ZGSTALE-fix','ZGSTALE',%s,'once',1,%s,'model',now(),now())""",
    (AMT, ENTRY))
con.commit()

# ── pass 1 through the REAL board ────────────────────────────────────────────
bucket, grade, cluster, qs_buys, exit_of = PS.compute_board(cur)

check("S22a grade 74 board BUY is in the adopt queue", "ZGPASS" in qs_buys)
check("S22b grade exactly 68 is in the queue (>= not >)", "ZGEDGE" in qs_buys)
check("S22c grade 50 board BUY is excluded", "ZGLOW" not in qs_buys
      and bucket.get("ZGLOW") == "buy")
check("S22d ungraded board BUY is excluded", "ZGNONE" not in qs_buys
      and bucket.get("ZGNONE") == "buy")
check("S25a dismissed name still passes the gate into the queue",
      "ZGDISS" in qs_buys)
check("S28a gate-today SELL overrides the ledger walk (never queued)",
      bucket.get("ZGGATE") == "sell" and "ZGGATE" not in qs_buys,
      str(bucket.get("ZGGATE")))
check("S28b delisted name flips to the sell bucket",
      bucket.get("ZGDEAD") == "sell", str(bucket.get("ZGDEAD")))
check("S28c stale exit is out of the bucket but carried in exit_of",
      "ZGSTALE" not in bucket and exit_of.get("ZGSTALE") is not None,
      str((bucket.get("ZGSTALE"), exit_of.get("ZGSTALE"))))

# drive sync_core scoped to the fixtures so no real dev row is touched
zb = {t: b for t, b in bucket.items() if t in FIX}
zq = [t for t in qs_buys if t in FIX]
zx = {t: d for t, d in exit_of.items() if t in FIX}
adopts, snaps, archived, recoups, skipped, reentries = PS.sync_core(
    cur, zb, grade, cluster, zq, AMT, THR, RUN_D, dry=False, exit_of=zx)
con.commit()
msgs = PS.build_msgs(adopts, archived, recoups, AMT, grade, cluster, skipped,
                     reentries=reentries)

a_tks = {tk for _, tk in adopts}
check("S22 pass-1 adopts exactly the gate passers {ZGPASS, ZGEDGE}",
      a_tks == {"ZGPASS", "ZGEDGE"}, str(a_tks))
check("S25b dismissed name NOT adopted, no ping",
      "ZGDISS" not in a_tks and not any("ZGDISS" in m for m in msgs))
check("S22c/d below-bar and ungraded are silent",
      not any(("ZGLOW" in m or "ZGNONE" in m) for m in msgs))
arch_tks = {tk for tk, *_ in archived}
check("S28b delisted holding archived + SELL ping",
      "ZGDEAD" in arch_tks and any("SELL ZGDEAD" in m for m in msgs),
      str((arch_tks, [m for m in msgs if "ZGDEAD" in m])))
check("S28c stale-exit holding archived despite the lapsed 30d window",
      "ZGSTALE" in arch_tks, str(arch_tks))
check("S28 both archive rows landed and left the book",
      q("SELECT count(*) FROM portfolio.closed_positions "
        "WHERE upper(ticker) IN ('ZGDEAD','ZGSTALE')")[0][0] == 2
      and q("SELECT count(*) FROM portfolio.positions "
            "WHERE upper(ticker) IN ('ZGDEAD','ZGSTALE')")[0][0] == 0)
check("adopt-day snapshot written for the fresh adopts (day-0 trail)",
      {r[0] for r in q("SELECT upper(ticker) FROM portfolio.state_history "
                       "WHERE as_of=%s AND state='buy' AND why='daily sync' "
                       "AND upper(ticker) IN ('ZGPASS','ZGEDGE')", (RUN_D,))}
      == {"ZGPASS", "ZGEDGE"})

# ── pass 2: the ungraded name gets its grade -> delayed adopt ────────────────
grade_tk("ZGNONE", 70, as_of=D0)
con.commit()
bucket2, grade2, cluster2, qs_buys2, exit_of2 = PS.compute_board(cur)
check("S22e freshly graded 70 enters the queue next pass", "ZGNONE" in qs_buys2)
zb2 = {t: b for t, b in bucket2.items() if t in FIX}
zq2 = [t for t in qs_buys2 if t in FIX]
adopts2, _, _, _, _, _ = PS.sync_core(
    cur, zb2, grade2, cluster2, zq2, AMT, THR, RUN_D, dry=False,
    exit_of={t: d for t, d in exit_of2.items() if t in FIX})
con.commit()
check("S22e delayed adopt fires (ZGNONE) and nothing else re-adopts",
      {tk for _, tk in adopts2} == {"ZGNONE"}, str(adopts2))

# ── cleanup ──────────────────────────────────────────────────────────────────
cur.execute("""UPDATE portfolio.positions SET recoup_pinged_at = NULL
               WHERE recoup_pinged_at = DATE '1900-01-01'""")
cur.execute("""DELETE FROM portfolio.state_history
               WHERE as_of = %s AND why = 'daily sync'""", (RUN_D,))
cleanup()
con.commit()

print("=" * 74)
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL':4}  {name}" + (f"   [{detail}]" if detail and not ok else ""))
print("=" * 74)
print("\nfailures:", sum(1 for _, ok, _ in results if not ok))
con.close()
sys.exit(1 if any(not ok for _, ok, _ in results) else 0)
