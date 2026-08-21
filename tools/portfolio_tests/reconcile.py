#!/usr/bin/env python3
"""Reconcile the live board vs the tracked book vs the stored parity record.

Run inside the PROD airflow-scheduler container (read-only):

  ssh root@<droplet> 'docker exec -i airflow-scheduler python3 -' \
      < tools/portfolio_tests/reconcile.py

Prints: today's capped adopt queue (fresh from compute_board), the tracked
book's buy-bucket rows, the diff between them, and - the R/Python drift check -
today's queue as portfolio_sync stored it in sync_runs.qs_buys at run time.
The queue is the ENTRY gate only: a tracked buy missing from today's queue is
normal (still endorsed, capped out or out-ranked), never an exit signal.
"""
import os
import sys

sys.path.insert(0, "/opt/airflow/dags")
import psycopg2
import portfolio_sync as PS

con = psycopg2.connect(os.environ["DATABASE_URL"])
cur = con.cursor()
bucket, grade, cluster, qs_buys = PS.compute_board(cur)
cur.execute("SELECT DISTINCT upper(ticker) FROM portfolio.positions")
tracked = {r[0] for r in cur.fetchall()}

board_buys = {t for t, b in bucket.items() if b == "buy"}
tracked_buy = sorted(tracked & board_buys)

print(f"queue (capped adopt list)          : {len(qs_buys)} -> {', '.join(qs_buys)}")
print(f"board buy-bucket total             : {len(board_buys)}")
print(f"tracked rows                       : {len(tracked)} tickers")
print(f"tracked AND in buy bucket          : {len(tracked_buy)} -> {', '.join(tracked_buy)}")
print(f"in queue but NOT tracked           : {sorted(set(qs_buys) - tracked)}")
print(f"tracked buys NOT in queue (normal) : {sorted(set(tracked_buy) - set(qs_buys))}")

cur.execute("""SELECT as_of, qs_buys, n_adopt, n_arch, n_recoup, sent
               FROM portfolio.sync_runs ORDER BY as_of DESC LIMIT 1""")
row = cur.fetchone()
if row:
    as_of, stored, n_adopt, n_arch, n_recoup, sent = row
    stored = [s.upper() for s in (stored or [])]
    print(f"\nlast sync run {as_of}: adopts={n_adopt} archives={n_arch} "
          f"recoups={n_recoup} pings_delivered={sent}")
    drift = set(stored) ^ set(qs_buys)
    if drift and str(as_of) == __import__("datetime").date.today().isoformat():
        print(f"PARITY DRIFT vs stored qs_buys     : {sorted(drift)}  <- investigate")
    else:
        print("parity vs stored qs_buys           : "
              + ("OK (match)" if not drift else f"stale run day; diff {sorted(drift)}"))
else:
    print("\nno sync_runs row yet - the DAG has not run since the outbox shipped")
con.close()
