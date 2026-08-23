#!/usr/bin/env python3
"""Scenario matrix for portfolio_sync against the DEV database.

Synthetic ZZT* tickers with synthetic prices; injected board states; asserts
the DB writes AND the exact Telegram texts (build_msgs). Cleans up after
itself. Run inside the LOCAL airflow-scheduler container (dev DB reachable at
host.docker.internal:5432, password in DB_PASSWORD):

  docker exec -i -e DB_PASSWORD -e LOCAL_DB_USER airflow-scheduler \
      python3 - < tools/portfolio_tests/scenarios.py

Covers (2026-08-21 hardening):
  S1  new buy adopted at the configured amount
  S2  hold snapshot written
  S3  hold/buy flips never ping
  S4  profit sell archived (exact P&L)
  S5  loss sell archived as loss
  S6  recoup ping at the high-water threshold + exact wording
  S7  hand-sold row archived at its stamped date
  S8  re-entry after an archived episode re-adopts
  S9  unpriced row is never archived (stays active)
  S13 gap-past-threshold recoup still pings (old two-close rule missed it)
  S14 re-run pings no duplicate (recoup_pinged_at) / dip-back stays silent
  S15 hand-sale against a live BUY dismisses -> not re-adopted next run
  S16 auto_adopt_amount unset -> adopts nothing, warns with the names
  S17 delta-safe positions save: a row another writer added mid-session
      survives a dashboard-style save (same SQL shape as db_save_positions)
  S18 delta-safe dismissed save: DAG dismissal survives dashboard-style save
  S19 partial-sale slices: final exit realizes only the remaining stake;
      recoup still fires on a partially-banked row, scaled to the rest
      (policy fix 2026-08-23: a 10% trim used to silence it forever)
  S20 ping outbox: failed sends retry and only then mark sent; total failure
      leaves the row unsent for the next run
  S21 state_history user-override rows survive the daily snapshot

Added 2026-08-23 (audit round 2):
  S23 outbox multi-message day: a mid-batch failure re-delivers NOTHING that
      already went out (sent_n cursor), and the poison day drains after the
      attempts cap with a give-up warning
  S24 weekend/no-fill entries: a position whose first fill is today (or has
      no fill at all yet) neither recoup-pings nor archives - silent skip,
      no crash
  S26 same-day trim + full exit: the suffixed slice id (~p1) and the bare-id
      final both land in closed_positions and sum to the full stake (the old
      bare-id slice collided on the (id, sold_date) PK and ON CONFLICT DO
      NOTHING silently ate the final exit's money)
"""
import os
import sys
from datetime import timedelta

sys.path.insert(0, "/opt/airflow/dags")
import psycopg2
import portfolio_sync as PS

AMT, THR = 5.0, 50.0               # Kevin's live settings

con = psycopg2.connect(host="host.docker.internal", port=5432, dbname="dev",
                       user=os.environ.get("LOCAL_DB_USER", "postgres"),
                       password=os.environ["DB_PASSWORD"])
con.autocommit = False
cur = con.cursor()


def q(sql, p=None):
    if p is None:
        cur.execute(sql)
    else:
        cur.execute(sql, p)
    try:
        return cur.fetchall()
    except psycopg2.ProgrammingError:
        return []


# ── setup ────────────────────────────────────────────────────────────────────
cur.execute("""CREATE TABLE IF NOT EXISTS portfolio.closed_positions (
    id text NOT NULL, ticker text NOT NULL, cluster_id integer, cadence text,
    amount_usd numeric, invested_usd numeric, value_usd numeric, pnl_usd numeric,
    ret_pct numeric, spy_ret_pct numeric, vs_spy_pp numeric, vs_lump_pp numeric,
    entry_date date, sold_date date NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('profit','loss')),
    archived_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (id, sold_date))""")
cur.execute("""CREATE TABLE IF NOT EXISTS portfolio.app_settings (
    key text PRIMARY KEY, value text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now())""")
cur.execute("ALTER TABLE portfolio.positions ADD COLUMN IF NOT EXISTS recoup_pinged_at date")
cur.execute("""CREATE TABLE IF NOT EXISTS portfolio.sync_runs (
    as_of date PRIMARY KEY, qs_buys text[],
    n_adopt integer, n_snap integer, n_arch integer, n_recoup integer,
    msgs jsonb, sent boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now())""")
cur.execute("ALTER TABLE portfolio.sync_runs ADD COLUMN IF NOT EXISTS sent_n integer NOT NULL DEFAULT 0")
cur.execute("ALTER TABLE portfolio.sync_runs ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0")


def cleanup():
    for sql in ("DELETE FROM cdm.ingest_combined WHERE ticker LIKE 'ZZT%'",
                "DELETE FROM portfolio.positions WHERE upper(ticker) LIKE 'ZZT%'",
                "DELETE FROM portfolio.closed_positions WHERE upper(ticker) LIKE 'ZZT%'",
                "DELETE FROM portfolio.state_history WHERE upper(ticker) LIKE 'ZZT%'",
                "DELETE FROM portfolio.dismissed WHERE upper(ticker) LIKE 'ZZT%'",
                "DELETE FROM portfolio.sync_runs WHERE as_of < DATE '2021-01-01'"):
        cur.execute(sql)


cleanup()

# dev calendar: anchor everything to the dev SPY series so fills always price
TODAY = q("SELECT max(date) FROM cdm.ingest_combined WHERE ticker='SPY'")[0][0]
ENTRY = TODAY - timedelta(days=30)
spy_dates = [r[0] for r in q("""SELECT DISTINCT date FROM cdm.ingest_combined
    WHERE ticker='SPY' AND date BETWEEN %s AND %s ORDER BY date""", (ENTRY, TODAY))]
assert len(spy_dates) > 10, f"dev SPY calendar too thin: {len(spy_dates)}"
MID = spy_dates[len(spy_dates) // 2]


def seed_prices(tk, path):
    """(start, end) linear over the SPY calendar; optional 3rd element
    overrides the second-to-last close."""
    n = len(spy_dates)
    start, end = path[0], path[1]
    px = [start + (end - start) * i / (n - 1) for i in range(n)]
    if len(path) == 3:
        px[-2] = path[2]
    cur.executemany(
        "INSERT INTO cdm.ingest_combined (ticker, date, adj_close, source) "
        "VALUES (%s,%s,%s,'ZZTEST')",
        [(tk, d, p) for d, p in zip(spy_dates, px)])


def seed_pos(tk, rid=None, sold=None, cadence="once", mode="model", sfrac=None,
             start=None):
    cur.execute("""INSERT INTO portfolio.positions
        (id, ticker, amount_usd, cadence, day1, start_date, sold_date,
         sold_fraction, mode, adopted_at, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())""",
        (rid or f"{tk}-fix", tk, AMT, cadence, ENTRY.day, start or ENTRY,
         sold, sfrac, mode))


# fixtures
seed_prices("ZZTA", (10.0, 11.0))                       # S1 adopt candidate
seed_prices("ZZTB", (10.0, 10.5)); seed_pos("ZZTB")     # S2/S3 hold flip
seed_prices("ZZTC", (10.0, 13.0)); seed_pos("ZZTC")     # S4 profit sell (+30%)
seed_prices("ZZTD", (10.0, 8.5));  seed_pos("ZZTD")     # S5 loss sell (-15%)
seed_prices("ZZTE", (10.0, 15.5, 14.8)); seed_pos("ZZTE")  # S6 recoup +55% (hw = now)
seed_prices("ZZTF", (10.0, 12.0)); seed_pos("ZZTF", sold=MID)  # S7/S15 hand-sold on live BUY
seed_prices("ZZTG", (10.0, 11.0))                       # S8 re-entry after archive
cur.execute("""INSERT INTO portfolio.closed_positions
    (id, ticker, cadence, amount_usd, invested_usd, value_usd, pnl_usd, ret_pct,
     spy_ret_pct, vs_spy_pp, vs_lump_pp, entry_date, sold_date, outcome)
    VALUES ('ZZTG-old','ZZTG','once',5,5,6,1,20,5,15,0,%s,%s,'profit')""",
    (ENTRY - timedelta(days=60), ENTRY - timedelta(days=3)))
seed_pos("ZZTH")                                        # S9 unpriced (no prices!)
seed_prices("ZZTI", (10.0, 16.0)); seed_pos("ZZTI")     # S13 both last closes > thr
seed_prices("ZZTJ", (10.0, 12.0)); seed_pos("ZZTJ")     # S14 dip-back: spike then fade
cur.execute("UPDATE cdm.ingest_combined SET adj_close = 16.0 "
            "WHERE ticker = 'ZZTJ' AND date = %s", (MID,))
seed_prices("ZZTK", (10.0, 13.0)); seed_pos("ZZTK", sfrac=0.4)  # S19 partial then sell
cur.execute("""INSERT INTO portfolio.closed_positions
    (id, ticker, cadence, amount_usd, invested_usd, value_usd, pnl_usd, ret_pct,
     spy_ret_pct, vs_spy_pp, vs_lump_pp, entry_date, sold_date, outcome)
    VALUES ('ZZTK-fix','ZZTK','once',5,2,2.6,0.6,30,5,25,0,%s,%s,'profit')""",
    (ENTRY, MID))                                        # the 40% trim slice
seed_prices("ZZTL", (10.0, 16.0)); seed_pos("ZZTL", sfrac=0.3)  # S19c recoup w/ banked note
# S26: 50% trimmed today (suffixed slice pre-archived) then hand-sold today -
# the final exit must NOT collide with the slice's (id, sold_date) key.
# bucket "hold" also exercises the hand-sell-on-HOLD dismiss (fix 2026-08-23).
seed_prices("ZZTS", (10.0, 13.0)); seed_pos("ZZTS", sold=TODAY, sfrac=0.5)
cur.execute("""INSERT INTO portfolio.closed_positions
    (id, ticker, cadence, amount_usd, invested_usd, value_usd, pnl_usd, ret_pct,
     spy_ret_pct, vs_spy_pp, vs_lump_pp, entry_date, sold_date, outcome)
    VALUES ('ZZTS-fix~p1','ZZTS','once',5,2.5,3.25,0.75,30,5,25,0,%s,%s,'profit')""",
    (ENTRY, TODAY))
# S24: weekend/no-fill shapes - first fill IS today (no recoup basis yet) /
# start after every price (hand-sold but nothing ever filled -> left active)
seed_prices("ZZTW", (10.0, 16.0)); seed_pos("ZZTW", start=TODAY)
seed_prices("ZZTV", (10.0, 12.0)); seed_pos("ZZTV", sold=TODAY,
                                            start=TODAY + timedelta(days=1))
# S21: user-override snapshot that the daily sync must NOT clobber
cur.execute("""INSERT INTO portfolio.state_history (ticker, as_of, hz, state, why)
    VALUES ('ZZTE', %s, 12, 'sell', 'sold by user')""", (TODAY,))

bucket = {"ZZTA": "buy", "ZZTB": "hold", "ZZTC": "sell", "ZZTD": "sell",
          "ZZTE": "buy", "ZZTF": "buy", "ZZTG": "buy", "ZZTH": "sell",
          "ZZTI": "buy", "ZZTJ": "buy", "ZZTK": "sell", "ZZTL": "buy",
          "ZZTS": "hold", "ZZTW": "buy", "ZZTV": "hold"}
grade   = {"ZZTA": 74.0, "ZZTG": 71.0}
cluster = {"ZZTA": 9, "ZZTG": 7}
qs_buys = ["ZZTA", "ZZTG", "ZZTF"]

adopts, snaps, archived, recoups, skipped = PS.sync_core(
    cur, bucket, grade, cluster, qs_buys, AMT, THR, TODAY, dry=False)
con.commit()
msgs = PS.build_msgs(adopts, archived, recoups, AMT, grade, cluster, skipped)

# ── asserts, pass 1 ──────────────────────────────────────────────────────────
results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))


a_tks = {tk for _, tk in adopts}
check("S1 new buy adopted (ZZTA)", "ZZTA" in a_tks)
check("S1b adopted at the configured $5",
      q("SELECT amount_usd FROM portfolio.positions WHERE upper(ticker)='ZZTA'")[0][0] == 5)
check("S8 re-entry re-adopted (ZZTG, archived before)", "ZZTG" in a_tks)
st = {r[0]: (r[1], r[2]) for r in q(
    "SELECT upper(ticker), state, why FROM portfolio.state_history "
    "WHERE as_of=%s AND upper(ticker) LIKE 'ZZT%%'", (TODAY,))}
check("S2 hold snapshot written", st.get("ZZTB", ("",))[0] == "hold", str(st.get("ZZTB")))
check("S3 no ping for hold/buy flips", not any("ZZTB" in m for m in msgs))
check("S21 user-override snapshot survives the sync",
      st.get("ZZTE") == ("sell", "sold by user"), str(st.get("ZZTE")))
arch = {tk: (rz, o) for tk, rz, o, _rid in archived}
check("S4 profit sell archived", arch.get("ZZTC", (None, ""))[1] == "profit",
      str(arch.get("ZZTC")))
check("S4b pnl ≈ +1.50$", "ZZTC" in arch and abs(arch["ZZTC"][0]["pnl"] - 1.50) < 0.01)
check("S5 loss sell archived as loss", arch.get("ZZTD", (None, ""))[1] == "loss")
check("S7 hand-sold archived at its stamped date",
      "ZZTF" in arch and q("SELECT sold_date FROM portfolio.closed_positions "
                           "WHERE ticker='ZZTF'")[0][0] == MID)
left = {r[0] for r in q("SELECT upper(ticker) FROM portfolio.positions WHERE upper(ticker) LIKE 'ZZT%'")}
check("S4/5/7 archived rows removed from active book",
      not ({"ZZTC", "ZZTD", "ZZTF", "ZZTK", "ZZTS"} & left), str(left))
check("S9 unpriced row NOT archived (stays active)", "ZZTH" in left and "ZZTH" not in arch)
check("S24a first-fill-today row does not recoup-ping (no basis yet, no crash)",
      "ZZTW" in left and "ZZTW" not in {tk for tk, *_ in recoups})
check("S24b sold row with no fills yet is left active (nothing to realize)",
      "ZZTV" in left and "ZZTV" not in arch)
rec_tks = {tk for tk, *_ in recoups}
check("S6 recoup ping at high-water (ZZTE +55% over thr 50)", "ZZTE" in rec_tks, str(recoups))
check("S6b recoup wording carries the $5 stake",
      any("ZZTE" in m and "$5.00" in m and "stake back" in m for m in msgs))
check("S13 gap-past-threshold pings (ZZTI, both closes ≥ thr - the old "
      "two-close crossing missed this)", "ZZTI" in rec_tks)
check("S14 dip-back stays silent (ZZTJ peak +60% but now +20%)", "ZZTJ" not in rec_tks)
check("S14b silent dip-back keeps its stamp unset (can ping on recovery)",
      q("SELECT recoup_pinged_at FROM portfolio.positions WHERE upper(ticker)='ZZTJ'")[0][0] is None)
check("S19 final exit realizes only the remaining stake (ZZTK: 60% of 5 = 3.0 "
      "invested, pnl 0.9)",
      "ZZTK" in arch and abs(arch["ZZTK"][0]["invested"] - 3.0) < 0.01
      and abs(arch["ZZTK"][0]["pnl"] - 0.9) < 0.01, str(arch.get("ZZTK")))
check("S19b closed slices + final exit sum to the full stake (ZZTK Σinvested = 5)",
      float(q("SELECT sum(invested_usd) FROM portfolio.closed_positions "
              "WHERE ticker='ZZTK'")[0][0]) == 5.0)
check("S19c recoup STILL fires on a partially-banked row (ZZTL +60%, 30% sold)",
      "ZZTL" in rec_tks, str(recoups))
check("S19d partial-row recoup scales to the rest + says what's banked "
      "(ZZTL ≈$3.50, 'banked 30%')",
      any("ZZTL" in m and "$3.50" in m and "banked 30%" in m for m in msgs),
      str([m for m in msgs if "ZZTL" in m]))
check("S26 same-day trim + final exit: BOTH rows in closed_positions "
      "(suffixed slice ~p1 + bare-id final, no PK collision)",
      q("SELECT count(*) FROM portfolio.closed_positions WHERE ticker='ZZTS' "
        "AND sold_date=%s", (TODAY,))[0][0] == 2)
check("S26b trim + final sum to the full stake (ZZTS Σinvested = 5)",
      float(q("SELECT sum(invested_usd) FROM portfolio.closed_positions "
              "WHERE ticker='ZZTS'")[0][0]) == 5.0)
check("S15a hand-sale on live BUY dismissed (ZZTF)",
      q("SELECT count(*) FROM portfolio.dismissed WHERE upper(ticker)='ZZTF'")[0][0] == 1)
check("S15d hand-sale on a HOLD also dismissed (ZZTS - deliberate exit, "
      "fix 2026-08-23)",
      q("SELECT count(*) FROM portfolio.dismissed WHERE upper(ticker)='ZZTS'")[0][0] == 1)
check("S15b model sells NOT dismissed (ZZTC/ZZTD/ZZTK re-enterable)",
      q("SELECT count(*) FROM portfolio.dismissed WHERE upper(ticker) IN "
        "('ZZTC','ZZTD','ZZTK')")[0][0] == 0)
closed_n = q("SELECT count(*) FROM portfolio.closed_positions WHERE upper(ticker) LIKE 'ZZT%'")[0][0]
check("closed table rows = 8 (ZZTG-old + ZZTK-slice + C/D/F + ZZTK-final + "
      "ZZTS-slice + ZZTS-final)", closed_n == 8, str(closed_n))

# ── pass 2: same day re-run (idempotence + no-dup recoup + no re-adopt) ──────
adopts2, _, archived2, recoups2, _ = PS.sync_core(
    cur, bucket, grade, cluster, qs_buys, AMT, THR, TODAY, dry=False)
con.commit()
check("S14 re-run pings no duplicate recoup (recoup_pinged_at stamps)",
      not {tk for tk, *_ in recoups2} & {"ZZTE", "ZZTI"}, str(recoups2))
check("S15c dismissed hand-sold name NOT re-adopted on the next run",
      "ZZTF" not in {tk for _, tk in adopts2}, str(adopts2))
check("pass-2 adopts nothing new (idempotent day)", not adopts2, str(adopts2))

# ── S16: auto_adopt_amount unset -> skip + warn ──────────────────────────────
a3, _, _, _, sk3 = PS.sync_core(
    cur, {"ZZTM": "buy"}, {}, {}, ["ZZTM"], None, THR, TODAY, dry=False)
m3 = PS.build_msgs(a3, [], [], None, {}, {}, sk3)
check("S16a amount unset -> no adoption", not a3 and sk3 == ["ZZTM"], str((a3, sk3)))
check("S16b warn ping names the skipped buys",
      any("NOTHING BOUGHT" in m and "ZZTM" in m for m in m3), str(m3))
check("S16c nothing inserted for the skipped name",
      q("SELECT count(*) FROM portfolio.positions WHERE upper(ticker)='ZZTM'")[0][0] == 0)

# ── S17: delta-safe positions save (db_save_positions SQL shape) ─────────────
# another writer (the DAG) inserts mid-session; the dashboard then saves its
# stale frame. The save upserts its own rows and deletes ONLY ids it knew.
cur.execute("""INSERT INTO portfolio.positions
    (id, ticker, amount_usd, cadence, day1, start_date, mode, created_at)
    VALUES ('ZZTQ-sync1','ZZTQ',5,'once',1,%s,'model',now())""", (TODAY,))
cur.execute("DELETE FROM portfolio.positions WHERE id = ANY(%s)", (["ZZTB-fix"],))
cur.execute("""INSERT INTO portfolio.positions
      (id, ticker, amount_usd, cadence, day1, start_date, mode, created_at)
    VALUES ('ZZTE-fix','ZZTE',7,'once',1,%s,'model',now())
    ON CONFLICT (id) DO UPDATE SET amount_usd = EXCLUDED.amount_usd""", (ENTRY,))
con.commit()
left17 = {r[0]: float(r[1]) for r in q(
    "SELECT id, amount_usd FROM portfolio.positions WHERE upper(ticker) LIKE 'ZZT%'")}
check("S17 mid-session writer row survives a dashboard-style save",
      "ZZTQ-sync1" in left17 and "ZZTB-fix" not in left17
      and left17.get("ZZTE-fix") == 7.0, str(left17))

# ── S18: delta-safe dismissed save ───────────────────────────────────────────
cur.execute("INSERT INTO portfolio.dismissed (ticker) VALUES ('ZZTZ') ON CONFLICT DO NOTHING")
cur.execute("DELETE FROM portfolio.dismissed WHERE upper(ticker) = ANY(%s)", (["ZZTY"],))
con.commit()
dis18 = {r[0] for r in q("SELECT upper(ticker) FROM portfolio.dismissed WHERE upper(ticker) LIKE 'ZZT%'")}
check("S18 DAG dismissal survives a dashboard-style dismissed save",
      {"ZZTF", "ZZTZ"} <= dis18, str(dis18))

# ── S20: ping outbox retry + partial failure ─────────────────────────────────
import utils.alerting as UA
_real_notify = UA.notify
calls = {"n": 0}


def flaky(m):
    calls["n"] += 1
    return calls["n"] >= 3          # attempts 1-2 fail, 3rd succeeds


cur.execute("""INSERT INTO portfolio.sync_runs (as_of, msgs, sent)
    VALUES (DATE '2020-01-01', %s::jsonb, false)
    ON CONFLICT (as_of) DO UPDATE SET msgs = EXCLUDED.msgs, sent = false""",
    ('["outbox test msg 1"]',))
con.commit()
UA.notify = flaky
try:
    PS.deliver_outbox(con)
finally:
    UA.notify = _real_notify
check("S20a flaky send retries then marks sent",
      q("SELECT sent FROM portfolio.sync_runs WHERE as_of = DATE '2020-01-01'")[0][0] is True,
      str(calls))
cur.execute("""INSERT INTO portfolio.sync_runs (as_of, msgs, sent)
    VALUES (DATE '2020-01-02', %s::jsonb, false)
    ON CONFLICT (as_of) DO UPDATE SET msgs = EXCLUDED.msgs, sent = false""",
    ('["outbox test msg 2"]',))
con.commit()
UA.notify = lambda m: False
try:
    PS.deliver_outbox(con)
finally:
    UA.notify = _real_notify
check("S20b total failure leaves the row unsent for the next run",
      q("SELECT sent FROM portfolio.sync_runs WHERE as_of = DATE '2020-01-02'")[0][0] is False)

# ── S23: multi-message day - the sent_n cursor never re-delivers ─────────────
cur.execute("UPDATE portfolio.sync_runs SET sent = true WHERE as_of = DATE '2020-01-02'")
cur.execute("""INSERT INTO portfolio.sync_runs (as_of, msgs, sent, sent_n, attempts)
    VALUES (DATE '2020-01-03', %s::jsonb, false, 0, 0)
    ON CONFLICT (as_of) DO UPDATE SET msgs = EXCLUDED.msgs, sent = false,
      sent_n = 0, attempts = 0""",
    ('["m-one", "m-two", "m-three"]',))
con.commit()
calls23 = {"n": 0}


def first_only(m):
    calls23["n"] += 1
    return calls23["n"] == 1      # msg 1 delivers; msg 2 fails all 3 tries


UA.notify = first_only
try:
    PS.deliver_outbox(con)
finally:
    UA.notify = _real_notify
row23 = q("""SELECT sent, sent_n, attempts FROM portfolio.sync_runs
             WHERE as_of = DATE '2020-01-03'""")[0]
check("S23a mid-batch failure: delivered prefix recorded, day stays unsent",
      row23[0] is False and row23[1] == 1 and row23[2] == 1, str(row23))
delivered = []
UA.notify = lambda m: (delivered.append(m), True)[1]
try:
    PS.deliver_outbox(con)
finally:
    UA.notify = _real_notify
check("S23b retry resumes AT the cursor - no duplicate of the delivered msg",
      delivered == ["m-two", "m-three"]
      and q("SELECT sent FROM portfolio.sync_runs WHERE as_of = DATE '2020-01-03'")[0][0] is True,
      str(delivered))
# poison day: one message that always fails, already at the attempts cap - it
# is abandoned (sent=true + warning) and the day behind it drains in the SAME run
cur.execute("""INSERT INTO portfolio.sync_runs (as_of, msgs, sent, sent_n, attempts)
    VALUES (DATE '2020-01-04', '["POISON msg"]'::jsonb, false, 0, 9)
    ON CONFLICT (as_of) DO UPDATE SET msgs = EXCLUDED.msgs, sent = false,
      sent_n = 0, attempts = 9""")
cur.execute("""INSERT INTO portfolio.sync_runs (as_of, msgs, sent, sent_n, attempts)
    VALUES (DATE '2020-01-05', '["healthy msg"]'::jsonb, false, 0, 0)
    ON CONFLICT (as_of) DO UPDATE SET msgs = EXCLUDED.msgs, sent = false,
      sent_n = 0, attempts = 0""")
con.commit()
UA.notify = lambda m: "POISON" not in m
try:
    PS.deliver_outbox(con)
finally:
    UA.notify = _real_notify
check("S23c poison day gives up at the attempts cap (sent=true, queue undammed)",
      q("SELECT sent FROM portfolio.sync_runs WHERE as_of = DATE '2020-01-04'")[0][0] is True)
check("S23d the day queued behind the poison drains in the same run",
      q("SELECT sent FROM portfolio.sync_runs WHERE as_of = DATE '2020-01-05'")[0][0] is True)

# ── report ───────────────────────────────────────────────────────────────────
print("=" * 72)
for name, ok, detail in results:
    print(f"{'PASS' if ok else 'FAIL':4}  {name}" + (f"   [{detail}]" if detail and not ok else ""))
print("=" * 72)
print("\nTELEGRAM MESSAGES EXACTLY AS THEY WOULD SEND (pass 1):")
for m in msgs:
    print("  ", m)
print("\nfailures:", sum(1 for _, ok, _ in results if not ok))

cleanup()
con.commit()
con.close()
sys.exit(1 if any(not ok for _, ok, _ in results) else 0)
