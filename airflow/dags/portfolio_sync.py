"""Hands-off portfolio maintenance -> portfolio.* (dashboard-owned schema).

Daily, after the ledger + gate + grades rebuild (triggered by
qual_scorecards_on_entry), this task mirrors the dashboard's auto-seed and exit
flow so the strategy-follower book maintains itself with no Generate click:

  1. ADOPT   every current validated-board BUY (the dashboard's dv$qs_buys rule:
             open ledger position + proven slot at the 12-month canonical
             horizon + monthly majority; vetoes excluded; grade ranks; max 5
             per cluster, 25 overall) that is not tracked, not dismissed and
             has NO closed model episode, as a one-off plan at the user's
             auto_adopt_amount setting. A name already round-tripped once
             (closed_positions row exists) is NEVER auto-re-added (policy
             2026-08-28): it pings once per episode ("2nd time around", with
             the last round's result) and waits for a manual Adopt.
  2. SNAPSHOT every tracked ticker's bucket to portfolio.state_history
             (ticker, as_of=today, hz=12) - the Buy->Hold->Sell trail no longer
             depends on opening the dashboard.
  3. ARCHIVE positions whose ticker flipped to SELL (and rows the user hand
             -stamped sold): frozen realized numbers into
             portfolio.closed_positions (scaled to the remaining stake when
             partial sales were recorded via sold_fraction), row leaves the
             active book, Telegram ping carries the P&L verdict. Losses archive
             like profits (follow-the-model decision, 2026-08-21). Model-sell
             archives do NOT dismiss, but the closed episode blocks any
             auto-re-adopt (step 1): a later re-fire is announced, not
             bought. A hand-sale AGAINST a live BUY does dismiss, so a
             deliberately exited name is not even announced again.
  3b RECOUP  one ping per position (recoup_pinged_at) when it has EVER closed
             >= recoup_threshold_pct above entry and still sits near it -
             high-water based, so outages and gap-ups can't lose the event.
  4. PING    via a durable outbox (portfolio.sync_runs.msgs/sent): messages are
             committed with the day's writes, then delivered with retries;
             a Telegram outage delays pings, never loses them. sync_runs also
             logs every run (deadman + qs_buys parity record). If
             auto_adopt_amount is unset, nothing adopts at a default - the ping
             says which names were skipped and why.

Writes ONLY the portfolio schema (authorized 2026-08-21); every model table is
read-only here. Runs only when ENV resolves to prod. Idempotent per day.
Dry-run: `python3 /opt/airflow/dags/portfolio_sync.py` inside the scheduler
container prints what it would do, writes nothing, pings nothing.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta

import pendulum

log = logging.getLogger(__name__)

LEDGER_EPOCH = date(2026, 7, 3)
HZ = 12                    # canonical horizon: board rule + grader universe
QS_CAP = 25                # overall auto-seed cap (mirrors app.R QS_CAP)
QS_CLUSTER_CAP = 5         # per-cluster cap (mirrors app.R QS_CLUSTER_CAP)
QS_MIN_GRADE = 68          # adoption gate (mirrors app.R QS_MIN): only
                           # qualstream passers enter the queue - Kevin's
                           # policy call 2026-08-23; ungraded/below-bar names
                           # stay board-visible but are never adopted
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
    cluster: {ticker: int}, qs_buys: [ticker], exit_of: {ticker: date}).
    exit_of carries every ledger exit regardless of age - the 30d
    SELL_RECENT_DAYS window shapes only `bucket`, so sync_core can still
    archive a tracked name whose exit scrolled out of the window."""
    led = _rows(cur, """
        SELECT prediction_date, upper(ticker), global_action
        FROM monitoring.prediction_ledger
        WHERE prediction_date >= %s
        ORDER BY upper(ticker), prediction_date""", (LEDGER_EPOCH,))
    run_dates = sorted({r[0] for r in led})
    if not run_dates:
        return {}, {}, {}, [], {}
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
            open_, sold, exit_d = True, None, None   # a re-entry voids the old exit
            buys_of.setdefault(tk, []).append(d)
        elif a == "SELL" and open_:
            open_, sold, exit_d = False, True, d

    # proven slot at hz (verbatim shortlist rule: latest cutoff, bin 1 of a
    # long id, >=55% hit on >=100 obs). The eid rides along as the cluster-id
    # fallback below (mirrors app.R's cohort augment of id_seed).
    proven_rows = _rows(cur, """
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
        SELECT DISTINCT ON (w.ticker) w.ticker, w.eid
        FROM wfbin w
        JOIN validation.walk_forward_pctile_summary ps
          ON ps.id = w.eid AND ps.fut_lag = %s AND ps.pctile_bin = w.wf_bin
        WHERE w.wf_bin = 1 AND w.eid <= 12
          AND ps.hit_rate >= 0.55 AND ps.n_obs >= 100
        ORDER BY w.ticker, w.eid""", (HZ, HZ, HZ))
    proven = {r[0].upper() for r in proven_rows}
    proven_id = {r[0].upper(): int(r[1]) for r in proven_rows}

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

    # Dashboard parity (audit 2026-08-23): the ledger walk alone never sees a
    # delisting - a dead name records no SELL row, so its position would sit
    # "open" forever, never archived, never pinged - and a same-day gate flip
    # only reaches the ledger tomorrow. Mirror app.R's state_now overrides:
    # presence in raw.ticker_metadata = delisted; today's serving gate SELL
    # wins over the walk. Flipping to "sell" both archives a tracked name and
    # blocks adopting one the dashboard would already mark as an exit.
    delisted = {r[0].upper() for r in _rows(
        cur, "SELECT ticker FROM raw.ticker_metadata")}
    gate_now = {r[0].upper(): r[1] for r in _rows(cur, """
        SELECT ticker, global_action
        FROM serving.return_cluster_ticker_global_action_current""")}
    for tk, b in list(bucket.items()):
        if b in ("buy", "hold") and (tk in delisted or gate_now.get(tk) == "SELL"):
            bucket[tk] = "sell"

    # overall is NOT NULL by schema; the guard is free insurance so one odd
    # grade row could never take down the whole daily sync (float(None)).
    grade = {r[0].upper(): float(r[1]) for r in _rows(cur, """
        SELECT DISTINCT ON (ticker) ticker, overall
        FROM qual.ticker_scorecards
        WHERE NOT veto AND rubric_version = 'buy_decision_v1'
          AND overall IS NOT NULL
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
    for tk, eid in proven_id.items():
        cluster.setdefault(tk, eid)   # cohort fallback, same as app.R id_seed

    # persistence rank input: BUY count over the trailing 4 months (epoch floor)
    per_lo = max(D - timedelta(days=122), LEDGER_EPOCH)
    runs_of = {tk: sum(1 for d in ds if d >= per_lo)
               for tk, ds in buys_of.items()}

    # dv$qs_buys (policy 2026-08-23): buy-bucket names that PASS qualstream
    # (grade >= QS_MIN_GRADE, not vetoed), ranked grade-first then
    # persistence, capped 5/cluster then 25 overall
    cand = [tk for tk, b in bucket.items()
            if b == "buy" and tk not in vetoed
            and grade.get(tk, 0) >= QS_MIN_GRADE]
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
    return bucket, grade, cluster, qs_buys, exit_of


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


def sync_core(cur, bucket, grade, cluster, qs_buys, amount, thr, today, dry,
              exit_of=None):
    """Steps 1-3b against an open cursor: adopt, snapshot, archive, recoup.
    Board and settings are injected so the scenario harness can drive this
    against dev with synthetic states; run() drives it with the real board.
    amount=None means auto_adopt_amount is UNSET: adoption is skipped (never a
    silent $100 default) and the would-be names come back in `skipped` so the
    ping can say exactly what was not done.
    exit_of (optional): ledger exit dates from compute_board, ANY age - it
    lets a tracked model position whose SELL scrolled out of the 30d bucket
    window (e.g. the chain was down for a month) still be archived instead of
    sticking in the book forever.
    Returns (adopts, snaps, archived, recoups, skipped, reentries); caller
    commits. reentries = [(ticker, episode_n, (sold_date, ret_pct, outcome)
    | None)] - queue names with a prior closed episode, announced but NOT
    adopted (policy 2026-08-28)."""
    pos = _rows(cur, """
        SELECT id, upper(ticker), amount_usd, cadence, day1,
               start_date, end_date, sold_date, mode,
               sold_fraction, recoup_pinged_at
        FROM portfolio.positions""")
    dismissed = {r[0].upper() for r in _rows(cur, "SELECT ticker FROM portfolio.dismissed")}
    tracked = {r[1] for r in pos}

    # 1. ADOPT - but a name we already round-tripped (a closed model episode
    # exists) is NEVER auto-re-added (policy 2026-08-28): a model sell means
    # that trade is finished, and a later BUY re-fire must not rebuild the
    # position on autopilot. Instead it pings ONCE per new episode ("2nd
    # time" etc., keyed ticker+episode in reentry_pinged so a name sitting in
    # the queue for a week doesn't nag daily) with the last round's result -
    # Kevin decides re-buys by hand via Adopt in the dashboard.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS portfolio.reentry_pinged (
            ticker    text        NOT NULL,
            episode   int         NOT NULL,
            pinged_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (ticker, episode))""")
    # count EPISODES, not archive rows: partial-sale slices land as separate
    # rows under suffixed ids (<id>~p1, ~p2, ... - see the dashboard's
    # 'Sell part %' flow), so a raw count(*) would inflate the ordinal
    # ("3rd time around" on a genuine 2nd entry). Strip the slice suffix and
    # count distinct base ids = one per round trip.
    closed_n = {t: int(n) for t, n in _rows(cur, """
        SELECT upper(ticker),
               count(DISTINCT regexp_replace(id, '~p[0-9]+$', ''))
        FROM portfolio.closed_positions
        GROUP BY upper(ticker)""")}
    queue = [tk for tk in qs_buys if tk not in tracked and tk not in dismissed]
    to_add = [tk for tk in queue if not closed_n.get(tk)]
    reentries = []
    for tk in (t for t in queue if closed_n.get(t)):
        ep = closed_n[tk] + 1
        if _rows(cur, """SELECT 1 FROM portfolio.reentry_pinged
                         WHERE ticker = %s AND episode = %s""", (tk, ep)):
            continue  # this episode was already announced
        last = _rows(cur, """
            SELECT sold_date, ret_pct, outcome FROM portfolio.closed_positions
            WHERE upper(ticker) = %s ORDER BY sold_date DESC LIMIT 1""", (tk,))
        reentries.append((tk, ep, last[0] if last else None))
        if not dry:
            cur.execute("""
                INSERT INTO portfolio.reentry_pinged (ticker, episode)
                VALUES (%s, %s) ON CONFLICT DO NOTHING""", (tk, ep))
    adopts, skipped = [], []
    if amount is None:
        skipped = to_add
    else:
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

    # 2. SNAPSHOT tracked states - including today's adopts, so a position's
    # trail starts with its day-0 BUY instead of tomorrow. The conditional
    # DO UPDATE never clobbers a row the dashboard stamped with a
    # user-override why ("sold by user" etc.) - only our own daily rows are
    # refreshed.
    snaps = 0
    for tk in [r[1] for r in pos] + [tk for _, tk in adopts]:
        st = bucket.get(tk, "closed")
        if not dry:
            cur.execute("""
                INSERT INTO portfolio.state_history AS sh
                  (ticker, as_of, hz, state, why, grade)
                VALUES (%s,%s,%s,%s,'daily sync',%s)
                ON CONFLICT (ticker, as_of, hz) DO UPDATE
                  SET state = EXCLUDED.state, why = EXCLUDED.why,
                      grade = EXCLUDED.grade, captured_at = now()
                  WHERE sh.why = 'daily sync' OR sh.why IS NULL""",
                (tk, today, HZ, st, grade.get(tk)))
        snaps += 1

    # 3. ARCHIVE: model flipped to sell, or the user hand-stamped sold.
    # sold_fraction in (0,1) records earlier partial sales whose slices are
    # already archived - the final exit realizes only the remaining stake.
    archived = []
    for rid, tk, amt, cadence, day1, start_d, end_d, sold_d, mode, sfrac, rping in pos:
        # a ledger exit older than 30d has left `bucket` (audit 2026-08-23:
        # without this branch a missed archive became permanently stuck) -
        # tk absent from bucket + a recorded exit = still a model sell
        stale_exit = (tk not in bucket
                      and (exit_of or {}).get(tk) is not None)
        model_sell = (bucket.get(tk) == "sell" or stale_exit) and mode == "model"
        hand_sold = sold_d is not None
        if not (model_sell or hand_sold):
            continue
        exit_day = sold_d or today
        rz = realized(cur, tk, float(amt), cadence, start_d or today,
                      day1, exit_day)
        if rz is None:
            log.warning("no priced fills for %s - left active", tk)
            continue
        rem = 1.0
        if sfrac is not None and 0 < float(sfrac) < 1:
            rem = 1.0 - float(sfrac)
        if rem < 1.0:
            for k in ("invested", "value", "pnl"):
                rz[k] = round(rz[k] * rem, 2)
        outcome = "profit" if rz["pnl"] > 0 else "loss"
        archived.append((tk, rz, outcome, rid))
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
            if hand_sold and not model_sell and bucket.get(tk) in ("buy", "hold"):
                # A hand-sale AGAINST a live BUY or HOLD is a deliberate exit;
                # dismissing silences it completely - no re-adopt AND no
                # re-entry announcement (audit 2026-08-23: the old buy-only
                # check let a hand-sold HOLD slip back in). Model sells stay
                # un-dismissed: their closed episode already blocks the
                # auto-re-adopt (step 1, policy 2026-08-28), so a re-fire is
                # announced once per episode instead; un-dismiss = Adopt
                # manually in the dashboard.
                cur.execute("""
                    INSERT INTO portfolio.dismissed (ticker) VALUES (%s)
                    ON CONFLICT (ticker) DO NOTHING""", (tk,))

    # 3b. RECOUP, high-water flavored: ping ONCE per position (recoup_pinged_at
    # stamps it) when the position has EVER closed >= thr above its entry fill
    # (max close since entry, so an outage day or a gap straight past the
    # threshold cannot lose the event), provided today's mark is still within
    # 10pp of the threshold - a stake-back sale advised long after the peak
    # faded is bad advice, and with the stamp unset the ping simply fires when
    # the name climbs back near it. Audit 2026-08-21: +100% is the
    # performance-free level; Kevin chose +50 (safety-first: fires on ~31% of
    # historical picks at ~-1.5pp average upside cost).
    arch_tks = {a[0] for a in archived}
    recoups = []
    for rid, tk, amt, cadence, day1, start_d, end_d, sold_d, mode, sfrac, rping in pos:
        if sold_d is not None or tk in arch_tks or rping is not None:
            continue
        # A prior partial sale does NOT suppress the ping (audit 2026-08-23:
        # a 10% trim used to silence it forever). The numbers below scale to
        # the REMAINING stake; the message says what is already banked.
        rem = 1.0
        if sfrac is not None and 0 < float(sfrac) < 1:
            rem = 1.0 - float(sfrac)
        fd, fpx = _px_asof(cur, tk, start_d or today, forward=True)
        if fpx is None or fd is None or fd >= today:
            continue
        hw = _rows(cur, """
            SELECT max(adj_close) FROM cdm.ingest_combined
            WHERE upper(ticker) = %s AND date >= %s""", (tk, fd))
        last = _rows(cur, """
            SELECT adj_close FROM cdm.ingest_combined
            WHERE upper(ticker) = %s AND date >= %s
            ORDER BY date DESC LIMIT 1""", (tk, fd))
        if not hw or hw[0][0] is None or not last:
            continue
        r_high = 100 * (float(hw[0][0]) / fpx - 1)
        r_now = 100 * (float(last[0][0]) / fpx - 1)
        if r_high >= thr and r_now >= thr - 10:
            # fraction of the CURRENT (remaining) position that recovers the
            # remaining original stake - algebraically identical whether or
            # not a slice was already sold, so only the $ figure scales by rem
            frac = 100.0 / (1 + r_now / 100.0)
            banked_pct = round(100.0 * (1.0 - rem))
            recoups.append((tk, r_now, r_high, frac, float(amt) * rem, rid,
                            banked_pct))
            if not dry:
                cur.execute(
                    "UPDATE portfolio.positions SET recoup_pinged_at = %s WHERE id = %s",
                    (today, rid))
    return adopts, snaps, archived, recoups, skipped, reentries


def build_msgs(adopts, archived, recoups, amount, grade, cluster, skipped=None,
               bad_amount=None, reentries=None):
    """The exact Telegram texts. Single source shared by run() and the test
    harnesses, so the wording the tests validate is the wording that ships.
    Format (Kevin, 2026-08-21): the ACTION comes first in plain words, one
    short line per fact, rounded numbers, no cluster codes or jargon."""
    msgs = []

    def money(x):
        return f"{'+' if x >= 0 else '-'}${abs(x):.2f}"

    if adopts:
        def gtxt(tk):
            try:
                return f"grade {float(grade.get(tk)):.0f}"
            except (TypeError, ValueError):
                # unreachable in prod since the >=68 gate (2026-08-23) means
                # every adopt carries a grade; kept for the test harnesses,
                # which adopt ungraded fixture names via injected queues
                return "no grade yet"
        # each line ends with the row's PK (positions.id / closed_positions key)
        # so any ping can be referenced later (Kevin, 2026-08-22)
        lines = "\n".join(f"• {tk} ({gtxt(tk)}) [{rid}]" for rid, tk in adopts)
        msgs.append(f"\U0001F7E2 BUY in Robinhood - ${amount:g} each\n"
                    f"{lines}\n"
                    "The table tracks these from today.")
    # Re-entries: a name already round-tripped once fires BUY again. Announce
    # the episode number and the last result, but do NOT rebuy or re-add -
    # that call is Kevin's (policy 2026-08-28).
    if reentries:
        ord_of = {2: "2nd", 3: "3rd"}
        for tk, ep, last in reentries:
            nth = ord_of.get(ep, f"{ep}th")
            hist = ""
            if last:
                sold_d, ret, outcome = last
                try:
                    hist = (f"Last round: sold {sold_d} at {float(ret):+.0f}% "
                            f"({outcome}).\n")
                except (TypeError, ValueError):
                    hist = f"Last round: sold {sold_d} ({outcome}).\n"
            msgs.append(f"\U0001F501 {tk} says BUY again - {nth} time around, "
                        "NOT bought, NOT re-added to your table\n"
                        f"{hist}"
                        "You already played this one. Rebuy only on purpose: "
                        "Adopt in the dashboard puts it back on the books.")
    if bad_amount is not None:
        msgs.append("\U000026A0\U0000FE0F The '$ per new BUY' setting looks wrong "
                    f"(${bad_amount:g}) - ignored, nothing adopts until it is "
                    "fixed in the dashboard (1 to 1000).")
    if skipped:
        msgs.append("\U000026A0\U0000FE0F NOTHING BOUGHT - the buy amount is not set\n"
                    "Would have bought: " + ", ".join(skipped) + "\n"
                    "Fix: dashboard > Production > Connect > set "
                    "'$ per new BUY'. Tomorrow's run picks them up.")
    for tk, rz, outcome, rid in archived:
        verdict = "PROFIT \U00002705" if outcome == "profit" else "LOSS \U0000274C"
        rel = "beat SPY by" if rz["vs_spy"] >= 0 else "behind SPY by"
        msgs.append(f"\U0001F534 SELL {tk} in Robinhood - sell all of it [{rid}]\n"
                    f"Result: {money(rz['pnl'])} {verdict} "
                    f"({rz['ret']:+.0f}%, {rel} {abs(rz['vs_spy']):.0f}pp)\n"
                    "Moved to your Realized history. "
                    "(Skip if you never bought it.)")
    for tk, r_now, r_high, frac, amt, rid, *extra in recoups:
        banked_pct = extra[0] if extra else 0
        peak = f" (peaked at {r_high:+.0f}%)" if r_high - r_now >= 1 else ""
        banked = (f" You already banked {banked_pct:.0f}% earlier."
                  if banked_pct else "")
        msgs.append(f"\U0001F3AF {tk} is up {r_now:+.0f}%{peak} - time to play it safe [{rid}]\n"
                    f"SELL {frac:.0f}% of it in Robinhood (≈${amt:.2f}) - "
                    f"that takes your original stake back out.{banked}\n"
                    "What's left keeps running as pure profit. One-time alert - "
                    "record it in the dashboard with 'Sell part %'.")
    return msgs


OUTBOX_GIVE_UP_RUNS = 10   # failed delivery runs before a day is abandoned
OUTBOX_MSG_MAX_CHARS = 4000  # Telegram hard limit is 4096; stay under it


def deliver_outbox(con):
    """Send every undelivered sync_runs message, oldest day first. sent_n is a
    per-MESSAGE cursor committed after each successful send, so a mid-batch
    Telegram failure never re-delivers what already went out (audit
    2026-08-23: the old per-day flag duplicated the delivered prefix on
    retry). A day is marked sent once the cursor covers all its messages. A
    failing day still blocks later days (chronological order), but after
    OUTBOX_GIVE_UP_RUNS failed runs it is abandoned with a warning ping so
    one poison message can't dam the queue forever. Messages are truncated to
    the Telegram size limit - an oversized body was the one poison shape we
    could construct. 3 tries per message with a short pause."""
    import time
    try:
        from utils.alerting import notify
    except Exception:
        log.exception("alerting unavailable - outbox left undelivered")
        return
    cur = con.cursor()
    rows = _rows(cur, """
        SELECT as_of, msgs, COALESCE(sent_n, 0), COALESCE(attempts, 0)
        FROM portfolio.sync_runs
        WHERE NOT sent AND jsonb_array_length(msgs) > 0
        ORDER BY as_of""")
    for as_of, msgs, sent_n, attempts in rows:
        all_ok = True
        for i in range(sent_n, len(msgs)):
            m = str(msgs[i])[:OUTBOX_MSG_MAX_CHARS]
            ok = False
            for _ in range(3):
                try:
                    if notify(m):
                        ok = True
                        break
                except Exception:
                    log.exception("notify raised")
                time.sleep(2)
            if not ok:
                all_ok = False
                break
            cur.execute("""
                UPDATE portfolio.sync_runs SET sent_n = %s WHERE as_of = %s""",
                (i + 1, as_of))
            con.commit()
        if all_ok:
            cur.execute("UPDATE portfolio.sync_runs SET sent = true WHERE as_of = %s",
                        (as_of,))
            con.commit()
            continue
        if attempts + 1 >= OUTBOX_GIVE_UP_RUNS:
            undel = len(msgs) - sent_n
            cur.execute("""
                UPDATE portfolio.sync_runs SET sent = true, attempts = %s
                WHERE as_of = %s""", (attempts + 1, as_of))
            con.commit()
            log.error("outbox: gave up on %d message(s) for %s after %d runs",
                      undel, as_of, attempts + 1)
            try:
                notify(f"\U000026A0\U0000FE0F gave up on {undel} undelivered "
                       f"ping(s) for {as_of} - see sync_runs.msgs")
            except Exception:
                pass
            continue
        cur.execute("UPDATE portfolio.sync_runs SET attempts = %s WHERE as_of = %s",
                    (attempts + 1, as_of))
        con.commit()
        log.warning("outbox delivery incomplete for %s - retrying next run", as_of)
        break  # keep chronological order; later days wait behind this one


def verify_delivery(con):
    """Real-time money-path alarm: after deliver_outbox, any message still
    short of its cursor (sent_n < len) is a ping that did NOT reach the phone
    THIS run. The old design only learned of this from the 16:30 deadman the
    NEXT day, and only for whole unsent days - a single BUY instruction could
    vanish for hours unnoticed (2026-08-23). This pings immediately, checks the
    CURSOR not the `sent` flag (so a mis-flagged row can't hide), and calls out
    when an undelivered ping is an actionable BUY/SELL/recoup vs a narrative.
    Best-effort: never raises into the sync."""
    try:
        from utils.alerting import notify
    except Exception:
        log.exception("alerting unavailable - delivery verification skipped")
        return
    try:
        cur = con.cursor()
        # Scope to NOT sent: the write-side invariant (sent = cursor >= len, in
        # run()) guarantees a still-unsent row is genuinely undelivered, so this
        # never fires on the legacy pre-cursor rows (sent=true, sent_n=0 default)
        # that a bare `cursor < len` would wrongly flag - the trap that a first
        # cut of this fix fell into.
        rows = _rows(cur, """
            SELECT as_of, msgs, COALESCE(sent_n, 0)
            FROM portfolio.sync_runs
            WHERE NOT sent AND COALESCE(sent_n, 0) < jsonb_array_length(msgs)
            ORDER BY as_of""")
        undelivered = []
        for as_of, msgs, sent_n in rows:
            undelivered.extend(str(m) for m in list(msgs)[sent_n:])
        if not undelivered:
            return
        # An actionable ping starts with one of the money emojis (BUY/SELL/
        # recoup); a board-narrative line does not.
        action_markers = ("\U0001F7E2", "\U0001F534", "\U0001F3AF")  # 🟢 🔴 🎯
        n_action = sum(1 for m in undelivered if m[:4].strip().startswith(action_markers))
        head = (f"\U0001F6A8 {len(undelivered)} board ping(s) FAILED to deliver "
                f"(Telegram down?)")
        if n_action:
            head += f" - {n_action} ACTIONABLE (buy/sell/recoup) - check the dashboard now"
        # Try to surface the first actionable body so the trade isn't lost even
        # if only this alarm gets through.
        first_action = next((m for m in undelivered
                             if m[:4].strip().startswith(action_markers)), None)
        body = ("\n—\n" + first_action[:600]) if first_action else ""
        if not notify(head + body):
            log.error("verify_delivery: alarm itself failed to send - "
                      "%d undelivered, %d actionable", len(undelivered), n_action)
    except Exception:
        log.exception("verify_delivery errored (swallowed)")


def run(dry=False):
    # env guard: this maintains the PROD book only (local schedulers skip).
    # An UNRESOLVABLE env raises instead of quietly defaulting to dev (audit
    # 2026-08-23: on prod a dropped ENV made the task a silent green no-op
    # that only the 16:30 deadman would notice, 6.5h late and ambiguously).
    env = os.environ.get("ENV")
    if not env:
        try:
            from airflow.models import Variable
            env = Variable.get("ENV", default_var=None)
        except Exception:
            env = None
    if not env and not dry:
        raise RuntimeError(
            "ENV is set neither in the environment nor as an Airflow "
            "Variable - refusing to guess (a prod container missing ENV "
            "would silently skip the daily sync). Set ENV=prod|dev|staging.")
    if env != "prod" and not dry:
        log.info("ENV=%s - portfolio_sync only runs on prod; skipping.", env)
        return

    today = date.today()
    con = _connect()
    con.autocommit = False
    try:
        cur = con.cursor()
        bucket, grade, cluster, qs_buys, exit_of = compute_board(cur)
        # An absent or invalid auto_adopt_amount means UNSET: adopt nothing and
        # say so, never fall back to a silent $100 book entry. Out-of-range
        # values (a fat-fingered $100000) are ignored the same way, with a
        # ping naming the bad value.
        amt_rows = _rows(cur, "SELECT value FROM portfolio.app_settings WHERE key='auto_adopt_amount'")
        try:
            amount = float(amt_rows[0][0]) if amt_rows else None
        except Exception:
            amount = None
        bad_amount = None
        if amount is not None and not (1 <= amount <= 1000):
            bad_amount, amount = amount, None
        thr_rows = _rows(cur, "SELECT value FROM portfolio.app_settings WHERE key='recoup_threshold_pct'")
        try:
            thr = float(thr_rows[0][0]) if thr_rows else 100.0
        except Exception:
            thr = 100.0
        if not (thr >= 10):
            thr = 100.0

        adopts, snaps, archived, recoups, skipped, reentries = sync_core(
            cur, bucket, grade, cluster, qs_buys, amount, thr, today, dry,
            exit_of=exit_of)
        msgs = build_msgs(adopts, archived, recoups, amount, grade, cluster,
                          skipped, bad_amount=bad_amount, reentries=reentries)

        if dry:
            con.rollback()
            print(f"DRY RUN {today} (env={env})")
            print(f"board: {sum(1 for b in bucket.values() if b == 'buy')} buy / "
                  f"{sum(1 for b in bucket.values() if b == 'hold')} hold / "
                  f"{sum(1 for b in bucket.values() if b == 'sell')} sell")
            print(f"settings: amount={'UNSET' if amount is None else amount} thr={thr}")
            print(f"qs_buys ({len(qs_buys)}): {', '.join(qs_buys)}")
            print(f"would adopt ({len(adopts)}): {', '.join(tk for _, tk in adopts) or '-'}")
            if reentries:
                print("would announce re-entry (NOT adopt): "
                      + ", ".join(f"{tk} (ep {ep})" for tk, ep, _ in reentries))
            if skipped:
                print(f"would SKIP (amount unset): {', '.join(skipped)}")
            print(f"would snapshot: {snaps} tracked rows")
            print(f"would archive ({len(archived)}): "
                  + (", ".join(f"{tk} {rz['pnl']:+.2f}$ {o}" for tk, rz, o, _ in archived) or "-"))
            for m in msgs:
                print("would ping:", m)
        else:
            # Outbox row inside the SAME commit as the day's writes: run log,
            # parity record (qs_buys), and the delivery ledger deliver_outbox
            # and the deadman cron read. Same-day re-runs append new msgs to
            # the day's undelivered set instead of erasing it.
            prior = _rows(cur, """
                SELECT msgs, sent, COALESCE(sent_n, 0)
                FROM portfolio.sync_runs WHERE as_of = %s""", (today,))
            day_msgs, keep_sent_n = list(msgs), 0
            if prior and prior[0][0] and not prior[0][1]:
                # append to the undelivered set; the delivered prefix stays a
                # prefix of the new array, so the sent_n cursor stays valid
                day_msgs = list(prior[0][0]) + day_msgs
                keep_sent_n = int(prior[0][2])
            cur.execute("""
                INSERT INTO portfolio.sync_runs
                  (as_of, qs_buys, n_adopt, n_snap, n_arch, n_recoup, msgs,
                   sent, sent_n)
                VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)
                ON CONFLICT (as_of) DO UPDATE SET
                  qs_buys = EXCLUDED.qs_buys, n_adopt = EXCLUDED.n_adopt,
                  n_snap = EXCLUDED.n_snap, n_arch = EXCLUDED.n_arch,
                  n_recoup = EXCLUDED.n_recoup, msgs = EXCLUDED.msgs,
                  sent = EXCLUDED.sent, sent_n = EXCLUDED.sent_n""",
                (today, qs_buys, len(adopts), snaps, len(archived),
                 len(recoups), json.dumps(day_msgs),
                 keep_sent_n >= len(day_msgs), keep_sent_n))
            con.commit()
            # 4. PING after commit (a failed write can't produce a false ping;
            # a failed ping is retried from the outbox, never lost)
            deliver_outbox(con)
            verify_delivery(con)
            log.info("sync done: %d adopted, %d snapshots, %d archived, %d recoup",
                     len(adopts), snaps, len(archived), len(recoups))
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
