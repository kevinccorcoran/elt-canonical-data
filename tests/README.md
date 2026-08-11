# tests/

pytest suite for `elt-canonical-data`: data-quality checks and the Lifecycle
board **transition notifier** (WhatsApp).

## Layout

| path | what |
|------|------|
| `utilities/db.py` | env-driven Postgres connections |
| `utilities/notify.py` | WhatsApp send (CallMeBot; swap this file to change transport) |
| `monitoring/board_state.py` | board column state machine, ported from the R dashboard |
| `monitoring/notifier.py` | diff current columns vs last run → WhatsApp |
| `monitoring/test_board_transitions.py` | regression tests pinning the port + diff |
| `data_quality/test_price_nulls.py` | price-table integrity (skips if table absent) |
| `sql/` | ad-hoc analysis queries (not tests) |

## Setup

```bash
pip install pytest psycopg2-binary requests python-dotenv
cp tests/.env.example tests/.env      # fill DB_PASSWORD + WhatsApp creds
```

## Run

```bash
pytest                                # from the repo root
```

DB-touching tests run against `dev` and roll back every change (the `db_conn`
fixture), so they never mutate the database.

## The board notifier

`monitoring/board_state.py` recomputes each qualstream-graded ticker's board
column (buy / hold / sell / closed) directly from Postgres — a faithful port of
`scripts/app.R` `derivedLC`, validated against the live board by the tests.

`monitoring/notifier.py` diffs the current columns against the last run (stored
in `monitoring.board_state_snapshot`) and WhatsApps every change:

```bash
python -m tests.monitoring.notifier
```

First run seeds the snapshot **silently**; later runs message only real
transitions, e.g. `🟡 COKE (72): buy → hold`. Schedule it (cron/Airflow) after
the ledger + gate DAGs and once daily (maturities move the board with no DB write).

**Source of truth is the R dashboard.** If `app.R`'s board rules change,
re-validate `board_state.py` — the tests pin the known cases (buy→hold on a
ledger wash, →sell on a gate flip).
