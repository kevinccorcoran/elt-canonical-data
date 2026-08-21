# portfolio_sync test tooling

The hands-off portfolio loop (airflow/dags/portfolio_sync.py + the dashboard's
portfolio tab) is guarded by three runnable scripts. They were born as
scratchpad one-offs during the 2026-08-21 hardening audit; they live here so
every future change to the loop re-runs them.

| script | where it runs | what it proves |
|---|---|---|
| `scenarios.py` | LOCAL airflow-scheduler container, dev DB | the full scenario matrix (S1-S21): adopts, archives, recoup high-water, hand-sold dismissal, partial-sale slices, delta-safe saves, ping outbox. Synthetic ZZT* fixtures, cleans up after itself, exit code 1 on any FAIL. |
| `reconcile.py` | PROD scheduler container (read-only) | queue vs tracked book vs the sync_runs parity record; flags R/Python board drift. |
| `send_samples.py` | PROD scheduler container (after deploy) | one Telegram sample of every message type from the DEPLOYED build_msgs - verify the wording and the dollar amounts on the phone. |

The deadman watchdog is `../portfolio_deadman.sh` (droplet root cron, 16:30
UTC): alerts when today's sync_runs row is missing or old pings sit
undelivered, so silence stays trustworthy.

Invocation lines are in each script's docstring. All credentials come from
container envs - nothing in this directory holds a secret.
