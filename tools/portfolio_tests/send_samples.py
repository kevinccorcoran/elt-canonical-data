#!/usr/bin/env python3
"""Send one sample of every Telegram message type from the DEPLOYED build_msgs.

Run inside the PROD airflow-scheduler container AFTER a deploy, so the words
that land on the phone are exactly the words the shipped code produces:

  ssh root@<droplet> 'docker exec -i airflow-scheduler python3 -' \
      < tools/portfolio_tests/send_samples.py

Lesson (2026-08-21): never re-type message samples through a shell - a bash
double-quote layer once ate the dollar amounts ($5 -> ""). Import the shipped
function and pipe the script over stdin instead.
"""
import sys

sys.path.insert(0, "/opt/airflow/dags")
from portfolio_sync import build_msgs
from utils.alerting import notify

TAG = "[TEST v3]"

adopts = [("rid1", "ZZTA")]
archived = [
    ("ZZTC", dict(pnl=1.50, ret=30.0, vs_spy=28.1), "profit"),
    ("ZZTD", dict(pnl=-0.75, ret=-15.0, vs_spy=-16.9), "loss"),
]
recoups = [
    ("ZZTE", 55.0, 55.0, 64.5, 5.0),   # peak == now: no "(peak)" suffix
    ("ZZTI", 52.0, 61.0, 65.8, 5.0),   # gap-past: "(peak +61%)" shows
]
skipped = ["ZZTM", "ZZTN"]

msgs = build_msgs(adopts, archived, recoups, 5.0,
                  grade={"ZZTA": 74}, cluster={"ZZTA": 9}, skipped=skipped)
text = (f"{TAG} exact wording from the DEPLOYED build_msgs - one sample of "
        "every message type, then ignore:\n\n" + "\n\n".join(msgs))
print(text)
print("send:", "OK" if notify(text) else "FAILED")
