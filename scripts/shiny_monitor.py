#!/usr/bin/env python3
"""Log monitoring for the Shiny dashboard (airflow-shiny container).

The supervisor (shiny_entrypoint.sh) truncates shiny.log on every app start and
keeps only the *last* crash in shiny_crash_last.log, so there is no durable
record of what has been killing the dashboard over time. This tool fills that:

  record   append one classified JSONL event when the app exits or the watchdog
           restarts it. Called from shiny_entrypoint.sh. Classifies by exit-code
           signal (segfault / OOM-kill / SIGTERM) corroborated by log-tail
           patterns (httpuv segfault / DNS blip / DB timeout / clean / app error).

  status   print a health snapshot: is port 3838 answering, restart counts over
           the last 1h and 24h, a category breakdown, and the most recent events.
           Exits 0 if the dashboard is up, 1 if it is down (usable as a check).

History lives in shiny_events.log (JSONL) next to this script -- the scripts/
dir is host-mounted, so it survives container restarts and image rebuilds
(unlike shiny.log). Auto-rotated to the last MAX_EVENTS lines. Stdlib only; no
third-party packages, so it runs as-is in the container.

Examples:
  docker exec airflow-shiny python3 /opt/airflow/scripts/shiny_monitor.py status
  # from the droplet host (scripts/ is mounted, 3838 is published):
  python3 scripts/shiny_monitor.py status --limit 20
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_EVENTS = os.path.join(HERE, "shiny_events.log")
DEFAULT_LOG = os.path.join(HERE, "shiny.log")
DEFAULT_URL = "http://localhost:3838/"
MAX_EVENTS = 2000          # rotate history so the file can't grow unbounded
SNIPPET_LINES = 20         # log-tail lines kept per event

# Log-tail substrings that corroborate (or, for a clean exit, reveal) a cause.
SEGFAULT_MARKERS = (
    "caught segfault", "SIGSEGV", "memory not mapped", "address 0x",
    "segmentation fault", "requestToEnv",
)
OOM_MARKERS = (
    "Cannot allocate memory", "std::bad_alloc", "out of memory",
    "OOM", "Killed",
)
DNS_MARKERS = (
    "could not translate host name", "Temporary failure in name resolution",
    "EAI_AGAIN", "getaddrinfo", "Name or service not known",
)
DB_MARKERS = (
    "statement timeout", "canceling statement", "connection timed out",
    "could not connect", "server closed the connection",
)


def now():
    """(iso string, epoch seconds) in local/container time."""
    dt = datetime.now().astimezone()
    return dt.isoformat(timespec="seconds"), dt.timestamp()


def read_tail(path, max_bytes=8000):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            data = f.read()
        return data.decode("utf-8", "replace")
    except OSError:
        return ""


def classify(exit_code, log_text):
    """Return (category, human_signal). The exit-code signal is primary
    (Rscript exits 128+N when killed by signal N); log patterns refine it and
    cover cases with no useful code, e.g. an exit 0 that logged a DNS storm."""
    low = (log_text or "").lower()

    def has(markers):
        return any(m.lower() in low for m in markers)

    signal = None
    if exit_code is not None:
        if exit_code == 0:
            signal = "exit 0 (clean)"
        elif exit_code > 128:
            signal = "signal %d" % (exit_code - 128)
        else:
            signal = "exit %d" % exit_code

    # Primary: signal-driven.
    if exit_code == 139:                       # 128 + SIGSEGV(11)
        return "segfault", signal
    if exit_code == 137:                       # 128 + SIGKILL(9): usually OOM
        return ("oom" if has(OOM_MARKERS) else "killed"), signal
    if exit_code == 143:                       # 128 + SIGTERM(15): watchdog/stop
        return "sigterm", signal

    # Secondary: log-pattern driven.
    if has(SEGFAULT_MARKERS):
        return "segfault", signal or "log:segfault"
    if has(OOM_MARKERS):
        return "oom", signal or "log:oom"
    if has(DNS_MARKERS):
        return "dns", signal or "log:dns"
    if has(DB_MARKERS):
        return "db_timeout", signal or "log:db"

    if exit_code == 0:
        return "clean", signal
    return "app_error", signal


def snippet(log_text):
    lines = [ln.rstrip() for ln in (log_text or "").splitlines() if ln.strip()]
    return "\n".join(lines[-SNIPPET_LINES:])[-1600:]


def rotate(events_path):
    try:
        with open(events_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > MAX_EVENTS:
            with open(events_path, "w", encoding="utf-8") as f:
                f.writelines(lines[-MAX_EVENTS:])
    except OSError:
        pass


def append_event(events_path, event):
    try:
        with open(events_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
        rotate(events_path)
    except OSError as e:
        sys.stderr.write("shiny_monitor: could not write event: %s\n" % e)


def load_events(events_path):
    events = []
    try:
        with open(events_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except ValueError:
                    continue
    except OSError:
        pass
    return events


def http_health(url, timeout=8.0):
    import urllib.request
    start = time.monotonic()
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = resp.getcode()
        ms = (time.monotonic() - start) * 1000.0
        return (200 <= code < 500), code, ms
    except Exception as e:            # any failure == down, report the reason
        ms = (time.monotonic() - start) * 1000.0
        return False, str(e), ms


def cmd_record(args):
    ts_iso, ts_epoch = now()
    log_text = read_tail(args.logfile) if args.logfile else ""
    category, signal = classify(args.exit_code, log_text)
    append_event(args.events, {
        "ts": ts_iso,
        "epoch": round(ts_epoch, 3),
        "kind": args.kind,                 # supervisor | watchdog | manual
        "exit_code": args.exit_code,
        "category": category,
        "signal": signal,
        "note": args.note or "",
        "snippet": snippet(log_text),
    })
    print("[monitor] %s kind=%s code=%s -> %s"
          % (ts_iso, args.kind, args.exit_code, category))
    return 0


def counts_since(events, now_epoch, window_s):
    cutoff = now_epoch - window_s
    by_cat = {}
    for e in events:
        ep = e.get("epoch")
        if ep is None or ep < cutoff:
            continue
        cat = e.get("category", "?")
        by_cat[cat] = by_cat.get(cat, 0) + 1
    return by_cat


def fmt_counts(d):
    return ", ".join("%s=%d" % (k, v)
                     for k, v in sorted(d.items(), key=lambda kv: -kv[1]))


def hint(h24):
    if h24.get("segfault"):
        return ("Hint: segfault restarts point at httpuv#171. Confirm the image "
                "has PATCHED httpuv baked in (Dockerfile), not stock from CRAN.")
    if h24.get("oom") or h24.get("killed"):
        return ("Hint: SIGKILL restarts usually mean OOM. Consider a mem_limit or "
                "reducing peak memory of the heavy Forecast valuations.")
    if h24.get("dns"):
        return ("Hint: DNS restarts are the EAI_AGAIN resolver blip. See the "
                "pinned dns: resolvers in docker-compose.yml.")
    return ""


def cmd_status(args):
    up, code, ms = http_health(args.url, timeout=args.timeout)
    events = load_events(args.events)
    _, now_epoch = now()
    h1 = counts_since(events, now_epoch, 3600)
    h24 = counts_since(events, now_epoch, 86400)

    if args.json:
        print(json.dumps({
            "up": up,
            "http": code,
            "latency_ms": round(ms, 1),
            "events_total": len(events),
            "last_1h": h1,
            "last_24h": h24,
            "recent": events[-args.limit:],
        }, indent=2, ensure_ascii=False))
        return 0 if up else 1

    print("Shiny dashboard: %s  (%s, %.0f ms)  %s"
          % ("UP" if up else "DOWN", code, ms, args.url))
    print("History: %d events in %s" % (len(events), args.events))
    print("\nRestarts by cause:")
    print("  last 1h : %s" % (fmt_counts(h1) or "none"))
    print("  last 24h: %s" % (fmt_counts(h24) or "none"))

    recent = events[-args.limit:]
    if recent:
        print("\nMost recent %d events:" % len(recent))
        for e in reversed(recent):
            print("  %s  %-10s kind=%-10s code=%s  %s" % (
                e.get("ts", "?"), e.get("category", "?"), e.get("kind", "?"),
                e.get("exit_code"), (e.get("note") or "").strip()))
    else:
        print("\nNo recorded events yet.")

    tip = hint(h24)
    if tip:
        print("\n" + tip)
    return 0 if up else 1


def build_parser():
    p = argparse.ArgumentParser(description="Shiny dashboard log monitor.")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("record", help="append a classified restart/exit event")
    r.add_argument("--kind", default="supervisor",
                   choices=["supervisor", "watchdog", "manual"])
    r.add_argument("--exit-code", type=int, default=None)
    r.add_argument("--logfile", default=DEFAULT_LOG)
    r.add_argument("--events", default=DEFAULT_EVENTS)
    r.add_argument("--note", default="")
    r.set_defaults(func=cmd_record)

    s = sub.add_parser("status", help="print health + restart summary")
    s.add_argument("--events", default=DEFAULT_EVENTS)
    s.add_argument("--url", default=DEFAULT_URL)
    s.add_argument("--timeout", type=float, default=8.0)
    s.add_argument("--limit", type=int, default=10)
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_status)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except Exception as e:            # monitoring must never crash its caller
        sys.stderr.write("shiny_monitor: %s\n" % e)
        return 0 if getattr(args, "cmd", None) == "record" else 2


if __name__ == "__main__":
    sys.exit(main())
