#!/usr/bin/env python3
"""
SEC Financial Statement and Notes data sets -> edgar.fsn_* (raw layer).

Adds what companyfacts (edgar.facts) leaves out: the splits inside a line
(Products / Services, segments, regions), company-specific custom tags and
the statement layout (statement, line order, the company's own label).

    # Load every published zip not yet loaded (first run = the backfill,
    # 79 zips / ~28 GB downloaded one at a time; later runs = the new month).
    python -m datapipeline.ingestion.edgar_statements_to_raw_etl

    # Dev: load already-downloaded zips, optionally for a few CIKs.
    python -m datapipeline.ingestion.edgar_statements_to_raw_etl \\
        --zip 2024q4_notes.zip --ciks 320193

Scope: filings by CIKs in raw.ticker_metadata or edgar.company_tickers; the
five primary statements (BS, IS, CF, EQ, CI); every number whose tag is a
line on one of them, with all its splits. Insert-only and idempotent.
DDL: tools/create_edgar_statements_schema.sql.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import re
import sys
import tempfile
import zipfile
from datetime import datetime
from typing import Any, Iterator

import psycopg2

from datapipeline.ingestion.edgar_to_raw_etl import (
    DEFAULT_MAX_RPS,
    SecClient,
    _copy_rows,
    _get_user_agent,
    _get_var,
    _resolve_dsn,
    _safe_dsn,
    pad_cik,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

csv.field_size_limit(sys.maxsize)


FSN_PAGE_URL = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets"
FSN_BASE_URL = "https://www.sec.gov"
STATEMENTS = {"BS", "IS", "CF", "EQ", "CI"}
NO_DIMENSION = "0x00000000"
FLUSH_ROWS = 250_000

SUB_COLUMNS = (
    "adsh", "cik", "name", "sic", "countryba", "stprba", "countryinc", "afs",
    "wksi", "fye", "form", "period", "fy", "fp", "filed", "accepted", "detail",
    "instance", "nciks", "aciks", "pubfloatusd", "floatdate",
)
PRE_COLUMNS = ("adsh", "report", "line", "stmt", "inpth", "tag", "version", "prole", "plabel", "negating")
NUM_COLUMNS = (
    "adsh", "tag", "version", "ddate", "qtrs", "uom", "dimh", "iprx", "value",
    "footnote", "coreg", "durp", "datp", "dcml",
)
TAG_COLUMNS = ("tag", "version", "custom", "abstract", "datatype", "iord", "crdr", "tlabel")
DIM_COLUMNS = ("dimhash", "segments", "segt")


# ──────────────────────────────────────────────
# Parsing (pure)
# ──────────────────────────────────────────────

def iter_tsv(fh: io.BufferedIOBase) -> Iterator[dict[str, str]]:
    """SEC TSV: utf-8, tab separated, lowercase header. A field holding a
    tab or quote is wrapped in double quotes with inner quotes doubled
    (CSV style), so a plain split on tabs shifts columns: parse with csv."""
    it = io.TextIOWrapper(fh, encoding="utf-8", newline="")
    reader = csv.reader(it, delimiter="\t", quotechar='"', doublequote=True)
    header = next(reader)
    for vals in reader:
        yield dict(zip(header, vals))


def parse_listing(html: str) -> list[tuple[str, str]]:
    """Data-set page -> [(file_name, absolute url)], oldest first."""
    seen: dict[str, str] = {}
    for href in re.findall(r'href="([^"]+_notes[^"/]*\.zip)"', html):
        name = href.rsplit("/", 1)[-1].removesuffix(".zip")
        seen[name] = href if href.startswith("http") else FSN_BASE_URL + href
    return sorted(seen.items(), key=lambda kv: period_key(kv[0]))


def period_key(name: str) -> tuple[int, int]:
    """'2024q4_notes' -> (2024, 12); '2025_10_notes' -> (2025, 10)."""
    m = re.match(r"(\d{4})q([1-4])", name)
    if m:
        return int(m.group(1)), int(m.group(2)) * 3
    m = re.match(r"(\d{4})_(\d{2})", name)
    if m:
        return int(m.group(1)), int(m.group(2))
    raise ValueError(f"unrecognised data-set name {name!r}")


def in_universe(sub: dict[str, str], universe: set[str]) -> bool:
    """Filer or any co-registrant is one of ours."""
    if pad_cik(sub["cik"]) in universe:
        return True
    return any(a.isdigit() and pad_cik(a) in universe for a in (sub.get("aciks") or "").split())


def _key(adsh: str, tag: str, version: str) -> str:
    return f"{adsh}\t{tag}\t{version}"


# ──────────────────────────────────────────────
# DB
# ──────────────────────────────────────────────

STAGE_DDL = {
    "stage_sub": SUB_COLUMNS,
    "stage_pre": PRE_COLUMNS,
    "stage_num": NUM_COLUMNS,
    "stage_tag": TAG_COLUMNS,
    "stage_dim": DIM_COLUMNS,
}


def _prepare_stage(conn) -> None:
    with conn.cursor() as cur:
        for table, cols in STAGE_DDL.items():
            cur.execute(
                f"CREATE TEMP TABLE IF NOT EXISTS {table} ({', '.join(c + ' TEXT' for c in cols)})"
            )
            cur.execute(f"TRUNCATE {table}")


def load_universe(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cik FROM raw.ticker_metadata WHERE cik IS NOT NULL "
            "UNION SELECT cik FROM edgar.company_tickers"
        )
        return {r[0] for r in cur.fetchall()}


INSERT_SUB_SQL = """
INSERT INTO edgar.fsn_submissions (
    accession_number, cik, name, sic, countryba, stprba, countryinc, afs, wksi,
    fye, form, period, fy, fp, filed, accepted_at, detail, instance, nciks,
    aciks, pubfloatusd, floatdate, source_file
)
SELECT adsh, lpad(cik, 10, '0'), name, sic, countryba, stprba, countryinc, afs,
       wksi::boolean, fye, form, period::date, fy::integer, fp, filed::date,
       -- SUB.accepted is New York wall time
       (accepted::timestamp AT TIME ZONE 'America/New_York'),
       detail::boolean, instance, nciks::integer, aciks, pubfloatusd::numeric,
       floatdate::date, %s
FROM stage_sub
ON CONFLICT DO NOTHING
"""

INSERT_TAGS_SQL = """
INSERT INTO edgar.fsn_tags (tag, version, custom, abstract, datatype, iord, crdr, tlabel)
SELECT tag, version, custom::boolean, abstract::boolean, datatype, iord, crdr, tlabel
FROM stage_tag
ON CONFLICT DO NOTHING;

-- Referenced but absent from tag.tsv: keep the id mapping complete.
INSERT INTO edgar.fsn_tags (tag, version)
SELECT tag, version FROM stage_pre
UNION
SELECT tag, version FROM stage_num
ON CONFLICT DO NOTHING;
"""

INSERT_DIMS_SQL = """
INSERT INTO edgar.fsn_dimensions (dimh, segments, segt)
SELECT dimhash, coalesce(segments, ''), segt::boolean FROM stage_dim
ON CONFLICT DO NOTHING;

-- Referenced but absent from dim.tsv: flag it rather than drop the number.
INSERT INTO edgar.fsn_dimensions (dimh, segments, segt)
SELECT DISTINCT dimh, '[not in dim.tsv]', NULL::boolean FROM stage_num
ON CONFLICT DO NOTHING;
"""

INSERT_PRE_SQL = """
INSERT INTO edgar.fsn_presentation (sub_id, report, line, stmt, inpth, tag_id, prole, plabel, negating)
SELECT s.sub_id, p.report::integer, p.line::integer, p.stmt, p.inpth::boolean,
       t.tag_id, p.prole, p.plabel, p.negating::boolean
FROM stage_pre p
JOIN edgar.fsn_submissions s ON s.accession_number = p.adsh
JOIN edgar.fsn_tags        t ON t.tag = p.tag AND t.version = p.version
ON CONFLICT DO NOTHING
"""

INSERT_NUM_SQL = """
INSERT INTO edgar.fsn_numbers (
    sub_id, tag_id, ddate, qtrs, uom, dim_id, iprx, value, coreg, footnote, durp, datp, dcml
)
SELECT s.sub_id, t.tag_id, n.ddate::date, n.qtrs::smallint, n.uom, d.dim_id,
       n.iprx::smallint, n.value::numeric, n.coreg, n.footnote,
       n.durp::real, n.datp::real, n.dcml::smallint
FROM stage_num n
JOIN edgar.fsn_submissions s ON s.accession_number = n.adsh
JOIN edgar.fsn_tags        t ON t.tag = n.tag AND t.version = n.version
JOIN edgar.fsn_dimensions  d ON d.dimh = n.dimh
ORDER BY s.sub_id, t.tag_id
ON CONFLICT DO NOTHING
"""


def _flush(cur, table: str, rows: list[tuple]) -> None:
    if rows:
        _copy_rows(cur, table, STAGE_DDL[table], rows)
        rows.clear()


def load_zip(conn, path: str, file_name: str, universe: set[str]) -> dict[str, int]:
    """One data-set zip -> edgar.fsn_*, in one transaction."""
    stats = {"submissions_inserted": 0, "presentation_inserted": 0, "numbers_inserted": 0}
    _prepare_stage(conn)
    with zipfile.ZipFile(path) as zf, conn.cursor() as cur:
        # SUB: our filings only.
        keep: set[str] = set()
        rows: list[tuple] = []
        with zf.open("sub.tsv") as fh:
            for r in iter_tsv(fh):
                if in_universe(r, universe):
                    keep.add(r["adsh"])
                    rows.append(tuple(r.get(c, "") for c in SUB_COLUMNS))
        _flush(cur, "stage_sub", rows)
        cur.execute(INSERT_SUB_SQL, (file_name,))
        stats["submissions_inserted"] = cur.rowcount

        # PRE: lines of the five statements; their (adsh, tag, version) decide
        # which numbers we keep.
        lines: set[str] = set()
        tags: set[str] = set()
        with zf.open("pre.tsv") as fh:
            for r in iter_tsv(fh):
                if r["adsh"] in keep and r["stmt"] in STATEMENTS:
                    lines.add(_key(r["adsh"], r["tag"], r["version"]))
                    tags.add(f"{r['tag']}\t{r['version']}")
                    rows.append(tuple(r.get(c, "") for c in PRE_COLUMNS))
                    if len(rows) >= FLUSH_ROWS:
                        _flush(cur, "stage_pre", rows)
        _flush(cur, "stage_pre", rows)

        # NUM: every number of a statement-line tag, all periods and splits.
        dims: set[str] = set()
        with zf.open("num.tsv") as fh:
            for r in iter_tsv(fh):
                if r["adsh"] in keep and _key(r["adsh"], r["tag"], r["version"]) in lines:
                    dims.add(r["dimh"])
                    rows.append(tuple(r.get(c, "") for c in NUM_COLUMNS))
                    if len(rows) >= FLUSH_ROWS:
                        _flush(cur, "stage_num", rows)
        _flush(cur, "stage_num", rows)

        with zf.open("tag.tsv") as fh:
            for r in iter_tsv(fh):
                if f"{r['tag']}\t{r['version']}" in tags:
                    rows.append(tuple(r.get(c, "") for c in TAG_COLUMNS))
        _flush(cur, "stage_tag", rows)
        cur.execute(INSERT_TAGS_SQL)

        dims.discard(NO_DIMENSION)
        with zf.open("dim.tsv") as fh:
            for r in iter_tsv(fh):
                if r["dimhash"] in dims:
                    rows.append(tuple(r.get(c, "") for c in DIM_COLUMNS))
        _flush(cur, "stage_dim", rows)
        cur.execute(INSERT_DIMS_SQL)

        cur.execute(INSERT_PRE_SQL)
        stats["presentation_inserted"] = cur.rowcount
        cur.execute(INSERT_NUM_SQL)
        stats["numbers_inserted"] = cur.rowcount
    conn.commit()
    logging.info(
        "%s: %d filings in scope, %d statement lines, %s",
        file_name, len(keep), len(lines), stats,
    )
    return stats


def _start_file(conn, file_name: str, url: str, last_modified: datetime | None, size: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO edgar.fsn_files (file_name, url, last_modified, size_bytes, status)
            VALUES (%s, %s, %s, %s, 'running')
            ON CONFLICT (file_name) DO UPDATE SET
                url = EXCLUDED.url, last_modified = EXCLUDED.last_modified,
                size_bytes = EXCLUDED.size_bytes, status = 'running',
                started_at = NOW(), finished_at = NULL, error = NULL,
                submissions_inserted = NULL, presentation_inserted = NULL,
                numbers_inserted = NULL
            """,
            (file_name, url, last_modified, size),
        )
    conn.commit()


def _finish_file(conn, file_name: str, status: str, stats: dict[str, int], error: str | None = None) -> None:
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE edgar.fsn_files SET
                status = %s, finished_at = NOW(), error = %s,
                submissions_inserted = %s, presentation_inserted = %s, numbers_inserted = %s
            WHERE file_name = %s
            """,
            (status, error, stats.get("submissions_inserted"), stats.get("presentation_inserted"),
             stats.get("numbers_inserted"), file_name),
        )
    conn.commit()


def load_one(conn, path: str, file_name: str, url: str, last_modified: datetime | None, universe: set[str]) -> None:
    _start_file(conn, file_name, url, last_modified, os.path.getsize(path))
    try:
        stats = load_zip(conn, path, file_name, universe)
    except BaseException as exc:
        _finish_file(conn, file_name, "failed", {}, error=repr(exc)[:2000])
        raise
    _finish_file(conn, file_name, "success", stats)


# ──────────────────────────────────────────────
# Run
# ──────────────────────────────────────────────

def run(
    local_zips: list[str],
    only_files: set[str] | None,
    only_ciks: set[str] | None,
    work_dir: str,
    keep_files: bool,
    force: bool,
) -> None:
    dsn = _resolve_dsn()
    logging.info("statements: DSN=%s", _safe_dsn(dsn))
    conn = psycopg2.connect(dsn)
    try:
        universe = only_ciks or load_universe(conn)
        logging.info("universe: %d CIKs", len(universe))

        if local_zips:
            for path in local_zips:
                name = os.path.basename(path).removesuffix(".zip")
                mtime = datetime.fromtimestamp(os.path.getmtime(path)).astimezone()
                load_one(conn, path, name, "file://" + os.path.abspath(path), mtime, universe)
            return

        sec = SecClient(_get_user_agent(), float(_get_var("SEC_MAX_RPS", DEFAULT_MAX_RPS)))
        listing = parse_listing(sec.get(FSN_PAGE_URL).text)
        if only_files:
            listing = [(n, u) for n, u in listing if n in only_files]
        with conn.cursor() as cur:
            cur.execute("SELECT file_name, last_modified FROM edgar.fsn_files WHERE status = 'success'")
            done = dict(cur.fetchall())
        conn.commit()
        logging.info("listing: %d data sets, %d already loaded", len(listing), len(done))

        os.makedirs(work_dir, exist_ok=True)
        for name, url in listing:
            lm = sec.last_modified(url)
            if not force and name in done and done[name] == lm:
                continue
            path = os.path.join(work_dir, name + ".zip")
            lm = sec.download(url, path) or lm
            try:
                load_one(conn, path, name, url, lm, universe)
            finally:
                if not keep_files:
                    try:
                        os.remove(path)
                    except OSError:
                        pass
        logging.info("statements done (SEC requests: %d)", sec.requests_made)
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", action="append", default=[], help="Load this local data-set zip (repeatable); no download.")
    parser.add_argument("--files", help="Comma-separated data-set names to restrict to, e.g. 2024q4_notes,2026_09_notes.")
    parser.add_argument("--ciks", help="Comma-separated CIKs to restrict to (dev / repair). Default: the universe.")
    parser.add_argument(
        "--work-dir",
        default=os.path.join(tempfile.gettempdir(), "edgar_fsn"),
        help="Where each zip is downloaded (one at a time, up to ~850 MB).",
    )
    parser.add_argument("--keep-files", action="store_true")
    parser.add_argument("--force", action="store_true", help="Reload zips even if already loaded (insert-only, so harmless).")
    args = parser.parse_args()

    run(
        local_zips=args.zip,
        only_files={f.strip() for f in args.files.split(",")} if args.files else None,
        only_ciks={pad_cik(c) for c in args.ciks.split(",")} if args.ciks else None,
        work_dir=args.work_dir,
        keep_files=args.keep_files,
        force=args.force,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
