"""Database connections for the test/monitoring suite.

Env-driven; defaults to the local native Postgres 'dev' database the dashboard
reads (127.0.0.1:5432). Set DB_* in tests/.env. psycopg2 talks libpq directly,
so no psql client binary is needed on the host.
"""
from __future__ import annotations

import os

import psycopg2


def get_conn(dbname: str | None = None):
    """Open a psycopg2 connection (autocommit off, the default)."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=dbname or os.getenv("DB_NAME", "dev"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )
