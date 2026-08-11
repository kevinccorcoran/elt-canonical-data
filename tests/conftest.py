"""Shared fixtures. Loads tests/.env so DB + WhatsApp creds stay out of the repo."""
import pathlib

import pytest
from dotenv import load_dotenv

load_dotenv(pathlib.Path(__file__).parent / ".env")


@pytest.fixture
def db_conn():
    """A dev connection with autocommit OFF, rolled back after each test so any
    DB edits a test makes (e.g. simulating a ticker's ledger/gate change) never
    persist to the database."""
    from tests.utilities.db import get_conn

    conn = get_conn()
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()
