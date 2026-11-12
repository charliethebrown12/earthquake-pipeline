"""Shared configuration. Secrets come from env / GitHub Actions — never from the repo."""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"

DUCKDB_DATABASE = os.getenv("DUCKDB_DATABASE", "earthquakes")
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "").strip()
LOCAL_DB_PATH = DATA_DIR / f"{DUCKDB_DATABASE}.duckdb"

# Default seed: ~1 year of M2.5+ events (schema-consistent with live feed).
_today = date.today()
SEED_START = os.getenv("SEED_START", (_today - timedelta(days=365)).isoformat())
SEED_END = os.getenv("SEED_END", _today.isoformat())
SEED_MIN_MAGNITUDE = float(os.getenv("SEED_MIN_MAGNITUDE", "2.5"))

# Live trickle: overlapping window so retries/idempotent upserts cover lag + missed runs.
INGEST_LOOKBACK_HOURS = int(os.getenv("INGEST_LOOKBACK_HOURS", "48"))
INGEST_MIN_MAGNITUDE = float(os.getenv("INGEST_MIN_MAGNITUDE", "0.0"))

USGS_FDSN_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_FEED_ALL_DAY = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
)
USGS_PAGE_LIMIT = 20000
USGS_USER_AGENT = "byu-cs452-earthquake-pipeline/1.0 (student project; contact via GitHub)"


def connection_target() -> str:
    """Human-readable store target for logs / dashboard footer."""
    if MOTHERDUCK_TOKEN:
        return f"motherduck:{DUCKDB_DATABASE}"
    return f"local:{LOCAL_DB_PATH}"