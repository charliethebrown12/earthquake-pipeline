"""DuckDB / MotherDuck analytical store with idempotent upserts on USGS event id."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

import duckdb

from src.config import DATA_DIR, DUCKDB_DATABASE, LOCAL_DB_PATH, MOTHERDUCK_TOKEN


DDL = """
CREATE TABLE IF NOT EXISTS earthquakes (
    id VARCHAR PRIMARY KEY,
    time TIMESTAMP,
    updated TIMESTAMP,
    latitude DOUBLE,
    longitude DOUBLE,
    depth_km DOUBLE,
    mag DOUBLE,
    mag_type VARCHAR,
    place VARCHAR,
    type VARCHAR,
    status VARCHAR,
    tsunami INTEGER,
    sig INTEGER,
    url VARCHAR,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    mode VARCHAR,
    rows_seen INTEGER,
    rows_upserted INTEGER,
    status VARCHAR,
    detail VARCHAR
);
"""


def connect() -> duckdb.DuckDBPyConnection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if MOTHERDUCK_TOKEN:
        con = duckdb.connect(f"md:{DUCKDB_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}")
    else:
        con = duckdb.connect(str(LOCAL_DB_PATH))
    con.execute(DDL)
    return con


@contextmanager
def db_session() -> Iterator[duckdb.DuckDBPyConnection]:
    con = connect()
    try:
        yield con
    finally:
        con.close()


def upsert_earthquakes(con: duckdb.DuckDBPyConnection, rows: Iterable[dict[str, Any]]) -> int:
    """
    Idempotent load: INSERT ... ON CONFLICT (id) DO UPDATE.

    Overlapping re-fetches are safe — same USGS id updates in place.
    """
    materialised = [r for r in rows if r and r.get("id")]
    if not materialised:
        return 0

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for row in materialised:
        row.setdefault("ingested_at", now)

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE earthquakes_staging (
            id VARCHAR,
            time TIMESTAMP,
            updated TIMESTAMP,
            latitude DOUBLE,
            longitude DOUBLE,
            depth_km DOUBLE,
            mag DOUBLE,
            mag_type VARCHAR,
            place VARCHAR,
            type VARCHAR,
            status VARCHAR,
            tsunami INTEGER,
            sig INTEGER,
            url VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    con.executemany(
        """
        INSERT INTO earthquakes_staging
        (id, time, updated, latitude, longitude, depth_km, mag, mag_type, place, type, status, tsunami, sig, url, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r["id"],
                r.get("time"),
                r.get("updated"),
                r.get("latitude"),
                r.get("longitude"),
                r.get("depth_km"),
                r.get("mag"),
                r.get("mag_type"),
                r.get("place"),
                r.get("type"),
                r.get("status"),
                r.get("tsunami"),
                r.get("sig"),
                r.get("url"),
                r.get("ingested_at", now),
            )
            for r in materialised
        ],
    )

    con.execute(
        """
        INSERT INTO earthquakes AS t
        SELECT * FROM earthquakes_staging
        ON CONFLICT (id) DO UPDATE SET
            time = excluded.time,
            updated = excluded.updated,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            depth_km = excluded.depth_km,
            mag = excluded.mag,
            mag_type = excluded.mag_type,
            place = excluded.place,
            type = excluded.type,
            status = excluded.status,
            tsunami = excluded.tsunami,
            sig = excluded.sig,
            url = excluded.url,
            ingested_at = excluded.ingested_at
        """
    )
    return len(materialised)


def record_run(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    mode: str,
    rows_seen: int,
    rows_upserted: int,
    status: str,
    detail: str = "",
) -> None:
    con.execute(
        """
        INSERT INTO pipeline_runs
        (run_id, started_at, finished_at, mode, rows_seen, rows_upserted, status, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (run_id) DO UPDATE SET
            finished_at = excluded.finished_at,
            rows_seen = excluded.rows_seen,
            rows_upserted = excluded.rows_upserted,
            status = excluded.status,
            detail = excluded.detail
        """,
        [run_id, started_at, finished_at, mode, rows_seen, rows_upserted, status, detail],
    )


def stats(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    total = con.execute("SELECT COUNT(*) FROM earthquakes").fetchone()[0]
    min_t, max_t = con.execute("SELECT MIN(time), MAX(time) FROM earthquakes").fetchone()
    last_ingest = con.execute("SELECT MAX(ingested_at) FROM earthquakes").fetchone()[0]
    runs = con.execute("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'ok'").fetchone()[0]
    return {
        "total_events": total,
        "min_time": min_t,
        "max_time": max_t,
        "last_ingested_at": last_ingest,
        "successful_runs": runs,
    }