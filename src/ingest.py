#!/usr/bin/env python3
"""
Hourly trickle ingest.

Pulls an overlapping recent window from USGS and upserts by event id.
Empty fetches are success (nothing new / API lag) — not a failure.
"""

from __future__ import annotations

import argparse
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import (
    INGEST_LOOKBACK_HOURS,
    INGEST_MIN_MAGNITUDE,
    connection_target,
)
from src.db import db_session, record_run, stats, upsert_earthquakes
from src.usgs_client import fetch_live_day_feed, fetch_recent_window, normalize_feature


def run_ingest(*, lookback_hours: float, min_magnitude: float, use_feed: bool) -> dict:
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    run_id = f"ingest-{started.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"

    if use_feed:
        features = fetch_live_day_feed()
        source = "feed:all_day"
    else:
        features = fetch_recent_window(lookback_hours, min_magnitude=min_magnitude)
        source = f"fdsn:{lookback_hours}h"

    rows = [normalize_feature(f) for f in features]
    rows = [r for r in rows if r]

    with db_session() as con:
        before_max = con.execute("SELECT MAX(time) FROM earthquakes").fetchone()[0]
        upserted = upsert_earthquakes(con, rows)
        finished = datetime.now(timezone.utc).replace(tzinfo=None)
        detail = (
            f"source={source}; before_max={before_max}; "
            f"rows_seen={len(rows)}"
        )
        # Zero new rows is normal — overlapping window + idempotent upserts.
        record_run(
            con,
            run_id=run_id,
            started_at=started,
            finished_at=finished,
            mode="ingest",
            rows_seen=len(rows),
            rows_upserted=upserted,
            status="ok",
            detail=detail,
        )
        after = stats(con)
        detail = f"{detail}; after_max={after['max_time']}"
        return {
            "run_id": run_id,
            "rows_seen": len(rows),
            "rows_upserted": upserted,
            "stats": after,
            "detail": detail,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest recent USGS earthquakes")
    parser.add_argument("--lookback-hours", type=float, default=INGEST_LOOKBACK_HOURS)
    parser.add_argument("--min-magnitude", type=float, default=INGEST_MIN_MAGNITUDE)
    parser.add_argument(
        "--use-day-feed",
        action="store_true",
        help="Use the all_day GeoJSON feed instead of FDSN lookback window",
    )
    args = parser.parse_args()

    print(f"store: {connection_target()}")
    try:
        result = run_ingest(
            lookback_hours=args.lookback_hours,
            min_magnitude=args.min_magnitude,
            use_feed=args.use_day_feed,
        )
    except Exception as exc:
        # Record failure so the dashboard / week-later postmortem can see it.
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        with db_session() as con:
            record_run(
                con,
                run_id=f"ingest-fail-{uuid.uuid4().hex[:8]}",
                started_at=started,
                finished_at=started,
                mode="ingest",
                rows_seen=0,
                rows_upserted=0,
                status="error",
                detail=f"{type(exc).__name__}: {exc}",
            )
        traceback.print_exc()
        return 1

    print(
        f"ok run_id={result['run_id']} seen={result['rows_seen']} "
        f"upserted={result['rows_upserted']}"
    )
    print("stats:", result["stats"])
    print(result["detail"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())