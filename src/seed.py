#!/usr/bin/env python3
"""
One-time historical seed.

Etiquette: download each chunk once to data/raw/, then bulk-load from disk.
Re-running with --from-disk skips the network and only reloads saved GeoJSON.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Allow `python src/seed.py` from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import RAW_DIR, SEED_END, SEED_MIN_MAGNITUDE, SEED_START, connection_target
from src.db import db_session, record_run, stats, upsert_earthquakes
from src.usgs_client import download_seed_chunk, load_features_from_raw_file, normalize_feature


def month_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor < end:
        if cursor.month == 12:
            nxt = cursor.replace(year=cursor.year + 1, month=1, day=1)
        else:
            nxt = cursor.replace(month=cursor.month + 1, day=1)
        windows.append((cursor, min(nxt, end)))
        cursor = nxt
    return windows


def parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def raw_path_for(start: datetime, end: datetime, min_mag: float) -> Path:
    name = f"usgs_{start.date()}_{end.date()}_m{min_mag}.geojson"
    return RAW_DIR / name


def download_seed(start: datetime, end: datetime, min_mag: float) -> list[Path]:
    paths: list[Path] = []
    for w_start, w_end in month_windows(start, end):
        dest = raw_path_for(w_start, w_end, min_mag)
        if dest.exists():
            print(f"skip download (exists): {dest.name}")
            paths.append(dest)
            continue
        print(f"downloading {w_start.date()} → {w_end.date()} …")
        count = download_seed_chunk(w_start, w_end, dest, min_magnitude=min_mag)
        print(f"  wrote {count:,} features → {dest}")
        paths.append(dest)
    return paths


def load_from_disk(paths: list[Path]) -> int:
    total = 0
    with db_session() as con:
        for path in paths:
            features = load_features_from_raw_file(path)
            rows = [normalize_feature(f) for f in features]
            rows = [r for r in rows if r]
            n = upsert_earthquakes(con, rows)
            total += n
            print(f"loaded {n:,} from {path.name}")
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed USGS earthquake history into DuckDB/MotherDuck")
    parser.add_argument("--start", default=SEED_START, help="YYYY-MM-DD inclusive")
    parser.add_argument("--end", default=SEED_END, help="YYYY-MM-DD exclusive-ish end date")
    parser.add_argument("--min-magnitude", type=float, default=SEED_MIN_MAGNITUDE)
    parser.add_argument(
        "--from-disk",
        action="store_true",
        help="Only bulk-load existing data/raw/*.geojson (no network)",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Download raw files only; do not load into the store",
    )
    args = parser.parse_args()

    start = parse_date(args.start)
    end = parse_date(args.end)
    if end <= start:
        raise SystemExit("--end must be after --start")

    print(f"store: {connection_target()}")
    run_id = f"seed-{uuid.uuid4().hex[:10]}"
    started = datetime.now(timezone.utc).replace(tzinfo=None)

    if args.from_disk:
        paths = sorted(RAW_DIR.glob("usgs_*.geojson"))
        if not paths:
            raise SystemExit(f"No raw files under {RAW_DIR}. Run without --from-disk first.")
    else:
        paths = download_seed(start, end, args.min_magnitude)

    if args.download_only:
        print("download-only complete")
        return 0

    rows_upserted = load_from_disk(paths)
    finished = datetime.now(timezone.utc).replace(tzinfo=None)

    with db_session() as con:
        record_run(
            con,
            run_id=run_id,
            started_at=started,
            finished_at=finished,
            mode="seed",
            rows_seen=rows_upserted,
            rows_upserted=rows_upserted,
            status="ok",
            detail=f"{start.date()}..{end.date()} m>={args.min_magnitude}",
        )
        print("stats:", stats(con))

    print(f"seed complete: {rows_upserted:,} rows upserted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())