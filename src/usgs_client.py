"""USGS Earthquake clients: FDSN history (paginated) + live GeoJSON feeds."""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import requests

from src.config import (
    USGS_FDSN_URL,
    USGS_FEED_ALL_DAY,
    USGS_PAGE_LIMIT,
    USGS_USER_AGENT,
)


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USGS_USER_AGENT})


def _get_json(url: str, params: dict[str, Any] | None = None, retries: int = 4) -> dict:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=120)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"{resp.status_code} for {url}", response=resp)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            last_err = exc
            sleep_s = 2**attempt
            print(f"USGS request failed ({exc}); retry in {sleep_s}s")
            time.sleep(sleep_s)
    raise RuntimeError(f"USGS request failed after {retries} retries: {last_err}")


def features_from_geojson(payload: dict) -> list[dict]:
    return list(payload.get("features") or [])


def normalize_feature(feature: dict) -> dict | None:
    """Map a USGS GeoJSON Feature into a flat row. Returns None if unusable."""
    props = feature.get("properties") or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or []
    event_id = feature.get("id") or props.get("code")
    if not event_id:
        return None
    if len(coords) < 2:
        return None

    lon, lat = float(coords[0]), float(coords[1])
    depth = float(coords[2]) if len(coords) > 2 and coords[2] is not None else None

    time_ms = props.get("time")
    updated_ms = props.get("updated")
    if time_ms is None:
        return None

    return {
        "id": str(event_id),
        "time": datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc).replace(tzinfo=None),
        "updated": (
            datetime.fromtimestamp(updated_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
            if updated_ms is not None
            else None
        ),
        "latitude": lat,
        "longitude": lon,
        "depth_km": depth,
        "mag": props.get("mag"),
        "mag_type": props.get("magType"),
        "place": props.get("place"),
        "type": props.get("type"),
        "status": props.get("status"),
        "tsunami": props.get("tsunami"),
        "sig": props.get("sig"),
        "url": props.get("url"),
    }


def iter_fdsn_window(
    start: datetime,
    end: datetime,
    *,
    min_magnitude: float | None = None,
) -> Iterator[dict]:
    """
    Page through USGS FDSN for [start, end).

    USGS caps each response at 20k events. We walk forward by event time so a
    dense window never silently truncates.
    """
    cursor = start
    while cursor < end:
        params: dict[str, Any] = {
            "format": "geojson",
            "starttime": cursor.strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": end.strftime("%Y-%m-%dT%H:%M:%S"),
            "orderby": "time-asc",
            "limit": USGS_PAGE_LIMIT,
        }
        if min_magnitude is not None:
            params["minmagnitude"] = min_magnitude

        payload = _get_json(USGS_FDSN_URL, params=params)
        features = features_from_geojson(payload)
        if not features:
            break

        for feature in features:
            yield feature

        last_time_ms = features[-1]["properties"]["time"]
        # Nudge past the last event to avoid an infinite loop on equal timestamps.
        next_cursor = datetime.fromtimestamp(last_time_ms / 1000, tz=timezone.utc).replace(
            tzinfo=None
        ) + timedelta(milliseconds=1)
        if next_cursor <= cursor:
            break
        cursor = next_cursor

        if len(features) < USGS_PAGE_LIMIT:
            break


def download_seed_chunk(
    start: datetime,
    end: datetime,
    dest: Path,
    *,
    min_magnitude: float,
) -> int:
    """Fetch one seed window and write raw GeoJSON FeatureCollection to dest."""
    features = list(iter_fdsn_window(start, end, min_magnitude=min_magnitude))
    dest.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "type": "FeatureCollection",
        "metadata": {
            "generated": datetime.now(timezone.utc).isoformat(),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "minmagnitude": min_magnitude,
            "count": len(features),
            "source": USGS_FDSN_URL,
        },
        "features": features,
    }
    dest.write_text(json.dumps(payload), encoding="utf-8")
    return len(features)


def fetch_live_day_feed() -> list[dict]:
    """USGS all_day GeoJSON feed (updates ~every minute)."""
    payload = _get_json(USGS_FEED_ALL_DAY)
    return features_from_geojson(payload)


def fetch_recent_window(hours: float, *, min_magnitude: float = 0.0) -> list[dict]:
    """FDSN pull for an overlapping recent window (used by hourly ingest)."""
    end = datetime.now(timezone.utc).replace(tzinfo=None)
    start = end - timedelta(hours=hours)
    min_mag = min_magnitude if min_magnitude > 0 else None
    return list(iter_fdsn_window(start, end, min_magnitude=min_mag))


def load_features_from_raw_file(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return features_from_geojson(payload)