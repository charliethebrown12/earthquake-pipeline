"""
Earthquake pipeline dashboard.

Shows:
  1) Quakes per day (full store)
  2) Map of the last 7 days of earthquakes
  3) Pipeline liveness (recent ingest runs / newest event)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import connection_target
from src.db import connect

st.set_page_config(page_title="USGS Earthquake Pipeline", layout="wide")


@st.cache_data(ttl=60)
def load_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    con = connect()
    try:
        quakes = con.execute(
            """
            SELECT id, time, latitude, longitude, depth_km, mag, place, ingested_at
            FROM earthquakes
            ORDER BY time
            """
        ).df()
        daily = con.execute(
            """
            SELECT CAST(time AS DATE) AS day, COUNT(*) AS quakes
            FROM earthquakes
            GROUP BY 1
            ORDER BY 1
            """
        ).df()
        runs = con.execute(
            """
            SELECT run_id, started_at, finished_at, mode, rows_seen, rows_upserted, status, detail
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 30
            """
        ).df()
    finally:
        con.close()
    return quakes, daily, runs


st.title("USGS Earthquake Pipeline")
st.caption(f"Analytical store: `{connection_target()}` · auto-refreshes cached queries every 60s")

try:
    quakes, daily, runs = load_frames()
except Exception as exc:
    st.error(
        "Could not open the database. Seed locally first (`python src/seed.py`) "
        "or set `MOTHERDUCK_TOKEN` for the hosted store.\n\n"
        f"Details: `{exc}`"
    )
    st.stop()

if quakes.empty:
    st.warning("No earthquake rows yet. Run `python src/seed.py` then `python src/ingest.py`.")
    st.stop()

newest = quakes["time"].max()
oldest = quakes["time"].min()
last_ingest = quakes["ingested_at"].max()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Events in store", f"{len(quakes):,}")
c2.metric("Oldest event", str(oldest)[:19])
c3.metric("Newest event", str(newest)[:19])
c4.metric("Last ingested_at", str(last_ingest)[:19])

st.subheader("Quakes per day")
fig = px.bar(daily, x="day", y="quakes", labels={"day": "Day (UTC)", "quakes": "Count"})
fig.update_layout(margin=dict(l=10, r=10, t=30, b=10), height=360)
st.plotly_chart(fig, use_container_width=True)

st.subheader("Map — last 7 days")
cutoff = pd.Timestamp(newest) - pd.Timedelta(days=7)
week = quakes[quakes["time"] >= cutoff].copy()
st.write(f"{len(week):,} events since {cutoff} (relative to newest event in store)")

if week.empty:
    st.info("No events in the last week window.")
else:
    week["mag_plot"] = week["mag"].fillna(0).clip(lower=0)
    st.map(
        week.rename(columns={"latitude": "lat", "longitude": "lon"}),
        latitude="lat",
        longitude="lon",
        size="mag_plot",
    )
    st.dataframe(
        week.sort_values("time", ascending=False)[
            ["time", "mag", "place", "depth_km", "latitude", "longitude"]
        ].head(50),
        use_container_width=True,
    )

st.subheader("Pipeline liveness")
st.write(
    "Successful unattended ingest runs prove the trickle is alive. "
    "`rows_upserted` can be >0 while net-new events are few — overlapping windows are expected."
)
if runs.empty:
    st.info("No pipeline_runs yet.")
else:
    st.dataframe(runs, use_container_width=True)

# Recommended demo: today vs historical daily average
st.subheader("Today vs historical daily average")
if not daily.empty:
    today = pd.Timestamp(newest).date()
    today_count = int(daily.loc[daily["day"] == today, "quakes"].sum()) if today in set(daily["day"]) else 0
    hist = daily[daily["day"] < today]
    avg = float(hist["quakes"].mean()) if not hist.empty else float("nan")
    st.write(
        f"Events on **{today}** (partial day OK): **{today_count}**. "
        f"Average across earlier seeded days: **{avg:.1f}**."
    )
