# USGS Earthquake Data Pipeline

BYU CS 452 Data Pipeline project: bulk-load historical earthquakes, keep a live trickle flowing unattended, and prove it with a dashboard (quakes per day + last-week map).

## Architecture

```text
USGS FDSN / GeoJSON ──(seed once)──► data/raw/*.geojson ──► DuckDB / MotherDuck
                                              ▲
USGS recent window ──(GitHub Actions hourly)──┘ upsert on event id
                                              │
                                              ▼
                                    Streamlit dashboard
                         (quakes/day, 7-day map, pipeline_runs)
```

| Stage | Choice | Why |
|-------|--------|-----|
| 1 Source | USGS Earthquakes | Schema-consistent history + live feed, no API key |
| 2 Ingest | GitHub Actions (`cron` hourly) | Free, version-controlled, runs while the laptop sleeps |
| 3 Buffer | None (direct write) | Volume is small; less cost/ops |
| 4 Store | Local DuckDB for seed/dev; MotherDuck for hosted trickle | Fast laptop seed; cloud store keeps the dashboard alive. Pin `duckdb==1.5.6` (MotherDuck does not support DuckDB 2.x yet). |
| 5 Prove alive | Streamlit | Quakes/day chart + last-week map + ingest run log |

## Quick start (local)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # optional; leave MOTHERDUCK_TOKEN empty for local DuckDB

# 1) Download history once to data/raw/, then bulk-load
python src/seed.py

# Re-load later without hitting USGS again:
python src/seed.py --from-disk

# 2) Trickle ingest (same code Actions runs)
python src/ingest.py

# 3) Dashboard
streamlit run dashboard/app.py
```

Default seed window: last ~365 days, magnitude ≥ 2.5 (override with `--start` / `--end` / `--min-magnitude` or `.env`).

## MotherDuck + GitHub Actions (keep it alive for a week)

1. Create a free account at [motherduck.com](https://motherduck.com) and copy an access token.
2. Locally: put `MOTHERDUCK_TOKEN=...` in `.env` (gitignored) and run `python src/seed.py` so the hosted DB gets the historical seed.
3. In the GitHub repo: **Settings → Secrets and variables → Actions → New repository secret**
   - Name: `MOTHERDUCK_TOKEN`
   - Value: your token
4. Open **Actions → Hourly USGS ingest → Run workflow** once to verify.
5. Leave it scheduled (`17 * * * *`). The week-later check should show new `pipeline_runs` and a moving `MAX(time)`.

Without the secret, Actions cannot reach MotherDuck — local DuckDB on the runner would be discarded after the job.

## Requirements checklist

**Idempotency** — ingest always re-fetches an overlapping ~48h window (feeds are windows, not deltas). Rows upsert on USGS `id` (`INSERT … ON CONFLICT DO UPDATE`).

**Gaps / failures** — seed ends at “today”; live FDSN/feed covers the recent hours. A run that sees zero net-new events still records `pipeline_runs.status='ok'` (overlap + lag are normal). If USGS/MotherDuck is down, the run records `status='error'` with the exception and exits non-zero; the next hourly schedule backfills via the lookback window (no separate backfill job).

**Secrets** — tokens only in `.env` / GitHub Actions secrets. Never commit `.env`.

**Cost** — expected **$0** (GitHub Actions free minutes on a public repo, MotherDuck free tier, USGS free). Report the actual billing console figure in your writeup.

## Demo talking points (30s)

1. Show architecture sketch (this README diagram is enough; hand-drawn is fine for class).
2. Open the dashboard: quakes/day + map of last 7 days.
3. Show Actions run history (unattended) and/or `pipeline_runs` table growing.
4. Optional: “today vs historical daily average” panel on the dashboard.

## Project layout

```text
src/usgs_client.py   # FDSN pagination + live feed
src/db.py            # DuckDB/MotherDuck + upserts
src/seed.py          # one-time download-to-disk + bulk load
src/ingest.py        # hourly trickle
dashboard/app.py     # Streamlit UI
.github/workflows/ingest.yml
data/raw/            # gitignored raw GeoJSON seed files
```
