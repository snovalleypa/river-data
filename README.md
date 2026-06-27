# river-data

A public data repository of stream-gauge readings for the **Snoqualmie River** system, assembled from USGS and Floodzilla gauges, plus a small pipeline that detects flood crests and computes how long a crest takes to travel downstream.

The goal is to make clean, aligned historical gauge data easy to work with — for analysis, visualization, and building models that provide more granular forecasts during flooding events.

## Background

The Snoqualmie Valley floods regularly, and accurate, timely information matters to the farmers and residents who live there.

- **[SVPA](https://snoqualmievalley.org/)** — Snoqualmie Valley Preservation Alliance, a local non-profit that supports farmers with a focus on flood protection.
- **[Floodzilla](https://floodzilla.com/)** — a flood-gauge network and website in the Snoqualmie Valley operated by SVPA. Stations are identified as `SVPA-NN`.
- **USGS** — the U.S. Geological Survey operates stream-flow gauges on the Snoqualmie River and its tributaries. Stations are identified by numeric USGS site IDs.
- **NWRFC** — the NWS Northwest River Forecast Center, which publishes forecasts for some of the USGS gauges on the Snoqualmie.

## Stations

Gauges from the three forks down to Duvall, ordered upstream → downstream by river mile. USGS gauges report **stage and discharge**; SVPA gauges report **stage** only.

| River mile | Name | ID | Source |
|---:|---|---|---|
| — | South Fork | `12143400` | USGS |
| — | Middle Fork | `12141300` | USGS |
| — | North Fork | `12142000` | USGS |
| 38 | Falls | `12144500` | USGS |
| 37 | Sno Falls Golf | `SVPA-37` | SVPA |
| 36 | Neal Road | `SVPA-36` | SVPA |
| 34 | The T | `SVPA-34` | SVPA |
| 33 | Keller Dairy | `SVPA-33` | SVPA |
| 29 | Jubilee Farm | `SVPA-29` | SVPA |
| 26 | Blue Heron Golf | `SVPA-26` | SVPA |
| 25 | Tolt Hill Road | `SVPA-25` | SVPA |
| 22 | Carnation | `12149000` | USGS |
| 19 | Stuart Landing | `SVPA-19` | SVPA |
| 17 | Goose & Gander Farm | `SVPA-17` | SVPA |
| 15 | 124th East | `SVPA-15` | SVPA |
| 15 | 124th West | `SVPA-15B` | SVPA |
| 12 | Wild Canary Farm | `SVPA-12` | SVPA |
| 9 | Duvall | `12150400` | USGS |
| 6 | Cherry Creek | `SVPA-6` | SVPA |
| 6 | Cherry Valley Wildlife Area | `SVPA-6B` | SVPA |

The three forks join above the Falls gauge; the pipeline also builds a synthetic **`FORKS`** station by summing their discharge.

[`locations.json`](locations.json) is the single source of truth for every station — `id`, `name`, `source`, and optional `upstreamOf`, `riverMile`, and `stageOffset`.

## Historical Crests Archive

For most users this is the useful part of the repo: a committed, ready-to-use archive of every flood crest the pipeline has detected — no need to download or regenerate the raw data. It lives in [`crests/`](crests/).

Each station has one tab-delimited CSV at `crests/{source}/{id}/{source}-{id}-crests.csv`, with one row per detected crest. The first column is always `utc` (the crest timestamp), followed by that reading's values:

- **USGS stations** — `utc, discharge, discharge_qualifiers, location, stage, stage_qualifiers`
- **SVPA stations** — `utc, id, stage, stage_qualifiers, location, discharge, discharge_qualifiers` (discharge is blank — SVPA gauges report stage only)
- **`FORKS`** (synthetic sum of the three forks) — `utc, location, discharge, discharge_qualifiers`, detected on discharge

A crest is recorded when a new high is followed by an 18-hour quiet period and the rise clears a minimum threshold above the trailing low (1 ft of stage, or a 25% jump in discharge). See [`src/calc-crests.py`](src/calc-crests.py) for the exact logic and tunable constants.

### `crests/aligned.csv`

The headline product: upstream crests paired to the matching downstream crest, giving observed **travel times** through the valley. Each row pairs a `FORKS` (above the Falls) crest with the nearest Falls crest within 24 hours:

| Column | Description |
|---|---|
| `up_utc` | upstream (`FORKS`) crest time |
| `down_utc` | matched downstream (Falls, `12144500`) crest time |
| `crest_hours` | travel time between them, in hours |
| `up_discharge` | upstream crest discharge (cfs) |
| `down_discharge` | downstream crest discharge (cfs) |

> Crest detection is heuristic. `data_files/historic-crest-overrides.json` is a manual correction layer used by the analysis notebooks to suppress false positives or override a detected value.

## Raw Readings Data

Raw readings are stored as Parquet under `data/{source}/{id}/{id}-{year}.parquet`, normalized to a common schema across both sources:

| Column | Description |
|---|---|
| `datetimeUTC` | reading timestamp (index, UTC) |
| `stage` | water height, feet |
| `stage_qualifiers` | USGS data qualifiers (e.g. `P` provisional) |
| `discharge` | flow, cubic feet per second (USGS only) |
| `discharge_qualifiers` | USGS data qualifiers |
| `location` | station id |

Files are organized by **water year** (Oct 1 → Sep 30, named by the ending year), e.g. `-2022.parquet` covers `2021-10-01` through `2022-09-30`.

> **Note:** `data/` is **not** committed — it is large and regenerated from the source APIs. Run the fetch step below to populate it. Crest outputs in `crests/` and reference files in `data_files/` *are* committed.

## Pipeline

The scripts in [`src/`](src/) form a sequential pipeline; each step feeds the next. All scripts assume the **repo root** as the working directory.

1. **`fetch-gauge-data.py`** — downloads readings for each station into `data/`. USGS data comes via [`hydrofunctions`](https://hydrofunctions.readthedocs.io/); SVPA data comes from the Floodzilla `GetGageReadingsUTC` endpoint.
2. **`sum-forks.py`** — sums the discharge of the three forks into a synthetic `FORKS` station at `data/usgs/forks/forks.parquet`.
3. **`calc-crests.py`** — detects flood crests with a state machine over sorted readings (a crest needs a new high, an 18-hour quiet gap after it, and a minimum rise above the trailing low). Output: `crests/{source}/{id}/...-crests.csv` (tab-delimited).
4. **`align-crests.py`** — pairs each upstream crest with the nearest downstream crest within 24 hours and records the travel time (`crest_hours`). Output: `crests/aligned.csv`.

### Setup & run

Create a virtualenv and install the dependencies (one-time):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r src/requirements.txt
```

Activate the environment in each new terminal (`source .venv/bin/activate`), then run the scripts with plain `python`:

```bash
# fetch all stations for a water-year range
python src/fetch-gauge-data.py -y 2001-2026

# one station, one year
python src/fetch-gauge-data.py -y 2022 -id SVPA-26

# all USGS stations only
python src/fetch-gauge-data.py -y 2022 -s USGS

# then build the derived crest data
python src/sum-forks.py
python src/calc-crests.py
python src/align-crests.py
```

`-y` / `--floodyears` takes a single water year (`2022`) or an inclusive range (`2001-2026`).

## Flood Visualizer map data

Separate from the gauge pipeline, the repo also holds the map data behind the
Floodzilla app's **Flood Visualizer** (the "Map" tab), which shows — per flood
level (minor / moderate / major, keyed by CFS) — the river's inundation extent plus
road-closure overlays. This data is published to and served from Google Cloud
Storage; [`floodzuki_data/`](floodzuki_data/) is the committed mirror.

- **`flood-viz-levels-region-N.json`** — the level config: each level's CFS, its
  inundation polygon file, and its road-closure file. New levels are added here.
- **`FloodExtent_<cfs>CFS_fixed_simplified.geojson`** — the inundation polygons for
  each level, cleaned and simplified by [`flood_extents/fix_and_simplify.py`](flood_extents/fix_and_simplify.py).
- **`floodzilla_roads_region_N.geojson`** — the single source for road closures.
  Each road is one line whose `status` (`normal` / `nearFlooding` / `flooding`) is
  recorded **per flood level**.

[`flood_extents/generate_flood_viz_dist.py`](flood_extents/generate_flood_viz_dist.py)
turns those sources into a ready-to-publish bundle in `floodzuki_dist/` (gitignored):
the per-level road-closure files (line color and caption — "Open" / "Possible
Closure" / "Closed" — derived from each road's status per level, so you maintain one
file instead of one per level), an enriched region config that adds a gage height
(`feet`) per level from the USGS rating table, and each level's FloodExtent file.

```bash
python3 flood_extents/generate_flood_viz_dist.py
```

## Repository layout

```
locations.json     # station registry (source of truth)
src/               # the fetch → sum → crest → align pipeline
data/              # raw Parquet readings (gitignored; regenerate via fetch)
crests/            # detected crests per station + aligned.csv (committed)
data_files/        # manual reference/correction files (committed)
notebooks/         # analysis on top of pipeline outputs (not automated)
floodzuki_data/    # Flood Visualizer map data: level config, inundation
                   #   polygons, road-closure source (committed mirror of GCS)
flood_extents/     # scripts to clean inundation polygons & generate road closures
floodzuki_dist/    # generated, publishable Flood Visualizer bundle (gitignored)
```

`notebooks/` holds exploratory analysis — crest timing, mapping peak annual height, etc. — that reads directly from `data/` and `crests/`. `data_files/historic-crest-overrides.json` is a manual correction layer for the crest-analysis notebooks (suppress false positives or override a detected value).

## License

[MIT](LICENSE) © Snoqualmie Valley Preservation Alliance
