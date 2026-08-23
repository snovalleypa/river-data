#!/usr/bin/env python3
"""
generate_flood_viz_dist.py  [--region N] [--data-dir DIR] [--dist-dir DIR] [--rating-site ID]

Assemble the publishable Flood Visualizer bundle in --dist-dir from the maintained
inputs in --data-dir. This:

  1. Builds the per-flood-level RoadClosures GeoJSON files (one per level) from a
     single maintained source roads file (see "Road closures" below).
  2. Enriches the region config with a gage height (`feet`) per level, converting
     each level's `cfs` via the USGS stage-discharge rating table for the region's
     gauge (default Carnation, USGS 12149000), and writes the enriched config into
     the bundle. The committed source config is left untouched.
  3. Copies each level's FloodExtent file into the bundle.
  4. Copies the region's 2D-model boundary file (`modelBoundaryFile` in the config,
     e.g. ModelBoundary.geojson — converted from the King County 2D_Model_Boundary
     KML) into the bundle. The app draws it as an unfilled outline whenever a flood
     level is shown, so people can see the extent of the modeled area.

Publish the resulting --dist-dir to gs://fz-dev-public to update the live map.

Road closures
-------------
Instead of hand-editing one RoadClosures file per level, you maintain one source
FeatureCollection:

    floodzuki_data/floodzilla_roads_region_<N>.geojson

Each road is one LineString feature whose `status` property is a *map* keyed by
flood-level key, giving that road's status at each level:

    "properties": {
      "name":  "NE 124th St",          # feature id
      "label": "NE 124th",             # caption base (defaults to name)
      "status": { "minor": "nearFlooding", "moderate": "flooding", "major": "flooding" }
    }

The level keys + output filenames come from the region config
(flood-viz-levels-region-<N>.json), so adding a new level there (no code change)
flows straight through. For each level this writes a RoadClosures file containing
every road that has a status for that level, with the per-level `status` and a
`label` caption derived from it:

    normal       -> "<label> - Open"               (green in the app)
    nearFlooding -> "<label> - Possible Closure"   (yellow)
    flooding     -> "<label> - Closed"             (red)

The app colors each line from `status` (no color is stored in the GeoJSON).

Outputs everything the Flood Visualizer needs into --dist-dir (default
./floodzuki_dist, gitignored): the generated RoadClosures_*.geojson, the enriched
region config, and a copy of each level's referenced FloodExtent file.
"""
import argparse
import json
import os
import shutil
import sys
import urllib.request

# USGS gauge whose rating table converts each level's `cfs` to a gage height
# (`feet`). Region 1's inundation extents are keyed to Snoqualmie River near
# Carnation. Override with --rating-site.
DEFAULT_RATING_SITE = "12149000"  # Snoqualmie River near Carnation (CRNW1)

# status -> caption suffix. The presence of a status here means it is rendered;
# the app maps the same status to a color (normal=green, nearFlooding=yellow,
# flooding=red). Add a status here if the config/app grows a new one.
STATUS_LABEL_SUFFIX = {
    "normal": "Open",
    "nearFlooding": "Possible Closure",
    "flooding": "Closed",
}


def fetch_usgs_rating(site_no):
    """Fetch the USGS stage-discharge rating for `site_no`.

    Uses the official "exsa" (expanded, shift-adjusted) rating depot table, which is
    authoritative and matches what NWRFC uses. Columns: INDEP (gage height ft),
    SHIFT, DEP (discharge cfs), STOR. Returns (stage[], disch[]) sorted ascending by
    stage. Stdlib-only so the bundle build needs no extra deps.
    """
    url = ("https://nwis.waterdata.usgs.gov/nwisweb/get_ratings"
           f"?file_type=exsa&site_no={site_no}")
    text = urllib.request.urlopen(url, timeout=60).read().decode("utf-8", "replace")
    pairs = []
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split("\t")
        if parts[0] in ("INDEP", "16N"):  # column-name and format header rows
            continue
        try:
            pairs.append((float(parts[0]), float(parts[2])))
        except (ValueError, IndexError):
            continue
    if not pairs:
        sys.exit(f"USGS rating for site {site_no} returned no usable rows.")
    pairs.sort()
    return [s for s, _ in pairs], [d for _, d in pairs]


def cfs_to_feet(cfs, stage, disch):
    """Convert discharge `cfs` to gage height via the rating, or None if out of range.

    Linear interpolation on the (monotonic) discharge->stage curve. Returns None
    outside the rating's discharge span rather than extrapolating.
    """
    if cfs < disch[0] or cfs > disch[-1]:
        return None
    for i in range(1, len(disch)):
        if cfs <= disch[i]:
            d0, d1 = disch[i - 1], disch[i]
            s0, s1 = stage[i - 1], stage[i]
            if d1 == d0:
                return round(s0, 2)
            return round(s0 + (s1 - s0) * (cfs - d0) / (d1 - d0), 2)
    return round(stage[-1], 2)


def load_json(path):
    with open(path) as fh:
        return json.load(fh)


def write_json(path, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as fh:
        fh.write(json.dumps(obj, indent=2) + "\n")


def closure_feature(road, level_key):
    """Build one RoadClosures feature for `road` at `level_key`, or None to skip.

    Skips a road that has no status for this level. Errors on an unknown status so
    a typo in the source surfaces instead of silently dropping a road.
    """
    props = road.get("properties", {})
    status_map = props.get("status", {})
    if level_key not in status_map:
        return None
    status = status_map[level_key]
    if status not in STATUS_LABEL_SUFFIX:
        sys.exit(
            f"Road {props.get('name')!r}: unknown status {status!r} at level "
            f"{level_key!r}. Expected one of {sorted(STATUS_LABEL_SUFFIX)}."
        )
    label_base = props.get("label") or props.get("name", "")
    return {
        "type": "Feature",
        "properties": {
            "name": props.get("name"),
            "level": level_key,
            "status": status,
            "label": f"{label_base} - {STATUS_LABEL_SUFFIX[status]}",
        },
        "geometry": road.get("geometry"),
    }


def main():
    ap = argparse.ArgumentParser(description="Assemble the publishable Flood Visualizer dist bundle from a single source.")
    ap.add_argument("--region", default="1", help="Region id; selects the config and source filenames (default 1).")
    ap.add_argument("--data-dir", default="floodzuki_data", help="Input dir holding the config, source roads, and FloodExtent files (default ./floodzuki_data).")
    ap.add_argument("--dist-dir", default="floodzuki_dist", help="Output bundle dir, rebuilt from scratch (default ./floodzuki_dist).")
    ap.add_argument("--rating-site", default=DEFAULT_RATING_SITE, help=f"USGS site whose rating converts each level's cfs to feet (default {DEFAULT_RATING_SITE}, Carnation).")
    args = ap.parse_args()

    config_name = f"flood-viz-levels-region-{args.region}.json"
    roads_name = f"floodzilla_roads_region_{args.region}.geojson"
    config_path = os.path.join(args.data_dir, config_name)
    roads_path = os.path.join(args.data_dir, roads_name)

    for path in (config_path, roads_path):
        if not os.path.exists(path):
            sys.exit(f"Missing input: {path}")

    config = load_json(config_path)
    levels = config.get("levels", [])
    if not levels:
        sys.exit(f"No levels found in {config_path}")
    source = load_json(roads_path)
    roads = source.get("features", [])
    print(f"region {args.region}: {len(roads)} road(s), {len(levels)} level(s)")

    # Rebuild the dist dir from scratch so removed roads/levels don't linger.
    if os.path.isdir(args.dist_dir):
        shutil.rmtree(args.dist_dir)
    os.makedirs(args.dist_dir, exist_ok=True)

    # One RoadClosures file per level.
    for level in levels:
        key = level["key"]
        out_name = level.get("roadClosuresFile")
        if not out_name:
            print(f"  level {key!r}: no roadClosuresFile in config, skipping road closures")
            continue
        feats = [f for f in (closure_feature(r, key) for r in roads) if f is not None]
        write_json(os.path.join(args.dist_dir, out_name), {"type": "FeatureCollection", "features": feats})
        print(f"  {out_name}: {len(feats)} road(s) [{key}]")

    # Enrich each level with a gage height (`feet`) from the USGS rating, then write
    # the enriched config into the bundle. The committed source config is untouched.
    print(f"  fetching rating for USGS site {args.rating_site} ...")
    try:
        stage, disch = fetch_usgs_rating(args.rating_site)
    except Exception as exc:  # network/HTTP failure -- fail loudly, don't publish heightless
        sys.exit(f"Failed to fetch USGS rating for site {args.rating_site}: {exc}")
    for level in levels:
        cfs = level.get("cfs")
        feet = cfs_to_feet(cfs, stage, disch) if cfs is not None else None
        level["feet"] = feet
        if feet is None:
            print(f"  ! level {level.get('key')!r}: cfs {cfs} outside rating range "
                  f"[{disch[0]:.0f}, {disch[-1]:.0f}] cfs -> feet=null")
        else:
            print(f"  level {level.get('key')!r}: {cfs} cfs -> {feet} ft")
    write_json(os.path.join(args.dist_dir, config_name), config)
    print(f"  wrote enriched {config_name}")
    for level in levels:
        extent = level.get("file")
        if not extent:
            continue
        src = os.path.join(args.data_dir, extent)
        if not os.path.exists(src):
            print(f"  ! FloodExtent missing, not copied: {src}")
            continue
        shutil.copy2(src, os.path.join(args.dist_dir, extent))
        print(f"  copied {extent}")

    # The 2D-model boundary outline, shared by all levels.
    boundary = config.get("modelBoundaryFile")
    if boundary:
        src = os.path.join(args.data_dir, boundary)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(args.dist_dir, boundary))
            print(f"  copied {boundary}")
        else:
            print(f"  ! model boundary missing, not copied: {src}")

    print(f"done -> {args.dist_dir}")


if __name__ == "__main__":
    main()
