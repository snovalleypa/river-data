#!/usr/bin/env python3
"""
fix_and_simplify.py  INPUT.geojson  [OUTPUT.geojson]  [SIMPLIFY%]

Pipeline for ArcGIS-exported flood/coverage GeoJSON that QGIS renders wrong:

  1. Check exterior-ring winding.
       RFC 7946 / "correct for QGIS"  = exterior rings counter-clockwise (CCW).
       ArcGIS export                  = exterior rings clockwise (CW), which signals
                                        even-odd-encoded holes QGIS fills in solid.
  2. If winding is wrong, rebuild each feature into valid OGC polygons-with-holes
     using even-odd ring nesting:  depth parity  ->  odd = filled shell, even = hole.
     Output is re-oriented to RFC 7946 (CCW exterior) so it is idempotent.
  3. Run `mapshaper -simplify <pct> keep-shapes` (topology-aware, so shared borders
     simplify together and small islands are not deleted).

Requires: shapely (only when a fix is needed) and `npx mapshaper`.
"""
import json, sys, os, subprocess, tempfile
from collections import defaultdict


def signed_area(ring):
    """Shoelace; >0 = CCW, <0 = CW."""
    s = 0.0
    for (x1, y1), (x2, y2) in zip(ring, ring[1:]):
        s += x1 * y2 - x2 * y1
    return s / 2.0


def iter_polygons(geom):
    """Yield each polygon's ring list [exterior, hole, ...] from a GeoJSON geometry."""
    if not geom:
        return
    t = geom.get("type")
    if t == "Polygon":
        yield geom["coordinates"]
    elif t == "MultiPolygon":
        for poly in geom["coordinates"]:
            yield poly


def winding_is_correct(gj):
    """Correct (for QGIS/RFC 7946) when exterior rings are predominantly CCW."""
    ccw = cw = 0
    for f in gj["features"]:
        for poly in iter_polygons(f.get("geometry")):
            if not poly:
                continue
            a = signed_area(poly[0])
            if a > 0:
                ccw += 1
            elif a < 0:
                cw += 1
    return (ccw >= cw), ccw, cw


def evenodd_rebuild(rings):
    """rings: every ring (exterior+interior) of one feature, treated as filled disks.
       Returns shapely Polygons with proper holes via even-odd nesting depth."""
    from shapely.geometry import Polygon
    from shapely.strtree import STRtree
    from shapely.prepared import prep
    from shapely.geometry.polygon import orient

    polys = [Polygon(r) for r in rings]
    n = len(polys)
    if n == 0:
        return []
    tree = STRtree(polys)
    areas = [p.area for p in polys]
    prepared = {}
    ancestors = [[] for _ in range(n)]
    depth = [1] * n
    for i, gi in enumerate(polys):
        for j in tree.query(gi):                 # bbox candidates
            if j == i or areas[j] <= areas[i]:
                continue
            pj = prepared.get(j) or prepared.setdefault(j, prep(polys[j]))
            if pj.contains(gi):                  # j strictly contains i
                ancestors[i].append(j)
        depth[i] = len(ancestors[i]) + 1

    holes_of = defaultdict(list)
    for i in range(n):
        if depth[i] % 2 == 0:                    # even depth = hole
            parent = min((j for j in ancestors[i] if depth[j] == depth[i] - 1),
                         key=lambda j: areas[j], default=None)
            if parent is not None:
                holes_of[parent].append(rings[i])

    out = []
    for i in range(n):
        if depth[i] % 2 == 1:                    # odd depth = filled shell
            out.append(orient(Polygon(rings[i], holes_of.get(i, [])), sign=1.0))
    return out


def apply_evenodd(gj):
    from shapely.geometry import MultiPolygon, mapping
    feats = []
    for f in gj["features"]:
        g = f.get("geometry") or {}
        if g.get("type") in ("Polygon", "MultiPolygon"):
            rings = [ring for poly in iter_polygons(g) for ring in poly]
            shells = evenodd_rebuild(rings)
            if shells:
                geom = shells[0] if len(shells) == 1 else MultiPolygon(shells)
                f = {**f, "geometry": mapping(geom)}
        feats.append(f)
    return {**gj, "features": feats}


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: fix_and_simplify.py INPUT.geojson [OUTPUT.geojson] [SIMPLIFY%]")
    inp = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else \
        os.path.splitext(inp)[0] + "_fixed_simplified.geojson"
    simplify = sys.argv[3] if len(sys.argv) > 3 else "10%"

    print(f"reading {inp}")
    gj = json.load(open(inp))
    ok, ccw, cw = winding_is_correct(gj)
    print(f"exterior rings: {ccw} CCW, {cw} CW  ->  "
          f"{'OK (RFC 7946); skipping even-odd fix' if ok else 'WRONG (ArcGIS); applying even-odd fix'}")

    fixed = gj if ok else apply_evenodd(gj)

    with tempfile.NamedTemporaryFile("w", suffix=".geojson", delete=False) as tf:
        json.dump(fixed, tf, separators=(",", ":"))
        tmp = tf.name
    try:
        print(f"mapshaper -simplify {simplify} keep-shapes  ->  {out}")
        subprocess.run(["npx", "mapshaper", tmp,
                        "-simplify", simplify, "keep-shapes",
                        "-o", "precision=0.000001", out], check=True)
    finally:
        os.remove(tmp)
    print("done:", out)


if __name__ == "__main__":
    main()
