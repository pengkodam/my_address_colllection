#!/usr/bin/env python3
"""Build a Malaysian street/address reference from an OpenStreetMap PBF.

The output is two Parquet files:

    roads.parquet      named highways, with line geometry and a centroid
    addresses.parquet  objects carrying addr:housenumber / addr:street

Both are filtered to Malaysian land using Overture divisions polygons. The
scripts intentionally keep raw names and a normalised `street_norm` column so a
validator can do fuzzy matching without re-reading the PBF.

Usage:
    py -3 build_osm_reference.py `
        --pbf data/osm/my-sg-bn.osm.pbf `
        --out-dir data/osm `
        --regions data/my_regions.parquet

Licence: OpenStreetMap data is ODbL 1.0. Keep the attribution if you
redistribute anything derived from these files.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

log = logging.getLogger("osmref")

# Normalise the abbreviations that appear in Malaysian addresses. Only map
# tokens where the expansion is unambiguous.
ABBREV = {
    "jln": "jalan",
    "lrg": "lorong",
    "kg": "kampung",
    "bt": "batu",
    "tmn": "taman",
    "pst": "persiaran",
    "lbh": "lebuh",
    "bdr": "bandar",
    "wly": "wilayah",
    "persekutuan": "persekutuan",
    "sdn": "sdn",
    "bhd": "bhd",
}


def lit(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def normalize_street(value: str) -> str:
    """Lowercase, strip punctuation, expand common Malaysian abbreviations."""
    if value is None:
        return ""
    text = str(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [ABBREV.get(t, t) for t in text.split()]
    return " ".join(t for t in tokens if t)


def norm_sql(col: str) -> str:
    """SQL equivalent of normalize_street() for use in COPY queries."""
    expr = f"lower(regexp_replace(coalesce({col}, ''), '[^a-zA-Z0-9]+', ' ', 'g'))"
    for abbr, full in ABBREV.items():
        if abbr == full:
            continue
        expr = f"regexp_replace({expr}, '\\b{abbr}\\b', '{full}', 'g')"
    return f"trim(regexp_replace({expr}, '\\s+', ' ', 'g'))"


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
    except duckdb.Error as exc:  # pragma: no cover - environment problem
        raise SystemExit(f"Could not load the DuckDB spatial extension: {exc}") from exc
    if args.memory_limit:
        con.execute(f"SET memory_limit={lit(args.memory_limit)};")
    if args.temp_dir:
        Path(args.temp_dir).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={lit(args.temp_dir)};")
    return con


def build_regions(con: duckdb.DuckDBPyConnection, path: str) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE my_union AS
        SELECT ST_Union_Agg(geom) AS geom
        FROM read_parquet({lit(path)})
        """
    )


def build_roads(con: duckdb.DuckDBPyConnection, pbf: str, out: Path) -> int:
    started = time.time()
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE road_ways AS
        SELECT id,
               coalesce(tags->>'name', tags->>'name:en') AS name,
               tags->>'highway' AS highway,
               tags->>'ref' AS ref,
               refs
        FROM st_readosm({lit(pbf)})
        WHERE kind = 'way'
          AND tags->>'highway' IS NOT NULL
          AND coalesce(tags->>'name', tags->>'name:en') IS NOT NULL
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE road_refs AS
        SELECT id AS way_id,
               unnest(refs) AS node_id,
               generate_subscripts(refs, 1) AS ord
        FROM road_ways
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE road_nodes AS
        SELECT n.id, n.lat, n.lon
        FROM st_readosm({lit(pbf)}) n
        SEMI JOIN (SELECT DISTINCT node_id FROM road_refs) x ON n.id = x.node_id
        WHERE n.kind = 'node'
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE roads AS
        SELECT w.id,
               w.name,
               w.highway,
               w.ref,
               ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY r.ord)) AS geom
        FROM road_ways w
        JOIN road_refs r ON w.id = r.way_id
        JOIN road_nodes n ON r.node_id = n.id
        GROUP BY w.id, w.name, w.highway, w.ref
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT r.id AS osm_id,
                   r.name,
                   r.highway,
                   r.ref,
                   {norm_sql('r.name')} AS street_norm,
                   ST_X(ST_Centroid(r.geom)) AS lon,
                   ST_Y(ST_Centroid(r.geom)) AS lat,
                   ST_AsWKB(r.geom) AS geom_wkb
            FROM roads r, my_union m
            WHERE ST_Intersects(r.geom, m.geom)
        ) TO {lit(str(out))} (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    rows = con.execute(f"SELECT count(*) FROM read_parquet({lit(str(out))})").fetchone()[0]
    log.info("roads: %s rows in %.1fs", f"{rows:,}", time.time() - started)
    return rows


def build_addresses(con: duckdb.DuckDBPyConnection, pbf: str, out: Path) -> int:
    started = time.time()
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE addr_nodes AS
        SELECT id, 'node' AS kind, tags, lat, lon
        FROM st_readosm({lit(pbf)})
        WHERE kind = 'node'
          AND (tags->>'addr:housenumber' IS NOT NULL
               OR tags->>'addr:street' IS NOT NULL)
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE addr_ways AS
        SELECT id, tags, refs
        FROM st_readosm({lit(pbf)})
        WHERE kind = 'way'
          AND (tags->>'addr:housenumber' IS NOT NULL
               OR tags->>'addr:street' IS NOT NULL)
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE addr_refs AS
        SELECT id AS way_id, unnest(refs) AS node_id
        FROM addr_ways
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE addr_node_coords AS
        SELECT n.id, n.lat, n.lon
        FROM st_readosm({lit(pbf)}) n
        SEMI JOIN (SELECT DISTINCT node_id FROM addr_refs) x ON n.id = x.node_id
        WHERE n.kind = 'node'
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE addr_way_points AS
        SELECT w.id, avg(n.lat) AS lat, avg(n.lon) AS lon
        FROM addr_ways w
        JOIN addr_refs r ON w.id = r.way_id
        JOIN addr_node_coords n ON r.node_id = n.id
        GROUP BY w.id
        """
    )
    con.execute(
        f"""
        COPY (
            WITH all_addr AS (
                SELECT id AS osm_id, kind,
                       tags->>'name' AS name,
                       tags->>'addr:housenumber' AS housenumber,
                       tags->>'addr:street' AS street,
                       tags->>'addr:postcode' AS postcode,
                       tags->>'addr:city' AS city,
                       tags->>'addr:state' AS state,
                       lat, lon
                FROM addr_nodes
                UNION ALL
                SELECT w.id, 'way',
                       w.tags->>'name',
                       w.tags->>'addr:housenumber',
                       w.tags->>'addr:street',
                       w.tags->>'addr:postcode',
                       w.tags->>'addr:city',
                       w.tags->>'addr:state',
                       p.lat, p.lon
                FROM addr_ways w
                JOIN addr_way_points p ON w.id = p.id
            )
            SELECT a.*, {norm_sql('a.street')} AS street_norm
            FROM all_addr a, my_union m
            WHERE a.lat IS NOT NULL AND a.lon IS NOT NULL
              AND ST_Within(ST_Point(a.lon, a.lat), m.geom)
        ) TO {lit(str(out))} (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    rows = con.execute(f"SELECT count(*) FROM read_parquet({lit(str(out))})").fetchone()[0]
    log.info("addresses: %s rows in %.1fs", f"{rows:,}", time.time() - started)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pbf", required=True, help="OSM .osm.pbf extract")
    parser.add_argument("--out-dir", default="data/osm", help="Output directory")
    parser.add_argument("--regions", default="data/my_regions.parquet",
                        help="Parquet with a `geom` state-polygon column")
    parser.add_argument("--memory-limit", default=None)
    parser.add_argument("--temp-dir", default=None)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    started = time.time()

    con = connect(args)
    build_regions(con, args.regions)
    roads = build_roads(con, args.pbf, out_dir / "roads.parquet")
    addresses = build_addresses(con, args.pbf, out_dir / "addresses.parquet")

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tool": "build_osm_reference.py",
        "source": args.pbf,
        "regions": args.regions,
        "roads": roads,
        "addresses": addresses,
        "elapsed_seconds": round(time.time() - started, 1),
        "licence": "OpenStreetMap ODbL 1.0; © OpenStreetMap contributors",
    }
    (out_dir / "reference.meta.json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )
    log.info("done in %.1fs", time.time() - started)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
