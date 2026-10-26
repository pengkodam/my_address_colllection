#!/usr/bin/env python3
"""Pilot: score a sample of Overture addresses against OSM + MCMC references.

This is deliberately an evaluation script, not the validator itself. It measures
how often an address can be parsed, its street matched to an OSM road, its house
number found, and its postcode/state confirmed by the official MCMC table.

Outputs data/pilot_results.csv and prints a report.

Usage:
    py -3 pilot_validate.py --sample 1000
"""

from __future__ import annotations

import argparse
import re
import statistics
from collections import Counter
from pathlib import Path

import duckdb

from address_norm import address_type, parse_candidates, parse_housenumber
from overture_my import canonical_state, state_ctes


def build_sample(con: duckdb.DuckDBPyConnection, n: int) -> None:
    ctes = state_ctes("SELECT * FROM read_parquet('data/my_places_raw.parquet')")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE sample AS
        WITH {ctes}
        SELECT row_number() OVER () AS input_id,
               gers_id, name, address, locality, postcode, region, state, lat, lon
        FROM enriched
        WHERE coalesce(address, '') <> ''
          AND regexp_matches(postcode, '^[0-9]{{5}}$')
          AND lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY hash(gers_id)
        LIMIT {int(n)}
        """
    )


def build_parsed(con: duckdb.DuckDBPyConnection) -> None:
    rows = con.execute(
        "SELECT input_id, address FROM sample ORDER BY input_id"
    ).fetchall()
    parsed: list[tuple[int, str]] = []
    numbers: list[tuple[int, str | None]] = []
    types: list[tuple[int, str]] = []
    for input_id, address in rows:
        for candidate in parse_candidates(address):
            parsed.append((input_id, candidate))
        numbers.append((input_id, parse_housenumber(address)))
        types.append((input_id, address_type(address)))
    con.execute("CREATE OR REPLACE TEMP TABLE parsed (input_id INTEGER, candidate VARCHAR)")
    if parsed:
        con.executemany("INSERT INTO parsed VALUES (?, ?)", parsed)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE sample_hn (input_id INTEGER, housenumber VARCHAR)"
    )
    if numbers:
        con.executemany("INSERT INTO sample_hn VALUES (?, ?)", numbers)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE sample_type "
        "(input_id INTEGER, address_type VARCHAR)"
    )
    con.executemany("INSERT INTO sample_type VALUES (?, ?)", types)


def build_matches(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        "CREATE OR REPLACE TEMP TABLE roads AS "
        "SELECT * FROM read_parquet('data/osm/roads.parquet')"
    )
    con.execute(
        "CREATE OR REPLACE TEMP TABLE osm_addr AS "
        "SELECT * FROM read_parquet('data/osm/addresses.parquet')"
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE road_matches AS
        SELECT p.input_id,
               p.candidate,
               s.lat AS in_lat,
               s.lon AS in_lon,
               r.name AS road_name,
               r.street_norm AS road_norm,
               r.highway,
               CASE
                 WHEN p.candidate = r.street_norm THEN 1.0
                 WHEN length(p.candidate) >= 8
                  AND strpos(r.street_norm, p.candidate) > 0 THEN 0.97
                 WHEN length(r.street_norm) >= 8
                  AND strpos(p.candidate, r.street_norm) > 0 THEN 0.97
                 ELSE jaro_winkler_similarity(p.candidate, r.street_norm)
               END AS sim,
               levenshtein(p.candidate, r.street_norm) AS edit_dist,
               ST_Distance(
                   ST_Point(s.lon, s.lat), ST_GeomFromWKB(r.geom_wkb)
               ) * 111320.0 AS distance_m
        FROM parsed p
        JOIN sample s USING (input_id)
        JOIN roads r
          ON abs(r.lat - s.lat) <= 0.015
         AND abs(r.lon - s.lon) <= 0.015
        WHERE jaro_winkler_similarity(p.candidate, r.street_norm) >= 0.4
           OR (length(p.candidate) >= 8
               AND strpos(r.street_norm, p.candidate) > 0)
           OR (length(r.street_norm) >= 8
               AND strpos(p.candidate, r.street_norm) > 0)
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE best AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY input_id
                       ORDER BY sim - least(distance_m / 10000.0, 0.2) DESC,
                                distance_m ASC
                   ) AS rn
            FROM road_matches
        ) WHERE rn = 1
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE number_hits AS
        SELECT b.input_id,
               count(a.osm_id) FILTER (
                   WHERE a.street_norm = b.road_norm
                     AND lower(regexp_replace(
                         coalesce(a.housenumber, ''), '[^a-zA-Z0-9]', '', 'g'
                     )) = h.housenumber
               ) AS number_hits,
               count(a.osm_id) FILTER (
                   WHERE a.street_norm = b.road_norm
               ) AS street_hits
        FROM best b
        JOIN sample s USING (input_id)
        JOIN sample_hn h USING (input_id)
        LEFT JOIN osm_addr a
          ON abs(a.lat - s.lat) <= 0.005
         AND abs(a.lon - s.lon) <= 0.005
        GROUP BY b.input_id
        """
    )


def build_results(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE mcmc AS
        SELECT postcode, any_value(state) AS state, any_value(city) AS city
        FROM read_parquet('data/postcodes.parquet')
        GROUP BY postcode
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE osm_pc AS
        SELECT DISTINCT postcode FROM osm_addr WHERE coalesce(postcode, '') <> ''
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE overture_pc AS
        SELECT DISTINCT postcode
        FROM read_parquet('data/my_places_raw.parquet')
        WHERE regexp_matches(postcode, '^[0-9]{5}$')
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE results AS
        SELECT s.input_id, s.gers_id, s.name, s.address, s.locality,
               s.postcode, s.state, t.address_type,
               b.candidate, b.road_name, b.road_norm, b.highway,
               b.sim, b.edit_dist, b.distance_m,
               h.housenumber,
               coalesce(n.number_hits, 0) AS number_hits,
               coalesce(n.street_hits, 0) AS street_hits,
               (m.postcode IS NOT NULL) AS in_mcmc,
               (o.postcode IS NOT NULL) AS in_osm,
               (v.postcode IS NOT NULL) AS in_overture,
               m.state AS mcmc_state,
               m.city  AS mcmc_city,
               CASE
                 WHEN m.city IS NULL THEN 'no_official'
                 WHEN coalesce(s.locality, '') = '' THEN 'unknown'
                 WHEN jaro_winkler_similarity(
                          lower(s.locality), lower(m.city)
                      ) >= 0.8 THEN 'match'
                 ELSE 'mismatch'
               END AS city_check
        FROM sample s
        LEFT JOIN sample_type t USING (input_id)
        LEFT JOIN best b USING (input_id)
        LEFT JOIN sample_hn h USING (input_id)
        LEFT JOIN number_hits n USING (input_id)
        LEFT JOIN mcmc m USING (postcode)
        LEFT JOIN osm_pc o USING (postcode)
        LEFT JOIN overture_pc v USING (postcode)
        """
    )


def verdict(row: dict) -> str:
    if not row["candidate"]:
        if row["address_type"] == "area":
            return "unverified_area"
        return "unverified_no_street"
    if not re.fullmatch(r"\d{5}", row["postcode"] or ""):
        return "invalid_postcode"
    sim = row["sim"] or 0.0
    dist = row["distance_m"] if row["distance_m"] is not None else 1e9
    edit = row["edit_dist"] if row["edit_dist"] is not None else 99
    if sim >= 0.95 and dist <= 200:
        if row["housenumber"] and not row["number_hits"]:
            return "valid_street_number_unverified"
        return "valid"
    if sim >= 0.88 and dist <= 100 and edit <= 2:
        return "corrected"
    if sim >= 0.92 and dist <= 500:
        return "probable"
    return "unverified"


def state_check(row: dict) -> str:
    if not row["mcmc_state"]:
        return "no_official"
    ours = canonical_state(row["state"] or "")
    theirs = canonical_state(row["mcmc_state"] or "")
    if not ours or not theirs:
        return "unknown"
    return "match" if ours == theirs else "mismatch"


def load_results(con: duckdb.DuckDBPyConnection) -> list[dict]:
    cur = con.execute("SELECT * FROM results ORDER BY input_id")
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def report(rows: list[dict]) -> None:
    total = len(rows)
    street = [r for r in rows if r["address_type"] == "street"]
    area = [r for r in rows if r["address_type"] == "area"]
    other = [r for r in rows if r["address_type"] == "other"]
    parsed = sum(1 for r in street if r["candidate"])
    sims = [r["sim"] for r in street if r["sim"] is not None]
    distances = [r["distance_m"] for r in street if r["distance_m"] is not None]
    with_number = [r for r in street if r["housenumber"]]
    number_ok = sum(1 for r in with_number if r["number_hits"] > 0)
    verdicts = Counter(verdict(r) for r in rows)
    states = Counter(state_check(r) for r in rows)
    cities = Counter(r["city_check"] for r in rows)
    street_n = max(len(street), 1)

    print(f"Sample: {total:,}")
    print(f"  street addresses:   {len(street):,} ({len(street)/total:.1%})")
    print(f"  area-only addresses:{len(area):,} ({len(area)/total:.1%})")
    print(f"  other:              {len(other):,} ({len(other)/total:.1%})")
    print()
    print(f"Street parsed: {parsed:,} / {len(street):,} ({parsed/street_n:.1%})")
    for threshold in (0.95, 0.92, 0.85, 0.78, 0.60):
        hits = sum(1 for s in sims if s >= threshold)
        print(f"  best road sim >= {threshold:.2f}: {hits:,} ({hits/street_n:.1%} of street)")
    exact = sum(1 for s in sims if s >= 0.999)
    print(f"  exact normalised match: {exact:,} ({exact/street_n:.1%} of street)")
    if distances:
        print(f"  median distance to best road: {statistics.median(distances):.0f} m")
    print()
    print(f"House numbers parsed: {len(with_number):,} of {len(street):,} street rows")
    if with_number:
        print(f"  number found in OSM: {number_ok:,} ({number_ok/len(with_number):.1%})")
    print()
    print("Postcode presence (all rows):")
    for label, key in (("MCMC official", "in_mcmc"), ("OSM", "in_osm"),
                       ("Overture", "in_overture")):
        n = sum(1 for r in rows if r[key])
        print(f"  {label:14} {n:,} ({n/total:.1%})")
    print()
    print("State check (derived vs MCMC postcode):", dict(states))
    print("City check (locality vs MCMC city):    ", dict(cities))
    print()
    print("Verdicts:", dict(verdicts))
    print()
    print("Examples with weak street matches:")
    weak = [r for r in street if r["sim"] is None or r["sim"] < 0.78]
    for r in weak[:10]:
        print(f"  {r['address'][:90]!r} | locality={r['locality']!r} "
              f"| best={r['road_name']!r} sim={r['sim']}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=int, default=1000)
    parser.add_argument("--out", default="data/pilot_results.csv")
    parser.add_argument("--memory-limit", default=None)
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    if args.memory_limit:
        con.execute(f"SET memory_limit='{args.memory_limit}';")

    build_sample(con, args.sample)
    build_parsed(con)
    build_matches(con)
    build_results(con)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM results) TO '{out.as_posix()}' (HEADER, DELIMITER ',')"
    )
    rows = load_results(con)
    report(rows)
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
