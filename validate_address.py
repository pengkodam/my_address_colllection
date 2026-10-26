#!/usr/bin/env python3
"""Validate Malaysian addresses against OSM roads and the official MCMC postcodes.

Designed for internal use, in two modes:

    # One address (prints JSON)
    py -3 validate_address.py --address "12, Jalan Ampang" `
        --postcode 50450 --city "Kuala Lumpur" --state "Kuala Lumpur"

    # Batch (CSV or JSONL in, same format out)
    py -3 validate_address.py --input addresses.csv --out validated.csv

Python:

    from validate_address import AddressValidator
    validator = AddressValidator()          # keep this object warm
    result = validator.validate(
        address="12, Jalan Ampang", postcode="50450", state="Kuala Lumpur"
    )

The validator never turns missing reference data into "invalid". It returns
`valid`, `valid_street_number_unverified`, `probable`, `corrected`, `unverified`,
`unverified_area`, `unverified_no_street`, `invalid_postcode`, `invalid_state`
or `invalid_input`.

Licensing: OSM data is ODbL 1.0; MCMC postcodes are CC BY 4.0; Overture is
CDLA-Permissive/Apache. See the README.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable

import duckdb

from address_norm import (
    address_type,
    normalize_street,
    normalize_text,
    numeric_compatible,
    parse_candidates,
    parse_housenumber,
    street_match_level,
)
from overture_my import canonical_state

OUTPUT_FIELDS = [
    "input_id", "address", "postcode", "city", "state",
    "verdict", "confidence",
    "parsed_street", "parsed_number", "address_type",
    "matched_road", "road_highway", "street_similarity", "edit_distance",
    "road_distance_m", "anchor_source", "street_source", "postcode_status",
    "official_city", "official_state", "state_status", "city_status",
    "number_status", "street_in_postcode_hits",
]

ADDRESS_KEYS = ("address", "full_address", "addr", "line1", "street_address")
POSTCODE_KEYS = ("postcode", "postal_code", "postalcode", "zip", "zipcode")
CITY_KEYS = ("city", "locality", "town", "mukim")
STATE_KEYS = ("state", "region", "province")
LAT_KEYS = ("lat", "latitude")
LON_KEYS = ("lon", "lng", "longitude")

GENERIC_CORE = {"besar", "kecil", "baru", "lama", "tengah", "utama", "bestari"}
ROAD_PREFIXES = {"jalan", "jln", "lorong", "lrg", "persiaran", "lebuh", "off"}

VERDICT_CONFIDENCE = {
    "valid": 0.95,
    "valid_street_number_unverified": 0.85,
    "probable": 0.65,
    "corrected": 0.85,
    "unverified": 0.15,
    "unverified_area": 0.10,
    "unverified_no_street": 0.05,
    "invalid_postcode": 0.99,
    "invalid_state": 0.95,
    "invalid_input": 0.99,
}


def lit(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pick(row: dict[str, Any], keys: Iterable[str]) -> Any:
    lowered = {str(k).lower().strip(): v for k, v in row.items()}
    for key in keys:
        if key in lowered and lowered[key] not in (None, ""):
            return lowered[key]
    return None


def is_generic_road(street_norm: str | None) -> bool:
    if not street_norm:
        return True
    core = [t for t in street_norm.split() if t not in ROAD_PREFIXES]
    if not core:
        return True
    if all(t.isdigit() for t in core):
        return True
    return len(core) == 1 and core[0] in GENERIC_CORE


def city_status(input_city: str | None, official_city: str | None) -> str:
    if not official_city:
        return "unknown"
    if not input_city:
        return "not_provided"
    a = normalize_text(input_city)
    b = normalize_text(official_city)
    if a == b or SequenceMatcher(None, a, b).ratio() >= 0.8:
        return "match"
    return "different"


class AddressValidator:
    """Loads the reference once; call validate() or validate_many()."""

    def __init__(self, data_dir: str | Path = "data", memory_limit: str | None = None):
        self.data_dir = Path(data_dir)
        self.con = duckdb.connect()
        try:
            self.con.execute("INSTALL spatial; LOAD spatial;")
        except duckdb.Error as exc:  # pragma: no cover
            raise SystemExit(f"Could not load the DuckDB spatial extension: {exc}") from exc
        if memory_limit:
            self.con.execute(f"SET memory_limit={lit(memory_limit)};")
        self._load()

    # ---------------------------------------------------------------- loading
    def _require(self, name: str) -> Path:
        path = self.data_dir / name
        if not path.exists():
            raise FileNotFoundError(
                f"{path} is missing. Build the reference first; see README.md "
                "(Address validation pilot)."
            )
        return path

    def _load(self) -> None:
        roads = self._require("osm/roads.parquet")
        addresses = self._require("osm/addresses.parquet")
        postcodes = self._require("postcodes.parquet")

        self.con.execute(
            f"CREATE TEMP TABLE roads AS SELECT * FROM read_parquet({lit(str(roads))})"
        )
        self.con.execute(
            f"CREATE TEMP TABLE addr AS SELECT * FROM read_parquet({lit(str(addresses))})"
        )
        self.con.execute(
            f"""
            CREATE TEMP TABLE mcmc AS
            SELECT postcode, any_value(state) AS state, any_value(city) AS city
            FROM read_parquet({lit(str(postcodes))})
            GROUP BY postcode
            """
        )
        self.con.execute(
            "CREATE TEMP TABLE osm_pc AS "
            "SELECT DISTINCT postcode FROM addr WHERE coalesce(postcode, '') <> ''"
        )
        self.con.execute(
            """
            CREATE TEMP TABLE pc_centroid_osm AS
            SELECT postcode, median(lat) AS lat, median(lon) AS lon
            FROM addr
            WHERE coalesce(postcode, '') <> ''
              AND lat IS NOT NULL AND lon IS NOT NULL
            GROUP BY postcode
            """
        )

        # Overture is optional: used for postcode/locality anchors and as a
        # fallback postcode universe.
        places = self.data_dir / "my_places_raw.parquet"
        if places.exists():
            self.con.execute(
                f"""
                CREATE TEMP TABLE ov AS
                SELECT postcode, locality, lat, lon
                FROM read_parquet({lit(str(places))})
                WHERE regexp_matches(postcode, '^[0-9]{{5}}$')
                  AND lat IS NOT NULL AND lon IS NOT NULL
                """
            )
            self.con.execute(
                """
                CREATE TEMP TABLE pc_centroid_ov AS
                SELECT postcode, median(lat) AS lat, median(lon) AS lon
                FROM ov GROUP BY postcode
                """
            )
            self.con.execute(
                """
                CREATE TEMP TABLE city_centroid AS
                SELECT regexp_replace(lower(trim(locality)), '[^a-z0-9]+', ' ', 'g')
                           AS city_key,
                       median(lat) AS lat, median(lon) AS lon
                FROM ov
                WHERE coalesce(locality, '') <> ''
                GROUP BY 1
                HAVING count(*) >= 3
                """
            )
            self.con.execute(
                "CREATE TEMP TABLE overture_pc AS "
                "SELECT DISTINCT postcode FROM ov"
            )
        else:
            self.con.execute(
                "CREATE TEMP TABLE ov (postcode VARCHAR, locality VARCHAR, "
                "lat DOUBLE, lon DOUBLE)"
            )
            self.con.execute(
                "CREATE TEMP TABLE pc_centroid_ov (postcode VARCHAR, "
                "lat DOUBLE, lon DOUBLE)"
            )
            self.con.execute(
                "CREATE TEMP TABLE city_centroid (city_key VARCHAR, "
                "lat DOUBLE, lon DOUBLE)"
            )
            self.con.execute(
                "CREATE TEMP TABLE overture_pc (postcode VARCHAR)"
            )

        regions = self.data_dir / "my_regions.parquet"
        if regions.exists():
            self.con.execute(
                f"""
                CREATE TEMP TABLE state_centroid AS
                SELECT state,
                       ST_Y(ST_Centroid(ST_Union_Agg(geom))) AS lat,
                       ST_X(ST_Centroid(ST_Union_Agg(geom))) AS lon
                FROM read_parquet({lit(str(regions))})
                GROUP BY state
                """
            )
        else:
            self.con.execute(
                "CREATE TEMP TABLE state_centroid (state VARCHAR, "
                "lat DOUBLE, lon DOUBLE)"
            )

    # ---------------------------------------------------------------- input
    def _prepare(self, row: dict[str, Any], input_id: int) -> tuple[dict, list, dict]:
        address = str(row.get("address") or "").strip()
        postcode = str(row.get("postcode") or "").strip()
        if not postcode:
            found = re.findall(r"\b\d{5}\b", address)
            if found:
                postcode = found[-1]
        postcode = re.sub(r"\s+", "", postcode)
        city = str(row.get("city") or "").strip()
        state = str(row.get("state") or "").strip()
        lat = to_float(row.get("lat"))
        lon = to_float(row.get("lon"))
        atype = address_type(address)
        candidates = parse_candidates(address)
        housenumber = parse_housenumber(address)
        state_norm = canonical_state(state) if state else None

        meta = {
            "input_id": input_id,
            "address": address,
            "postcode": postcode,
            "city": city,
            "city_key": normalize_text(city),
            "state": state,
            "state_norm": state_norm,
            "lat": lat,
            "lon": lon,
            "address_type": atype,
            "housenumber": housenumber,
        }
        return meta, candidates, {"input_id": input_id, "housenumber": housenumber}

    def _build_inputs(self, rows: list[dict[str, Any]]) -> None:
        metas: list[tuple] = []
        candidates: list[tuple[int, str]] = []
        for idx, row in enumerate(rows, start=1):
            meta, cands, _ = self._prepare(row, idx)
            metas.append((
                meta["input_id"], meta["address"], meta["postcode"], meta["city"],
                meta["city_key"], meta["state"], meta["state_norm"], meta["lat"],
                meta["lon"], meta["address_type"], meta["housenumber"],
            ))
            for candidate in cands:
                candidates.append((meta["input_id"], candidate))

        self.con.execute(
            """
            CREATE OR REPLACE TEMP TABLE input_meta (
                input_id INTEGER, address VARCHAR, postcode VARCHAR, city VARCHAR,
                city_key VARCHAR, state VARCHAR, state_norm VARCHAR,
                lat DOUBLE, lon DOUBLE, address_type VARCHAR, housenumber VARCHAR
            )
            """
        )
        if metas:
            self.con.executemany(
                "INSERT INTO input_meta VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                metas,
            )
        self.con.execute(
            "CREATE OR REPLACE TEMP TABLE input_candidates "
            "(input_id INTEGER, candidate VARCHAR)"
        )
        if candidates:
            self.con.executemany(
                "INSERT INTO input_candidates VALUES (?, ?)", candidates
            )

    # ---------------------------------------------------------------- matching
    def _match(self) -> None:
        self.con.execute(
            """
            CREATE OR REPLACE TEMP TABLE anchors AS
            SELECT m.input_id,
                   CASE
                     WHEN m.lat IS NOT NULL AND m.lon IS NOT NULL THEN 'coordinates'
                     WHEN p.lat IS NOT NULL THEN 'postcode_osm'
                     WHEN o.lat IS NOT NULL THEN 'postcode_overture'
                     WHEN c.lat IS NOT NULL THEN 'city_overture'
                     WHEN s.lat IS NOT NULL THEN 'state'
                     ELSE 'none'
                   END AS anchor_source,
                   coalesce(m.lat, p.lat, o.lat, c.lat, s.lat) AS anchor_lat,
                   coalesce(m.lon, p.lon, o.lon, c.lon, s.lon) AS anchor_lon,
                   CASE
                     WHEN m.lat IS NOT NULL AND m.lon IS NOT NULL THEN 2.0
                     WHEN p.lat IS NOT NULL OR o.lat IS NOT NULL THEN 20.0
                     WHEN c.lat IS NOT NULL THEN 25.0
                     WHEN s.lat IS NOT NULL THEN 50.0
                     ELSE 0.0
                   END AS radius_km
            FROM input_meta m
            LEFT JOIN pc_centroid_osm p USING (postcode)
            LEFT JOIN pc_centroid_ov o USING (postcode)
            LEFT JOIN city_centroid c ON m.city_key = c.city_key
            LEFT JOIN state_centroid s ON m.state_norm = s.state
            """
        )
        self.con.execute(
            """
            CREATE OR REPLACE TEMP TABLE candidate_anchor AS
            SELECT c.input_id, c.candidate,
                   median(a.lat) AS lat, median(a.lon) AS lon,
                   count(a.osm_id) AS hits,
                   any_value(a.street) AS osm_street
            FROM input_candidates c
            JOIN input_meta m USING (input_id)
            JOIN addr a ON a.postcode = m.postcode
            WHERE coalesce(m.postcode, '') <> ''
              AND coalesce(a.street_norm, '') <> ''
              AND (
                    a.street_norm = c.candidate
                    OR jaro_winkler_similarity(a.street_norm, c.candidate) >= 0.92
                  )
            GROUP BY c.input_id, c.candidate
            """
        )
        self.con.execute(
            """
            CREATE OR REPLACE TEMP TABLE road_matches AS
            SELECT c.input_id, c.candidate,
                   CASE WHEN ca.lat IS NOT NULL
                        THEN 'street_osm' ELSE a.anchor_source END AS anchor_source,
                   CASE WHEN ca.lat IS NOT NULL
                        THEN 2.0 ELSE a.radius_km END AS radius_km,
                   r.name AS road_name, r.street_norm AS road_norm, r.highway,
                   CASE
                     WHEN c.candidate = r.street_norm THEN 1.0
                     WHEN length(c.candidate) >= 8
                      AND strpos(r.street_norm, c.candidate) > 0 THEN 0.97
                     WHEN length(r.street_norm) >= 8
                      AND strpos(c.candidate, r.street_norm) > 0 THEN 0.97
                     ELSE jaro_winkler_similarity(c.candidate, r.street_norm)
                   END AS sim,
                   levenshtein(c.candidate, r.street_norm) AS edit_dist,
                   ST_Distance(
                       ST_Point(
                           coalesce(ca.lon, a.anchor_lon),
                           coalesce(ca.lat, a.anchor_lat)
                       ),
                       ST_GeomFromWKB(r.geom_wkb)
                   ) * 111320.0 AS distance_m,
                   ca.osm_street,
                   coalesce(ca.hits, 0) AS pc_hits
            FROM input_candidates c
            JOIN anchors a USING (input_id)
            LEFT JOIN candidate_anchor ca USING (input_id, candidate)
            JOIN roads r
              ON abs(r.lat - coalesce(ca.lat, a.anchor_lat))
                     <= (CASE WHEN ca.lat IS NOT NULL THEN 2.0 ELSE a.radius_km END) / 111.0
             AND abs(r.lon - coalesce(ca.lon, a.anchor_lon))
                     <= (CASE WHEN ca.lat IS NOT NULL THEN 2.0 ELSE a.radius_km END) / 111.0
            WHERE coalesce(ca.lat, a.anchor_lat) IS NOT NULL
              AND (
                    jaro_winkler_similarity(c.candidate, r.street_norm) >= 0.4
                    OR (length(c.candidate) >= 8
                        AND strpos(r.street_norm, c.candidate) > 0)
                    OR (length(r.street_norm) >= 8
                        AND strpos(c.candidate, r.street_norm) > 0)
                  )
            """
        )
        self.con.execute(
            """
            CREATE OR REPLACE TEMP TABLE best AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY input_id
                           ORDER BY
                               sim + CASE WHEN pc_hits > 0 THEN 0.05 ELSE 0 END
                                   - CASE
                                       WHEN anchor_source IN ('coordinates', 'street_osm')
                                       THEN least(distance_m / 5000.0, 0.2)
                                       ELSE least(distance_m / 100000.0, 0.05)
                                     END
                               DESC,
                               distance_m ASC
                       ) AS rn
                FROM road_matches
            ) WHERE rn = 1
            """
        )
        self.con.execute(
            """
            CREATE OR REPLACE TEMP TABLE number_hits AS
            SELECT b.input_id,
                   count(a.osm_id) FILTER (
                       WHERE a.street_norm = b.road_norm
                         AND lower(regexp_replace(
                             coalesce(a.housenumber, ''),
                             '[^a-zA-Z0-9]', '', 'g'
                         )) = m.housenumber
                   ) AS number_hits,
                   count(a.osm_id) FILTER (
                       WHERE a.street_norm = b.road_norm
                   ) AS street_hits
            FROM best b
            JOIN input_meta m USING (input_id)
            JOIN anchors an USING (input_id)
            LEFT JOIN addr a
              ON (
                   coalesce(m.postcode, '') <> '' AND a.postcode = m.postcode
                 )
                 OR (
                   coalesce(m.postcode, '') = ''
                   AND abs(a.lat - an.anchor_lat) <= 0.005
                   AND abs(a.lon - an.anchor_lon) <= 0.005
                 )
            GROUP BY b.input_id
            """
        )
        self.con.execute(
            """
            CREATE OR REPLACE TEMP TABLE joined AS
            SELECT m.input_id, m.address, m.postcode, m.city, m.state,
                   m.address_type, m.housenumber,
                   b.candidate, b.road_name, b.road_norm, b.highway,
                   b.osm_street,
                   b.sim, b.edit_dist, b.distance_m, b.anchor_source, b.pc_hits,
                   coalesce(n.number_hits, 0) AS number_hits,
                   coalesce(n.street_hits, 0) AS street_hits,
                   (mc.postcode IS NOT NULL) AS in_mcmc,
                   mc.state AS official_state,
                   mc.city  AS official_city,
                   (op.postcode IS NOT NULL) AS in_osm,
                   (vp.postcode IS NOT NULL) AS in_overture
            FROM input_meta m
            LEFT JOIN best b USING (input_id)
            LEFT JOIN number_hits n USING (input_id)
            LEFT JOIN mcmc mc USING (postcode)
            LEFT JOIN osm_pc op USING (postcode)
            LEFT JOIN overture_pc vp USING (postcode)
            """
        )

    # ---------------------------------------------------------------- verdicts
    def _verdict(self, row: dict[str, Any]) -> dict[str, Any]:
        postcode = row.get("postcode") or ""
        postcode_status = (
            "missing" if not postcode
            else "invalid_format" if not re.fullmatch(r"\d{5}", postcode)
            else "official" if row.get("in_mcmc")
            else "osm_only" if row.get("in_osm")
            else "overture_only" if row.get("in_overture")
            else "unknown"
        )
        official_state = canonical_state(row.get("official_state") or "")
        input_state = canonical_state(row.get("state") or "")
        if not official_state:
            state_status = "unknown"
        elif not input_state:
            state_status = "not_provided"
        elif input_state == official_state:
            state_status = "match"
        else:
            state_status = "mismatch"

        city = city_status(row.get("city"), row.get("official_city"))
        number_status = (
            "not_provided" if not row.get("housenumber")
            else "verified" if (row.get("number_hits") or 0) > 0
            else "unverified"
        )

        sim = row.get("sim") or 0.0
        distance = row.get("distance_m")
        distance = distance if distance is not None else 1e9
        edit = row.get("edit_dist")
        edit = edit if edit is not None else 99
        pc_hits = row.get("pc_hits") or 0
        anchor = row.get("anchor_source") or "none"
        candidate_norm = row.get("candidate") or ""
        road_norm = row.get("road_norm") or ""
        osm_norm = normalize_street(row.get("osm_street") or "")
        generic = is_generic_road(road_norm)
        strong_distance = {
            "coordinates": 1000.0,
            "street_osm": 2000.0,
            "postcode_osm": 20000.0,
            "postcode_overture": 20000.0,
            "city_overture": 25000.0,
            "state": 50000.0,
        }.get(anchor, 0.0)

        # An OSM address point on the exact street in the same postcode is
        # stronger evidence than a nearby road segment with a similar name.
        evidence = False
        if pc_hits > 0 and osm_norm and candidate_norm:
            ratio = SequenceMatcher(None, candidate_norm, osm_norm).ratio()
            evidence = (
                candidate_norm == osm_norm
                or (
                    numeric_compatible(candidate_norm, osm_norm)
                    and ratio >= 0.95
                    and edit <= 2
                )
            )
        if evidence:
            sim = 1.0
            distance = 0.0
            anchor = "street_osm"
            road_norm = osm_norm
            generic = False
            strong_distance = 2000.0

        level = street_match_level(candidate_norm, road_norm)
        evidence_typo = evidence and level == "typo"
        strong = (
            level in {"exact", "format", "suffix"}
            and sim >= 0.95
            and distance <= strong_distance
            and (not generic or pc_hits > 0 or distance <= 300)
        )
        correct_distance = (
            200.0 if anchor == "coordinates"
            else 500.0 if anchor == "street_osm"
            else 2000.0
        )
        corrected = (
            not evidence
            and level == "typo"
            and sim >= 0.85
            and distance <= correct_distance
            and not generic
        )
        probable = (
            level != "none"
            and (
                (sim >= 0.92 and distance <= strong_distance * 1.5)
                or (pc_hits > 0 and sim >= 0.90)
            )
        )

        if not row.get("address") and not postcode:
            verdict = "invalid_input"
        elif postcode_status == "invalid_format":
            verdict = "invalid_postcode"
        elif state_status == "mismatch":
            verdict = "invalid_state"
        elif not row.get("candidate"):
            verdict = "unverified_area" if row.get("address_type") == "area" else "unverified_no_street"
        elif evidence_typo:
            verdict = "corrected"
        elif strong:
            if postcode_status == "official" and state_status in {"match", "not_provided"}:
                verdict = (
                    "valid_street_number_unverified"
                    if number_status == "unverified"
                    else "valid"
                )
            else:
                verdict = "probable"
        elif corrected:
            verdict = "corrected"
        elif probable:
            verdict = "probable"
        else:
            verdict = "unverified"

        confidence = VERDICT_CONFIDENCE.get(verdict, 0.1)
        if pc_hits > 0 and verdict in {"valid", "probable"}:
            confidence = min(0.99, confidence + 0.03)
        if state_status == "match" and verdict == "probable":
            confidence = min(0.99, confidence + 0.05)
        if generic and verdict in {"valid", "valid_street_number_unverified", "probable"}:
            confidence = max(0.05, confidence - 0.10)

        return {
            "input_id": row.get("input_id"),
            "address": row.get("address") or "",
            "postcode": postcode,
            "city": row.get("city") or "",
            "state": row.get("state") or "",
            "verdict": verdict,
            "confidence": round(confidence, 2),
            "parsed_street": row.get("candidate") or "",
            "parsed_number": row.get("housenumber") or "",
            "address_type": row.get("address_type") or "",
            "matched_road": (
                (row.get("osm_street") or candidate_norm)
                if evidence else (row.get("road_name") or "")
            ),
            "road_highway": "" if evidence else (row.get("highway") or ""),
            "street_similarity": 1.0 if evidence else (round(sim, 3) if sim else 0.0),
            "edit_distance": None if evidence else (edit if edit != 99 else None),
            "road_distance_m": 0.0 if evidence else (
                round(distance, 1) if distance < 1e8 else None
            ),
            "anchor_source": anchor,
            "street_source": (
                "osm_address" if evidence
                else "osm_road" if row.get("road_name")
                else "none"
            ),
            "postcode_status": postcode_status,
            "official_city": row.get("official_city") or "",
            "official_state": row.get("official_state") or "",
            "state_status": state_status,
            "city_status": city,
            "number_status": number_status,
            "street_in_postcode_hits": pc_hits,
        }

    # ---------------------------------------------------------------- public
    def validate(
        self,
        address: str | None = None,
        postcode: str | None = None,
        city: str | None = None,
        state: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
    ) -> dict[str, Any]:
        """Validate one address. The validator object is reusable."""
        return self.validate_many([{
            "address": address, "postcode": postcode, "city": city,
            "state": state, "lat": lat, "lon": lon,
        }])[0]

    def validate_many(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        self._build_inputs(rows)
        self._match()
        cur = self.con.execute("SELECT * FROM joined ORDER BY input_id")
        columns = [d[0] for d in cur.description]
        return [self._verdict(dict(zip(columns, row))) for row in cur.fetchall()]


# ---------------------------------------------------------------- file I/O
def read_rows(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix in {".csv", ".tsv"}:
        for encoding in ("utf-8-sig", "cp1252", "latin-1"):
            try:
                with path.open("r", encoding=encoding, newline="") as handle:
                    return list(csv.DictReader(handle))
            except UnicodeDecodeError:
                continue
        raise SystemExit(f"Could not decode {path}")
    if suffix == ".jsonl":
        rows = []
        with path.open("r", encoding="utf-8-sig") as handle:
            for line in handle:
                if line.strip():
                    rows.append(json.loads(line))
        return rows
    if suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        return data if isinstance(data, list) else [data]
    if suffix in {".parquet", ".pq"}:
        con = duckdb.connect()
        cur = con.execute(f"SELECT * FROM read_parquet({lit(str(path))})")
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    raise SystemExit(f"Unsupported input format: {suffix}")


def normalize_row(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "address": pick(raw, ADDRESS_KEYS),
        "postcode": pick(raw, POSTCODE_KEYS),
        "city": pick(raw, CITY_KEYS),
        "state": pick(raw, STATE_KEYS),
        "lat": pick(raw, LAT_KEYS),
        "lon": pick(raw, LON_KEYS),
    }


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    suffix = path.suffix.lower()
    path.parent.mkdir(parents=True, exist_ok=True)
    if suffix == ".jsonl":
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        return
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in OUTPUT_FIELDS})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", help="One freeform address")
    parser.add_argument("--postcode")
    parser.add_argument("--city")
    parser.add_argument("--state")
    parser.add_argument("--lat", type=float)
    parser.add_argument("--lon", type=float)
    parser.add_argument("--input", help="CSV, JSONL, JSON or Parquet of addresses")
    parser.add_argument("--out", help="Output CSV or JSONL")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--memory-limit", default=None)
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N batch rows")
    args = parser.parse_args()

    if not args.address and not args.input:
        parser.error("pass --address (one address) or --input (batch)")

    validator = AddressValidator(data_dir=args.data_dir, memory_limit=args.memory_limit)

    if args.address:
        result = validator.validate(
            address=args.address, postcode=args.postcode, city=args.city,
            state=args.state, lat=args.lat, lon=args.lon,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    raw_rows = read_rows(Path(args.input))
    if args.limit:
        raw_rows = raw_rows[: args.limit]
    rows = [normalize_row(row) for row in raw_rows]
    results = validator.validate_many(rows)

    if args.out:
        write_rows(Path(args.out), results)
        print(f"Wrote {len(results)} rows to {args.out}")
    else:
        for result in results:
            print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
