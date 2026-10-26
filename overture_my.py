#!/usr/bin/env python3
"""
Extract Malaysian business places from Overture Maps with DuckDB.

No scraping, no rate limits, no Cloudflare: this reads Overture's public
Parquet releases directly from S3/Azure.

Typical use
-----------
    py -m pip install -r requirements.txt

    # One expensive pass over the global dataset -> small local Parquet
    py overture_my.py pull --out data/my_places_raw.parquet --min-confidence 0.6

    # Fast local slicing, no network
    py overture_my.py export --input data/my_places_raw.parquet \
        --out data/kl_restaurants --category restaurant \
        --region "Kuala Lumpur" --require-contact --dedupe

    py overture_my.py stats --input data/my_places_raw.parquet

Licence: Overture places data is CDLA-Permissive-2.0 / Apache-2.0 depending on
the source. Commercial use is allowed; keep the per-row `source_licenses`
column and the attribution note in the metadata file.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DEFAULT_RELEASE = "2026-10-21.0"
S3_TEMPLATE = (
    "s3://overturemaps-us-west-2/release/{release}/theme=places/type=place/*.parquet"
)
AZURE_TEMPLATE = (
    "https://overturemapswestus2.blob.core.windows.net/release/"
    "{release}/theme=places/type=place/*.parquet"
)
RELEASE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}\.[0-9]+$")

log = logging.getLogger("overture")


# --------------------------------------------------------------------------
# SQL helpers
# --------------------------------------------------------------------------
def lit(value: str) -> str:
    """Single-quoted SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


PROJECTION = """
    id AS gers_id,
    names.primary AS name,
    brand.names.primary AS brand,
    brand.wikidata AS brand_wikidata,
    basic_category,
    taxonomy.primary AS category,
    array_to_string(taxonomy.hierarchy, ' > ') AS category_path,
    addresses[1].freeform AS address,
    addresses[1].locality AS locality,
    addresses[1].region AS region,
    addresses[1].postcode AS postcode,
    addresses[1].country AS country,
    array_to_string(phones, ' | ') AS phones,
    phones[1] AS phone,
    array_to_string(emails, ' | ') AS emails,
    emails[1] AS email,
    array_to_string(websites, ' | ') AS websites,
    websites[1] AS website,
    array_to_string(socials, ' | ') AS socials,
    ST_Y(geometry) AS lat,
    ST_X(geometry) AS lon,
    confidence,
    operating_status,
    array_to_string(
        list_distinct(list_transform(sources, lambda s: s.dataset)), ' | '
    ) AS source_datasets,
    array_to_string(
        list_distinct(list_transform(sources, lambda s: s.license)), ' | '
    ) AS source_licenses,
    len(sources) AS source_count
"""

HAS_CONTACT = (
    "(coalesce(len(phones), 0) > 0 "
    "OR coalesce(len(websites), 0) > 0 "
    "OR coalesce(len(emails), 0) > 0)"
)

# --------------------------------------------------------------------------
# Derived state
# --------------------------------------------------------------------------
# Overture's `region` is populated for only ~5% of Malaysian places and mixes
# English names with ISO 3166-2:MY codes. Postcode (~79%) and locality (~99%)
# are far more complete, so derive a canonical state by majority vote of the
# three signals. The locality -> state lookup is learned from the data itself.
STATE_CODES = {
    "01": "Johor",
    "02": "Kedah",
    "03": "Kelantan",
    "04": "Melaka",
    "05": "Negeri Sembilan",
    "06": "Pahang",
    "07": "Pulau Pinang",
    "08": "Perak",
    "09": "Perlis",
    "10": "Selangor",
    "11": "Terengganu",
    "12": "Sabah",
    "13": "Sarawak",
    "14": "Kuala Lumpur",
    "15": "Labuan",
    "16": "Putrajaya",
}

# Exact aliases that are unsafe as substrings ("kl" would match Kluang).
STATE_ALIASES = {
    "kl": "Kuala Lumpur",
    "wp kl": "Kuala Lumpur",
}

# Ordered: specific names first, then substring matches. "wilayah persekutuan"
# on its own is deliberately absent because it is ambiguous between KL, Labuan
# and Putrajaya.
STATE_PATTERNS = [
    ("kuala lumpur", "Kuala Lumpur"),
    ("labuan", "Labuan"),
    ("putrajaya", "Putrajaya"),
    ("johor", "Johor"),
    ("kedah", "Kedah"),
    ("kelantan", "Kelantan"),
    ("melaka", "Melaka"),
    ("malacca", "Melaka"),
    ("sembilan", "Negeri Sembilan"),
    ("pahang", "Pahang"),
    ("perak", "Perak"),
    ("perlis", "Perlis"),
    ("pinang", "Pulau Pinang"),
    ("penang", "Pulau Pinang"),
    ("sabah", "Sabah"),
    ("sarawak", "Sarawak"),
    ("selangor", "Selangor"),
    ("terengganu", "Terengganu"),
    ("trengganu", "Terengganu"),
]


def canonical_state(value: str) -> str | None:
    """Map a user-supplied state name or ISO code to the canonical name."""
    key = re.sub(r"\s+", " ", str(value).strip().lower())
    if key in STATE_CODES:
        return STATE_CODES[key]
    if key in STATE_ALIASES:
        return STATE_ALIASES[key]
    for pattern, state in STATE_PATTERNS:
        if pattern in key:
            return state
    return None


def state_from_region_sql(col: str = "region") -> str:
    r = f"lower(trim(coalesce({col}, '')))"
    whens = [
        f"WHEN {r} = {lit(code)} THEN {lit(state)}"
        for code, state in STATE_CODES.items()
    ]
    whens += [
        f"WHEN {r} = {lit(alias)} THEN {lit(state)}"
        for alias, state in STATE_ALIASES.items()
    ]
    whens += [
        f"WHEN {r} LIKE {lit('%' + pattern + '%')} THEN {lit(state)}"
        for pattern, state in STATE_PATTERNS
    ]
    body = "\n        ".join(whens)
    return f"CASE\n        {body}\n    END"


def state_from_postcode_sql(col: str = "postcode") -> str:
    p = f"try_cast(left(nullif(trim({col}), ''), 2) AS INTEGER)"
    return f"""CASE
        WHEN {p} BETWEEN 1 AND 2 THEN 'Perlis'
        WHEN {p} BETWEEN 5 AND 9 THEN 'Kedah'
        WHEN {p} BETWEEN 10 AND 14 THEN 'Pulau Pinang'
        WHEN {p} BETWEEN 15 AND 18 THEN 'Kelantan'
        WHEN {p} BETWEEN 20 AND 24 THEN 'Terengganu'
        WHEN {p} BETWEEN 25 AND 28 OR {p} IN (39, 49, 69) THEN 'Pahang'
        WHEN {p} BETWEEN 30 AND 36 THEN 'Perak'
        WHEN {p} BETWEEN 40 AND 48 OR {p} IN (63, 64, 68) THEN 'Selangor'
        WHEN {p} BETWEEN 50 AND 60 THEN 'Kuala Lumpur'
        WHEN {p} = 62 THEN 'Putrajaya'
        WHEN {p} BETWEEN 70 AND 73 THEN 'Negeri Sembilan'
        WHEN {p} BETWEEN 75 AND 78 THEN 'Melaka'
        WHEN {p} BETWEEN 79 AND 86 THEN 'Johor'
        WHEN {p} = 87 THEN 'Labuan'
        WHEN {p} BETWEEN 88 AND 91 THEN 'Sabah'
        WHEN {p} BETWEEN 93 AND 98 THEN 'Sarawak'
    END"""


def state_ctes(source: str, has_state: bool = False) -> str:
    """CTE chain yielding `enriched`, with a derived `state` column.

    Signals are the normalised region, the postcode range, and a locality
    majority lookup. Two agreeing signals win; otherwise postcode, then
    locality, then region is preferred. If the input already has a `state`
    column (a previous export), it is preserved rather than recomputed.
    """
    if has_state:
        return f"""projected AS (
        {source}
    ),
    enriched AS (
        SELECT * FROM projected
    )"""
    return f"""projected AS (
        {source}
    ),
    signals AS (
        SELECT *,
               {state_from_region_sql()} AS r_state,
               {state_from_postcode_sql()} AS p_state
        FROM projected
    ),
    locality_state AS (
        SELECT locality, state FROM (
            SELECT locality,
                   coalesce(r_state, p_state) AS state,
                   count(*) FILTER (WHERE r_state = p_state) AS agree_n,
                   count(*) AS n,
                   row_number() OVER (
                       PARTITION BY locality
                       ORDER BY agree_n DESC, n DESC, coalesce(r_state, p_state)
                   ) AS rn
            FROM signals
            WHERE coalesce(locality, '') <> ''
              AND coalesce(r_state, p_state) IS NOT NULL
            GROUP BY 1, 2
        ) WHERE rn = 1
    ),
    voted AS (
        SELECT s.*, l.state AS l_state
        FROM signals s
        LEFT JOIN locality_state l USING (locality)
    ),
    enriched AS (
        SELECT * EXCLUDE (r_state, p_state, l_state),
               CASE
                 WHEN r_state IS NOT NULL AND r_state = p_state THEN r_state
                 WHEN p_state IS NOT NULL AND p_state = l_state THEN p_state
                 WHEN r_state IS NOT NULL AND r_state = l_state THEN r_state
                 WHEN p_state IS NOT NULL THEN p_state
                 WHEN l_state IS NOT NULL THEN l_state
                 ELSE r_state
               END AS state
        FROM voted
    )"""


def parse_bbox(raw: str) -> tuple[float, float, float, float]:
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            "bbox must be 'min_lon,min_lat,max_lon,max_lat'"
        )
    try:
        min_lon, min_lat, max_lon, max_lat = (float(p) for p in parts)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"bbox values must be numbers: {exc}") from exc
    if min_lon >= max_lon or min_lat >= max_lat:
        raise argparse.ArgumentTypeError("bbox min values must be less than max values")
    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        raise argparse.ArgumentTypeError("longitude must be between -180 and 180")
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise argparse.ArgumentTypeError("latitude must be between -90 and 90")
    return min_lon, min_lat, max_lon, max_lat


def bbox_clause(bbox: tuple[float, float, float, float]) -> str:
    min_lon, min_lat, max_lon, max_lat = bbox
    # bbox stats on the row group can prune I/O; ST_Within does the exact test.
    return (
        f"bbox.xmin <= {max_lon} AND bbox.xmax >= {min_lon} "
        f"AND bbox.ymin <= {max_lat} AND bbox.ymax >= {min_lat} "
        f"AND ST_Within(geometry, ST_MakeEnvelope({min_lon}, {min_lat}, "
        f"{max_lon}, {max_lat}))"
    )


# --------------------------------------------------------------------------
# DuckDB setup
# --------------------------------------------------------------------------
def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL spatial; LOAD spatial;")
    except duckdb.Error as exc:
        raise SystemExit(
            f"Could not load DuckDB extensions (httpfs/spatial). "
            f"Check network/proxy access to extensions.duckdb.org.\n{exc}"
        ) from exc

    con.execute("SET s3_region='us-west-2';")
    con.execute("SET preserve_insertion_order=false;")
    con.execute(
        f"SET enable_progress_bar={'true' if args.verbose else 'false'};"
    )
    if args.threads:
        con.execute(f"SET threads={int(args.threads)};")
    if args.memory_limit:
        con.execute(f"SET memory_limit={lit(args.memory_limit)};")
    if args.temp_dir:
        Path(args.temp_dir).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={lit(args.temp_dir)};")
    return con


def release_url(release: str, use_azure: bool) -> str:
    template = AZURE_TEMPLATE if use_azure else S3_TEMPLATE
    return template.format(release=release)


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------
def cmd_pull(args: argparse.Namespace) -> int:
    release = args.release
    if not RELEASE_RE.match(release):
        log.error("Invalid --release %r (expected e.g. 2026-10-21.0)", release)
        return 1

    url = release_url(release, args.azure)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    conditions: list[str] = []
    country = "" if args.all_countries else args.country
    if country:
        code = country.upper()
        if not re.fullmatch(r"[A-Z]{2}", code):
            log.error("--country must be a 2-letter ISO code, e.g. MY")
            return 1
        conditions.append(
            "list_contains(list_transform(addresses, lambda a: a.country), "
            f"{lit(code)})"
        )
    if args.bbox:
        conditions.append(bbox_clause(args.bbox))
    if args.min_confidence is not None:
        conditions.append(f"confidence >= {float(args.min_confidence)}")
    if not args.include_closed:
        conditions.append("coalesce(operating_status, '') <> 'permanently_closed'")
    if args.require_contact:
        conditions.append(HAS_CONTACT)

    where = " AND ".join(conditions) if conditions else "TRUE"
    limit = f" LIMIT {int(args.limit)}" if args.limit else ""
    sql = (
        f"COPY (SELECT {PROJECTION} FROM read_parquet({lit(url)}) "
        f"WHERE {where}{limit}) TO {lit(str(out))} "
        f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);"
    )

    if args.dry_run:
        print(sql)
        return 0

    log.warning(
        "Scanning the global Overture places dataset. There is no country "
        "partition, so this can read tens of GB and take a long time. "
        "Run it in us-west-2 (S3) or use a bbox extract for a city."
    )
    log.info("release=%s country=%s bbox=%s", release, country or "*", args.bbox)
    started = time.time()
    con = connect(args)
    try:
        con.execute(sql)
        rows = con.execute(
            f"SELECT count(*) FROM read_parquet({lit(str(out))})"
        ).fetchone()[0]
    except duckdb.Error as exc:
        log.error("Pull failed: %s", exc)
        log.error(
            "Hints: check the release date, S3 region, disk space and "
            "--memory-limit/--temp-dir. Do not retry in a loop while it is "
            "still writing."
        )
        return 1

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tool": "overture_my.py",
        "mode": "pull",
        "release": release,
        "source": url,
        "output": str(out),
        "rows": rows,
        "filters": {
            "country": country or None,
            "bbox": args.bbox,
            "min_confidence": args.min_confidence,
            "include_closed": args.include_closed,
            "require_contact": args.require_contact,
            "limit": args.limit,
        },
        "licence": (
            "Overture places: CDLA-Permissive-2.0 / Apache-2.0 depending on "
            "source. See source_licenses per row."
        ),
        "elapsed_seconds": round(time.time() - started, 1),
    }
    out.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    log.info("Wrote %s rows to %s (%.1fs)", rows, out, time.time() - started)
    return 0


def input_columns(con: duckdb.DuckDBPyConnection, source: str) -> set[str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({lit(source)})"
    ).fetchall()
    return {row[0] for row in rows}


def source_select(con: duckdb.DuckDBPyConnection, source: str) -> str:
    """Return a SELECT that works for raw Overture files and pulled files."""
    if "gers_id" in input_columns(con, source):
        return f"SELECT * FROM read_parquet({lit(source)})"
    return f"SELECT {PROJECTION} FROM read_parquet({lit(source)})"


def build_export_query(args: argparse.Namespace, con: duckdb.DuckDBPyConnection) -> str:
    filters: list[str] = []
    if args.category:
        needle = lit(f"%{args.category}%")
        filters.append(
            f"(category ILIKE {needle} OR category_path ILIKE {needle} "
            f"OR basic_category ILIKE {needle})"
        )
    if args.region:
        filters.append(f"region ILIKE {lit(f'%{args.region}%')}")
    if args.state:
        canonical = canonical_state(args.state)
        if canonical:
            filters.append(f"state = {lit(canonical)}")
        else:
            filters.append(f"state ILIKE {lit(f'%{args.state}%')}")
    if args.locality:
        filters.append(f"locality ILIKE {lit(f'%{args.locality}%')}")
    if args.min_confidence is not None:
        filters.append(f"confidence >= {float(args.min_confidence)}")
    if args.require_contact:
        filters.append(
            "(coalesce(phones, '') <> '' OR coalesce(emails, '') <> '' "
            "OR coalesce(websites, '') <> '')"
        )
    if args.open_only:
        filters.append("operating_status = 'open'")
    elif not args.include_closed:
        filters.append("coalesce(operating_status, '') <> 'permanently_closed'")

    where = " AND ".join(filters) if filters else "TRUE"
    cols = input_columns(con, args.input)
    query = (
        f"WITH {state_ctes(source_select(con, args.input), has_state='state' in cols)} "
        f"SELECT * FROM enriched WHERE {where}"
    )
    if args.dedupe:
        query += """
        QUALIFY row_number() OVER (
            PARTITION BY
                lower(trim(coalesce(name, ''))),
                round(lat, 5),
                round(lon, 5)
            ORDER BY confidence DESC NULLS LAST
        ) = 1
        """
    if args.limit:
        query += f" LIMIT {int(args.limit)}"
    return query


def cmd_export(args: argparse.Namespace) -> int:
    inp = Path(args.input)
    if not inp.exists() and "://" not in args.input and not re.search(r"[*?\[]", args.input):
        log.error("Input not found: %s", inp)
        return 1

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    con = connect(args)
    query = build_export_query(args, con)
    formats = [f.strip().lower() for f in args.formats.split(",") if f.strip()]
    unknown = set(formats) - {"parquet", "csv", "jsonl"}
    if unknown:
        log.error("Unknown format(s): %s", ", ".join(sorted(unknown)))
        return 1

    if args.dry_run:
        print(query)
        for fmt in formats:
            print(f"-- would write {out}.{fmt}")
        return 0

    try:
        con.execute(f"CREATE OR REPLACE TEMP TABLE filtered AS {query}")
        rows = con.execute("SELECT count(*) FROM filtered").fetchone()[0]
        for fmt in formats:
            target = f"{out}.{fmt}"
            if fmt == "parquet":
                con.execute(
                    f"COPY filtered TO {lit(target)} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD);"
                )
            elif fmt == "csv":
                con.execute(
                    f"COPY filtered TO {lit(target)} "
                    "(FORMAT CSV, HEADER, DELIMITER ',');"
                )
            else:
                con.execute(f"COPY filtered TO {lit(target)} (FORMAT JSON);")
            log.info("Wrote %s", target)
    except duckdb.Error as exc:
        log.error("Export failed: %s", exc)
        return 1

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tool": "overture_my.py",
        "mode": "export",
        "input": str(inp),
        "output_base": str(out),
        "rows": rows,
        "filters": {
            "category": args.category,
            "region": args.region,
            "state": args.state,
            "locality": args.locality,
            "min_confidence": args.min_confidence,
            "require_contact": args.require_contact,
            "include_closed": args.include_closed,
            "open_only": args.open_only,
            "dedupe": args.dedupe,
            "limit": args.limit,
        },
        "licence": (
            "Overture places: CDLA-Permissive-2.0 / Apache-2.0 depending on "
            "source. See source_licenses per row."
        ),
    }
    out.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    log.info("Exported %s rows", rows)
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    inp = Path(args.input)
    if not inp.exists() and "://" not in args.input and not re.search(r"[*?\[]", args.input):
        log.error("Input not found: %s", inp)
        return 1
    con = connect(args)
    cols = input_columns(con, args.input)
    ctes = state_ctes(source_select(con, args.input), has_state="state" in cols)

    (
        total,
        with_phone,
        with_site,
        with_email,
        with_address,
        with_postcode,
        with_locality,
        with_region,
        complete_address,
        with_state,
    ) = con.execute(
        f"""
        WITH {ctes}
        SELECT count(*),
               count(*) FILTER (WHERE coalesce(phone, '') <> ''),
               count(*) FILTER (WHERE coalesce(website, '') <> ''),
               count(*) FILTER (WHERE coalesce(email, '') <> ''),
               count(*) FILTER (WHERE coalesce(address, '') <> ''),
               count(*) FILTER (WHERE coalesce(postcode, '') <> ''),
               count(*) FILTER (WHERE coalesce(locality, '') <> ''),
               count(*) FILTER (WHERE coalesce(region, '') <> ''),
               count(*) FILTER (WHERE coalesce(address, '') <> ''
                                  AND coalesce(postcode, '') <> ''
                                  AND coalesce(locality, '') <> ''),
               count(*) FILTER (WHERE state IS NOT NULL)
        FROM enriched
        """
    ).fetchone()

    print(f"Rows: {total:,}")
    if total:
        print(f"With phone:    {with_phone:,} ({with_phone / total:.0%})")
        print(f"With website:  {with_site:,} ({with_site / total:.0%})")
        print(f"With email:    {with_email:,} ({with_email / total:.0%})")
        print(f"With address:  {with_address:,} ({with_address / total:.0%})")
        print(f"With postcode: {with_postcode:,} ({with_postcode / total:.0%})")
        print(f"With locality: {with_locality:,} ({with_locality / total:.0%})")
        print(f"With region:   {with_region:,} ({with_region / total:.0%}) (raw field)")
        print(f"Address + postcode + locality: {complete_address:,} "
              f"({complete_address / total:.0%})")
        print(f"With state:    {with_state:,} ({with_state / total:.0%})")

    def table(title: str, sql: str) -> None:
        rows = con.execute(sql).fetchall()
        print(f"\n{title}")
        for row in rows:
            print("  " + "  ".join(f"{v:>10,}" if isinstance(v, int) else str(v) for v in row))

    table(
        f"Top {args.top} categories",
        f"""
        WITH {ctes}
        SELECT category, count(*) AS n
        FROM enriched
        WHERE category IS NOT NULL
        GROUP BY 1 ORDER BY n DESC LIMIT {int(args.top)}
        """,
    )
    table(
        f"Top {args.top} states (derived)",
        f"""
        WITH {ctes}
        SELECT coalesce(state, '(unknown)') AS state, count(*) AS n,
               round(100.0 * count(*) FILTER (
                   WHERE coalesce(address, '') <> ''
               ) / count(*), 1) AS address_pct
        FROM enriched
        GROUP BY 1 ORDER BY n DESC LIMIT {int(args.top)}
        """,
    )
    return 0


# --------------------------------------------------------------------------
# Release diff
# --------------------------------------------------------------------------
def ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def table_columns(con: duckdb.DuckDBPyConnection, path: str) -> list[str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({lit(path)})"
    ).fetchall()
    return [row[0] for row in rows]


def cmd_diff(args: argparse.Namespace) -> int:
    formats = [f.strip().lower() for f in args.formats.split(",") if f.strip()]
    if not formats or set(formats) - {"parquet", "csv"}:
        log.error("--formats must contain parquet and/or csv")
        return 1

    con = connect(args)
    old_cols = table_columns(con, args.old)
    new_cols = table_columns(con, args.new)
    missing = [c for c in (args.key,) if c not in old_cols or c not in new_cols]
    if missing:
        log.error(
            "--key %r not found in both inputs. old=%s new=%s",
            args.key, old_cols, new_cols,
        )
        return 1

    compare = [
        c for c in (
            "name", "address", "phone", "website", "email", "category",
            "operating_status", "postcode", "region",
        )
        if c in old_cols and c in new_cols
    ]
    key = ident(args.key)
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE old_t AS "
        f"SELECT * FROM read_parquet({lit(args.old)})"
    )
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE new_t AS "
        f"SELECT * FROM read_parquet({lit(args.new)})"
    )

    added = (
        f"SELECT n.* FROM new_t n WHERE NOT EXISTS ("
        f"SELECT 1 FROM old_t o WHERE o.{key} = n.{key})"
    )
    removed = (
        f"SELECT o.* FROM old_t o WHERE NOT EXISTS ("
        f"SELECT 1 FROM new_t n WHERE n.{key} = o.{key})"
    )
    if compare:
        changed_pred = " OR ".join(
            f"coalesce(cast(n.{ident(c)} AS VARCHAR), '') <> "
            f"coalesce(cast(o.{ident(c)} AS VARCHAR), '')"
            for c in compare
        )
        old_fields = ", ".join(
            f"o.{ident(c)} AS old_{c}" for c in compare
        )
        changed = (
            f"SELECT n.*, {old_fields} FROM new_t n "
            f"JOIN old_t o ON o.{key} = n.{key} WHERE {changed_pred}"
        )
    else:
        changed = "SELECT * FROM new_t WHERE FALSE"

    if args.dry_run:
        for label, query in (("added", added), ("removed", removed), ("changed", changed)):
            print(f"-- {label}")
            print(query + ";")
        return 0

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for label, query in (("added", added), ("removed", removed), ("changed", changed)):
        target = f"{out}_{label}.parquet"
        con.execute(
            f"COPY ({query}) TO {lit(target)} "
            "(FORMAT PARQUET, COMPRESSION ZSTD);"
        )
        counts[label] = con.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
        if "csv" in formats:
            con.execute(
                f"COPY ({query}) TO {lit(f'{out}_{label}.csv')} "
                "(FORMAT CSV, HEADER, DELIMITER ',');"
            )
        log.info("%s: %s rows", label, f"{counts[label]:,}")

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tool": "overture_my.py",
        "mode": "diff",
        "old": args.old,
        "new": args.new,
        "key": args.key,
        "compared_fields": compare,
        "counts": counts,
    }
    Path(f"{out}.meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
GLOBAL_VALUE_FLAGS = {
    "--release",
    "--memory-limit",
    "--threads",
    "--temp-dir",
}
GLOBAL_BOOL_FLAGS = {"--azure", "--dry-run", "-v", "--verbose"}
COMMANDS = {"pull", "export", "stats", "diff"}


def reorder_global_args(argv: list[str]) -> list[str]:
    try:
        cmd_index = next(i for i, arg in enumerate(argv) if arg in COMMANDS)
    except StopIteration:
        return argv
    before, after = argv[:cmd_index], argv[cmd_index + 1 :]
    moved: list[str] = []
    kept: list[str] = []
    i = 0
    while i < len(after):
        arg = after[i]
        name = arg.split("=", 1)[0]
        if name in GLOBAL_VALUE_FLAGS:
            moved.append(arg)
            if "=" not in arg and i + 1 < len(after):
                moved.append(after[i + 1])
                i += 2
                continue
        elif arg in GLOBAL_BOOL_FLAGS:
            moved.append(arg)
        else:
            kept.append(arg)
        i += 1
    return before + moved + [argv[cmd_index]] + kept


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract Malaysian places from Overture Maps (DuckDB, no scraping)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--release", default=DEFAULT_RELEASE, help="Overture release date")
    parser.add_argument("--azure", action="store_true", help="Use Azure Blob instead of S3")
    parser.add_argument("--memory-limit", default=None, help="DuckDB memory limit, e.g. 8GB")
    parser.add_argument("--threads", type=int, default=None, help="DuckDB threads")
    parser.add_argument("--temp-dir", default=None, help="DuckDB spill directory")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only")
    parser.add_argument("-v", "--verbose", action="store_true")

    sub = parser.add_subparsers(dest="command", required=True)

    p_pull = sub.add_parser("pull", help="Remote Overture -> local Parquet subset")
    p_pull.add_argument("--out", required=True, help="Output Parquet path")
    p_pull.add_argument("--country", default="MY", help='ISO code (default: MY)')
    p_pull.add_argument("--all-countries", action="store_true",
                        help="Disable the country filter (useful for a bbox test)")
    p_pull.add_argument("--bbox", type=parse_bbox, default=None,
                        help="min_lon,min_lat,max_lon,max_lat")
    p_pull.add_argument("--min-confidence", type=float, default=0.5)
    p_pull.add_argument("--include-closed", action="store_true")
    p_pull.add_argument("--require-contact", action="store_true")
    p_pull.add_argument("--limit", type=int, default=None, help="For a quick test run")
    p_pull.set_defaults(func=cmd_pull)

    p_export = sub.add_parser("export", help="Filter a local Parquet subset")
    p_export.add_argument("--input", required=True, help="Parquet file or glob")
    p_export.add_argument("--out", required=True, help="Output base path (no extension)")
    p_export.add_argument("--formats", default="parquet,csv", help="parquet,csv,jsonl")
    p_export.add_argument("--category", default=None, help="e.g. restaurant")
    p_export.add_argument("--region", default=None, help="Raw Overture region (sparse)")
    p_export.add_argument("--state", default=None,
                          help='Derived state, e.g. "Selangor" or "penang"')
    p_export.add_argument("--locality", default=None, help="e.g. Petaling Jaya")
    p_export.add_argument("--min-confidence", type=float, default=None)
    p_export.add_argument("--require-contact", action="store_true")
    p_export.add_argument("--include-closed", action="store_true")
    p_export.add_argument("--open-only", action="store_true")
    p_export.add_argument("--dedupe", action="store_true")
    p_export.add_argument("--limit", type=int, default=None)
    p_export.set_defaults(func=cmd_export)

    p_stats = sub.add_parser("stats", help="Coverage and category summary")
    p_stats.add_argument("--input", required=True, help="Parquet file or glob")
    p_stats.add_argument("--top", type=int, default=20)
    p_stats.set_defaults(func=cmd_stats)

    p_diff = sub.add_parser("diff", help="Compare two pulls (added/removed/changed)")
    p_diff.add_argument("--old", required=True, help="Older pulled Parquet")
    p_diff.add_argument("--new", required=True, help="Newer pulled Parquet")
    p_diff.add_argument("--out", required=True, help="Output base path")
    p_diff.add_argument("--key", default="gers_id")
    p_diff.add_argument("--formats", default="parquet,csv")
    p_diff.set_defaults(func=cmd_diff)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    raw = list(sys.argv[1:] if argv is None else argv)
    args = parser.parse_args(reorder_global_args(raw))
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        return int(args.func(args) or 0)
    except KeyboardInterrupt:
        log.error("Interrupted.")
        return 130
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 - top-level guard
        log.error("%s: %s", type(exc).__name__, exc)
        if args.verbose:
            raise
        return 1


if __name__ == "__main__":
    sys.exit(main())
