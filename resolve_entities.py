#!/usr/bin/env python3
"""
Entity resolution for Malaysian business data.

Links physical outlets (Overture POIs) to legal entities (SSM-style records)
using normalised names, phones, domains, addresses, postcodes, states and
coordinates. Produces an auditable `links` table with a score and evidence.

Pipeline
--------
    py resolve_entities.py init --db data/yp.duckdb
    py resolve_entities.py load-outlets  --db data/yp.duckdb --input data/my_places_raw.parquet
    py resolve_entities.py load-entities --db data/yp.duckdb --input ssm_export.csv
    py resolve_entities.py resolve --db data/yp.duckdb
    py resolve_entities.py report  --db data/yp.duckdb
    py resolve_entities.py export  --db data/yp.duckdb --out data/resolved

Entity input columns are auto-detected (English/Malay aliases); override with
`--column-map '{"name":"Company Name","reg_no":"Registration No"}'`.

The resolver never invents a match. Anything below the auto threshold goes to a
review queue with the evidence that produced the score.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlparse

import duckdb

log = logging.getLogger("resolve")

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------
ENTITY_COLS = [
    "entity_id", "name", "name_norm", "name_core", "reg_no", "status",
    "is_active", "entity_type", "msic", "address", "address_norm", "postcode",
    "city", "state", "phone", "phone_norm", "phone_suffix", "phones_all",
    "email", "website", "domain", "lat", "lon", "source", "source_id", "raw",
]
OUTLET_COLS = [
    "outlet_id", "gers_id", "name", "name_norm", "name_core", "brand",
    "brand_wikidata", "category", "category_path", "address", "address_norm",
    "locality", "region", "state", "postcode", "country", "phone",
    "phone_norm", "phone_suffix", "phones_all", "email", "website", "domain",
    "socials", "social_keys", "lat", "lon", "confidence", "operating_status",
    "source_datasets", "source_licenses", "raw",
]

ENTITY_ALIASES = {
    "name": ["name", "companyname", "entityname", "businessname", "nama",
             "namasyarikat", "company", "business", "tradename"],
    "reg_no": ["regno", "registrationno", "registrationnumber", "ssmno",
               "companyno", "companynumber", "nopendaftaran", "no pendaftaran",
               "entityid", "refid", "registration"],
    "status": ["status", "companystatus", "entitystatus", "statussyarikat",
               "statusperniagaan"],
    "entity_type": ["type", "entitytype", "companytype", "jenis", "jenissyarikat"],
    "msic": ["msic", "msiccode", "msicno", "industry", "industrycode",
             "businesscode", "kodmsic"],
    "address": ["address", "registeredaddress", "businessaddress", "alamat",
                "alamatberdaftar", "address1", "addressline1"],
    "postcode": ["postcode", "post_code", "poskod", "zip", "zipcode"],
    "city": ["city", "town", "bandar", "locality"],
    "state": ["state", "negeri", "region", "province"],
    "phone": ["phone", "telephone", "tel", "phoneno", "telephoneno",
              "notelefon", "contactno", "contactnumber"],
    "email": ["email", "emailaddress", "emel"],
    "website": ["website", "web", "url", "companywebsite", "syarikatweb"],
    "lat": ["lat", "latitude"],
    "lon": ["lon", "lng", "long", "longitude"],
}

OUTLET_ALIASES = {
    "outlet_id": ["outletid", "gersid", "id", "placeid"],
    "gers_id": ["gersid", "id"],
    "name": ["name", "title", "businessname", "placename"],
    "brand": ["brand", "brandname"],
    "brand_wikidata": ["brandwikidata", "wikidata", "brandqid"],
    "category": ["category", "categories", "basiccategory", "primarycategory"],
    "category_path": ["categorypath", "taxonomy"],
    "address": ["address", "freeform", "street", "address1"],
    "locality": ["locality", "city", "town", "bandar"],
    "region": ["region", "state", "negeri"],
    "postcode": ["postcode", "post_code", "poskod", "zip"],
    "country": ["country", "negara"],
    "phone": ["phone", "phones", "telephone", "tel", "phoneno"],
    "email": ["email", "emails", "emailaddress"],
    "website": ["website", "websites", "url", "web"],
    "socials": ["socials", "social", "socialmedia"],
    "lat": ["lat", "latitude"],
    "lon": ["lon", "lng", "long", "longitude"],
    "confidence": ["confidence", "score"],
    "operating_status": ["operatingstatus", "status"],
    "source_datasets": ["sourcedatasets", "sources"],
    "source_licenses": ["sourcelicenses", "license", "licence"],
}

LABEL_ALIASES = {
    "outlet_id": ["outletid", "outlet", "gersid", "placeid"],
    "entity_id": ["entityid", "entity", "regno", "registrationno", "ssmno"],
    "label": ["label", "match", "ismatch", "y", "target", "correct"],
}

LEGAL_TOKENS = {
    "sdn", "bhd", "berhad", "sendirian", "enterprise", "trading", "services",
    "service", "supplies", "supply", "company", "co", "holdings", "holding",
    "resources", "industries", "industry", "marketing", "management",
    "consultancy", "consulting", "solutions", "technologies", "technology",
    "associates", "partners", "partnership", "llp", "plc", "corp",
    "corporation", "inc", "international", "intl",
}
ADDRESS_ABBREV = {
    "jln": "jalan", "tmn": "taman", "bkt": "bukit", "kg": "kampung",
    "lr": "lorong", "lor": "lorong", "psn": "persiaran",
    "persiaran": "persiaran", "jkr": "", "no": "", "pt": "", "lot": "lot",
}
STATE_ALIASES = {
    "kl": "kuala lumpur", "kuala lumpur": "kuala lumpur",
    "wilayah persekutuan kuala lumpur": "kuala lumpur",
    "wp kuala lumpur": "kuala lumpur", "w.p. kuala lumpur": "kuala lumpur",
    "penang": "pulau pinang", "pulau pinang": "pulau pinang",
    "pinang": "pulau pinang", "melaka": "melaka", "malacca": "melaka",
    "negeri sembilan": "negeri sembilan", "n sembilan": "negeri sembilan",
    "ns": "negeri sembilan", "trengganu": "terengganu",
    "terengganu": "terengganu", "johor": "johor", "kedah": "kedah",
    "kelantan": "kelantan", "pahang": "pahang", "perak": "perak",
    "perlis": "perlis", "sabah": "sabah", "sarawak": "sarawak",
    "selangor": "selangor", "putrajaya": "putrajaya", "labuan": "labuan",
}
INACTIVE_WORDS = {
    "struck off", "strike off", "dissolved", "wound up", "in liquidation",
    "liquidated", "dormant", "not active", "tidak aktif", "cancelled",
    "removed", "expired", "ceased", "gulung tikar",
}
ACTIVE_WORDS = {
    "active", "live", "existing", "berdaftar", "didaftarkan", "operating",
    "in operation", "aktif", "registered",
}
GENERIC_HOSTS = {
    "facebook.com", "instagram.com", "wa.me", "linktr.ee", "google.com",
    "business.site", "wixsite.com", "blogspot.com", "wordpress.com",
    "shopee.com.my", "lazada.com.my", "mudah.my", "carousell.com.my",
    "grab.com", "foodpanda.com.my", "tiktok.com", "youtube.com",
    "twitter.com", "x.com", "linkedin.com",
}
MULTI_SUFFIXES = {
    "com.my", "net.my", "org.my", "gov.my", "edu.my", "co.uk", "com.au",
    "co.id", "com.sg", "co.th", "com.br", "com.hk",
}


# --------------------------------------------------------------------------
# Normalisation
# --------------------------------------------------------------------------
def fold(text: Any) -> str:
    if text is None:
        return ""
    return unicodedata.normalize("NFKC", str(text)).casefold()


def norm_text(text: Any) -> str:
    value = fold(text).replace("&", " and ")
    value = re.sub(r"[^\w\s]+", " ", value, flags=re.UNICODE)
    return re.sub(r"\s+", " ", value).strip()


def name_core(name: str) -> str:
    tokens = [t for t in norm_text(name).split() if t]
    while tokens and tokens[0] == "the":
        tokens.pop(0)
    while tokens and tokens[-1] in LEGAL_TOKENS:
        tokens.pop()
    tokens = [t for t in tokens if t not in {"sdn", "bhd", "sendirian", "berhad"}]
    core = " ".join(tokens)
    return core or norm_text(name)


def norm_phone(value: Any) -> str | None:
    text = re.sub(r"(?:ext\.?|x|#)\s*\d+\s*$", "", fold(value))
    digits = re.sub(r"\D", "", text)
    if not digits:
        return None
    if digits.startswith("0060"):
        digits = digits[2:]
    if digits.startswith("60"):
        pass
    elif digits.startswith("0"):
        digits = "60" + digits[1:]
    elif digits.startswith("1"):
        digits = "60" + digits
    else:
        return None
    if not (9 <= len(digits) <= 13):
        return None
    return digits


def split_multi(value: Any) -> list[str]:
    if value is None:
        return []
    text = str(value)
    if not text.strip():
        return []
    parts = re.split(r"\s*[|;/\n]\s*", text)
    return [p.strip() for p in parts if p.strip()]


def split_urls(value: Any) -> list[str]:
    if value is None:
        return []
    text = str(value)
    if not text.strip():
        return []
    parts = re.split(r"\s*[|;]\s*|\s+", text)
    return [p.strip() for p in parts if p.strip()]


def norm_phones(value: Any) -> list[str]:
    seen: list[str] = []
    for item in split_multi(value):
        for piece in re.split(r"\s*,\s*", item):
            phone = norm_phone(piece)
            if phone and phone not in seen:
                seen.append(phone)
    return seen


def domain_of(value: Any) -> str | None:
    for raw in split_urls(value):
        candidate = raw.strip()
        if not candidate:
            continue
        if "://" not in candidate:
            candidate = "http://" + candidate
        host = (urlparse(candidate).hostname or "").lower().rstrip(".")
        if host.startswith("www."):
            host = host[4:]
        if not host or "." not in host or host in GENERIC_HOSTS:
            continue
        parts = host.split(".")
        if len(parts) >= 3 and ".".join(parts[-2:]) in MULTI_SUFFIXES:
            return ".".join(parts[-3:])
        return ".".join(parts[-2:])
    return None


def social_keys(value: Any) -> list[str]:
    """Normalise social profile URLs into platform:handle keys."""
    keys: list[str] = []
    for raw in split_urls(value):
        candidate = raw.strip()
        if not candidate:
            continue
        if "://" not in candidate:
            candidate = "http://" + candidate
        parsed = urlparse(candidate)
        host = (parsed.hostname or "").lower().removeprefix("www.")
        segments = [p for p in parsed.path.split("/") if p]
        if not segments:
            continue
        handle = segments[-1].lstrip("@").lower()
        if not re.fullmatch(r"[a-z0-9._\-]{2,60}", handle):
            continue
        platform = None
        if "facebook.com" in host:
            if any(s in {"pages", "groups", "events", "profile.php"} for s in segments):
                continue
            platform = "fb"
        elif "instagram.com" in host:
            platform = "ig"
        elif "tiktok.com" in host:
            platform = "tt"
        elif host in {"twitter.com", "x.com"}:
            platform = "tw"
        elif "linkedin.com" in host and "company" in segments:
            platform = "li"
        elif "youtube.com" in host:
            platform = "yt"
        if platform:
            key = f"{platform}:{handle}"
            if key not in keys:
                keys.append(key)
    return keys


def normalise_qid(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text if re.fullmatch(r"Q\d+", text) else None


def norm_address(value: Any) -> str:
    text = norm_text(value)
    tokens = [ADDRESS_ABBREV.get(t, t) for t in text.split()]
    return " ".join(t for t in tokens if t)


def norm_state(value: Any) -> str:
    text = norm_text(value)
    return STATE_ALIASES.get(text, text)


def postcode_of(value: Any, address: Any = None) -> str | None:
    text = fold(value).strip()
    if re.fullmatch(r"\d{5}", text):
        return text
    match = re.search(r"\b(\d{5})\b", fold(f"{value or ''} {address or ''}"))
    return match.group(1) if match else None


def is_active_status(status: Any, entity_type: Any = None) -> bool:
    text = norm_text(status)
    if not text:
        return True
    if any(word in text for word in INACTIVE_WORDS):
        return False
    if any(word in text for word in ACTIVE_WORDS):
        return True
    return True


def stable_hash(*parts: Any) -> str:
    payload = "|".join(fold(p) for p in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def clean_reg(value: Any) -> str | None:
    text = re.sub(r"[^A-Za-z0-9]", "", fold(value)).upper()
    return text or None


# --------------------------------------------------------------------------
# Row normalisation
# --------------------------------------------------------------------------
def pick(row: dict[str, Any], mapping: dict[str, str], key: str) -> Any:
    column = mapping.get(key)
    if not column:
        column = key
    return row.get(column)


def normalise_entity(
    row: dict[str, Any], mapping: dict[str, str], source: str
) -> dict[str, Any] | None:
    name = str(pick(row, mapping, "name") or "").strip()
    if not name:
        return None

    phones = norm_phones(pick(row, mapping, "phone"))
    website_raw = pick(row, mapping, "website")
    address = str(pick(row, mapping, "address") or "").strip()
    postcode = postcode_of(pick(row, mapping, "postcode"), address)
    state = norm_state(pick(row, mapping, "state"))
    reg_no = clean_reg(pick(row, mapping, "reg_no"))
    status = str(pick(row, mapping, "status") or "").strip()

    entity_id = f"ssm:{reg_no}" if reg_no else f"name:{stable_hash(name, address, postcode)}"
    lat = to_float(pick(row, mapping, "lat"))
    lon = to_float(pick(row, mapping, "lon"))

    return {
        "entity_id": entity_id,
        "name": name,
        "name_norm": norm_text(name),
        "name_core": name_core(name),
        "reg_no": reg_no,
        "status": status,
        "is_active": is_active_status(status),
        "entity_type": str(pick(row, mapping, "entity_type") or "").strip() or None,
        "msic": str(pick(row, mapping, "msic") or "").strip() or None,
        "address": address or None,
        "address_norm": norm_address(address) or None,
        "postcode": postcode,
        "city": str(pick(row, mapping, "city") or "").strip() or None,
        "state": state or None,
        "phone": str(pick(row, mapping, "phone") or "").strip() or None,
        "phone_norm": phones[0] if phones else None,
        "phone_suffix": phones[0][-7:] if phones else None,
        "phones_all": " | ".join(phones) or None,
        "email": str(pick(row, mapping, "email") or "").strip().lower() or None,
        "website": str(website_raw or "").strip() or None,
        "domain": domain_of(website_raw),
        "lat": lat,
        "lon": lon,
        "source": source,
        "source_id": reg_no,
        "raw": json.dumps(row, ensure_ascii=False, default=str),
    }


def normalise_outlet(
    row: dict[str, Any], mapping: dict[str, str]
) -> dict[str, Any] | None:
    name = str(pick(row, mapping, "name") or "").strip()
    if not name:
        return None

    gers_id = str(pick(row, mapping, "gers_id") or "").strip() or None
    given_id = str(pick(row, mapping, "outlet_id") or "").strip() or None
    phones = norm_phones(pick(row, mapping, "phone"))
    website_raw = pick(row, mapping, "website")
    socials_raw = pick(row, mapping, "socials")
    socials = social_keys(socials_raw)
    brand = str(pick(row, mapping, "brand") or "").strip() or None
    brand_wikidata = normalise_qid(pick(row, mapping, "brand_wikidata"))
    address = str(pick(row, mapping, "address") or "").strip()
    postcode = postcode_of(pick(row, mapping, "postcode"), address)
    region = str(pick(row, mapping, "region") or "").strip()
    lat = to_float(pick(row, mapping, "lat"))
    lon = to_float(pick(row, mapping, "lon"))

    outlet_id = gers_id or given_id
    if outlet_id:
        outlet_id = f"ov:{outlet_id}"
    else:
        outlet_id = f"ov:{stable_hash(name, address, lat, lon)}"

    return {
        "outlet_id": outlet_id,
        "gers_id": gers_id,
        "name": name,
        "name_norm": norm_text(name),
        "name_core": name_core(name),
        "brand": brand,
        "brand_wikidata": brand_wikidata,
        "category": str(pick(row, mapping, "category") or "").strip() or None,
        "category_path": str(pick(row, mapping, "category_path") or "").strip() or None,
        "address": address or None,
        "address_norm": norm_address(address) or None,
        "locality": str(pick(row, mapping, "locality") or "").strip() or None,
        "region": region or None,
        "state": norm_state(region) or None,
        "postcode": postcode,
        "country": str(pick(row, mapping, "country") or "").strip() or None,
        "phone": str(pick(row, mapping, "phone") or "").strip() or None,
        "phone_norm": phones[0] if phones else None,
        "phone_suffix": phones[0][-7:] if phones else None,
        "phones_all": " | ".join(phones) or None,
        "email": str(pick(row, mapping, "email") or "").strip().lower() or None,
        "website": str(website_raw or "").strip() or None,
        "domain": domain_of(website_raw),
        "socials": str(socials_raw or "").strip() or None,
        "social_keys": " | ".join(socials) or None,
        "lat": lat,
        "lon": lon,
        "confidence": to_float(pick(row, mapping, "confidence")),
        "operating_status": str(pick(row, mapping, "operating_status") or "").strip() or None,
        "source_datasets": str(pick(row, mapping, "source_datasets") or "").strip() or None,
        "source_licenses": str(pick(row, mapping, "source_licenses") or "").strip() or None,
        "raw": json.dumps(row, ensure_ascii=False, default=str),
    }


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------
# Input
# --------------------------------------------------------------------------
def read_rows(path: str) -> tuple[list[str], Iterator[dict[str, Any]]]:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Input not found: {p}")

    suffix = p.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        con = duckdb.connect()
        cur = con.execute(f"SELECT * FROM read_parquet({sql_str(str(p))})")
        columns = [d[0] for d in cur.description]
        return columns, _parquet_iter(cur, columns)

    if suffix in {".json", ".jsonl", ".ndjson"}:
        return _json_rows(p)

    if suffix in {".xlsx", ".xls"}:
        raise SystemExit("Excel is not supported; export the sheet to CSV first.")

    return _csv_rows(p)


def _parquet_iter(cur: Any, columns: list[str]) -> Iterator[dict[str, Any]]:
    while True:
        batch = cur.fetchmany(5000)
        if not batch:
            break
        for row in batch:
            yield dict(zip(columns, row))


def _csv_rows(p: Path) -> tuple[list[str], Iterator[dict[str, Any]]]:
    raw = p.read_bytes()
    text = None
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise SystemExit(f"Could not decode {p}")

    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel

    reader = csv.DictReader(text.splitlines(), dialect=dialect)
    columns = list(reader.fieldnames or [])

    def gen() -> Iterator[dict[str, Any]]:
        for row in reader:
            yield dict(row)

    return columns, gen()


def _json_rows(p: Path) -> tuple[list[str], Iterator[dict[str, Any]]]:
    text = p.read_text(encoding="utf-8-sig")
    stripped = text.lstrip()
    if stripped.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list) or not data:
            raise SystemExit("JSON array is empty")
        columns = list(data[0].keys())
        return columns, iter(data)

    def gen() -> Iterator[dict[str, Any]]:
        for line in text.splitlines():
            if line.strip():
                yield json.loads(line)

    first = next(gen(), None)
    if first is None:
        raise SystemExit("JSONL is empty")
    columns = list(first.keys())
    return columns, _prepend(first, gen())


def _prepend(first: dict[str, Any], rest: Iterator[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    yield first
    yield from rest


def sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def normalise_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", fold(value))


def build_mapping(
    columns: list[str],
    aliases: dict[str, list[str]],
    overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    norm_to_real = {normalise_header(c): c for c in columns}
    mapping: dict[str, str] = {}
    overrides = overrides or {}
    for canonical, options in aliases.items():
        if canonical in overrides:
            actual = overrides[canonical]
            if actual not in columns:
                raise SystemExit(
                    f"column-map target {actual!r} not found. Columns: {columns}"
                )
            mapping[canonical] = actual
            continue
        for option in options:
            hit = norm_to_real.get(normalise_header(option))
            if hit:
                mapping[canonical] = hit
                break
    return mapping


# --------------------------------------------------------------------------
# Schema
# --------------------------------------------------------------------------
DDL = """
CREATE TABLE IF NOT EXISTS entities (
    entity_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    name_norm VARCHAR,
    name_core VARCHAR,
    reg_no VARCHAR,
    status VARCHAR,
    is_active BOOLEAN,
    entity_type VARCHAR,
    msic VARCHAR,
    address VARCHAR,
    address_norm VARCHAR,
    postcode VARCHAR,
    city VARCHAR,
    state VARCHAR,
    phone VARCHAR,
    phone_norm VARCHAR,
    phone_suffix VARCHAR,
    phones_all VARCHAR,
    email VARCHAR,
    website VARCHAR,
    domain VARCHAR,
    lat DOUBLE,
    lon DOUBLE,
    source VARCHAR,
    source_id VARCHAR,
    raw JSON,
    loaded_at TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS outlets (
    outlet_id VARCHAR PRIMARY KEY,
    gers_id VARCHAR,
    name VARCHAR,
    name_norm VARCHAR,
    name_core VARCHAR,
    brand VARCHAR,
    brand_wikidata VARCHAR,
    category VARCHAR,
    category_path VARCHAR,
    address VARCHAR,
    address_norm VARCHAR,
    locality VARCHAR,
    region VARCHAR,
    state VARCHAR,
    postcode VARCHAR,
    country VARCHAR,
    phone VARCHAR,
    phone_norm VARCHAR,
    phone_suffix VARCHAR,
    phones_all VARCHAR,
    email VARCHAR,
    website VARCHAR,
    domain VARCHAR,
    socials VARCHAR,
    social_keys VARCHAR,
    lat DOUBLE,
    lon DOUBLE,
    confidence DOUBLE,
    operating_status VARCHAR,
    source_datasets VARCHAR,
    source_licenses VARCHAR,
    raw JSON,
    group_id VARCHAR,
    is_canonical BOOLEAN DEFAULT TRUE,
    loaded_at TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS outlet_groups (
    outlet_id VARCHAR PRIMARY KEY,
    group_id VARCHAR,
    canonical_outlet_id VARCHAR,
    is_canonical BOOLEAN,
    dup_score DOUBLE,
    reason JSON
);

CREATE TABLE IF NOT EXISTS links (
    outlet_id VARCHAR,
    entity_id VARCHAR,
    match_rank INTEGER,
    score DOUBLE,
    decision VARCHAR,
    name_score DOUBLE,
    phone_score DOUBLE,
    domain_score DOUBLE,
    postcode_score DOUBLE,
    state_score DOUBLE,
    address_score DOUBLE,
    geo_score DOUBLE,
    penalty DOUBLE,
    jw DOUBLE,
    distance_m DOUBLE,
    evidence JSON,
    created_at TIMESTAMP DEFAULT current_timestamp
);
"""

VIEWS = """
CREATE OR REPLACE VIEW best_links AS
SELECT * FROM links WHERE match_rank = 1;

CREATE OR REPLACE VIEW matched_auto AS
SELECT * FROM best_links WHERE decision = 'auto';

CREATE OR REPLACE VIEW review_queue AS
SELECT * FROM best_links WHERE decision = 'review';
"""


def init_db(con: duckdb.DuckDBPyConnection, force: bool = False) -> None:
    if force:
        con.execute(
            "DROP VIEW IF EXISTS review_queue;"
            "DROP VIEW IF EXISTS matched_auto;"
            "DROP VIEW IF EXISTS best_links;"
            "DROP TABLE IF EXISTS links;"
            "DROP TABLE IF EXISTS outlet_duplicates;"
            "DROP TABLE IF EXISTS outlet_groups;"
            "DROP TABLE IF EXISTS outlets;"
            "DROP TABLE IF EXISTS entities;"
        )
    con.execute(DDL)
    # Lightweight migration for databases created before dedupe existed.
    for statement in (
        "ALTER TABLE outlets ADD COLUMN group_id VARCHAR",
        "ALTER TABLE outlets ADD COLUMN is_canonical BOOLEAN DEFAULT TRUE",
        "ALTER TABLE outlets ADD COLUMN brand VARCHAR",
        "ALTER TABLE outlets ADD COLUMN brand_wikidata VARCHAR",
        "ALTER TABLE outlets ADD COLUMN socials VARCHAR",
        "ALTER TABLE outlets ADD COLUMN social_keys VARCHAR",
        "ALTER TABLE outlets ADD COLUMN org_id VARCHAR",
    ):
        try:
            con.execute(statement)
        except duckdb.Error:
            pass
    con.execute(VIEWS)


# --------------------------------------------------------------------------
# Loaders
# --------------------------------------------------------------------------
def insert_rows(
    con: duckdb.DuckDBPyConnection,
    table: str,
    cols: list[str],
    rows: Iterable[dict[str, Any]],
) -> int:
    placeholders = ",".join(
        "CAST(? AS JSON)" if c == "raw" else "?" for c in cols
    )
    sql = (
        f"INSERT OR REPLACE INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
    )
    buffer: list[tuple[Any, ...]] = []
    total = 0
    for row in rows:
        buffer.append(tuple(row.get(c) for c in cols))
        if len(buffer) >= 2000:
            con.executemany(sql, buffer)
            total += len(buffer)
            buffer.clear()
    if buffer:
        con.executemany(sql, buffer)
        total += len(buffer)
    return total


def cmd_init(args: argparse.Namespace) -> int:
    con = duckdb.connect(args.db)
    init_db(con, force=args.force)
    log.info("Initialised %s", args.db)
    return 0


def load_entities_into(
    con: duckdb.DuckDBPyConnection,
    input_path: str,
    overrides: dict[str, str],
    *,
    source: str = "ssm",
    active_only: bool = False,
) -> int:
    columns, rows = read_rows(input_path)
    mapping = build_mapping(columns, ENTITY_ALIASES, overrides)
    if "name" not in mapping:
        raise SystemExit(
            f"Could not find a name column. Detected: {columns}. "
            "Use --column-map-file with {\"name\": \"Your Name Column\"}."
        )
    log.info("Entity columns: %s", mapping)

    def prepared() -> Iterator[dict[str, Any]]:
        skipped = 0
        for row in rows:
            record = normalise_entity(row, mapping, source)
            if record is None:
                skipped += 1
                continue
            if active_only and not record["is_active"]:
                skipped += 1
                continue
            yield record
        if skipped:
            log.warning("Skipped %d rows without a usable name/status", skipped)

    return insert_rows(con, "entities", ENTITY_COLS, prepared())


def load_outlets_into(
    con: duckdb.DuckDBPyConnection,
    input_path: str,
    overrides: dict[str, str],
) -> int:
    columns, rows = read_rows(input_path)
    mapping = build_mapping(columns, OUTLET_ALIASES, overrides)
    if "name" not in mapping:
        raise SystemExit(
            f"Could not find a name column. Detected: {columns}. "
            "Use --column-map-file with {\"name\": \"Your Name Column\"}."
        )
    log.info("Outlet columns: %s", mapping)
    return insert_rows(
        con, "outlets", OUTLET_COLS,
        (r for r in (normalise_outlet(row, mapping) for row in rows) if r),
    )


def cmd_load_entities(args: argparse.Namespace) -> int:
    con = duckdb.connect(args.db)
    init_db(con)
    if not args.append:
        con.execute("DELETE FROM entities")
    count = load_entities_into(
        con, args.input, load_overrides(args),
        source=args.source, active_only=args.active_only,
    )
    log.info("Loaded %d entities into %s", count, args.db)
    return 0


def cmd_load_outlets(args: argparse.Namespace) -> int:
    con = duckdb.connect(args.db)
    init_db(con)
    if not args.append:
        con.execute("DELETE FROM outlets")
    else:
        log.warning(
            "Appended outlets replace their group_id/is_canonical values; "
            "re-run dedupe-outlets afterwards."
        )
    count = load_outlets_into(con, args.input, load_overrides(args))
    log.info("Loaded %d outlets into %s", count, args.db)
    return 0


def parse_json_arg(raw: str | None, flag: str) -> dict[str, str]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{flag} is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"{flag} must be a JSON object")
    return {str(k): str(v) for k, v in value.items()}


def load_overrides(args: argparse.Namespace) -> dict[str, str]:
    """Column map from --column-map or a UTF-8 file (PowerShell-friendly)."""
    if getattr(args, "column_map_file", None):
        return load_map_file(args.column_map_file)
    return parse_json_arg(getattr(args, "column_map", None), "--column-map")


def load_map_file(path: str | None) -> dict[str, str]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Column map file not found: {p}")
    return parse_json_arg(p.read_text(encoding="utf-8-sig"), "--column-map-file")


# --------------------------------------------------------------------------
# Resolution
# --------------------------------------------------------------------------
def run_resolve(
    con: duckdb.DuckDBPyConnection,
    *,
    auto: float = 0.82,
    review: float = 0.62,
    top_n: int | None = 5,
    active_only: bool = False,
    max_candidates: int = 20_000_000,
    candidate_table: str = "candidates",
    output_table: str = "links",
    build_candidates: bool = True,
    canonical_only: bool = False,
) -> dict[str, int]:
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE ent AS "
        f"SELECT * FROM entities WHERE NOT {str(active_only).lower()} OR is_active"
    )
    outlet_table = "outlets"
    if canonical_only:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE outlets_canonical AS "
            "SELECT * FROM outlets WHERE is_canonical"
        )
        outlet_table = "outlets_canonical"

    blocking = f"""
    CREATE OR REPLACE TEMP TABLE candidates AS
    WITH raw AS (
        SELECT o.outlet_id, e.entity_id
        FROM {outlet_table} o
        JOIN ent e ON o.name_core = e.name_core
        WHERE length(o.name_core) >= 3

        UNION

        SELECT o.outlet_id, e.entity_id
        FROM {outlet_table} o
        JOIN ent e ON o.phone_suffix = e.phone_suffix
        WHERE o.phone_suffix IS NOT NULL

        UNION

        SELECT o.outlet_id, e.entity_id
        FROM {outlet_table} o
        JOIN ent e ON o.domain = e.domain
        WHERE o.domain IS NOT NULL

        UNION

        SELECT o.outlet_id, e.entity_id
        FROM {outlet_table} o
        JOIN ent e
          ON left(o.name_core, 8) = left(e.name_core, 8)
         AND o.postcode = e.postcode
        WHERE length(o.name_core) >= 8 AND o.postcode IS NOT NULL

        UNION

        SELECT o.outlet_id, e.entity_id
        FROM {outlet_table} o
        JOIN ent e
          ON left(o.name_core, 12) = left(e.name_core, 12)
         AND o.state = e.state
        WHERE length(o.name_core) >= 12 AND o.state IS NOT NULL
    )
    SELECT DISTINCT outlet_id, entity_id FROM raw;
    """
    if build_candidates:
        con.execute(blocking)

    candidate_count = con.execute(
        f"SELECT count(*) FROM {candidate_table}"
    ).fetchone()[0]
    if candidate_count > max_candidates:
        raise SystemExit(
            f"{candidate_count:,} candidate pairs exceeds the safety cap "
            f"({max_candidates:,}). Use --active-only, or tighten the blocking "
            "rules before running."
        )
    if candidate_count == 0:
        con.execute(f"DELETE FROM {output_table}")
        return {"candidates": 0, "links": 0}

    rank_filter = f"WHERE match_rank <= {int(top_n)}" if top_n is not None else ""
    scoring = f"""
    CREATE OR REPLACE TABLE {output_table} AS
    WITH scored AS (
        SELECT
            c.outlet_id,
            c.entity_id,
            o.name AS outlet_name,
            e.name AS entity_name,
            o.name_core AS outlet_core,
            e.name_core AS entity_core,
            (o.name_core = e.name_core) AS name_core_equal,
            o.phone_suffix AS o_suffix,
            e.phone_suffix AS e_suffix,
            o.phone_norm AS o_phone,
            e.phone_norm AS e_phone,
            o.address_norm AS o_addr,
            e.address_norm AS e_addr,
            o.postcode AS o_pc,
            e.postcode AS e_pc,
            o.state AS o_state,
            e.state AS e_state,
            jaro_winkler_similarity(o.name_core, e.name_core) AS jw,
            (coalesce(o.phone_norm, '') <> ''
                AND coalesce(o.phone_norm, '') = coalesce(e.phone_norm, ''))
                AS phone_equal,
            (
                coalesce(o.phones_all, '') <> ''
                AND coalesce(e.phones_all, '') <> ''
                AND list_has_any(
                    string_split(o.phones_all, ' | '),
                    string_split(e.phones_all, ' | ')
                )
            ) AS phone_overlap,
            (coalesce(o.domain, '') <> ''
                AND coalesce(o.domain, '') = coalesce(e.domain, '')) AS domain_equal,
            (coalesce(o.postcode, '') <> ''
                AND coalesce(o.postcode, '') = coalesce(e.postcode, ''))
                AS postcode_equal,
            (coalesce(o.state, '') <> ''
                AND coalesce(o.state, '') = coalesce(e.state, '')) AS state_equal,
            jaro_winkler_similarity(
                coalesce(o.address_norm, ''), coalesce(e.address_norm, '')
            ) AS addr_jw,
            CASE
                WHEN o.lat IS NOT NULL AND o.lon IS NOT NULL
                 AND e.lat IS NOT NULL AND e.lon IS NOT NULL
                THEN 2 * 6371000 * asin(sqrt(
                    power(sin(radians(e.lat - o.lat) / 2), 2)
                    + cos(radians(o.lat)) * cos(radians(e.lat))
                      * power(sin(radians(e.lon - o.lon) / 2), 2)
                ))
            END AS distance_m
        FROM {candidate_table} c
        JOIN {outlet_table} o ON o.outlet_id = c.outlet_id
        JOIN ent e ON e.entity_id = c.entity_id
    ),
    components AS (
        SELECT *,
            CASE
                WHEN coalesce(outlet_name, '') = '' OR coalesce(entity_name, '') = ''
                    THEN 0
                WHEN jw >= 0.95 THEN 0.45
                WHEN jw >= 0.88 THEN 0.35
                WHEN jw >= 0.78 THEN 0.22
                WHEN jw >= 0.65 THEN 0.10
                ELSE 0.02
            END
            + CASE WHEN name_core_equal THEN 0.10 ELSE 0 END
                AS name_score,
            CASE
                WHEN phone_equal THEN 0.25
                WHEN phone_overlap THEN 0.20
                WHEN coalesce(o_suffix, '') <> '' AND o_suffix = e_suffix THEN 0.08
                ELSE 0
            END AS phone_score,
            CASE WHEN domain_equal THEN 0.15 ELSE 0 END AS domain_score,
            CASE WHEN postcode_equal THEN 0.07 ELSE 0 END AS postcode_score,
            CASE WHEN state_equal THEN 0.04 ELSE 0 END AS state_score,
            CASE
                WHEN coalesce(o_addr, '') = '' OR coalesce(e_addr, '') = ''
                    THEN 0
                WHEN addr_jw >= 0.90 THEN 0.12
                WHEN addr_jw >= 0.80 THEN 0.08
                WHEN addr_jw >= 0.65 THEN 0.04
                ELSE 0
            END AS address_score,
            CASE
                WHEN distance_m IS NULL THEN 0
                WHEN distance_m <= 100 THEN 0.06
                WHEN distance_m <= 500 THEN 0.04
                WHEN distance_m <= 2000 THEN 0.02
                ELSE 0
            END AS geo_score,
            (
                CASE WHEN coalesce(o_phone, '') <> '' AND coalesce(e_phone, '') <> ''
                       AND NOT phone_overlap THEN -0.10 ELSE 0 END
                + CASE WHEN coalesce(o_pc, '') <> '' AND coalesce(e_pc, '') <> ''
                        AND NOT postcode_equal THEN -0.05 ELSE 0 END
                + CASE WHEN coalesce(o_state, '') <> '' AND coalesce(e_state, '') <> ''
                        AND NOT state_equal THEN -0.15 ELSE 0 END
                + CASE WHEN coalesce(o_addr, '') <> '' AND coalesce(e_addr, '') <> ''
                        AND addr_jw < 0.45 THEN -0.05 ELSE 0 END
                + CASE WHEN distance_m > 20000 THEN -0.05 ELSE 0 END
                + CASE WHEN jw < 0.55 AND NOT phone_overlap AND NOT domain_equal
                        THEN -0.15 ELSE 0 END
            ) AS penalty
        FROM scored
    ),
    totals AS (
        SELECT *,
            least(1.0::DOUBLE, greatest(0.0::DOUBLE,
                name_score + phone_score + domain_score + postcode_score
                + state_score + address_score + geo_score + penalty
            )) AS score
        FROM components
    ),
    ranked AS (
        SELECT *,
            row_number() OVER (
                PARTITION BY outlet_id ORDER BY score DESC, entity_id
            ) AS match_rank
        FROM totals
    )
    SELECT
        outlet_id,
        entity_id,
        match_rank,
        score,
        CASE WHEN score >= {auto} THEN 'auto'
             WHEN score >= {review} THEN 'review'
             ELSE 'reject' END AS decision,
        name_score, phone_score, domain_score, postcode_score, state_score,
        address_score, geo_score, penalty, jw, distance_m,
        json_object(
            'outlet_name', outlet_name,
            'entity_name', entity_name,
            'jaro_winkler', jw,
            'phone_equal', phone_equal,
            'phone_overlap', phone_overlap,
            'domain_equal', domain_equal,
            'postcode_equal', postcode_equal,
            'state_equal', state_equal,
            'address_jw', addr_jw,
            'distance_m', distance_m
        ) AS evidence,
        current_timestamp AS created_at
    FROM ranked
    {rank_filter};
    """
    con.execute(scoring)
    link_count = con.execute(
        f"SELECT count(*) FROM {output_table}"
    ).fetchone()[0]
    return {"candidates": int(candidate_count), "links": int(link_count)}


def cmd_resolve(args: argparse.Namespace) -> int:
    con = duckdb.connect(args.db)
    init_db(con)
    started = datetime.now(timezone.utc)
    stats = run_resolve(
        con,
        auto=args.auto,
        review=args.review,
        top_n=args.top,
        active_only=args.active_only,
        max_candidates=args.max_candidates,
        canonical_only=args.canonical_only,
    )
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    log.info(
        "Scored %s candidate pairs into %s links (%.1fs)",
        f"{stats['candidates']:,}", f"{stats['links']:,}", elapsed,
    )
    cmd_report_inner(con, canonical_only=args.canonical_only)
    return 0


def cmd_report_inner(
    con: duckdb.DuckDBPyConnection, canonical_only: bool = False
) -> None:
    outlet_filter = "WHERE is_canonical" if canonical_only else ""
    outlets = con.execute(
        f"SELECT count(*) FROM outlets {outlet_filter}"
    ).fetchone()[0]
    entities = con.execute("SELECT count(*) FROM entities").fetchone()[0]
    active_entities = con.execute(
        "SELECT count(*) FROM entities WHERE is_active"
    ).fetchone()[0]
    auto = con.execute("SELECT count(*) FROM matched_auto").fetchone()[0]
    review = con.execute("SELECT count(*) FROM review_queue").fetchone()[0]
    rejected = con.execute(
        "SELECT count(*) FROM best_links WHERE decision = 'reject'"
    ).fetchone()[0]
    no_candidate = con.execute(
        f"SELECT count(*) FROM outlets o {outlet_filter} "
        + ("AND " if outlet_filter else "WHERE ")
        + "NOT EXISTS (SELECT 1 FROM best_links b WHERE b.outlet_id = o.outlet_id)"
    ).fetchone()[0]
    multi_outlet = con.execute(
        "SELECT count(*) FROM ("
        "SELECT entity_id FROM matched_auto GROUP BY 1 HAVING count(*) > 1)"
    ).fetchone()[0]

    print(f"Outlets:          {outlets:,}")
    print(f"Entities:         {entities:,} ({active_entities:,} active)")
    print(f"Auto:             {auto:,} ({auto / outlets:.1%})" if outlets else "Auto: 0")
    print(f"Review:           {review:,}")
    print(f"Rejected:         {rejected:,}")
    print(f"No candidate:     {no_candidate:,}")
    print(f"Entities w/ >1 outlet: {multi_outlet:,}")

    rows = con.execute(
        f"""
        SELECT coalesce(category, '(none)') AS category, count(*) AS n
        FROM outlets o
        WHERE NOT EXISTS (
            SELECT 1 FROM matched_auto a WHERE a.outlet_id = o.outlet_id
        )
        {"AND o.is_canonical" if canonical_only else ""}
        GROUP BY 1 ORDER BY n DESC LIMIT 10
        """
    ).fetchall()
    if rows:
        print("\nLargest categories without an auto link")
        for category, count in rows:
            print(f"  {count:>8,}  {category}")


def cmd_report(args: argparse.Namespace) -> int:
    con = duckdb.connect(args.db)
    init_db(con)
    cmd_report_inner(con, canonical_only=args.canonical_only)
    return 0


# --------------------------------------------------------------------------
# Outlet de-duplication
# --------------------------------------------------------------------------
def run_dedupe(
    con: duckdb.DuckDBPyConnection,
    *,
    threshold: float = 0.85,
    review: float = 0.65,
) -> dict[str, int]:
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE outlet_pairs AS
        WITH raw AS (
            SELECT a.outlet_id AS a_id, b.outlet_id AS b_id
            FROM outlets a
            JOIN outlets b
              ON a.phone_suffix = b.phone_suffix
             AND a.outlet_id < b.outlet_id
            WHERE a.phone_suffix IS NOT NULL

            UNION

            SELECT a.outlet_id, b.outlet_id
            FROM outlets a
            JOIN outlets b
              ON coalesce(a.domain, '') = coalesce(b.domain, '')
             AND a.outlet_id < b.outlet_id
            WHERE coalesce(a.domain, '') <> ''

            UNION

            SELECT a.outlet_id, b.outlet_id
            FROM outlets a
            JOIN outlets b
              ON a.name_core = b.name_core
             AND coalesce(a.postcode, '') = coalesce(b.postcode, '')
             AND a.outlet_id < b.outlet_id
            WHERE coalesce(a.postcode, '') <> ''

            UNION

            SELECT a.outlet_id, b.outlet_id
            FROM outlets a
            JOIN outlets b
              ON a.name_core = b.name_core
             AND left(coalesce(a.address_norm, ''), 15)
                 = left(coalesce(b.address_norm, ''), 15)
             AND a.outlet_id < b.outlet_id
            WHERE coalesce(a.address_norm, '') <> ''
        )
        SELECT DISTINCT a_id, b_id FROM raw;
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE outlet_duplicates AS
        WITH features AS (
            SELECT
                p.a_id,
                p.b_id,
                jaro_winkler_similarity(a.name_core, b.name_core) AS jw,
                (coalesce(a.phone_norm, '') <> ''
                    AND coalesce(a.phone_norm, '') = coalesce(b.phone_norm, ''))
                    AS phone_equal,
                (coalesce(a.phones_all, '') <> ''
                    AND coalesce(b.phones_all, '') <> ''
                    AND list_has_any(
                        string_split(a.phones_all, ' | '),
                        string_split(b.phones_all, ' | ')
                    )) AS phone_overlap,
                (coalesce(a.phone_suffix, '') <> ''
                    AND a.phone_suffix = b.phone_suffix) AS suffix_equal,
                (coalesce(a.domain, '') <> ''
                    AND coalesce(a.domain, '') = coalesce(b.domain, ''))
                    AS domain_equal,
                jaro_winkler_similarity(
                    coalesce(a.address_norm, ''), coalesce(b.address_norm, '')
                ) AS addr_jw,
                CASE
                    WHEN a.lat IS NOT NULL AND a.lon IS NOT NULL
                     AND b.lat IS NOT NULL AND b.lon IS NOT NULL
                    THEN 2 * 6371000 * asin(sqrt(
                        power(sin(radians(b.lat - a.lat) / 2), 2)
                        + cos(radians(a.lat)) * cos(radians(b.lat))
                          * power(sin(radians(b.lon - a.lon) / 2), 2)
                    ))
                END AS distance_m,
                (coalesce(a.postcode, '') <> ''
                    AND coalesce(a.postcode, '') = coalesce(b.postcode, ''))
                    AS postcode_equal,
                (coalesce(a.state, '') <> ''
                    AND coalesce(a.state, '') = coalesce(b.state, ''))
                    AS state_equal,
                (coalesce(a.phone_norm, '') <> '') AS a_has_phone,
                (coalesce(b.phone_norm, '') <> '') AS b_has_phone,
                (coalesce(a.address_norm, '') <> '') AS a_has_addr,
                (coalesce(b.address_norm, '') <> '') AS b_has_addr,
                (coalesce(a.postcode, '') <> '') AS a_has_pc,
                (coalesce(b.postcode, '') <> '') AS b_has_pc,
                (coalesce(a.state, '') <> '') AS a_has_state,
                (coalesce(b.state, '') <> '') AS b_has_state
            FROM outlet_pairs p
            JOIN outlets a ON a.outlet_id = p.a_id
            JOIN outlets b ON b.outlet_id = p.b_id
        ),
        scored AS (
            SELECT *,
                least(1.0::DOUBLE, greatest(0.0::DOUBLE,
                    CASE
                        WHEN jw >= 0.95 THEN 0.55
                        WHEN jw >= 0.85 THEN 0.40
                        WHEN jw >= 0.70 THEN 0.20
                        ELSE 0
                    END
                    + CASE
                        WHEN phone_equal THEN 0.30
                        WHEN phone_overlap THEN 0.20
                        WHEN suffix_equal THEN 0.08
                        ELSE 0
                    END
                    + CASE WHEN domain_equal THEN 0.15 ELSE 0 END
                    + CASE
                        WHEN addr_jw >= 0.80 THEN 0.10
                        WHEN addr_jw >= 0.60 THEN 0.05
                        ELSE 0
                    END
                    + CASE
                        WHEN distance_m <= 100 THEN 0.10
                        WHEN distance_m <= 500 THEN 0.05
                        ELSE 0
                    END
                    + CASE WHEN postcode_equal THEN 0.05 ELSE 0 END
                    + CASE WHEN state_equal THEN 0.03 ELSE 0 END
                    - CASE
                        WHEN a_has_phone AND b_has_phone AND NOT phone_overlap
                        THEN 0.10 ELSE 0
                    END
                    - CASE
                        WHEN a_has_pc AND b_has_pc AND NOT postcode_equal
                        THEN 0.05 ELSE 0
                    END
                    - CASE
                        WHEN a_has_state AND b_has_state AND NOT state_equal
                        THEN 0.10 ELSE 0
                    END
                    - CASE WHEN distance_m > 5000 THEN 0.30 ELSE 0 END
                    - CASE
                        WHEN a_has_addr AND b_has_addr AND addr_jw < 0.40
                        THEN 0.05 ELSE 0
                    END
                )) AS score
            FROM features
        )
        SELECT *,
            CASE WHEN score >= {threshold} THEN 'auto'
                 WHEN score >= {review} THEN 'review'
                 ELSE 'reject' END AS decision
        FROM scored;
        """
    )

    pair_count = con.execute("SELECT count(*) FROM outlet_duplicates").fetchone()[0]
    auto_pairs = con.execute(
        "SELECT a_id, b_id, score FROM outlet_duplicates WHERE decision = 'auto'"
    ).fetchall()

    meta_rows = con.execute(
        "SELECT outlet_id, confidence, phone_norm, website, email, address_norm "
        "FROM outlets"
    ).fetchall()
    meta = {
        row[0]: {
            "confidence": row[1] or 0.0,
            "phone_norm": row[2],
            "website": row[3],
            "email": row[4],
            "address_norm": row[5],
        }
        for row in meta_rows
    }

    parent = {outlet_id: outlet_id for outlet_id in meta}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        root_a, root_b = find(a), find(b)
        if root_a != root_b:
            parent[root_b] = root_a

    for a_id, b_id, _score in auto_pairs:
        union(a_id, b_id)

    cluster_score: dict[str, float] = {}
    for a_id, b_id, score in auto_pairs:
        root = find(a_id)
        cluster_score[root] = max(cluster_score.get(root, 0.0), float(score))

    clusters: dict[str, list[str]] = {}
    for outlet_id in parent:
        clusters.setdefault(find(outlet_id), []).append(outlet_id)

    def quality(outlet_id: str) -> tuple[float, int, int]:
        item = meta[outlet_id]
        contact = sum(
            1 for key in ("phone_norm", "website", "email", "address_norm")
            if item[key]
        )
        return (item["confidence"], contact, -len(outlet_id))

    rows: list[tuple[Any, ...]] = []
    duplicate_clusters = 0
    collapsed = 0
    for root, members in clusters.items():
        if len(members) == 1:
            outlet_id = members[0]
            rows.append((outlet_id, outlet_id, outlet_id, True, None, None))
            continue
        duplicate_clusters += 1
        collapsed += len(members) - 1
        canonical = max(members, key=quality)
        score = cluster_score.get(root)
        reason = json.dumps({
            "method": "dedupe",
            "cluster_size": len(members),
            "max_score": round(score, 3) if score is not None else None,
        })
        for outlet_id in members:
            rows.append((
                outlet_id, canonical, canonical,
                outlet_id == canonical, score, reason,
            ))

    con.execute("DELETE FROM outlet_groups")
    con.executemany(
        "INSERT INTO outlet_groups "
        "(outlet_id, group_id, canonical_outlet_id, is_canonical, dup_score, reason) "
        "VALUES (?, ?, ?, ?, ?, CAST(? AS JSON))",
        rows,
    )
    con.execute("UPDATE outlets SET group_id = outlet_id, is_canonical = TRUE")
    con.execute(
        "UPDATE outlets SET group_id = g.group_id, is_canonical = g.is_canonical "
        "FROM outlet_groups g WHERE outlets.outlet_id = g.outlet_id"
    )

    return {
        "outlets": len(meta),
        "pairs": int(pair_count),
        "auto_pairs": len(auto_pairs),
        "duplicate_clusters": duplicate_clusters,
        "collapsed": collapsed,
    }


def cmd_dedupe_outlets(args: argparse.Namespace) -> int:
    con = duckdb.connect(args.db)
    init_db(con)
    stats = run_dedupe(con, threshold=args.threshold, review=args.review)
    print(f"Outlets:            {stats['outlets']:,}")
    print(f"Pairs considered:   {stats['pairs']:,}")
    print(f"Auto-merged pairs:  {stats['auto_pairs']:,}")
    print(f"Duplicate clusters: {stats['duplicate_clusters']:,}")
    print(f"Outlets collapsed:  {stats['collapsed']:,}")
    if args.verbose:
        rows = con.execute(
            "SELECT group_id, count(*) AS n FROM outlet_groups "
            "GROUP BY 1 HAVING count(*) > 1 ORDER BY n DESC LIMIT 20"
        ).fetchall()
        for group_id, count in rows:
            print(f"  {count:>6}  {group_id}")
    return 0


# --------------------------------------------------------------------------
# Calibration
# --------------------------------------------------------------------------
def parse_label(value: Any) -> int | None:
    text = fold(value).strip()
    if text in {"1", "true", "yes", "y", "match", "positive", "correct"}:
        return 1
    if text in {"0", "false", "no", "n", "nomatch", "no match", "negative",
                "incorrect"}:
        return 0
    return None


def cmd_calibrate(args: argparse.Namespace) -> int:
    con = duckdb.connect(args.db)
    init_db(con)

    columns, rows = read_rows(args.labels)
    overrides: dict[str, str] = {}
    for canonical, actual in (
        ("outlet_id", args.outlet_col),
        ("entity_id", args.entity_col),
        ("label", args.label_col),
    ):
        if actual:
            overrides[canonical] = actual
    try:
        mapping = build_mapping(columns, LABEL_ALIASES, overrides)
    except SystemExit as exc:
        log.error("%s", exc)
        return 1
    for required in ("outlet_id", "entity_id", "label"):
        if required not in mapping:
            log.error(
                "Label file needs outlet_id, entity_id and label columns. "
                "Detected: %s", columns,
            )
            return 1

    outlet_ids = {
        r[0] for r in con.execute("SELECT outlet_id FROM outlets").fetchall()
    }
    entity_ids = {
        r[0] for r in con.execute("SELECT entity_id FROM entities").fetchall()
    }
    if not outlet_ids or not entity_ids:
        log.error("Load outlets and entities before calibrating.")
        return 1

    def map_id(value: Any, known: set[str], prefixes: tuple[str, ...]) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        if text in known:
            return text
        for prefix in prefixes:
            candidate = f"{prefix}{text}"
            if candidate in known:
                return candidate
        return None

    pair_map: dict[tuple[str, str], int] = {}
    skipped = 0
    for row in rows:
        outlet = map_id(pick(row, mapping, "outlet_id"), outlet_ids, ("ov:",))
        entity = map_id(
            pick(row, mapping, "entity_id"), entity_ids, ("ssm:", "name:")
        )
        label = parse_label(pick(row, mapping, "label"))
        if not outlet or not entity or label is None:
            skipped += 1
            continue
        key = (outlet, entity)
        if key in pair_map and pair_map[key] != label:
            log.warning(
                "Conflicting labels for %s / %s; keeping the latest",
                outlet, entity,
            )
        pair_map[key] = label

    if not pair_map:
        log.error("No usable labelled pairs found.")
        return 1
    if skipped:
        log.warning("Skipped %d labels with unknown ids or labels", skipped)

    pairs = [(o, e, label) for (o, e), label in pair_map.items()]
    con.execute(
        "CREATE OR REPLACE TEMP TABLE labelled_candidates "
        "(outlet_id VARCHAR, entity_id VARCHAR, label INTEGER)"
    )
    con.executemany(
        "INSERT INTO labelled_candidates VALUES (?, ?, ?)", pairs
    )

    run_resolve(
        con,
        auto=1.0,
        review=1.0,
        top_n=None,
        candidate_table="labelled_candidates",
        output_table="calibrated_scores",
        build_candidates=False,
    )

    scored_rows = con.execute(
        """
        SELECT l.outlet_id, l.entity_id, l.label, s.score
        FROM labelled_candidates l
        LEFT JOIN calibrated_scores s
          ON s.outlet_id = l.outlet_id AND s.entity_id = l.entity_id
        """
    ).fetchall()
    scored = [(label, score) for _, _, label, score in scored_rows if score is not None]
    missing = len(scored_rows) - len(scored)
    if missing:
        log.warning("%d labelled pairs could not be scored", missing)

    positives = sum(1 for label, _ in scored if label == 1)
    negatives = len(scored) - positives
    if positives == 0 or negatives == 0:
        log.error(
            "Calibration needs both positive and negative labels "
            "(%d positive, %d negative).", positives, negatives,
        )
        return 1

    results: list[dict[str, Any]] = []
    min_tp = max(1, int(0.1 * positives))
    for step in range(30, 99, 2):
        threshold = step / 100
        tp = sum(1 for label, score in scored if label == 1 and score >= threshold)
        fp = sum(1 for label, score in scored if label == 0 and score >= threshold)
        fn = positives - tp
        precision = tp / (tp + fp) if (tp + fp) else 1.0
        recall = tp / positives
        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) else 0.0
        )
        results.append({
            "threshold": threshold,
            "precision": round(precision, 3),
            "recall": round(recall, 3),
            "f1": round(f1, 3),
            "tp": tp,
            "fp": fp,
            "fn": fn,
        })

    print(f"Labelled pairs: {len(scored)} ({positives} positive, {negatives} negative)")
    print("\nthreshold  precision  recall     f1    tp  fp  fn")
    for row in results:
        print(
            f"  {row['threshold']:.2f}      {row['precision']:.3f}     "
            f"{row['recall']:.3f}   {row['f1']:.3f}  {row['tp']:>3} "
            f"{row['fp']:>3} {row['fn']:>3}"
        )

    auto_rows = [
        r for r in results
        if r["precision"] >= args.target_precision and r["tp"] >= min_tp
    ]
    if auto_rows:
        best_recall = max(r["recall"] for r in auto_rows)
        plateau = [r for r in auto_rows if r["recall"] == best_recall]
        auto = round((plateau[0]["threshold"] + plateau[-1]["threshold"]) / 2, 2)
    else:
        auto = 0.82
    review_rows = [
        r for r in results
        if r["recall"] >= args.target_recall and r["threshold"] <= auto
    ]
    if review_rows:
        review = round(
            (review_rows[0]["threshold"] + review_rows[-1]["threshold"]) / 2, 2
        )
    else:
        review = max(0.50, round(auto - 0.20, 2))

    print(
        f"\nSuggested auto  : {auto:.2f}  "
        f"(precision target {args.target_precision:.0%})"
    )
    print(
        f"Suggested review: {review:.2f}  "
        f"(recall target {args.target_recall:.0%})"
    )
    print("Current defaults: auto 0.82, review 0.62")
    if len(scored) < 50:
        print("Note: fewer than 50 labels; treat these thresholds as indicative.")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tool": "resolve_entities.py",
        "labels": len(scored),
        "positives": positives,
        "negatives": negatives,
        "target_precision": args.target_precision,
        "target_recall": args.target_recall,
        "suggested": {"auto": auto, "review": review},
        "thresholds": results,
    }
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(report, indent=2), encoding="utf-8")
        log.info("Wrote %s", args.out)
    return 0


# --------------------------------------------------------------------------
# Export
# --------------------------------------------------------------------------
def export_db(
    con: duckdb.DuckDBPyConnection,
    out_base: str,
    formats: list[str],
    *,
    canonical_only: bool = False,
    database: str | None = None,
) -> dict[str, int]:
    out = Path(out_base)
    out.parent.mkdir(parents=True, exist_ok=True)

    unmatched_filter = "AND o.is_canonical" if canonical_only else ""
    queries = {
        "matched": """
            SELECT b.outlet_id, o.name AS outlet_name, o.category,
                   o.address AS outlet_address, o.phone AS outlet_phone,
                   o.website AS outlet_website, o.lat, o.lon,
                   b.entity_id, e.name AS entity_name, e.reg_no, e.status,
                   e.msic, e.address AS entity_address, e.phone AS entity_phone,
                   e.website AS entity_website, b.score, b.decision, b.evidence
            FROM matched_auto b
            JOIN outlets o ON o.outlet_id = b.outlet_id
            LEFT JOIN entities e ON e.entity_id = b.entity_id
        """,
        "review": """
            SELECT b.*, o.name AS outlet_name, o.category, o.address AS outlet_address,
                   o.phone AS outlet_phone, o.website AS outlet_website,
                   e.name AS entity_name, e.reg_no, e.status, e.address AS entity_address
            FROM review_queue b
            JOIN outlets o ON o.outlet_id = b.outlet_id
            LEFT JOIN entities e ON e.entity_id = b.entity_id
        """,
        "unmatched": f"""
            SELECT o.* FROM outlets o
            WHERE NOT EXISTS (
                SELECT 1 FROM best_links b WHERE b.outlet_id = o.outlet_id
            )
            {unmatched_filter}
        """,
        "rejected": f"""
            SELECT b.*, o.name AS outlet_name, o.category,
                   o.address AS outlet_address, o.phone AS outlet_phone,
                   o.website AS outlet_website,
                   e.name AS entity_name, e.reg_no, e.status
            FROM best_links b
            JOIN outlets o ON o.outlet_id = b.outlet_id
            LEFT JOIN entities e ON e.entity_id = b.entity_id
            WHERE b.decision = 'reject'
            {unmatched_filter}
        """,
    }

    counts = {}
    for name, query in queries.items():
        target = f"{out}_{name}.parquet"
        con.execute(
            f"COPY ({query}) TO {sql_str(target)} "
            "(FORMAT PARQUET, COMPRESSION ZSTD);"
        )
        counts[name] = con.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
        if "csv" in formats:
            csv_target = f"{out}_{name}.csv"
            con.execute(
                f"COPY ({query}) TO {sql_str(csv_target)} "
                "(FORMAT CSV, HEADER, DELIMITER ',');"
            )
        log.info("Wrote %s (%s rows)", target, f"{counts[name]:,}")

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tool": "resolve_entities.py",
        "database": database,
        "counts": counts,
        "formats": formats,
        "canonical_only": canonical_only,
    }
    Path(f"{out}.meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return counts


def cmd_export(args: argparse.Namespace) -> int:
    formats = [f.strip().lower() for f in args.formats.split(",") if f.strip()]
    unknown = set(formats) - {"parquet", "csv"}
    if unknown or not formats:
        log.error("--formats must contain parquet and/or csv")
        return 1
    con = duckdb.connect(args.db)
    init_db(con)
    export_db(
        con, args.out, formats,
        canonical_only=args.canonical_only, database=args.db,
    )
    return 0


def cmd_build(args: argparse.Namespace) -> int:
    formats = [f.strip().lower() for f in args.formats.split(",") if f.strip()]
    unknown = set(formats) - {"parquet", "csv"}
    if unknown or not formats:
        log.error("--formats must contain parquet and/or csv")
        return 1

    con = duckdb.connect(args.db)
    init_db(con, force=True)

    n_ent = load_entities_into(
        con, args.entities, load_map_file(args.entities_column_map_file),
        source=args.source, active_only=args.active_only,
    )
    n_out = load_outlets_into(
        con, args.outlets, load_map_file(args.outlets_column_map_file)
    )
    log.info("Loaded %d entities and %d outlets", n_ent, n_out)

    if args.dedupe_outlets:
        dedupe_stats = run_dedupe(con, threshold=args.dedupe_threshold)
        log.info(
            "Dedupe: %d clusters, %d outlets collapsed",
            dedupe_stats["duplicate_clusters"], dedupe_stats["collapsed"],
        )

    stats = run_resolve(
        con,
        auto=args.auto,
        review=args.review,
        top_n=args.top,
        active_only=args.active_only,
        max_candidates=args.max_candidates,
        canonical_only=args.canonical_only,
    )
    log.info(
        "Resolve: %s candidates, %s links",
        f"{stats['candidates']:,}", f"{stats['links']:,}",
    )
    cmd_report_inner(con, canonical_only=args.canonical_only)

    if args.out:
        export_db(
            con, args.out, formats,
            canonical_only=args.canonical_only, database=args.db,
        )
    return 0


def cmd_sql(args: argparse.Namespace) -> int:
    con = duckdb.connect(args.db)
    result = con.execute(args.query)
    if result.description:
        columns = [d[0] for d in result.description]
        print(" | ".join(columns))
        for row in result.fetchall():
            print(" | ".join("" if v is None else str(v) for v in row))
    return 0


# --------------------------------------------------------------------------
# Self-test
# --------------------------------------------------------------------------
def cmd_self_test(_args: argparse.Namespace) -> int:
    con = duckdb.connect(":memory:")
    init_db(con, force=True)

    entities = [
        {"name": "Restoran Sedap Sdn Bhd", "reg_no": "202301234567",
         "status": "Active", "phone": "03-1234 5678",
         "address": "12 Jalan Ampang, 50450 Kuala Lumpur",
         "postcode": "50450", "state": "Kuala Lumpur"},
        {"name": "ABC Trading Sdn Bhd", "reg_no": "202001111111",
         "status": "Active", "address": "5 Jalan Perak, 50450 Kuala Lumpur",
         "postcode": "50450", "state": "Kuala Lumpur"},
        {"name": "XYZ Plumbing Sdn Bhd", "reg_no": "201901222222",
         "status": "Active", "phone": "03-9999 8888", "state": "Selangor"},
    ]
    outlets = [
        {"gers_id": "o1", "name": "Restoran Sedap", "phone": "+603-1234 5678",
         "address": "12 Jln Ampang", "postcode": "50450",
         "region": "Kuala Lumpur", "category": "restaurant"},
        {"gers_id": "o2", "name": "ABC Trading", "address": "5 Jalan Perak",
         "postcode": "50450", "region": "Kuala Lumpur", "category": "wholesaler"},
        {"gers_id": "o3", "name": "XYZ Plumbing", "phone": "03-7777 6666",
         "region": "Penang", "category": "plumber"},
        {"gers_id": "o4", "name": "Kedai Bunga Mawar", "region": "Kuala Lumpur",
         "category": "florist"},
        {"gers_id": "o5", "name": "Restoran Sedap", "phone": "03-1234 5678",
         "address": "12 Jalan Ampang", "postcode": "50450",
         "region": "Kuala Lumpur", "category": "restaurant"},
    ]

    insert_rows(
        con, "entities", ENTITY_COLS,
        (normalise_entity(r, {k: k for k in r}, "test") for r in entities),
    )
    insert_rows(
        con, "outlets", OUTLET_COLS,
        (normalise_outlet(r, {k: k for k in r}) for r in outlets),
    )
    dedupe_stats = run_dedupe(con, threshold=0.85)
    stats = run_resolve(con, auto=0.82, review=0.62, top_n=3)

    rows = con.execute(
        """
        SELECT o.gers_id, b.decision, round(b.score, 3), e.name
        FROM outlets o
        LEFT JOIN best_links b ON b.outlet_id = o.outlet_id
        LEFT JOIN entities e ON e.entity_id = b.entity_id
        ORDER BY o.gers_id
        """
    ).fetchall()
    print("\nSelf-test results")
    for gers, decision, score, entity in rows:
        print(f"  {gers}: {decision or 'unmatched':<9} {str(score or ''):<6} {entity or ''}")

    by_id = {r[0]: r for r in rows}
    duplicate_group = con.execute(
        "SELECT count(*) FROM outlet_groups WHERE group_id = "
        "(SELECT group_id FROM outlet_groups WHERE outlet_id = 'ov:o1')"
    ).fetchone()[0]
    checks = [
        ("o1 auto-linked", by_id["o1"][1] == "auto"),
        ("o2 sent to review", by_id["o2"][1] == "review"),
        ("o3 not auto-linked", by_id["o3"][1] in (None, "reject", "review")
            and by_id["o3"][1] != "auto"),
        ("o4 unmatched", by_id["o4"][1] is None),
        ("o1/o5 deduped", duplicate_group == 2),
    ]
    failed = [label for label, ok in checks if not ok]
    print(
        f"\nCandidates: {stats['candidates']}, links: {stats['links']}, "
        f"duplicate clusters: {dedupe_stats['duplicate_clusters']}"
    )
    if failed:
        print("FAILED: " + ", ".join(failed))
        return 1
    print("All self-test checks passed.")
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
GLOBAL_VALUE_FLAGS = {"--db"}
GLOBAL_BOOL_FLAGS = {"-v", "--verbose"}
COMMANDS = {"init", "load-outlets", "load-entities", "dedupe-outlets",
            "resolve", "report", "calibrate", "export", "build", "sql", "self-test"}


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
        description="Link Overture outlets to SSM-style entities",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default="data/yp.duckdb", help="DuckDB database path")
    parser.add_argument("-v", "--verbose", action="store_true")

    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="Create tables and views")
    p_init.add_argument("--force", action="store_true", help="Drop existing tables")
    p_init.set_defaults(func=cmd_init)

    p_build = sub.add_parser(
        "build", help="Load, optionally dedupe, resolve and export in one pass"
    )
    p_build.add_argument("--entities", required=True, help="SSM-style CSV/Parquet/JSONL")
    p_build.add_argument("--outlets", required=True, help="Overture-style CSV/Parquet/JSONL")
    p_build.add_argument("--out", default=None, help="Export base path")
    p_build.add_argument("--formats", default="parquet,csv")
    p_build.add_argument("--source", default="ssm")
    p_build.add_argument("--auto", type=float, default=0.82)
    p_build.add_argument("--review", type=float, default=0.62)
    p_build.add_argument("--top", type=int, default=5)
    p_build.add_argument("--active-only", action="store_true")
    p_build.add_argument("--canonical-only", action="store_true")
    p_build.add_argument("--dedupe-outlets", action="store_true")
    p_build.add_argument("--dedupe-threshold", type=float, default=0.85)
    p_build.add_argument("--max-candidates", type=int, default=20_000_000)
    p_build.add_argument("--entities-column-map-file", default=None)
    p_build.add_argument("--outlets-column-map-file", default=None)
    p_build.set_defaults(func=cmd_build)

    p_out = sub.add_parser("load-outlets", help="Load Overture-style outlet records")
    p_out.add_argument("--input", required=True, help="CSV/Parquet/JSONL")
    p_out.add_argument("--column-map", default=None, help="JSON canonical->column")
    p_out.add_argument("--column-map-file", default=None,
                       help="Path to a JSON file with canonical->column")
    p_out.add_argument("--append", action="store_true")
    p_out.set_defaults(func=cmd_load_outlets)

    p_ent = sub.add_parser("load-entities", help="Load SSM-style entity records")
    p_ent.add_argument("--input", required=True, help="CSV/Parquet/JSONL")
    p_ent.add_argument("--column-map", default=None, help="JSON canonical->column")
    p_ent.add_argument("--column-map-file", default=None,
                       help="Path to a JSON file with canonical->column")
    p_ent.add_argument("--source", default="ssm")
    p_ent.add_argument("--append", action="store_true")
    p_ent.add_argument("--active-only", action="store_true")
    p_ent.set_defaults(func=cmd_load_entities)

    p_dedupe = sub.add_parser("dedupe-outlets",
                              help="Cluster duplicate outlets and pick canonical rows")
    p_dedupe.add_argument("--threshold", type=float, default=0.85,
                          help="Auto-merge threshold")
    p_dedupe.add_argument("--review", type=float, default=0.65,
                          help="Keep pairs above this as duplicates for review")
    p_dedupe.set_defaults(func=cmd_dedupe_outlets)

    p_res = sub.add_parser("resolve", help="Block, score and link")
    p_res.add_argument("--auto", type=float, default=0.82, help="Auto-link threshold")
    p_res.add_argument("--review", type=float, default=0.62, help="Review threshold")
    p_res.add_argument("--top", type=int, default=5, help="Candidates kept per outlet")
    p_res.add_argument("--active-only", action="store_true")
    p_res.add_argument("--max-candidates", type=int, default=20_000_000)
    p_res.add_argument("--canonical-only", action="store_true",
                       help="Resolve only outlets not merged into another outlet")
    p_res.set_defaults(func=cmd_resolve)

    p_rep = sub.add_parser("report", help="Match counts and unmatched categories")
    p_rep.add_argument("--canonical-only", action="store_true")
    p_rep.set_defaults(func=cmd_report)

    p_cal = sub.add_parser("calibrate", help="Tune thresholds on labelled pairs")
    p_cal.add_argument("--labels", required=True,
                       help="CSV/Parquet/JSONL with outlet_id, entity_id, label")
    p_cal.add_argument("--outlet-col", default=None)
    p_cal.add_argument("--entity-col", default=None)
    p_cal.add_argument("--label-col", default=None)
    p_cal.add_argument("--target-precision", type=float, default=0.95)
    p_cal.add_argument("--target-recall", type=float, default=0.90)
    p_cal.add_argument("--out", default=None, help="Write JSON report here")
    p_cal.set_defaults(func=cmd_calibrate)

    p_exp = sub.add_parser("export", help="Export matched/review/unmatched sets")
    p_exp.add_argument("--out", required=True, help="Output base path")
    p_exp.add_argument("--formats", default="parquet,csv")
    p_exp.add_argument("--canonical-only", action="store_true")
    p_exp.set_defaults(func=cmd_export)

    p_sql = sub.add_parser("sql", help="Run a SQL query")
    p_sql.add_argument("--query", required=True)
    p_sql.set_defaults(func=cmd_sql)

    p_test = sub.add_parser("self-test", help="Run a synthetic end-to-end test")
    p_test.set_defaults(func=cmd_self_test)

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
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
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
