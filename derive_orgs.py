#!/usr/bin/env python3
"""
Build organizations from outlets, without a company registry.

Instead of asking "which SSM company owns this shop?", this tool groups outlets
that share identity signals:

  * same website domain (non-generic)
  * same phone number
  * same Overture brand / Wikidata QID
  * same social handle
  * same brand name
  * same normalised name + postcode / address

The result is a bottom-up `organizations` table with an auditable `org_edges`
table and an outlet membership list. It works on the output of
`resolve_entities.py load-outlets` (or `overture_my.py pull`).

Commands
--------
    py derive_orgs.py derive --db data/yp.duckdb
    py derive_orgs.py report --db data/yp.duckdb
    py derive_orgs.py export --db data/yp.duckdb --out data/orgs
    py derive_orgs.py enrich-wikidata --db data/yp.duckdb --limit 25
    py derive_orgs.py self-test

No SSM data is required. If you later obtain a registry, it can be attached as
evidence rather than treated as the only source of truth.
"""

from __future__ import annotations

import argparse
import hashlib
import html as html_mod
import json
import logging
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import duckdb
import requests

try:
    import resolve_entities as re_mod
except ImportError as exc:  # pragma: no cover - same-directory script
    raise SystemExit(
        "derive_orgs.py must sit next to resolve_entities.py"
    ) from exc

log = logging.getLogger("orgs")

ORG_DDL = """
CREATE TABLE IF NOT EXISTS organizations (
    org_id VARCHAR PRIMARY KEY,
    display_name VARCHAR,
    name_norm VARCHAR,
    name_core VARCHAR,
    brand VARCHAR,
    brand_wikidata VARCHAR,
    domain VARCHAR,
    phone VARCHAR,
    phone_norm VARCHAR,
    category VARCHAR,
    outlet_count INTEGER,
    confidence DOUBLE,
    lat DOUBLE,
    lon DOUBLE,
    key_type VARCHAR,
    evidence JSON,
    wikidata_label VARCHAR,
    wikidata_description VARCHAR,
    official_website VARCHAR,
    parent_org_qid VARCHAR,
    country_qid VARCHAR,
    stock_exchange_qid VARCHAR,
    wikidata_json JSON,
    web_legal_name VARCHAR,
    web_email VARCHAR,
    web_phone VARCHAR,
    web_address VARCHAR,
    web_description VARCHAR,
    web_same_as VARCHAR,
    web_status VARCHAR,
    web_fetched_at TIMESTAMP,
    web_contact_url VARCHAR,
    created_at TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS org_members (
    outlet_id VARCHAR PRIMARY KEY,
    org_id VARCHAR,
    role VARCHAR,
    join_score DOUBLE,
    evidence JSON
);

CREATE TABLE IF NOT EXISTS org_edges (
    a_id VARCHAR,
    b_id VARCHAR,
    score DOUBLE,
    decision VARCHAR,
    signal_count INTEGER,
    evidence JSON
);

CREATE TABLE IF NOT EXISTS external_records (
    evidence_id VARCHAR PRIMARY KEY,
    source VARCHAR,
    name VARCHAR,
    name_norm VARCHAR,
    name_core VARCHAR,
    reg_no VARCHAR,
    address VARCHAR,
    address_norm VARCHAR,
    postcode VARCHAR,
    state VARCHAR,
    phone VARCHAR,
    phone_norm VARCHAR,
    phone_suffix VARCHAR,
    website VARCHAR,
    domain VARCHAR,
    category VARCHAR,
    raw JSON,
    loaded_at TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS org_evidence (
    evidence_id VARCHAR PRIMARY KEY,
    org_id VARCHAR,
    score DOUBLE,
    decision VARCHAR,
    evidence JSON
);

CREATE TABLE IF NOT EXISTS review_decisions (
    decision_id VARCHAR PRIMARY KEY,
    action VARCHAR,
    org_a VARCHAR,
    org_b VARCHAR,
    outlet_id VARCHAR,
    evidence_id VARCHAR,
    target_org VARCHAR,
    note VARCHAR,
    created_at TIMESTAMP DEFAULT current_timestamp
);
"""

ORG_COLS = [
    "org_id", "display_name", "name_norm", "name_core", "brand",
    "brand_wikidata", "domain", "phone", "phone_norm", "category",
    "outlet_count", "confidence", "lat", "lon", "key_type", "evidence",
]

EVIDENCE_ALIASES = {
    **re_mod.ENTITY_ALIASES,
    "category": ["category", "sector", "industry", "product", "scheme",
                 "licence", "license", "certification"],
}

REVIEW_ALIASES = {
    "action": ["action", "decision", "type", "operation"],
    "org_a": ["org_a", "orga", "org_id_a", "source_org", "from_org"],
    "org_b": ["org_b", "orgb", "org_id_b", "merge_into"],
    "outlet_id": ["outlet_id", "outlet", "gers_id", "place_id"],
    "evidence_id": ["evidence_id", "evidence", "record_id"],
    "target_org": ["target_org", "target", "target_id", "new_org"],
    "note": ["note", "reason", "comment", "notes"],
}


def connect(db: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db)
    re_mod.init_db(con)
    con.execute(ORG_DDL)
    for statement in (
        "ALTER TABLE organizations ADD COLUMN name_core VARCHAR",
        "ALTER TABLE organizations ADD COLUMN phone_norm VARCHAR",
        "ALTER TABLE organizations ADD COLUMN wikidata_label VARCHAR",
        "ALTER TABLE organizations ADD COLUMN wikidata_description VARCHAR",
        "ALTER TABLE organizations ADD COLUMN official_website VARCHAR",
        "ALTER TABLE organizations ADD COLUMN parent_org_qid VARCHAR",
        "ALTER TABLE organizations ADD COLUMN country_qid VARCHAR",
        "ALTER TABLE organizations ADD COLUMN stock_exchange_qid VARCHAR",
        "ALTER TABLE organizations ADD COLUMN wikidata_json JSON",
        "ALTER TABLE organizations ADD COLUMN web_legal_name VARCHAR",
        "ALTER TABLE organizations ADD COLUMN web_email VARCHAR",
        "ALTER TABLE organizations ADD COLUMN web_phone VARCHAR",
        "ALTER TABLE organizations ADD COLUMN web_address VARCHAR",
        "ALTER TABLE organizations ADD COLUMN web_description VARCHAR",
        "ALTER TABLE organizations ADD COLUMN web_same_as VARCHAR",
        "ALTER TABLE organizations ADD COLUMN web_status VARCHAR",
        "ALTER TABLE organizations ADD COLUMN web_fetched_at TIMESTAMP",
        "ALTER TABLE organizations ADD COLUMN web_contact_url VARCHAR",
        "ALTER TABLE review_decisions ADD COLUMN evidence_id VARCHAR",
    ):
        try:
            con.execute(statement)
        except duckdb.Error:
            pass

    # Backfill name_core for organizations created before the column existed.
    missing = con.execute(
        "SELECT org_id, display_name FROM organizations WHERE name_core IS NULL"
    ).fetchall()
    for org_id, display_name in missing:
        con.execute(
            "UPDATE organizations SET name_core = ? WHERE org_id = ?",
            [re_mod.name_core(display_name), org_id],
        )
    missing_phones = con.execute(
        "SELECT org_id, phone FROM organizations "
        "WHERE phone_norm IS NULL AND phone IS NOT NULL"
    ).fetchall()
    for org_id, phone in missing_phones:
        con.execute(
            "UPDATE organizations SET phone_norm = ? WHERE org_id = ?",
            [re_mod.norm_phone(phone), org_id],
        )
    con.execute(
        """
        CREATE OR REPLACE VIEW org_hierarchy AS
        SELECT child.org_id AS child_id, child.display_name AS child_name,
               parent.org_id AS parent_id, parent.display_name AS parent_name,
               child.parent_org_qid
        FROM organizations child
        LEFT JOIN organizations parent
          ON parent.brand_wikidata = child.parent_org_qid
        """
    )
    return con


# --------------------------------------------------------------------------
# Derivation
# --------------------------------------------------------------------------
def run_derive(
    con: duckdb.DuckDBPyConnection,
    *,
    threshold: float = 0.72,
    review: float = 0.55,
    canonical_only: bool = True,
) -> dict[str, int]:
    outlet_filter = "WHERE is_canonical" if canonical_only else ""
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE outlet_nodes AS "
        f"SELECT * FROM outlets {outlet_filter}"
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE org_enrichment AS
        SELECT org_id, wikidata_label, wikidata_description, official_website,
               parent_org_qid, country_qid, stock_exchange_qid, wikidata_json,
               web_legal_name, web_email, web_phone, web_address,
               web_description, web_same_as, web_status, web_fetched_at,
               web_contact_url
        FROM organizations        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE outlet_socials AS
        SELECT outlet_id, unnest(string_split(social_keys, ' | ')) AS handle
        FROM outlet_nodes
        WHERE coalesce(social_keys, '') <> ''
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE org_pair_signals AS
        SELECT a.outlet_id AS a_id, b.outlet_id AS b_id, 'domain' AS signal
        FROM outlet_nodes a JOIN outlet_nodes b
          ON a.domain = b.domain AND a.outlet_id < b.outlet_id
        WHERE coalesce(a.domain, '') <> ''

        UNION ALL

        SELECT a.outlet_id, b.outlet_id, 'phone'
        FROM outlet_nodes a JOIN outlet_nodes b
          ON a.phone_norm = b.phone_norm AND a.outlet_id < b.outlet_id
        WHERE coalesce(a.phone_norm, '') <> ''

        UNION ALL

        SELECT a.outlet_id, b.outlet_id, 'suffix'
        FROM outlet_nodes a JOIN outlet_nodes b
          ON a.phone_suffix = b.phone_suffix AND a.outlet_id < b.outlet_id
        WHERE coalesce(a.phone_suffix, '') <> ''

        UNION ALL

        SELECT a.outlet_id, b.outlet_id, 'brand_qid'
        FROM outlet_nodes a JOIN outlet_nodes b
          ON a.brand_wikidata = b.brand_wikidata AND a.outlet_id < b.outlet_id
        WHERE coalesce(a.brand_wikidata, '') <> ''

        UNION ALL

        SELECT a.outlet_id, b.outlet_id, 'brand'
        FROM outlet_nodes a JOIN outlet_nodes b
          ON lower(trim(coalesce(a.brand, ''))) = lower(trim(coalesce(b.brand, '')))
         AND a.outlet_id < b.outlet_id
        WHERE length(trim(coalesce(a.brand, ''))) >= 2

        UNION ALL

        SELECT s1.outlet_id, s2.outlet_id, 'social'
        FROM outlet_socials s1 JOIN outlet_socials s2
          ON s1.handle = s2.handle AND s1.outlet_id < s2.outlet_id

        UNION ALL

        SELECT a.outlet_id, b.outlet_id, 'name_pc'
        FROM outlet_nodes a JOIN outlet_nodes b
          ON a.name_core = b.name_core
         AND coalesce(a.postcode, '') = coalesce(b.postcode, '')
         AND a.outlet_id < b.outlet_id
        WHERE coalesce(a.postcode, '') <> ''

        UNION ALL

        SELECT a.outlet_id, b.outlet_id, 'name_addr'
        FROM outlet_nodes a JOIN outlet_nodes b
          ON a.name_core = b.name_core
         AND left(coalesce(a.address_norm, ''), 15)
             = left(coalesce(b.address_norm, ''), 15)
         AND a.outlet_id < b.outlet_id
        WHERE coalesce(a.address_norm, '') <> ''
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE org_pairs AS
        SELECT
            a_id, b_id,
            bool_or(signal = 'domain') AS domain_edge,
            bool_or(signal = 'phone') AS phone_edge,
            bool_or(signal = 'suffix') AS suffix_edge,
            bool_or(signal = 'brand_qid') AS brand_qid_edge,
            bool_or(signal = 'brand') AS brand_edge,
            bool_or(signal = 'social') AS social_edge,
            bool_or(signal = 'name_pc') AS name_pc_edge,
            bool_or(signal = 'name_addr') AS name_addr_edge,
            count(*) AS signal_count
        FROM org_pair_signals
        GROUP BY a_id, b_id
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE org_edges AS
        WITH features AS (
            SELECT
                p.*,
                jaro_winkler_similarity(a.name_core, b.name_core) AS jw,
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
                (coalesce(a.domain, '') <> '') AS a_has_domain,
                (coalesce(b.domain, '') <> '') AS b_has_domain,
                (coalesce(a.phone_norm, '') <> '') AS a_has_phone,
                (coalesce(b.phone_norm, '') <> '') AS b_has_phone,
                (coalesce(a.brand_wikidata, '') <> '') AS a_has_qid,
                (coalesce(b.brand_wikidata, '') <> '') AS b_has_qid,
                (coalesce(a.state, '') <> '') AS a_has_state,
                (coalesce(b.state, '') <> '') AS b_has_state
            FROM org_pairs p
            JOIN outlet_nodes a ON a.outlet_id = p.a_id
            JOIN outlet_nodes b ON b.outlet_id = p.b_id
        ),
        scored AS (
            SELECT *,
                least(1.0::DOUBLE, greatest(0.0::DOUBLE,
                    CASE WHEN brand_qid_edge THEN 0.85 ELSE 0 END
                    + CASE WHEN domain_edge THEN 0.70 ELSE 0 END
                    + CASE WHEN phone_edge THEN 0.50 ELSE 0 END
                    + CASE WHEN brand_edge THEN 0.45 ELSE 0 END
                    + CASE WHEN social_edge THEN 0.40 ELSE 0 END
                    + CASE WHEN suffix_edge AND NOT phone_edge THEN 0.20 ELSE 0 END
                    + CASE
                        WHEN jw >= 0.95 THEN 0.35
                        WHEN jw >= 0.85 THEN 0.25
                        WHEN jw >= 0.70 THEN 0.15
                        ELSE 0
                    END
                    + CASE WHEN postcode_equal THEN 0.05 ELSE 0 END
                    + CASE
                        WHEN addr_jw >= 0.80 THEN 0.10
                        WHEN addr_jw >= 0.60 THEN 0.05
                        ELSE 0
                    END
                    + CASE WHEN distance_m <= 200 THEN 0.05 ELSE 0 END
                    - CASE
                        WHEN a_has_domain AND b_has_domain AND NOT domain_edge
                        THEN 0.15 ELSE 0
                    END
                    - CASE
                        WHEN a_has_phone AND b_has_phone AND NOT phone_edge
                        THEN 0.10 ELSE 0
                    END
                    - CASE
                        WHEN a_has_state AND b_has_state AND NOT state_equal
                        THEN 0.10 ELSE 0
                    END
                    - CASE
                        WHEN a_has_qid AND b_has_qid AND NOT brand_qid_edge
                        THEN 0.30 ELSE 0
                    END
                )) AS score
            FROM features
        )
        SELECT *,
            CASE WHEN score >= {threshold} THEN 'auto'
                 WHEN score >= {review} THEN 'review'
                 ELSE 'reject' END AS decision,
            json_object(
                'domain', domain_edge,
                'phone', phone_edge,
                'suffix', suffix_edge,
                'brand_qid', brand_qid_edge,
                'brand', brand_edge,
                'social', social_edge,
                'name_postcode', name_pc_edge,
                'name_address', name_addr_edge,
                'jaro_winkler', jw,
                'address_jw', addr_jw,
                'distance_m', distance_m,
                'signals', signal_count
            ) AS evidence
        FROM scored
        """
    )

    auto_edges = con.execute(
        "SELECT a_id, b_id, score FROM org_edges WHERE decision = 'auto'"
    ).fetchall()
    meta_rows = con.execute(
        """
        SELECT outlet_id, name, name_core, brand, brand_wikidata, domain, phone,
               phone_norm, category, lat, lon, confidence, postcode, address_norm
        FROM outlet_nodes
        """
    ).fetchall()
    meta = {
        row[0]: {
            "name": row[1],
            "name_core": row[2],
            "brand": row[3],
            "brand_wikidata": row[4],
            "domain": row[5],
            "phone": row[6],
            "phone_norm": row[7],
            "category": row[8],
            "lat": row[9],
            "lon": row[10],
            "confidence": row[11] or 0.0,
            "postcode": row[12],
            "address_norm": row[13],
        }
        for row in meta_rows
    }

    parent = {outlet_id: outlet_id for outlet_id in meta}
    if not parent:
        con.execute("DELETE FROM organizations")
        con.execute("DELETE FROM org_members")
        con.execute("DELETE FROM org_edges")
        con.execute("UPDATE outlets SET org_id = NULL")
        return {
            "outlets": 0, "organizations": 0, "multi_outlet_orgs": 0,
            "edges": 0, "auto_edges": 0,
        }

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for a_id, b_id, _score in auto_edges:
        union(a_id, b_id)

    cluster_score: dict[str, float] = {}
    for a_id, b_id, score in auto_edges:
        root = find(a_id)
        cluster_score[root] = max(cluster_score.get(root, 0.0), float(score))

    clusters: dict[str, list[str]] = {}
    for outlet_id in parent:
        clusters.setdefault(find(outlet_id), []).append(outlet_id)

    def contact_score(outlet_id: str) -> int:
        item = meta[outlet_id]
        return sum(
            1 for key in ("phone_norm", "domain", "address_norm")
            if item[key]
        )

    def quality(outlet_id: str) -> tuple[float, int, int]:
        return (meta[outlet_id]["confidence"], contact_score(outlet_id),
                -len(outlet_id))

    used_ids: set[str] = set()
    org_rows: list[tuple[Any, ...]] = []
    member_rows: list[tuple[Any, ...]] = []

    for root, members in clusters.items():
        items = [meta[outlet_id] for outlet_id in members]

        qids = [item["brand_wikidata"] for item in items if item["brand_wikidata"]]
        domains = [item["domain"] for item in items if item["domain"]]
        phones = [item["phone_norm"] for item in items if item["phone_norm"]]
        brands = [item["brand"] for item in items if item["brand"]]

        if qids:
            key_type = "brand_wikidata"
            key = Counter(qids).most_common(1)[0][0]
            org_id = f"wd:{key}"
        elif domains:
            key_type = "domain"
            key = Counter(domains).most_common(1)[0][0]
            org_id = f"web:{key}"
        elif phones:
            key_type = "phone"
            key = Counter(phones).most_common(1)[0][0]
            org_id = f"tel:{key}"
        else:
            key_type = "name"
            key = Counter(item["name_core"] for item in items).most_common(1)[0][0]
            org_id = f"org:{hashlib.sha1(key.encode()).hexdigest()[:16]}"

        if org_id in used_ids:
            org_id = f"org:{hashlib.sha1((org_id + str(len(used_ids))).encode()).hexdigest()[:16]}"
        used_ids.add(org_id)

        if brands:
            display_name = Counter(brands).most_common(1)[0][0]
            brand_value = display_name
        else:
            core = Counter(item["name_core"] for item in items).most_common(1)[0][0]
            candidates = [
                item["name"] for item in items if item["name_core"] == core
            ]
            display_name = min(candidates, key=len) if candidates else key
            brand_value = None

        category = Counter(
            item["category"] for item in items if item["category"]
        ).most_common(1)
        phone = Counter(item["phone"] for item in items if item["phone"]).most_common(1)
        phone_norm = Counter(
            item["phone_norm"] for item in items if item["phone_norm"]
        ).most_common(1)
        domain = Counter(domains).most_common(1)
        qid = Counter(qids).most_common(1)

        lats = [item["lat"] for item in items if item["lat"] is not None]
        lons = [item["lon"] for item in items if item["lon"] is not None]
        max_score = cluster_score.get(root, 0.0)

        evidence = json.dumps({
            "cluster_size": len(members),
            "max_edge_score": round(max_score, 3),
            "key_type": key_type,
            "with_domain": len(domains),
            "with_phone": len(phones),
            "with_brand_qid": len(qids),
        })

        org_rows.append((
            org_id,
            display_name,
            re_mod.norm_text(display_name),
            re_mod.name_core(display_name),
            brand_value,
            qid[0][0] if qid else None,
            domain[0][0] if domain else None,
            phone[0][0] if phone else None,
            phone_norm[0][0] if phone_norm else None,
            category[0][0] if category else None,
            len(members),
            max((item["confidence"] for item in items), default=0.0),
            sum(lats) / len(lats) if lats else None,
            sum(lons) / len(lons) if lons else None,
            key_type,
            evidence,
        ))

        canonical = max(members, key=quality)
        for outlet_id in members:
            member_rows.append((
                outlet_id,
                org_id,
                "canonical" if outlet_id == canonical else "branch",
                max_score,
                json.dumps({
                    "cluster_size": len(members),
                    "max_edge_score": round(max_score, 3),
                }),
            ))

    con.execute("DELETE FROM organizations")
    con.execute("DELETE FROM org_members")
    con.executemany(
        "INSERT INTO organizations "
        "(" + ",".join(ORG_COLS) + ") VALUES ("
        + ",".join("CAST(? AS JSON)" if c == "evidence" else "?" for c in ORG_COLS)
        + ")",
        org_rows,
    )
    con.executemany(
        "INSERT INTO org_members "
        "(outlet_id, org_id, role, join_score, evidence) "
        "VALUES (?, ?, ?, ?, CAST(? AS JSON))",
        member_rows,
    )
    con.execute("UPDATE outlets SET org_id = NULL")
    con.execute(
        "UPDATE outlets SET org_id = m.org_id FROM org_members m "
        "WHERE outlets.outlet_id = m.outlet_id"
    )
    con.execute(
        """
        UPDATE outlets o SET org_id = x.org_id
        FROM (
            SELECT g.outlet_id, m.org_id
            FROM outlet_groups g
            JOIN org_members m ON m.outlet_id = g.canonical_outlet_id
        ) x
        WHERE o.outlet_id = x.outlet_id AND o.org_id IS NULL
        """
    )

    decisions = con.execute(
        "SELECT count(*) FROM review_decisions"
    ).fetchone()[0]
    if decisions:
        applied = apply_decisions(con)
        log.info("Re-applied %d review decisions", applied)

    con.execute(
        """
        UPDATE organizations o SET
            wikidata_label = e.wikidata_label,
            wikidata_description = e.wikidata_description,
            official_website = e.official_website,
            parent_org_qid = e.parent_org_qid,
            country_qid = e.country_qid,
            stock_exchange_qid = e.stock_exchange_qid,
            wikidata_json = e.wikidata_json,
            web_legal_name = e.web_legal_name,
            web_email = e.web_email,
            web_phone = e.web_phone,
            web_address = e.web_address,
            web_description = e.web_description,
            web_same_as = e.web_same_as,
            web_status = e.web_status,
            web_fetched_at = e.web_fetched_at,
            web_contact_url = e.web_contact_url
        FROM org_enrichment e
        WHERE o.org_id = e.org_id
        """
    )

    return {
        "outlets": len(meta),
        "organizations": len(clusters),
        "multi_outlet_orgs": sum(1 for members in clusters.values() if len(members) > 1),
        "edges": con.execute("SELECT count(*) FROM org_edges").fetchone()[0],
        "auto_edges": len(auto_edges),
    }


# --------------------------------------------------------------------------
# Human review overrides
# --------------------------------------------------------------------------
def refresh_orgs(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        "DELETE FROM organizations WHERE org_id NOT IN "
        "(SELECT DISTINCT org_id FROM org_members)"
    )
    con.execute(
        """
        UPDATE organizations o SET
            outlet_count = x.n,
            lat = x.lat,
            lon = x.lon,
            confidence = x.conf,
            phone = x.phone,
            phone_norm = x.phone_norm,
            domain = x.domain,
            category = x.category
        FROM (
            SELECT m.org_id, count(*) AS n,
                   avg(o.lat) AS lat, avg(o.lon) AS lon,
                   max(o.confidence) AS conf,
                   max(o.phone) AS phone,
                   max(o.phone_norm) AS phone_norm,
                   max(o.domain) AS domain,
                   max(o.category) AS category
            FROM org_members m
            JOIN outlets o ON o.outlet_id = m.outlet_id
            GROUP BY 1
        ) x
        WHERE o.org_id = x.org_id
        """
    )
    con.execute("UPDATE outlets SET org_id = NULL")
    con.execute(
        "UPDATE outlets SET org_id = m.org_id FROM org_members m "
        "WHERE outlets.outlet_id = m.outlet_id"
    )
    con.execute(
        """
        UPDATE outlets o SET org_id = x.org_id
        FROM (
            SELECT g.outlet_id, m.org_id
            FROM outlet_groups g
            JOIN org_members m ON m.outlet_id = g.canonical_outlet_id
        ) x
        WHERE o.outlet_id = x.outlet_id AND o.org_id IS NULL
        """
    )


def validate_decisions(con: duckdb.DuckDBPyConnection) -> list[str]:
    warnings: list[str] = []
    merges = con.execute(
        "SELECT org_a, org_b FROM review_decisions "
        "WHERE action = 'merge' AND org_a IS NOT NULL AND org_b IS NOT NULL"
    ).fetchall()
    target: dict[str, str] = {}
    for org_a, org_b in merges:
        if org_a in target and target[org_a] != org_b:
            warnings.append(
                f"{org_a} is merged into both {target[org_a]} and {org_b}; "
                "file order decides"
            )
        target[org_a] = org_b

    for start in target:
        seen: set[str] = set()
        current = start
        while current in target:
            if current in seen:
                warnings.append(f"merge cycle detected around {current}")
                break
            seen.add(current)
            current = target[current]

    assigned: dict[str, str] = {}
    for outlet_id, target_org in con.execute(
        "SELECT outlet_id, target_org FROM review_decisions "
        "WHERE action = 'assign' AND outlet_id IS NOT NULL "
        "AND target_org IS NOT NULL"
    ).fetchall():
        if outlet_id in assigned and assigned[outlet_id] != target_org:
            warnings.append(
                f"outlet {outlet_id} is assigned to both "
                f"{assigned[outlet_id]} and {target_org}; file order decides"
            )
        assigned[outlet_id] = target_org
    return warnings


def apply_decisions(con: duckdb.DuckDBPyConnection) -> int:
    for warning in validate_decisions(con):
        log.warning("review: %s", warning)
    rows = con.execute(
        "SELECT action, org_a, org_b, outlet_id, evidence_id, target_org "
        "FROM review_decisions ORDER BY created_at, decision_id"
    ).fetchall()
    applied = 0
    for action, org_a, org_b, outlet_id, evidence_id, target_org in rows:
        action = re_mod.fold(action)
        if action == "merge":
            if not org_a or not org_b or org_a == org_b:
                continue
            exists = con.execute(
                "SELECT count(*) FROM organizations WHERE org_id IN (?, ?)",
                [org_a, org_b],
            ).fetchone()[0]
            if exists < 2:
                continue
            con.execute(
                "UPDATE org_members SET org_id = ? WHERE org_id = ?",
                [org_a, org_b],
            )
            con.execute("DELETE FROM organizations WHERE org_id = ?", [org_b])
            applied += 1
        elif action == "assign":
            if not outlet_id or not target_org:
                continue
            exists = con.execute(
                "SELECT 1 FROM organizations WHERE org_id = ?", [target_org]
            ).fetchone()
            if not exists:
                outlet = con.execute(
                    "SELECT name, confidence, lat, lon FROM outlets "
                    "WHERE outlet_id = ?", [outlet_id],
                ).fetchone()
                if not outlet:
                    continue
                name, confidence, lat, lon = outlet
                con.execute(
                    "INSERT INTO organizations "
                    "(org_id, display_name, name_norm, name_core, outlet_count, "
                    " confidence, lat, lon, key_type, evidence) "
                    "VALUES (?, ?, ?, ?, 0, ?, ?, ?, 'manual', CAST(? AS JSON))",
                    [target_org, name, re_mod.norm_text(name),
                     re_mod.name_core(name), confidence, lat, lon,
                     json.dumps({"method": "manual_assign"})],
                )
            con.execute(
                "DELETE FROM org_members WHERE outlet_id = ?", [outlet_id]
            )
            con.execute(
                "INSERT OR REPLACE INTO org_members "
                "(outlet_id, org_id, role, join_score, evidence) "
                "VALUES (?, ?, 'manual', 1.0, CAST(? AS JSON))",
                [outlet_id, target_org,
                 json.dumps({"method": "manual_assign"})],
            )
            applied += 1
        elif action == "exclude":
            if not outlet_id:
                continue
            con.execute(
                "DELETE FROM org_members WHERE outlet_id = ?", [outlet_id]
            )
            applied += 1
        elif action in {"confirm_evidence", "reject_evidence"}:
            if not evidence_id:
                continue
            decision = "auto" if action == "confirm_evidence" else "reject"
            con.execute(
                "UPDATE org_evidence SET decision = ? WHERE evidence_id = ?",
                [decision, evidence_id],
            )
            applied += 1

    if applied:
        refresh_orgs(con)
    return applied


def cmd_load_review(args: argparse.Namespace) -> int:
    con = connect(args.db)
    columns, rows = re_mod.read_rows(args.input)
    overrides = (
        re_mod.load_map_file(args.column_map_file)
        if args.column_map_file
        else re_mod.parse_json_arg(args.column_map, "--column-map")
    )
    try:
        mapping = re_mod.build_mapping(columns, REVIEW_ALIASES, overrides)
    except SystemExit as exc:
        log.error("%s", exc)
        return 1
    if "action" not in mapping:
        log.error("Review file needs an action column. Detected: %s", columns)
        return 1

    loaded = 0
    skipped = 0
    for row in rows:
        raw_action = re_mod.fold(re_mod.pick(row, mapping, "action"))
        if raw_action in {"merge", "merge_orgs", "mergeorgs"}:
            action = "merge"
        elif raw_action in {"assign", "move", "reassign"}:
            action = "assign"
        elif raw_action in {"exclude", "remove", "drop"}:
            action = "exclude"
        elif raw_action in {"confirm_evidence", "confirm", "approve"}:
            action = "confirm_evidence"
        elif raw_action in {"reject_evidence", "reject", "deny"}:
            action = "reject_evidence"
        else:
            skipped += 1
            continue

        org_a = str(re_mod.pick(row, mapping, "org_a") or "").strip() or None
        org_b = str(re_mod.pick(row, mapping, "org_b") or "").strip() or None
        outlet_id = str(
            re_mod.pick(row, mapping, "outlet_id") or ""
        ).strip() or None
        evidence_id = str(
            re_mod.pick(row, mapping, "evidence_id") or ""
        ).strip() or None
        target_org = str(
            re_mod.pick(row, mapping, "target_org") or ""
        ).strip() or None
        note = str(re_mod.pick(row, mapping, "note") or "").strip() or None

        if action == "merge" and not (org_a and org_b):
            skipped += 1
            continue
        if action == "assign" and not (outlet_id and target_org):
            skipped += 1
            continue
        if action == "exclude" and not outlet_id:
            skipped += 1
            continue
        if action in {"confirm_evidence", "reject_evidence"} and not evidence_id:
            skipped += 1
            continue

        digest = hashlib.sha1(
            "|".join([
                action, org_a or "", org_b or "", outlet_id or "",
                evidence_id or "", target_org or "",
            ]).encode("utf-8")
        ).hexdigest()[:20]
        con.execute(
            "INSERT OR REPLACE INTO review_decisions "
            "(decision_id, action, org_a, org_b, outlet_id, evidence_id, "
            " target_org, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [f"rev:{digest}", action, org_a, org_b, outlet_id, evidence_id,
             target_org, note],
        )
        loaded += 1

    if skipped:
        log.warning("Skipped %d invalid review rows", skipped)
    applied = apply_decisions(con)
    log.info("Loaded %d decisions, applied %d", loaded, applied)
    cmd_report_inner(con)
    return 0


def cmd_apply_review(args: argparse.Namespace) -> int:
    con = connect(args.db)
    applied = apply_decisions(con)
    log.info("Applied %d review decisions", applied)
    cmd_report_inner(con)
    return 0


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------
def cmd_derive(args: argparse.Namespace) -> int:
    con = connect(args.db)
    stats = run_derive(
        con,
        threshold=args.threshold,
        review=args.review,
        canonical_only=not args.include_duplicates,
    )
    log.info(
        "Derived %s organizations from %s outlets (%s multi-outlet, %s auto edges)",
        f"{stats['organizations']:,}", f"{stats['outlets']:,}",
        f"{stats['multi_outlet_orgs']:,}", f"{stats['auto_edges']:,}",
    )
    cmd_report_inner(con)
    return 0


def cmd_report_inner(con: duckdb.DuckDBPyConnection) -> None:
    outlets = con.execute("SELECT count(*) FROM outlets").fetchone()[0]
    orgs = con.execute("SELECT count(*) FROM organizations").fetchone()[0]
    multi = con.execute(
        "SELECT count(*) FROM organizations WHERE outlet_count > 1"
    ).fetchone()[0]
    in_multi = con.execute(
        "SELECT coalesce(sum(outlet_count), 0) FROM organizations WHERE outlet_count > 1"
    ).fetchone()[0]
    review = con.execute(
        "SELECT count(*) FROM org_edges WHERE decision = 'review'"
    ).fetchone()[0]
    unmatched = con.execute(
        "SELECT count(*) FROM outlets WHERE org_id IS NULL"
    ).fetchone()[0]

    print(f"Outlets:              {outlets:,}")
    print(f"Organizations:        {orgs:,}")
    print(f"Multi-outlet orgs:    {multi:,}")
    print(f"Outlets in those:     {in_multi:,}")
    print(f"Review edges:         {review:,}")
    print(f"Outlets without org:  {unmatched:,}")
    decisions = con.execute(
        "SELECT count(*) FROM review_decisions"
    ).fetchone()[0]
    if decisions:
        print(f"Review decisions:     {decisions:,}")
    web = con.execute(
        "SELECT count(*) FROM organizations WHERE web_fetched_at IS NOT NULL"
    ).fetchone()[0]
    if web:
        print(f"Web-enriched orgs:    {web:,}")

    evidence_total = con.execute(
        "SELECT count(*) FROM external_records"
    ).fetchone()[0]
    if evidence_total:
        evidence_auto = con.execute(
            "SELECT count(*) FROM org_evidence WHERE decision = 'auto'"
        ).fetchone()[0]
        evidence_review = con.execute(
            "SELECT count(*) FROM org_evidence WHERE decision = 'review'"
        ).fetchone()[0]
        print(f"External evidence:    {evidence_total:,} "
              f"({evidence_auto:,} auto, {evidence_review:,} review)")

    rows = con.execute(
        """
        SELECT display_name, outlet_count, category, key_type
        FROM organizations
        ORDER BY outlet_count DESC, display_name
        LIMIT 15
        """
    ).fetchall()
    if rows:
        print("\nLargest organizations")
        for name, count, category, key_type in rows:
            print(f"  {count:>5}  {name}  [{category or '-'}; {key_type}]")


def cmd_report(args: argparse.Namespace) -> int:
    con = connect(args.db)
    cmd_report_inner(con)
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    con = connect(args.db)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    formats = [f.strip().lower() for f in args.formats.split(",") if f.strip()]
    if not formats or set(formats) - {"parquet", "csv"}:
        log.error("--formats must contain parquet and/or csv")
        return 1

    queries = {
        "orgs": """
            SELECT * FROM organizations
            ORDER BY outlet_count DESC, display_name
        """,
        "members": """
            SELECT m.org_id, m.outlet_id, m.role, m.join_score,
                   o.name AS outlet_name, o.category, o.address, o.phone,
                   o.website, o.socials, o.lat, o.lon, o.confidence,
                   o.postcode, o.region
            FROM org_members m
            JOIN outlets o ON o.outlet_id = m.outlet_id
            ORDER BY m.org_id, o.name
        """,
        "review_edges": """
            SELECT * FROM org_edges WHERE decision = 'review'
            ORDER BY score DESC
        """,
        "evidence": """
            SELECT oe.evidence_id, oe.org_id, oe.score, oe.decision, oe.evidence,
                   e.source, e.name AS evidence_name, e.reg_no,
                   e.address AS evidence_address, e.phone AS evidence_phone,
                   e.website AS evidence_website, e.category AS evidence_category,
                   o.display_name AS org_name, o.domain AS org_domain,
                   o.phone AS org_phone
            FROM org_evidence oe
            JOIN external_records e ON e.evidence_id = oe.evidence_id
            JOIN organizations o ON o.org_id = oe.org_id
            WHERE oe.decision IN ('auto', 'review')
            ORDER BY oe.score DESC
        """,
        "decisions": """
            SELECT * FROM review_decisions ORDER BY created_at, decision_id
        """,
    }
    counts = {}
    for name, query in queries.items():
        target = f"{out}_{name}.parquet"
        con.execute(
            f"COPY ({query}) TO {re_mod.sql_str(target)} "
            "(FORMAT PARQUET, COMPRESSION ZSTD);"
        )
        counts[name] = con.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
        if "csv" in formats:
            con.execute(
                f"COPY ({query}) TO {re_mod.sql_str(f'{out}_{name}.csv')} "
                "(FORMAT CSV, HEADER, DELIMITER ',');"
            )
        log.info("Wrote %s (%s rows)", target, f"{counts[name]:,}")

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tool": "derive_orgs.py",
        "database": args.db,
        "counts": counts,
    }
    Path(f"{out}.meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return 0


# --------------------------------------------------------------------------
# External evidence (any public list, no SSM required)
# --------------------------------------------------------------------------
EVIDENCE_COLS = [
    "evidence_id", "source", "name", "name_norm", "name_core", "reg_no",
    "address", "address_norm", "postcode", "state", "phone", "phone_norm",
    "phone_suffix", "website", "domain", "category", "raw",
]


def cmd_load_evidence(args: argparse.Namespace) -> int:
    con = connect(args.db)
    columns, rows = re_mod.read_rows(args.input)
    overrides = (
        re_mod.load_map_file(args.column_map_file)
        if args.column_map_file
        else re_mod.parse_json_arg(args.column_map, "--column-map")
    )
    try:
        mapping = re_mod.build_mapping(columns, EVIDENCE_ALIASES, overrides)
    except SystemExit as exc:
        log.error("%s", exc)
        return 1
    if "name" not in mapping:
        log.error("Could not find a name column. Detected: %s", columns)
        return 1
    log.info("Evidence columns (%s): %s", args.source, mapping)

    if not args.append:
        con.execute("DELETE FROM external_records WHERE source = ?", [args.source])

    def prepared() -> Iterator[tuple[Any, ...]]:
        for row in rows:
            record = re_mod.normalise_entity(row, mapping, args.source)
            if record is None:
                continue
            category = str(
                re_mod.pick(row, mapping, "category") or ""
            ).strip() or None
            digest = hashlib.sha1(
                "|".join([
                    args.source, record["name"], record["reg_no"] or "",
                    record["address"] or "", record["phone_norm"] or "",
                ]).encode("utf-8")
            ).hexdigest()[:20]
            yield (
                f"{args.source}:{digest}",
                args.source,
                record["name"],
                record["name_norm"],
                record["name_core"],
                record["reg_no"],
                record["address"],
                record["address_norm"],
                record["postcode"],
                record["state"],
                record["phone"],
                record["phone_norm"],
                record["phone_suffix"],
                record["website"],
                record["domain"],
                category,
                record["raw"],
            )

    placeholders = ",".join(
        "CAST(? AS JSON)" if c == "raw" else "?" for c in EVIDENCE_COLS
    )
    buffer: list[tuple[Any, ...]] = []
    total = 0
    for item in prepared():
        buffer.append(item)
        if len(buffer) >= 2000:
            con.executemany(
                "INSERT OR REPLACE INTO external_records "
                f"({','.join(EVIDENCE_COLS)}) VALUES ({placeholders})",
                buffer,
            )
            total += len(buffer)
            buffer.clear()
    if buffer:
        con.executemany(
            "INSERT OR REPLACE INTO external_records "
            f"({','.join(EVIDENCE_COLS)}) VALUES ({placeholders})",
            buffer,
        )
        total += len(buffer)
    log.info("Loaded %d evidence records from %s", total, args.source)
    return 0


def apply_evidence_decisions(con: duckdb.DuckDBPyConnection) -> int:
    rows = con.execute(
        "SELECT action, evidence_id FROM review_decisions "
        "WHERE action IN ('confirm_evidence', 'reject_evidence') "
        "AND evidence_id IS NOT NULL"
    ).fetchall()
    applied = 0
    for action, evidence_id in rows:
        decision = "auto" if action == "confirm_evidence" else "reject"
        con.execute(
            "UPDATE org_evidence SET decision = ? WHERE evidence_id = ?",
            [decision, evidence_id],
        )
        applied += 1
    return applied


def run_match_evidence(
    con: duckdb.DuckDBPyConnection,
    *,
    threshold: float = 0.75,
    review: float = 0.55,
) -> dict[str, int]:
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE org_nodes AS
        SELECT o.org_id, o.display_name, o.name_core, o.domain, o.phone_norm,
               x.address_norm, x.postcode, x.state
        FROM organizations o
        LEFT JOIN org_members m
          ON m.org_id = o.org_id AND m.role = 'canonical'
        LEFT JOIN outlets x ON x.outlet_id = m.outlet_id
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE ev_pairs AS
        SELECT DISTINCT e.evidence_id, o.org_id
        FROM external_records e JOIN org_nodes o
          ON o.domain = e.domain
        WHERE coalesce(o.domain, '') <> ''

        UNION

        SELECT DISTINCT e.evidence_id, o.org_id
        FROM external_records e JOIN org_nodes o
          ON o.phone_norm = e.phone_norm
        WHERE coalesce(o.phone_norm, '') <> ''

        UNION

        SELECT DISTINCT e.evidence_id, o.org_id
        FROM external_records e JOIN org_nodes o
          ON o.name_core = e.name_core
        WHERE length(o.name_core) >= 3

        UNION

        SELECT DISTINCT e.evidence_id, o.org_id
        FROM external_records e JOIN org_nodes o
          ON left(o.name_core, 8) = left(e.name_core, 8)
         AND o.postcode = e.postcode
        WHERE length(o.name_core) >= 8 AND coalesce(e.postcode, '') <> ''
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE org_evidence AS
        WITH features AS (
            SELECT
                p.evidence_id,
                p.org_id,
                jaro_winkler_similarity(o.name_core, e.name_core) AS jw,
                (coalesce(o.domain, '') <> ''
                    AND coalesce(o.domain, '') = coalesce(e.domain, ''))
                    AS domain_equal,
                (coalesce(o.phone_norm, '') <> ''
                    AND coalesce(o.phone_norm, '') = coalesce(e.phone_norm, ''))
                    AS phone_equal,
                (coalesce(o.postcode, '') <> ''
                    AND coalesce(o.postcode, '') = coalesce(e.postcode, ''))
                    AS postcode_equal,
                (coalesce(o.state, '') <> ''
                    AND coalesce(o.state, '') = coalesce(e.state, ''))
                    AS state_equal,
                jaro_winkler_similarity(
                    coalesce(o.address_norm, ''), coalesce(e.address_norm, '')
                ) AS addr_jw,
                (coalesce(o.domain, '') <> '') AS o_has_domain,
                (coalesce(e.domain, '') <> '') AS e_has_domain,
                (coalesce(o.phone_norm, '') <> '') AS o_has_phone,
                (coalesce(e.phone_norm, '') <> '') AS e_has_phone
            FROM ev_pairs p
            JOIN org_nodes o ON o.org_id = p.org_id
            JOIN external_records e ON e.evidence_id = p.evidence_id
        ),
        scored AS (
            SELECT *,
                least(1.0::DOUBLE, greatest(0.0::DOUBLE,
                    CASE WHEN domain_equal THEN 0.45 ELSE 0 END
                    + CASE WHEN phone_equal THEN 0.40 ELSE 0 END
                    + CASE
                        WHEN jw >= 0.95 THEN 0.35
                        WHEN jw >= 0.85 THEN 0.25
                        WHEN jw >= 0.70 THEN 0.12
                        ELSE 0
                    END
                    + CASE WHEN postcode_equal THEN 0.10 ELSE 0 END
                    + CASE WHEN state_equal THEN 0.05 ELSE 0 END
                    + CASE
                        WHEN addr_jw >= 0.80 THEN 0.10
                        WHEN addr_jw >= 0.60 THEN 0.05
                        ELSE 0
                    END
                    - CASE
                        WHEN o_has_domain AND e_has_domain AND NOT domain_equal
                        THEN 0.15 ELSE 0
                    END
                    - CASE
                        WHEN o_has_phone AND e_has_phone AND NOT phone_equal
                        THEN 0.10 ELSE 0
                    END
                )) AS score
            FROM features
        ),
        ranked AS (
            SELECT *,
                row_number() OVER (
                    PARTITION BY evidence_id ORDER BY score DESC, org_id
                ) AS match_rank
            FROM scored
        )
        SELECT
            evidence_id,
            org_id,
            score,
            CASE WHEN score >= {threshold} THEN 'auto'
                 WHEN score >= {review} THEN 'review'
                 ELSE 'reject' END AS decision,
            json_object(
                'jaro_winkler', jw,
                'domain_equal', domain_equal,
                'phone_equal', phone_equal,
                'postcode_equal', postcode_equal,
                'state_equal', state_equal,
                'address_jw', addr_jw
            ) AS evidence
        FROM ranked
        WHERE match_rank = 1
        """
    )
    total = con.execute("SELECT count(*) FROM org_evidence").fetchone()[0]
    applied = apply_evidence_decisions(con)
    if applied:
        log.info("Re-applied %d evidence decisions", applied)
    auto = con.execute(
        "SELECT count(*) FROM org_evidence WHERE decision = 'auto'"
    ).fetchone()[0]
    review = con.execute(
        "SELECT count(*) FROM org_evidence WHERE decision = 'review'"
    ).fetchone()[0]
    return {"matched": int(total), "auto": int(auto), "review": int(review)}


def cmd_match_evidence(args: argparse.Namespace) -> int:
    con = connect(args.db)
    stats = run_match_evidence(
        con, threshold=args.threshold, review=args.review
    )
    print(f"Evidence matched: {stats['matched']:,}")
    print(f"  auto:           {stats['auto']:,}")
    print(f"  review:         {stats['review']:,}")
    return 0


# --------------------------------------------------------------------------
# Website enrichment (one homepage per organization, robots-aware)
# --------------------------------------------------------------------------
ORG_TYPES = {
    "organization", "localbusiness", "corporation", "store", "restaurant",
    "foodestablishment", "professionalservice", "medicalbusiness", "hotel",
    "lodgingbusiness", "automotivebusiness", "healthandbeautybusiness",
}


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = html_mod.unescape(str(value))
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _walk_jsonld(obj: Any) -> Iterator[dict[str, Any]]:
    if isinstance(obj, list):
        for item in obj:
            yield from _walk_jsonld(item)
    elif isinstance(obj, dict):
        yield obj
        if "@graph" in obj:
            yield from _walk_jsonld(obj["@graph"])


def parse_web_identity(html: str, domain: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "legal_name": None, "email": None, "phone": None,
        "address": None, "description": None, "same_as": None,
        "source": "none",
    }

    for match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.S | re.I,
    ):
        raw = html_mod.unescape(match.group(1).strip())
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for obj in _walk_jsonld(data):
            types = obj.get("@type")
            if isinstance(types, str):
                types = [types]
            if not isinstance(types, list):
                continue
            if not any(str(t).lower() in ORG_TYPES for t in types):
                continue

            name = _clean(obj.get("legalName") or obj.get("name"))
            email = _clean(obj.get("email"))
            phone = _clean(obj.get("telephone"))
            address = obj.get("address")
            if isinstance(address, dict):
                parts = [
                    address.get("streetAddress"),
                    address.get("addressLocality"),
                    address.get("addressRegion"),
                    address.get("postalCode"),
                    address.get("addressCountry"),
                ]
                address = ", ".join(str(p) for p in parts if p)
            address = _clean(address)
            same = obj.get("sameAs")
            if isinstance(same, str):
                same = [same]
            same_as = " | ".join(str(s) for s in same) if isinstance(same, list) else None

            if name or email or phone or address:
                result.update({
                    "legal_name": name,
                    "email": email,
                    "phone": phone,
                    "address": address,
                    "description": _clean(obj.get("description")),
                    "same_as": same_as,
                    "source": "jsonld",
                })
                return result

    meta: dict[str, str] = {}
    for tag in re.findall(r"<meta\b[^>]*>", html, re.I):
        attrs: dict[str, str] = {}
        for key, value in re.findall(
            r'([\w:-]+)\s*=\s*["\']([^"\']*)["\']', tag
        ):
            attrs[key.lower()] = value
        prop = attrs.get("property") or attrs.get("name")
        if prop:
            meta[prop.lower()] = attrs.get("content", "")

    emails = re.findall(r'mailto:([^"\'>\s?]+)', html, re.I)
    phones = re.findall(r'tel:([^"\'>\s]+)', html, re.I)

    result.update({
        "legal_name": _clean(meta.get("og:site_name")),
        "email": _clean(emails[0]) if emails else None,
        "phone": _clean(phones[0]) if phones else None,
        "description": _clean(
            meta.get("description") or meta.get("og:description")
        ),
    })
    if any(result[k] for k in ("legal_name", "email", "phone", "description")):
        result["source"] = "meta"
    return result


def find_contact_url(html: str, base_url: str) -> str | None:
    host = urlparse(base_url).hostname
    for href in re.findall(r'href=["\']([^"\']+)["\']', html, re.I):
        lowered = href.lower()
        if not any(k in lowered for k in ("contact", "hubungi", "about", "mengenai")):
            continue
        absolute = urljoin(base_url, href)
        if urlparse(absolute).hostname == host:
            return absolute
    return None


def robots_allowed(session: requests.Session, url: str, user_agent: str) -> bool:
    parts = urlparse(url)
    robots = f"{parts.scheme}://{parts.netloc}/robots.txt"
    parser = RobotFileParser()
    parser.set_url(robots)
    try:
        resp = session.get(robots, timeout=15)
        if resp.status_code == 200:
            parser.parse(resp.text.splitlines())
            return parser.can_fetch(user_agent, url)
        return True
    except requests.RequestException:
        return True


def cmd_enrich_web(args: argparse.Namespace) -> int:
    con = connect(args.db)
    where = "web_fetched_at IS NULL"
    if not args.retry_errors:
        where += " AND coalesce(web_status, '') NOT IN ('robots_disallow', 'error')"
    rows = con.execute(
        f"SELECT org_id, display_name, domain FROM organizations "
        f"WHERE {where} AND domain IS NOT NULL "
        f"ORDER BY outlet_count DESC LIMIT ?",
        [args.limit],
    ).fetchall()
    if not rows:
        log.info("No organizations need website enrichment.")
        return 0

    user_agent = f"YP-OrgGraph/1.0 (research; contact: {args.email})"
    session = requests.Session()
    session.headers.update({
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-MY,en;q=0.9",
    })
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    enriched = 0
    for org_id, name, domain in rows:
        url = f"https://{domain}"
        if not robots_allowed(session, url, user_agent):
            con.execute(
                "UPDATE organizations SET web_status = 'robots_disallow', "
                "web_fetched_at = current_timestamp WHERE org_id = ?",
                [org_id],
            )
            log.info("robots.txt disallows %s; skipped", domain)
            time.sleep(args.delay)
            continue

        safe = re.sub(r"[^a-z0-9.-]+", "_", domain.lower())
        cached = cache_dir / f"{safe}.html"
        try:
            if cached.exists():
                body = cached.read_text(encoding="utf-8", errors="ignore")
            else:
                resp = session.get(url, timeout=25)
                if resp.status_code >= 400:
                    resp = session.get(f"http://{domain}", timeout=25)
                resp.raise_for_status()
                body = resp.text[: args.max_bytes]
                cached.write_text(body, encoding="utf-8")
        except requests.RequestException as exc:
            con.execute(
                "UPDATE organizations SET web_status = 'error' WHERE org_id = ?",
                [org_id],
            )
            log.warning("Web fetch failed for %s: %s", domain, exc)
            time.sleep(args.delay)
            continue

        identity = parse_web_identity(body, domain)
        contact_url = None
        if args.follow_contact and (
            not identity["email"] or not identity["phone"]
        ):
            contact_url = find_contact_url(body, url)
            if contact_url and robots_allowed(session, contact_url, user_agent):
                try:
                    contact_cached = cache_dir / f"{safe}_contact.html"
                    if contact_cached.exists():
                        contact_body = contact_cached.read_text(
                            encoding="utf-8", errors="ignore"
                        )
                    else:
                        contact_resp = session.get(contact_url, timeout=25)
                        contact_resp.raise_for_status()
                        contact_body = contact_resp.text[: args.max_bytes]
                        contact_cached.write_text(contact_body, encoding="utf-8")
                    extra = parse_web_identity(contact_body, domain)
                    for key in (
                        "legal_name", "email", "phone", "address", "description"
                    ):
                        if not identity.get(key) and extra.get(key):
                            identity[key] = extra[key]
                    if not identity.get("same_as") and extra.get("same_as"):
                        identity["same_as"] = extra["same_as"]
                    if extra.get("source") not in (None, "none"):
                        identity["source"] = f"{identity['source']}+contact"
                    time.sleep(args.delay)
                except requests.RequestException as exc:
                    log.debug("Contact page failed for %s: %s", domain, exc)

        con.execute(
            """
            UPDATE organizations SET
                web_legal_name = ?,
                web_email = ?,
                web_phone = ?,
                web_address = ?,
                web_description = ?,
                web_same_as = ?,
                web_contact_url = ?,
                web_status = ?,
                web_fetched_at = current_timestamp
            WHERE org_id = ?
            """,
            [
                identity["legal_name"], identity["email"], identity["phone"],
                identity["address"], identity["description"],
                identity["same_as"], contact_url,
                f"ok:{identity['source']}", org_id,
            ],
        )
        enriched += 1
        log.info("Web-enriched %s (%s)", name, identity["source"])
        time.sleep(args.delay)

    log.info("Enriched %d organization(s) from their websites", enriched)
    return 0


# --------------------------------------------------------------------------
# Wikidata enrichment
# --------------------------------------------------------------------------
WIKIDATA_URL = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"


def _claim_value(entity: dict[str, Any], prop: str) -> str | None:
    claims = entity.get("claims", {}).get(prop) or []
    if not claims:
        return None
    snak = claims[0].get("mainsnak", {}).get("datavalue")
    if not isinstance(snak, dict):
        return None
    value = snak.get("value")
    if isinstance(value, dict):
        return value.get("id") or value.get("time") or value.get("text")
    return value


def fetch_wikidata(qid: str, cache_dir: Path, session: requests.Session) -> dict[str, Any]:
    cache = cache_dir / f"{qid}.json"
    if cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))
    resp = session.get(WIKIDATA_URL.format(qid=qid), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return data


def cmd_enrich_wikidata(args: argparse.Namespace) -> int:
    con = connect(args.db)
    rows = con.execute(
        """
        SELECT DISTINCT brand_wikidata FROM organizations
        WHERE brand_wikidata IS NOT NULL AND wikidata_label IS NULL
        ORDER BY brand_wikidata
        LIMIT ?
        """,
        [args.limit],
    ).fetchall()
    if not rows:
        log.info("No organizations need Wikidata enrichment.")
        return 0

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "YP-OrgGraph/1.0 (research; "
            f"contact: {args.email})"
        ),
        "Accept": "application/json",
    })
    cache_dir = Path(args.cache_dir) / "wikidata"
    enriched = 0
    for (qid,) in rows:
        try:
            data = fetch_wikidata(qid, cache_dir, session)
            entity = data.get("entities", {}).get(qid, {})
            label = (entity.get("labels", {}).get("en") or {}).get("value")
            description = (entity.get("descriptions", {}).get("en") or {}).get("value")
            website = _claim_value(entity, "P856")
            parent = _claim_value(entity, "P749")
            country = _claim_value(entity, "P17")
            exchange = _claim_value(entity, "P414")
            con.execute(
                """
                UPDATE organizations SET
                    wikidata_label = ?,
                    wikidata_description = ?,
                    official_website = ?,
                    parent_org_qid = ?,
                    country_qid = ?,
                    stock_exchange_qid = ?,
                    wikidata_json = CAST(? AS JSON)
                WHERE brand_wikidata = ?
                """,
                [label, description, website, parent, country, exchange,
                 json.dumps(entity, ensure_ascii=False), qid],
            )
            enriched += 1
            log.info("Enriched %s: %s", qid, label or "(no label)")
        except (requests.RequestException, ValueError) as exc:
            log.warning("Wikidata %s failed: %s", qid, exc)
        time.sleep(args.delay)
    log.info("Enriched %d QID(s)", enriched)
    return 0


# --------------------------------------------------------------------------
# Self-test
# --------------------------------------------------------------------------
def cmd_self_test(_args: argparse.Namespace) -> int:
    con = duckdb.connect(":memory:")
    re_mod.init_db(con, force=True)
    con.execute(ORG_DDL)

    outlets = [
        {"gers_id": "a1", "name": "Starbucks KLCC", "brand": "Starbucks",
         "brand_wikidata": "Q37158", "domain": "starbucks.com.my",
         "phone": "03-1111 1111", "address": "KLCC", "postcode": "50088",
         "region": "Kuala Lumpur", "category": "cafe", "lat": 3.15, "lon": 101.71},
        {"gers_id": "a2", "name": "Starbucks Mid Valley", "brand": "Starbucks",
         "brand_wikidata": "Q37158", "domain": "starbucks.com.my",
         "phone": "03-2222 2222", "address": "Mid Valley", "postcode": "59200",
         "region": "Kuala Lumpur", "category": "cafe", "lat": 3.11, "lon": 101.67},
        {"gers_id": "b1", "name": "Sedap Restaurant", "phone": "03-3333 3333",
         "address": "12 Jalan Makan", "postcode": "50450",
         "region": "Kuala Lumpur", "category": "restaurant", "lat": 3.16, "lon": 101.70},
        {"gers_id": "b2", "name": "Sedap Restaurant Branch", "phone": "03-3333 3333",
         "address": "12 Jalan Makan", "postcode": "50450",
         "region": "Kuala Lumpur", "category": "restaurant", "lat": 3.16, "lon": 101.70},
        {"gers_id": "c1", "name": "Kedai Bunga Mawar", "region": "Kuala Lumpur",
         "category": "florist"},
    ]
    re_mod.insert_rows(
        con, "outlets", re_mod.OUTLET_COLS,
        (re_mod.normalise_outlet(r, {k: k for k in r}) for r in outlets),
    )
    stats = run_derive(con, threshold=0.72)
    orgs = con.execute(
        "SELECT display_name, outlet_count, key_type FROM organizations "
        "ORDER BY outlet_count DESC"
    ).fetchall()
    print("\nSelf-test organizations")
    for name, count, key_type in orgs:
        print(f"  {count}  {name}  [{key_type}]")

    counts = {row[0]: row[1] for row in orgs}
    checks = [
        ("Starbucks merged", counts.get("Starbucks") == 2),
        ("Sedap merged", counts.get("Sedap Restaurant") == 2),
        ("Singleton retained", len(orgs) == 3),
        ("Wikidata key used", any(row[2] == "brand_wikidata" for row in orgs)),
    ]

    starbucks_id = con.execute(
        "SELECT org_id FROM organizations WHERE display_name = 'Starbucks'"
    ).fetchone()[0]
    sedap_id = con.execute(
        "SELECT org_id FROM organizations WHERE display_name = 'Sedap Restaurant'"
    ).fetchone()[0]
    con.execute(
        "INSERT INTO review_decisions (decision_id, action, org_a, org_b) "
        "VALUES ('rev:selftest', 'merge', ?, ?)",
        [starbucks_id, sedap_id],
    )
    applied = apply_decisions(con)
    merged_count = con.execute(
        "SELECT outlet_count FROM organizations WHERE org_id = ?",
        [starbucks_id],
    ).fetchone()[0]
    run_derive(con, threshold=0.72)
    after_rederive = con.execute(
        "SELECT count(*) FROM organizations"
    ).fetchone()[0]
    checks.extend([
        ("review merge applied", applied == 1 and merged_count == 4),
        ("review merge survived re-derive", after_rederive == 2),
    ])

    fixture = (
        '<script type="application/ld+json">'
        '{"@context":"https://schema.org","@type":"LocalBusiness",'
        '"name":"Acme Cafe","telephone":"+60312345678",'
        '"email":"hi@acme.my","address":{"streetAddress":"1 Jalan Test",'
        '"addressLocality":"Kuala Lumpur","postalCode":"50000"},'
        '"sameAs":["https://facebook.com/acmecafe"]}'
        '</script>'
    )
    identity = parse_web_identity(fixture, "acme.my")
    checks.append((
        "web JSON-LD parser",
        identity["legal_name"] == "Acme Cafe"
        and identity["email"] == "hi@acme.my"
        and identity["phone"] == "+60312345678",
    ))
    contact = find_contact_url(
        '<a href="/contact-us">Contact</a>', "https://acme.my/"
    )
    checks.append((
        "contact link parser", contact == "https://acme.my/contact-us"
    ))

    failed = [label for label, ok in checks if not ok]
    print(f"\nOrganizations: {stats['organizations']}, "
          f"multi-outlet: {stats['multi_outlet_orgs']}")
    if failed:
        print("FAILED: " + ", ".join(failed))
        return 1
    print("All self-test checks passed.")
    return 0


# --------------------------------------------------------------------------
# GraphML export
# --------------------------------------------------------------------------
def cmd_graph(args: argparse.Namespace) -> int:
    con = connect(args.db)
    orgs = con.execute(
        "SELECT org_id, display_name, category, outlet_count, lat, lon "
        "FROM organizations ORDER BY outlet_count DESC"
    ).fetchall()
    outlets = con.execute(
        "SELECT outlet_id, name, category, lat, lon, org_id FROM outlets "
        "WHERE org_id IS NOT NULL"
    ).fetchall()

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<graphml xmlns="http://graphml.graphdrawing.org/xmlns">',
        '  <key id="label" for="node" attr.name="label" attr.type="string"/>',
        '  <key id="kind" for="node" attr.name="kind" attr.type="string"/>',
        '  <key id="category" for="node" attr.name="category" attr.type="string"/>',
        '  <key id="outlet_count" for="node" attr.name="outlet_count" attr.type="int"/>',
        '  <key id="lat" for="node" attr.name="lat" attr.type="double"/>',
        '  <key id="lon" for="node" attr.name="lon" attr.type="double"/>',
        '  <key id="ref" for="node" attr.name="ref" attr.type="string"/>',
        '  <graph id="orgs" edgedefault="undirected">',
    ]

    org_node: dict[str, str] = {}
    for index, (org_id, name, category, count, lat, lon) in enumerate(orgs):
        node_id = f"o{index}"
        org_node[org_id] = node_id
        lines.append(f'    <node id="{node_id}">')
        lines.append(f'      <data key="label">{html_mod.escape(str(name or ""))}</data>')
        lines.append('      <data key="kind">organization</data>')
        lines.append(f'      <data key="category">{html_mod.escape(str(category or ""))}</data>')
        lines.append(f'      <data key="outlet_count">{int(count or 0)}</data>')
        lines.append(f'      <data key="ref">{html_mod.escape(org_id)}</data>')
        if lat is not None:
            lines.append(f'      <data key="lat">{float(lat)}</data>')
        if lon is not None:
            lines.append(f'      <data key="lon">{float(lon)}</data>')
        lines.append('    </node>')

    outlet_node: dict[str, str] = {}
    for index, (outlet_id, name, category, lat, lon, org_id) in enumerate(outlets):
        node_id = f"u{index}"
        outlet_node[outlet_id] = node_id
        lines.append(f'    <node id="{node_id}">')
        lines.append(f'      <data key="label">{html_mod.escape(str(name or ""))}</data>')
        lines.append('      <data key="kind">outlet</data>')
        lines.append(f'      <data key="category">{html_mod.escape(str(category or ""))}</data>')
        lines.append(f'      <data key="ref">{html_mod.escape(outlet_id)}</data>')
        if lat is not None:
            lines.append(f'      <data key="lat">{float(lat)}</data>')
        if lon is not None:
            lines.append(f'      <data key="lon">{float(lon)}</data>')
        lines.append('    </node>')

    edges = 0
    for outlet_id, _name, _category, _lat, _lon, org_id in outlets:
        source = outlet_node.get(outlet_id)
        target = org_node.get(org_id)
        if source and target:
            lines.append(
                f'    <edge source="{source}" target="{target}"/>'
            )
            edges += 1

    lines.extend(['  </graph>', '</graphml>'])
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    log.info(
        "Wrote %s (%d orgs, %d outlets, %d edges)",
        out, len(orgs), len(outlets), edges,
    )
    return 0


# --------------------------------------------------------------------------
# Static review page
# --------------------------------------------------------------------------
REVIEW_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Organization review</title>
<style>
  :root { color-scheme: light; }
  body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
         margin: 0; background: #f4f5f7; color: #1d232b; }
  header { position: sticky; top: 0; z-index: 2; background: #fff;
           border-bottom: 1px solid #dde1e6; padding: 16px 24px;
           display: flex; gap: 16px; align-items: center; flex-wrap: wrap; }
  h1 { font-size: 18px; margin: 0; }
  .muted { color: #66707a; font-size: 13px; }
  main { max-width: 1100px; margin: 0 auto; padding: 20px; }
  .card { background: #fff; border: 1px solid #dde1e6; border-radius: 10px;
          padding: 18px; margin-bottom: 14px; }
  .card.done { border-color: #2e7d32; background: #f2fbf3; }
  .card.kept { border-color: #9aa4ae; opacity: .65; }
  .score { display: inline-block; background: #eef2ff; color: #3049b7;
           border-radius: 999px; padding: 3px 10px; font-weight: 600;
           font-size: 13px; margin-bottom: 10px; }
  .pair { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
  .side { border: 1px solid #eef0f3; border-radius: 8px; padding: 12px; }
  .side h3 { margin: 0 0 6px; font-size: 16px; }
  .side p { margin: 3px 0; font-size: 13px; color: #4c5661; }
  .org { font-family: ui-monospace, Consolas, monospace; font-size: 12px;
         background: #f7f8fa; padding: 2px 6px; border-radius: 4px; }
  .signals { margin: 12px 0 0; font-size: 13px; color: #4c5661; }
  .actions { display: flex; gap: 10px; margin-top: 14px; flex-wrap: wrap; }
  button { border: 1px solid #c9d0d8; background: #fff; padding: 8px 14px;
           border-radius: 7px; cursor: pointer; font-size: 13px; }
  button.primary { background: #1f6feb; border-color: #1f6feb; color: #fff; }
  button.warn { color: #a12622; border-color: #e3b4b2; }
  #download { background: #1f6feb; color: #fff; border: 0; padding: 9px 16px;
              border-radius: 7px; cursor: pointer; }
  #count { font-weight: 600; }
  @media (max-width: 700px) { .pair { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<header>
  <h1>Organization review</h1>
  <span class="muted">__COUNT__ candidate pairs · generated __GENERATED__</span>
  <span class="muted">Decisions: <span id="count">0</span></span>
  <button id="download">Download decisions.csv</button>
</header>
<main id="app"></main>
<script>
const rows = __ROWS__;
const decisions = [];
const esc = v => {
  const s = String(v ?? '');
  return /[",\\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
};
const h = s => String(s ?? '').replace(/[&<>"']/g, c => (
  {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]
));
const keyOf = d => [d.action,d.org_a,d.org_b,d.outlet_id,d.target_org].join('|');
function add(d) {
  const k = keyOf(d);
  if (!decisions.some(x => keyOf(x) === k)) decisions.push(d);
  document.getElementById('count').textContent = decisions.length;
}
function decide(i, action) {
  const r = rows[i];
  const note = 'review score ' + r.score.toFixed(2);
  if (action === 'same') {
    if (r.a_org && r.b_org && r.a_org !== r.b_org) {
      add({action:'merge', org_a:r.a_org, org_b:r.b_org,
           outlet_id:'', target_org:'', note});
    } else if (r.a_org) {
      add({action:'assign', org_a:'', org_b:'', outlet_id:r.b_id,
           target_org:r.a_org, note});
    } else if (r.b_org) {
      add({action:'assign', org_a:'', org_b:'', outlet_id:r.a_id,
           target_org:r.b_org, note});
    } else {
      add({action:'assign', org_a:'', org_b:'', outlet_id:r.a_id,
           target_org:r.review_org, note});
      add({action:'assign', org_a:'', org_b:'', outlet_id:r.b_id,
           target_org:r.review_org, note});
    }
  } else if (action === 'exclude') {
    add({action:'exclude', org_a:'', org_b:'', outlet_id:r.b_id,
         target_org:'', note});
  }
  const card = document.getElementById('card-' + i);
  card.classList.remove('done', 'kept');
  card.classList.add(action === 'separate' ? 'kept' : 'done');
}
function download() {
  const cols = ['action','org_a','org_b','outlet_id','target_org','note'];
  const lines = [cols.join(',')].concat(
    decisions.map(d => cols.map(c => esc(d[c])).join(','))
  );
  const blob = new Blob([lines.join('\\n')], {type:'text/csv;charset=utf-8'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'decisions.csv';
  a.click();
  URL.revokeObjectURL(a.href);
}
document.getElementById('download').addEventListener('click', download);
const app = document.getElementById('app');
if (!rows.length) {
  app.innerHTML = '<p class="muted">No review edges. Lower the derive ' +
    'threshold or check org_edges.</p>';
}
rows.forEach((r, i) => {
  const signals = Object.entries(r.evidence)
    .filter(([, v]) => v === true).map(([k]) => k).join(', ') || '—';
  const el = document.createElement('section');
  el.className = 'card';
  el.id = 'card-' + i;
  el.innerHTML = `
    <div class="score">${r.score.toFixed(2)}</div>
    <div class="pair">
      <div class="side">
        <h3>${h(r.a_name)}</h3>
        <p>${h(r.a_category || 'no category')} · ${h(r.a_address || 'no address')}</p>
        <p>${h(r.a_phone || 'no phone')}</p>
        <p>org: <span class="org">${h(r.a_org || 'none')}</span></p>
      </div>
      <div class="side">
        <h3>${h(r.b_name)}</h3>
        <p>${h(r.b_category || 'no category')} · ${h(r.b_address || 'no address')}</p>
        <p>${h(r.b_phone || 'no phone')}</p>
        <p>org: <span class="org">${h(r.b_org || 'none')}</span></p>
      </div>
    </div>
    <div class="signals">Signals: ${h(signals)} · name ${r.evidence.jaro_winkler?.toFixed?.(2) ?? '—'}
      · distance ${r.distance_m == null ? '—' : Math.round(r.distance_m) + ' m'}</div>
    <div class="actions">
      <button class="primary" onclick="decide(${i}, 'same')">Same organization</button>
      <button onclick="decide(${i}, 'separate')">Keep separate</button>
      <button class="warn" onclick="decide(${i}, 'exclude')">Exclude B</button>
    </div>`;
  app.appendChild(el);
});
</script>
</body>
</html>
"""


def cmd_review_html(args: argparse.Namespace) -> int:
    con = connect(args.db)
    rows = con.execute(
        """
        SELECT e.a_id, e.b_id, e.score, e.evidence,
               a.name, a.category, a.address, a.phone, a.org_id,
               b.name, b.category, b.address, b.phone, b.org_id,
               e.distance_m
        FROM org_edges e
        JOIN outlets a ON a.outlet_id = e.a_id
        JOIN outlets b ON b.outlet_id = e.b_id
        WHERE e.decision = 'review'
        ORDER BY e.score DESC
        LIMIT ?
        """,
        [args.limit],
    ).fetchall()

    payload = []
    for (
        a_id, b_id, score, evidence, a_name, a_cat, a_addr, a_phone, a_org,
        b_name, b_cat, b_addr, b_phone, b_org, distance_m,
    ) in rows:
        try:
            evidence_obj = json.loads(evidence) if evidence else {}
        except (TypeError, json.JSONDecodeError):
            evidence_obj = {}
        review_org = "org:review:" + hashlib.sha1(
            f"{a_id}|{b_id}".encode("utf-8")
        ).hexdigest()[:12]
        payload.append({
            "a_id": a_id, "b_id": b_id,
            "a_name": a_name, "a_category": a_cat, "a_address": a_addr,
            "a_phone": a_phone, "a_org": a_org, "b_name": b_name,
            "b_category": b_cat, "b_address": b_addr, "b_phone": b_phone,
            "b_org": b_org, "score": float(score),
            "distance_m": float(distance_m) if distance_m is not None else None,
            "review_org": review_org,
            "evidence": evidence_obj,
        })

    rows_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    html = (
        REVIEW_HTML
        .replace("__ROWS__", rows_json)
        .replace("__COUNT__", f"{len(payload):,}")
        .replace("__GENERATED__", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    log.info("Wrote %s (%d review pairs)", out, len(payload))
    return 0


EVIDENCE_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Evidence review</title>
<style>
  body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
         margin: 0; background: #f4f5f7; color: #1d232b; }
  header { position: sticky; top: 0; z-index: 2; background: #fff;
           border-bottom: 1px solid #dde1e6; padding: 16px 24px;
           display: flex; gap: 16px; align-items: center; flex-wrap: wrap; }
  h1 { font-size: 18px; margin: 0; }
  .muted { color: #66707a; font-size: 13px; }
  main { max-width: 900px; margin: 0 auto; padding: 20px; }
  .card { background: #fff; border: 1px solid #dde1e6; border-radius: 10px;
          padding: 18px; margin-bottom: 14px; }
  .card.done { border-color: #2e7d32; background: #f2fbf3; }
  .score { display: inline-block; background: #eef2ff; color: #3049b7;
           border-radius: 999px; padding: 3px 10px; font-weight: 600;
           font-size: 13px; }
  .source { float: right; font-size: 12px; background: #f3f4f6;
            padding: 3px 8px; border-radius: 5px; }
  h3 { margin: 10px 0 4px; font-size: 17px; }
  p { margin: 3px 0; font-size: 13px; color: #4c5661; }
  .org { font-family: ui-monospace, Consolas, monospace; font-size: 12px;
         background: #f7f8fa; padding: 2px 6px; border-radius: 4px; }
  .actions { display: flex; gap: 10px; margin-top: 14px; }
  button { border: 1px solid #c9d0d8; background: #fff; padding: 8px 14px;
           border-radius: 7px; cursor: pointer; font-size: 13px; }
  button.primary { background: #1f6feb; border-color: #1f6feb; color: #fff; }
  button.warn { color: #a12622; border-color: #e3b4b2; }
  #download { background: #1f6feb; color: #fff; border: 0; padding: 9px 16px;
              border-radius: 7px; cursor: pointer; }
</style>
</head>
<body>
<header>
  <h1>Evidence review</h1>
  <span class="muted">__COUNT__ matches · generated __GENERATED__</span>
  <span class="muted">Decisions: <span id="count">0</span></span>
  <button id="download">Download evidence_decisions.csv</button>
</header>
<main id="app"></main>
<script>
const rows = __ROWS__;
const decisions = [];
const esc = v => {
  const s = String(v ?? '');
  return /[",\\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
};
const h = s => String(s ?? '').replace(/[&<>"']/g, c => (
  {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]
));
function render() {
  document.getElementById('count').textContent = decisions.length;
}
function decide(i, action) {
  const r = rows[i];
  decisions.push({
    action: action === 'confirm' ? 'confirm_evidence' : 'reject_evidence',
    evidence_id: r.evidence_id,
    note: 'review score ' + r.score.toFixed(2)
  });
  const card = document.getElementById('card-' + i);
  card.classList.add('done');
  render();
}
function download() {
  const cols = ['action','evidence_id','note'];
  const lines = [cols.join(',')].concat(
    decisions.map(d => cols.map(c => esc(d[c])).join(','))
  );
  const blob = new Blob([lines.join('\\n')], {type:'text/csv;charset=utf-8'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'evidence_decisions.csv';
  a.click();
  URL.revokeObjectURL(a.href);
}
document.getElementById('download').addEventListener('click', download);
const app = document.getElementById('app');
if (!rows.length) {
  app.innerHTML = '<p class="muted">No evidence matches need review.</p>';
}
rows.forEach((r, i) => {
  const signals = Object.entries(r.evidence)
    .filter(([, v]) => v === true).map(([k]) => k).join(', ') || '—';
  const el = document.createElement('section');
  el.className = 'card';
  el.id = 'card-' + i;
  el.innerHTML = `
    <span class="score">${r.score.toFixed(2)}</span>
    <span class="source">${h(r.source)}</span>
    <h3>${h(r.name)}</h3>
    <p>${h(r.address || 'no address')} · ${h(r.phone || 'no phone')}</p>
    <p>${h(r.website || 'no website')}</p>
    <p>Matched organization: <b>${h(r.org_name)}</b>
       <span class="org">${h(r.org_id)}</span></p>
    <p>Signals: ${h(signals)} · name ${r.evidence.jaro_winkler?.toFixed?.(2) ?? '—'}</p>
    <div class="actions">
      <button class="primary" onclick="decide(${i}, 'confirm')">Confirm match</button>
      <button class="warn" onclick="decide(${i}, 'reject')">Reject match</button>
    </div>`;
  app.appendChild(el);
});
</script>
</body>
</html>
"""


def cmd_review_evidence_html(args: argparse.Namespace) -> int:
    con = connect(args.db)
    rows = con.execute(
        """
        SELECT oe.evidence_id, oe.org_id, oe.score, oe.evidence,
               e.source, e.name, e.address, e.phone, e.website,
               o.display_name
        FROM org_evidence oe
        JOIN external_records e ON e.evidence_id = oe.evidence_id
        JOIN organizations o ON o.org_id = oe.org_id
        WHERE oe.decision = 'review'
        ORDER BY oe.score DESC
        LIMIT ?
        """,
        [args.limit],
    ).fetchall()

    payload = []
    for (
        evidence_id, org_id, score, evidence, source, name, address, phone,
        website, org_name,
    ) in rows:
        try:
            evidence_obj = json.loads(evidence) if evidence else {}
        except (TypeError, json.JSONDecodeError):
            evidence_obj = {}
        payload.append({
            "evidence_id": evidence_id, "org_id": org_id,
            "org_name": org_name, "score": float(score),
            "source": source, "name": name, "address": address,
            "phone": phone, "website": website,
            "evidence": evidence_obj,
        })

    rows_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    html = (
        EVIDENCE_HTML
        .replace("__ROWS__", rows_json)
        .replace("__COUNT__", f"{len(payload):,}")
        .replace("__GENERATED__", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    log.info("Wrote %s (%d evidence matches)", out, len(payload))
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
GLOBAL_VALUE_FLAGS = {"--db"}
GLOBAL_BOOL_FLAGS = {"-v", "--verbose"}
COMMANDS = {"derive", "report", "export", "load-evidence",
            "match-evidence", "load-review", "apply-review",
            "enrich-web", "enrich-wikidata", "review-html",
            "review-evidence-html", "graph", "self-test"}


def reorder_global_args(argv: list[str]) -> list[str]:
    try:
        cmd_index = next(i for i, arg in enumerate(argv) if arg in COMMANDS)
    except StopIteration:
        return argv
    before, after = argv[:cmd_index], argv[cmd_index + 1:]
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
        description="Derive organizations from outlets (no registry required)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default="data/yp.duckdb")
    parser.add_argument("-v", "--verbose", action="store_true")

    sub = parser.add_subparsers(dest="command", required=True)

    p_derive = sub.add_parser("derive", help="Cluster outlets into organizations")
    p_derive.add_argument("--threshold", type=float, default=0.72)
    p_derive.add_argument("--review", type=float, default=0.55)
    p_derive.add_argument("--include-duplicates", action="store_true",
                          help="Include outlets already merged by dedupe-outlets")
    p_derive.set_defaults(func=cmd_derive)

    p_report = sub.add_parser("report", help="Organization coverage report")
    p_report.set_defaults(func=cmd_report)

    p_export = sub.add_parser("export", help="Export organizations and members")
    p_export.add_argument("--out", required=True)
    p_export.add_argument("--formats", default="parquet,csv")
    p_export.set_defaults(func=cmd_export)

    p_ev = sub.add_parser(
        "load-evidence",
        help="Import any public list (JAKIM, ePerolehan, MyIPO, FMM, ...)",
    )
    p_ev.add_argument("--source", required=True, help="Short label, e.g. jakim")
    p_ev.add_argument("--input", required=True, help="CSV/Parquet/JSONL")
    p_ev.add_argument("--column-map", default=None)
    p_ev.add_argument("--column-map-file", default=None)
    p_ev.add_argument("--append", action="store_true")
    p_ev.set_defaults(func=cmd_load_evidence)

    p_me = sub.add_parser(
        "match-evidence", help="Attach imported records to organizations"
    )
    p_me.add_argument("--threshold", type=float, default=0.75)
    p_me.add_argument("--review", type=float, default=0.55)
    p_me.set_defaults(func=cmd_match_evidence)

    p_lr = sub.add_parser(
        "load-review", help="Load human review decisions"
    )
    p_lr.add_argument("--input", required=True)
    p_lr.add_argument("--column-map", default=None)
    p_lr.add_argument("--column-map-file", default=None)
    p_lr.set_defaults(func=cmd_load_review)

    p_ar = sub.add_parser(
        "apply-review", help="Re-apply saved review decisions"
    )
    p_ar.set_defaults(func=cmd_apply_review)

    p_wd = sub.add_parser("enrich-wikidata", help="Enrich brand QIDs from Wikidata")
    p_wd.add_argument("--limit", type=int, default=25)
    p_wd.add_argument("--delay", type=float, default=1.2)
    p_wd.add_argument("--cache-dir", default="cache")
    p_wd.add_argument("--email", default="set-your-contact@example.com")
    p_wd.set_defaults(func=cmd_enrich_wikidata)

    p_web = sub.add_parser(
        "enrich-web", help="Enrich organizations from their own homepage"
    )
    p_web.add_argument("--limit", type=int, default=25)
    p_web.add_argument("--delay", type=float, default=1.5)
    p_web.add_argument("--cache-dir", default="cache/web")
    p_web.add_argument("--max-bytes", type=int, default=2_000_000)
    p_web.add_argument("--email", default="set-your-contact@example.com")
    p_web.add_argument("--retry-errors", action="store_true")
    p_web.add_argument("--follow-contact", action="store_true",
                       help="Fetch one contact/about page if the homepage has no email/phone")
    p_web.set_defaults(func=cmd_enrich_web)

    p_html = sub.add_parser(
        "review-html", help="Generate a self-contained review page"
    )
    p_html.add_argument("--out", required=True, help="Output .html path")
    p_html.add_argument("--limit", type=int, default=200)
    p_html.set_defaults(func=cmd_review_html)

    p_ehtml = sub.add_parser(
        "review-evidence-html", help="Generate a review page for evidence matches"
    )
    p_ehtml.add_argument("--out", required=True)
    p_ehtml.add_argument("--limit", type=int, default=200)
    p_ehtml.set_defaults(func=cmd_review_evidence_html)

    p_graph = sub.add_parser(
        "graph", help="Export organizations and outlets as GraphML"
    )
    p_graph.add_argument("--out", required=True, help="Output .graphml path")
    p_graph.set_defaults(func=cmd_graph)

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
