#!/usr/bin/env python3
"""Build a geo-aware precision-review sheet with coordinates and map links.

Unlike make_review_sheet.py, this sheet lets a reviewer check whether the
matched road is actually at the address's location. It includes the input
coordinates (from the Overture pull) and Google Maps links for the address,
the pin, and the matched road.

The sheet is still blind: no verdict, confidence, similarity or evidence source.

Usage:
    py -3 make_geo_review_sheet.py
    py -3 make_geo_review_sheet.py --rows 50 --out data/review_sheet_geo_50.csv
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path
from urllib.parse import quote_plus

import duckdb

from make_review_sheet import sample_group, stable_key, state_group

# Share of the sheet by validator verdict. The uncertain rows from the first
# sheet are added on top, then duplicates are removed.
TARGET_SHARES = {
    "uncertain": 0.49,
    "corrected": 0.08,
    "probable": 0.15,
    "unverified": 0.10,
    "valid": 0.10,
    "valid_street_number_unverified": 0.08,
}


def maps_link(query: str) -> str:
    if not query.strip():
        return ""
    return "https://www.google.com/maps/search/?api=1&query=" + quote_plus(query)


def address_query(row: dict) -> str:
    parts = [row.get("address", "")]
    locality = " ".join(x for x in (row.get("postcode", ""), row.get("city", "")) if x)
    if locality:
        parts.append(locality)
    if row.get("state"):
        parts.append(row["state"])
    parts.append("Malaysia")
    return ", ".join(p for p in parts if p)


def road_query(row: dict) -> str:
    if not (row.get("matched_road") or "").strip():
        return ""
    locality = " ".join(x for x in (row.get("postcode", ""), row.get("city", "")) if x)
    parts = [row["matched_road"]]
    if locality:
        parts.append(locality)
    if row.get("state"):
        parts.append(row["state"])
    parts.append("Malaysia")
    return ", ".join(p for p in parts if p)


def fill_to(selected: list[dict], pools: list[list[dict]], target: int) -> list[dict]:
    seen = {row.get("input_id") for row in selected}
    for pool in pools:
        if len(selected) >= target:
            break
        for row in sorted(pool, key=lambda r: stable_key(r.get("input_id", ""))):
            if len(selected) >= target:
                break
            if row.get("input_id") in seen:
                continue
            selected.append(row)
            seen.add(row.get("input_id"))
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", default="data/validator_results.csv")
    parser.add_argument("--sample", default="data/validator_sample.csv")
    parser.add_argument("--completed", default="data/review_sheet_completed.csv")
    parser.add_argument("--places", default="data/my_places_raw.parquet")
    parser.add_argument("--out", default="data/review_sheet_geo.csv")
    parser.add_argument("--rows", type=int, default=100)
    args = parser.parse_args()

    with Path(args.results).open("r", encoding="utf-8-sig", newline="") as handle:
        results = list(csv.DictReader(handle))
    with Path(args.sample).open("r", encoding="utf-8-sig", newline="") as handle:
        sample_rows = list(csv.DictReader(handle))
    with Path(args.completed).open("r", encoding="utf-8-sig", newline="") as handle:
        completed = list(csv.DictReader(handle))

    id_to_gers = {
        str(i): row.get("gers_id", "").strip()
        for i, row in enumerate(sample_rows, start=1)
    }
    uncertain_ids = {
        (row.get("review_id") or "").strip()
        for row in completed
        if (row.get("label") or "").strip().lower() == "uncertain"
    }

    street = [r for r in results if r.get("address_type") == "street"]
    by_verdict: dict[str, list[dict]] = {}
    for row in street:
        by_verdict.setdefault(row.get("verdict", ""), []).append(row)

    selected: list[dict] = []
    uncertain_pool = [r for r in street if r.get("input_id") in uncertain_ids]
    uncertain_target = min(
        len(uncertain_pool), max(1, round(args.rows * TARGET_SHARES["uncertain"]))
    )
    selected = sample_group(uncertain_pool, uncertain_target)

    for verdict, share in TARGET_SHARES.items():
        if verdict == "uncertain":
            continue
        pool = by_verdict.get(verdict, [])
        target = min(
            len(pool), max(0, round(args.rows * share))
        )
        selected.extend(sample_group(pool, target))

    # Deduplicate, then fill from the risky tiers and finally the baseline.
    unique: dict[str, dict] = {}
    for row in selected:
        unique.setdefault(row.get("input_id"), row)
    selected = list(unique.values())

    fill_pools = [
        by_verdict.get("probable", []),
        by_verdict.get("unverified", []),
        by_verdict.get("valid", []),
        by_verdict.get("valid_street_number_unverified", []),
        by_verdict.get("corrected", []),
    ]
    selected = fill_to(selected, fill_pools, args.rows)

    # Keep the highest-risk rows if we went over.
    if len(selected) > args.rows:
        keep = [
            r for r in selected
            if r.get("verdict") in {"corrected", "probable"}
            or r.get("input_id") in uncertain_ids
        ]
        rest = [r for r in selected if r not in keep]
        selected = keep + rest[: max(0, args.rows - len(keep))]
        selected = selected[: args.rows]

    selected.sort(key=lambda r: stable_key(r.get("input_id", "")))

    # Coordinates from the Overture pull.
    coords: dict[str, tuple[float, float]] = {}
    con = duckdb.connect()
    con.execute("CREATE TEMP TABLE ids (input_id VARCHAR, gers_id VARCHAR)")
    con.executemany(
        "INSERT INTO ids VALUES (?, ?)",
        [(i, g) for i, g in id_to_gers.items() if g],
    )
    rows = con.execute(
        f"""
        SELECT i.input_id, p.lat, p.lon
        FROM ids i
        JOIN read_parquet('{args.places}') p USING (gers_id)
        WHERE p.lat IS NOT NULL AND p.lon IS NOT NULL
        """
    ).fetchall()
    coords = {str(r[0]): (float(r[1]), float(r[2])) for r in rows}

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "review_id", "address", "postcode", "city", "state",
        "input_lat", "input_lon", "parsed_street", "matched_road",
        "road_distance_m", "map_link", "pin_link", "road_link",
        "label", "notes",
    ]
    missing_coords = 0
    with out.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in selected:
            review_id = str(row.get("input_id", ""))
            coord = coords.get(review_id)
            if coord is None:
                missing_coords += 1
                lat = lon = ""
                pin = ""
            else:
                lat, lon = coord
                pin = maps_link(f"{lat:.6f},{lon:.6f}")
            distance = row.get("road_distance_m") or ""
            try:
                distance = str(round(float(distance)))
            except (TypeError, ValueError):
                distance = ""
            writer.writerow({
                "review_id": review_id,
                "address": row.get("address", ""),
                "postcode": row.get("postcode", ""),
                "city": row.get("city", ""),
                "state": row.get("state", ""),
                "input_lat": f"{lat:.6f}" if lat != "" else "",
                "input_lon": f"{lon:.6f}" if lon != "" else "",
                "parsed_street": row.get("parsed_street", ""),
                "matched_road": row.get("matched_road", ""),
                "road_distance_m": distance,
                "map_link": maps_link(address_query(row)),
                "pin_link": pin,
                "road_link": maps_link(road_query(row)),
                "label": "",
                "notes": "",
            })

    print(f"Wrote {len(selected)} rows to {out}")
    print(f"Uncertain rows included: {sum(1 for r in selected if r.get('input_id') in uncertain_ids)}")
    print(f"Missing coordinates: {missing_coords}")
    print("Distribution (hidden from the sheet):")
    print("  verdicts:", dict(Counter(r.get("verdict") for r in selected)))
    print("  sources: ", dict(Counter(r.get("street_source") for r in selected)))
    print("  states:  ", dict(Counter(state_group(r.get("state")) for r in selected)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
