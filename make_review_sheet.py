#!/usr/bin/env python3
"""Build a blind precision-review sheet from validator results.

The sheet deliberately hides the verdict, confidence, similarity and evidence
source so the reviewer judges only one thing: is `matched_road` the street in
the address?

Usage:
    py -3 make_review_sheet.py
    py -3 make_review_sheet.py --results data/validator_results.csv `
        --out data/review_sheet.csv

Label the `label` column with 1 (correct), 0 (wrong), or leave blank to skip,
then run score_review.py.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from collections import Counter
from pathlib import Path

# Stratified targets. `probable` and `corrected` are small, so take them all.
VERDICT_TARGETS = {
    "valid": 90,
    "valid_street_number_unverified": 90,
    "probable": 29,
    "corrected": 8,
    "unverified": 33,
}
MAJOR_STATES = {"Kuala Lumpur", "Selangor", "Johor", "Pulau Pinang"}


def state_group(state: str | None) -> str:
    if not state:
        return "unknown"
    return "major" if state in MAJOR_STATES else "other"


def stable_key(value: str) -> str:
    return hashlib.md5(str(value).encode("utf-8")).hexdigest()


def sample_group(rows: list[dict], target: int) -> list[dict]:
    """Round-robin over (street_source, state_group) strata."""
    if target <= 0:
        return []
    strata: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        key = (row.get("street_source") or "none", state_group(row.get("state")))
        strata.setdefault(key, []).append(row)
    for key in strata:
        strata[key].sort(key=lambda r: stable_key(r.get("input_id", "")))

    keys = sorted(strata)
    selected: list[dict] = []
    while len(selected) < target and any(strata[k] for k in keys):
        for key in keys:
            if strata[key] and len(selected) < target:
                selected.append(strata[key].pop(0))
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", default="data/validator_results.csv")
    parser.add_argument("--out", default="data/review_sheet.csv")
    parser.add_argument("--scale", type=float, default=1.0,
                        help="Scale the sample size (0.4 gives ~100 rows)")
    args = parser.parse_args()
    if args.scale <= 0:
        parser.error("--scale must be greater than zero")

    with Path(args.results).open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    street = [r for r in rows if r.get("address_type") == "street"]

    selected: list[dict] = []
    for verdict, base_target in VERDICT_TARGETS.items():
        group = [r for r in street if r.get("verdict") == verdict]
        if verdict == "unverified":
            # Review the near-misses: rows where a road was proposed but the
            # verdict was withheld.
            group = [r for r in group if (r.get("matched_road") or "").strip()]
        target = max(0, int(round(base_target * args.scale)))
        picked = sample_group(group, target)
        print(f"  {verdict:34} available {len(group):>4}  sampled {len(picked):>3}")
        selected.extend(picked)

    # Blind ordering: no verdict grouping.
    selected.sort(key=lambda r: stable_key(r.get("input_id", "")))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "review_id", "address", "postcode", "city", "state",
        "parsed_street", "matched_road", "road_distance_m",
        "label", "notes",
    ]
    with out.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in selected:
            distance = row.get("road_distance_m") or ""
            try:
                distance = str(round(float(distance)))
            except (TypeError, ValueError):
                distance = ""
            writer.writerow({
                "review_id": row.get("input_id", ""),
                "address": row.get("address", ""),
                "postcode": row.get("postcode", ""),
                "city": row.get("city", ""),
                "state": row.get("state", ""),
                "parsed_street": row.get("parsed_street", ""),
                "matched_road": row.get("matched_road", ""),
                "road_distance_m": distance,
                "label": "",
                "notes": "",
            })

    print(f"\nWrote {len(selected)} rows to {out}")
    print("Distribution (hidden from the sheet):")
    print("  verdicts:", dict(Counter(r.get("verdict") for r in selected)))
    print("  sources: ", dict(Counter(r.get("street_source") for r in selected)))
    print("  states:  ", dict(Counter(state_group(r.get("state")) for r in selected)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
