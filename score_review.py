#!/usr/bin/env python3
"""Score a filled precision-review sheet.

The reviewer labels `matched_road` in data/review_sheet.csv with:

    1  the matched road is the street in the address
    0  it is a different street (including a different numeric suffix)
    blank  skip / cannot tell

Usage:
    py -3 score_review.py
    py -3 score_review.py --labels data/review_sheet.csv `
        --out data/review_scores.json
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import OrderedDict
from pathlib import Path

TRUE_LABELS = {"1", "1.0", "yes", "y", "true", "correct", "ok", "match", "matched", "same"}
FALSE_LABELS = {
    "0", "0.0", "no", "n", "false", "wrong", "incorrect",
    "no_match", "nomatch", "mismatch", "different",
}
UNCERTAIN_LABELS = {"uncertain", "unsure", "maybe", "?"}
CLAIMED_VERDICTS = {
    "valid",
    "valid_street_number_unverified",
    "probable",
    "corrected",
}
MAJOR_STATES = {"Kuala Lumpur", "Selangor", "Johor", "Pulau Pinang"}


def state_group(state: str | None) -> str:
    if not state:
        return "unknown"
    return "major" if state in MAJOR_STATES else "other"


def distance_band(value: str | None) -> str:
    try:
        distance = float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        distance = None
    if distance is None:
        return "unknown"
    if distance <= 100:
        return "<=100m"
    if distance <= 500:
        return "101-500m"
    if distance <= 2000:
        return "501-2000m"
    return ">2000m"


def wilson(correct: int, total: int, z: float = 1.96) -> tuple[float, float, float]:
    if total == 0:
        return 0.0, 0.0, 0.0
    p = correct / total
    denom = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denom
    half = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denom
    return p, max(0.0, centre - half), min(1.0, centre + half)


def stats(rows: list[dict]) -> dict:
    n = len(rows)
    correct = sum(1 for row in rows if row["_correct"])
    p, lo, hi = wilson(correct, n)
    return {
        "n": n,
        "correct": correct,
        "precision": round(p, 3) if n else None,
        "ci95": [round(lo, 3), round(hi, 3)] if n else None,
    }


def read_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def normalize_id(value: str | None) -> str:
    text = str(value or "").strip()
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def print_group(title: str, groups: dict[str, dict], note: str = "precision") -> None:
    print(f"\n{title}")
    print(f"  {'group':34} {'n':>4} {'correct':>8} {note:>10}   95% CI")
    for label, data in groups.items():
        if not data["n"]:
            continue
        ci = data["ci95"] or [0, 0]
        print(
            f"  {label:34} {data['n']:>4} {data['correct']:>8} "
            f"{data['precision']:>10.3f}   {ci[0]:.2f}-{ci[1]:.2f}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--labels", default="data/review_sheet.csv")
    parser.add_argument("--results", default="data/validator_results.csv")
    parser.add_argument("--out", default="data/review_scores.json")
    args = parser.parse_args()

    label_rows = read_csv(Path(args.labels))
    result_rows = read_csv(Path(args.results))
    results = {normalize_id(row.get("input_id")): row for row in result_rows}

    algorithm_notes = sum(
        1 for row in label_rows
        if (row.get("notes") or "").lower().startswith("algorithm")
    )
    geo_unchecked = sum(
        1 for row in label_rows
        if "geographic correctness not assessed" in (row.get("notes") or "").lower()
    )

    labeled: list[dict] = []
    skipped = 0
    uncertain = 0
    invalid = 0
    missing = 0
    for row in label_rows:
        raw = (row.get("label") or "").strip().lower()
        if not raw:
            skipped += 1
            continue
        if raw in UNCERTAIN_LABELS:
            uncertain += 1
            continue
        if raw in TRUE_LABELS:
            correct = True
        elif raw in FALSE_LABELS:
            correct = False
        else:
            invalid += 1
            continue
        review_id = normalize_id(row.get("review_id"))
        result = results.get(review_id)
        if result is None:
            missing += 1
            continue
        labeled.append({
            "review_id": review_id,
            "correct": correct,
            "_correct": correct,
            "verdict": result.get("verdict", ""),
            "street_source": result.get("street_source", ""),
            "state": result.get("state", ""),
            "distance": row.get("road_distance_m", ""),
        })

    claimed = [r for r in labeled if r["verdict"] in CLAIMED_VERDICTS]
    unverified = [r for r in labeled if r["verdict"] == "unverified"]

    by_verdict = OrderedDict()
    for verdict in (
        "valid", "valid_street_number_unverified", "probable",
        "corrected", "unverified",
    ):
        by_verdict[verdict] = stats([r for r in labeled if r["verdict"] == verdict])

    by_source = OrderedDict()
    for source in ("osm_address", "osm_road", "none"):
        by_source[source] = stats(
            [r for r in claimed if r["street_source"] == source]
        )

    by_state = OrderedDict()
    for group in ("major", "other", "unknown"):
        by_state[group] = stats(
            [r for r in claimed if state_group(r["state"]) == group]
        )

    by_distance = OrderedDict()
    for band in ("<=100m", "101-500m", "501-2000m", ">2000m", "unknown"):
        by_distance[band] = stats(
            [r for r in claimed if distance_band(r["distance"]) == band]
        )

    overall = stats(claimed)
    unverified_stats = stats(unverified)
    completion = len(labeled) / len(label_rows) if label_rows else 0.0

    print(f"Review sheet: {len(label_rows):,} rows")
    print(
        f"Labeled: {len(labeled):,} ({completion:.1%})   "
        f"uncertain: {uncertain:,}   skipped: {skipped:,}   "
        f"invalid: {invalid:,}   missing result: {missing:,}"
    )
    print(
        f"\nClaimed matches: {overall['n']:,} labeled, {overall['correct']:,} correct"
    )
    if overall["n"]:
        ci = overall["ci95"]
        print(
            f"Overall precision: {overall['precision']:.3f} "
            f"(95% CI {ci[0]:.2f}-{ci[1]:.2f})"
        )
    print_group("By verdict", by_verdict)
    print("  (the unverified row is the best-road-correct rate, not precision)")
    print_group("By street source (claimed only)", by_source)
    print_group("By state group (claimed only)", by_state)
    print_group("By distance to matched road (claimed only)", by_distance)

    print("\nWithheld matches (unverified sample)")
    if unverified_stats["n"]:
        rate = unverified_stats["precision"] or 0.0
        print(
            f"  {unverified_stats['n']:,} labeled; the proposed road was actually "
            f"correct for {unverified_stats['correct']:,} ({rate:.1%})."
        )
        print("  This is a sampled estimate of recall headroom, not precision.")
    else:
        print("  No unverified rows labeled.")

    warnings: list[str] = []
    if label_rows and algorithm_notes >= 0.8 * len(label_rows):
        warnings.append(
            "Labels appear algorithm-generated (notes start with 'Algorithm'); "
            "this is a name-agreement check, not an independent precision audit."
        )
    if label_rows and geo_unchecked >= 0.8 * len(label_rows):
        warnings.append(
            "Labels state that geographic correctness was not assessed; the score "
            "does not verify that the road is in the right area."
        )
    if len(labeled) < 100:
        warnings.append("Fewer than 100 labels: treat all numbers as indicative.")
    for verdict in ("valid", "valid_street_number_unverified"):
        data = by_verdict.get(verdict, {})
        if data.get("n", 0) >= 10 and data["precision"] is not None and data["precision"] < 0.95:
            warnings.append(
                f"{verdict} precision is below 95%; tighten the threshold or demote it."
            )
    data = by_verdict.get("probable", {})
    if data.get("n", 0) >= 10 and data["precision"] is not None and data["precision"] < 0.80:
        warnings.append("probable precision is below 80%; demote it to unverified.")
    data = by_verdict.get("corrected", {})
    if data.get("n", 0) >= 5 and data["precision"] is not None and data["precision"] < 0.90:
        warnings.append("corrected precision is below 90%; disable corrections.")
    if overall["n"] >= 20 and overall["precision"] is not None and overall["precision"] < 0.90:
        warnings.append("Overall claimed precision is below 90%.")
    if unverified_stats["n"] >= 10 and (unverified_stats["precision"] or 0) > 0.25:
        warnings.append(
            "Many withheld matches were correct; consider a lower-confidence tier."
        )
    for label, data in list(by_verdict.items()) + list(by_source.items()):
        if 0 < data.get("n", 0) < 10:
            warnings.append(f"Small group ({label}, n={data['n']}); indicative only.")

    print("\nWarnings")
    if warnings:
        for warning in warnings:
            print(f"  - {warning}")
    else:
        print("  none")

    payload = {
        "labels": str(args.labels),
        "results": str(args.results),
        "rows": len(label_rows),
        "labeled": len(labeled),
        "algorithm_generated_labels": algorithm_notes,
        "geographic_correctness_unchecked": geo_unchecked,
        "uncertain": uncertain,
        "skipped": skipped,
        "invalid_labels": invalid,
        "missing_results": missing,
        "completion": round(completion, 3),
        "claimed": overall,
        "by_verdict": by_verdict,
        "by_source": by_source,
        "by_state_group": by_state,
        "by_distance": by_distance,
        "unverified_headroom": unverified_stats,
        "warnings": warnings,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
