# Precision review instructions

The validator is being audited for **precision**, not coverage. We already know
how often it proposes a match; this review asks how often the proposed match is
right.

The sheet is blind: it does not show the verdict, confidence, similarity score or
evidence source.

## Do not auto-label

Do not fill this sheet by comparing the address and the matched road as strings.
That reproduces the validator's own rule and reports artificially high
precision. The question is geographic: **is the matched road the road at this
address's location?** Use a map where possible, not just text similarity.

## What to label

For each row, read the `address` and the proposed `matched_road`, then fill the
`label` column:

| Label | Meaning |
| --- | --- |
| `1` | `matched_road` is the street named in the address. |
| `0` | It is a different street, a different numeric suffix/branch, or unrelated. |
| blank | Cannot tell, or skip. |

Judge only the street identity. Do not reward a match just because it is nearby.

## Rules

* **Accept** spelling, spacing and punctuation variants of the same name:
  `Jalan SS 15/4B` = `Jalan SS15/4b`.
* **Accept** a missing or added non-numeric word when it is clearly the same
  street: `Jalan Bukit Tengah` = `Jalan IKS Bukit Tengah`.
* **Reject** a different numeric suffix or branch:
  `Jalan Batu Nilam 34` != `Jalan Batu Nilam 4`;
  `Jalan Melaka Raya 16` != `Jalan Melaka Raya 6`;
  `Jalan 15` != `Jalan 35`.
* **Reject** a different name that happens to look similar:
  `Jalan TS 1` != `Jalan RT 1`;
  `Jalan Mahsan` != `Jalan Aman`.
* For the `unverified` sample, label `1` if the proposed road is actually the
  street in the address, even though the validator withheld the match. This
  measures missed opportunities, not precision.

`road_distance_m` is the distance from the anchor used for matching, not
necessarily from the address itself. Use it as context, not as the answer.

## Geo-aware sheet

`data/review_sheet_geo.csv` adds `input_lat`/`input_lon` and three links:

* `map_link` searches the full address.
* `pin_link` shows the Overture coordinate. This can be wrong: about 2.7% of the
  pull had address/coordinate conflicts.
* `road_link` searches the matched road near the postcode.

Use the map, not just the parsed string, to decide whether the matched road is at
the address. If the pin is clearly wrong, judge from the address search and note
it in `notes`.

## Workflow

1. Run `py -3 make_review_sheet.py` to build `data/review_sheet.csv`.
2. Open the CSV in Excel or another editor.
3. Fill `label` with `1` or `0`; use `notes` for ambiguous cases.
4. Keep the `review_id` column and the row order intact.
5. Save as CSV (UTF-8).
6. Run:

```powershell
py -3 score_review.py
```

The scorer prints precision with 95% confidence intervals, broken down by
verdict, street source, state group and distance, and writes
`data/review_scores.json`.

Partial labelling is fine; the scorer reports completion. A useful first pass is
100 rows; the full sheet is 250.
