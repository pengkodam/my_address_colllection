# Synthetic demo dataset

All names, registration numbers, phone numbers, social handles and coordinates
here are invented for testing. They do not describe real businesses.

| File | Purpose |
| --- | --- |
| `sample_outlets.csv` | Overture-style outlets: 12 rows, one duplicate (`o5`), three multi-branch brands |
| `sample_entities.csv` | Optional SSM-style entities, for the registry-linking demo |
| `sample_labels.csv` | Labelled outlet/entity pairs for threshold calibration |
| `sample_evidence.csv` | A third-party list (halal/IT/catering) to attach as evidence |
| `column_map.json` | Example explicit column mapping for the entity loader |

## Demo 1: organizations without a registry

This is the recommended path. No SSM data needed.

```powershell
py -3 -m pip install -r requirements.txt

py -3 resolve_entities.py init --db data/demo.duckdb --force
py -3 resolve_entities.py load-outlets `
  --db data/demo.duckdb --input examples/sample_outlets.csv
py -3 resolve_entities.py dedupe-outlets --db data/demo.duckdb

py -3 derive_orgs.py derive --db data/demo.duckdb
py -3 derive_orgs.py report --db data/demo.duckdb
py -3 derive_orgs.py export --db data/demo.duckdb --out data/demo_orgs
```

Expected shape:

```
Outlets:              12
Organizations:        8
Multi-outlet orgs:    3
Outlets in those:     6
Review edges:         0
Outlets without org:  0
```

The three clusters are `Tech Solutions` (domain), `Nasi Lemak Pak Ali` (brand
name), and `Salon Cantik` (social signal), each with two branches. `o5` is a
duplicate of `o1` and is collapsed by dedupe.

Inspect the graph:

```powershell
py -3 derive_orgs.py export --db data/demo.duckdb --out data/demo_orgs
```

or query it through the resolver's shell:

```powershell
py -3 resolve_entities.py sql --db data/demo.duckdb `
  --query "SELECT org_id, display_name, outlet_count, key_type FROM organizations ORDER BY outlet_count DESC"
```

## Demo 2: attach a public list as evidence

Any list with a name and optional address/phone/website can be attached to the
organization graph. No SSM required.

```powershell
py -3 derive_orgs.py load-evidence `
  --db data/demo.duckdb `
  --source example_registry `
  --input examples/sample_evidence.csv

py -3 derive_orgs.py match-evidence --db data/demo.duckdb
py -3 derive_orgs.py report --db data/demo.duckdb
```

Expected:

```
Evidence matched: 2
  auto:           2
  review:         0
```

`Restoran Sedap` and `Tech Solutions` match on domain, phone, name and address.
`Unknown Catering` stays unmatched.

## Demo 3: optional registry linking

Only if you have a licensed SSM-style export. `build` starts from a fresh
database.

```powershell
py -3 resolve_entities.py build `
  --db data/demo_registry.duckdb `
  --entities examples/sample_entities.csv `
  --entities-column-map-file examples/column_map.json `
  --outlets examples/sample_outlets.csv `
  --out data/demo_registry_out `
  --dedupe-outlets --canonical-only
```

Expected shape:

```
Outlets:          11        (o5 collapsed into o1)
Entities:         8 (7 active)
Auto:             3
Review:           3
Rejected:         3
No candidate:     2
```

Calibrate:

```powershell
py -3 resolve_entities.py calibrate `
  --db data/demo_registry.duckdb `
  --labels examples/sample_labels.csv `
  --out data/demo_calibration.json
```

The ten labels are too few to trust; the command will say so.

## Optional: correct and enrich

```powershell
# human correction (create data/decisions.csv first)
py -3 derive_orgs.py load-review --db data/demo.duckdb --input data/decisions.csv
py -3 derive_orgs.py apply-review --db data/demo.duckdb

# visual review page
py -3 derive_orgs.py review-html `
  --db data/demo.duckdb --out data/review.html
py -3 derive_orgs.py review-evidence-html `
  --db data/demo.duckdb --out data/evidence_review.html

# one homepage per organization, robots-aware
py -3 derive_orgs.py enrich-web `
  --db data/demo.duckdb --limit 10 --email you@example.com --follow-contact

# GraphML for Gephi / Cytoscape
py -3 derive_orgs.py graph --db data/demo.duckdb --out data/orgs.graphml
```

Decisions and enrichment survive a later `derive` because they are re-applied
by `org_id`.

## Notes

* `build` is destructive. Use the individual `load-*` / `dedupe-outlets` /
  `derive` steps for incremental work.
* `enrich-wikidata` needs network access and a real QID. The samples have no
  QIDs, so the enrichment step is a no-op on this dataset.
