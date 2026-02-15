# Resolving unmapped units and categorical measurement values

After ETL, some source units may have `unit_concept_id = 0` in `stg.map_units`, and some categorical lab result values may have `value_as_concept_id = 0` in `cdm.measurement`. You can export both for manual mapping.

## 1. Export unmapped units with their measurements

With a live DuckDB connection `con` (after ETL has run):

```r
ETLDelphi::export_unmapped_units(con, output_path = "unmapped_units.csv")
```

This writes a CSV with one row per **unmapped unit + measurement** combination:

- `source_value`: the unit string (e.g. `mg/dl`, `iu`)
- `measurement_source_value`: the measurement it appears with (e.g. LOINC code or vital name like "Height", "Weight")
- `n_occurrences`: how many times that pair appears in the data
- `target_concept_id`: empty for you to fill with the UCUM concept_id

Only combinations where `unit_concept_id = 0` in `cdm.measurement` are included. Seeing which measurements use each unit helps identify the correct UCUM concept.

## 2. Look up OMOP unit concepts

Use the vocabulary-search skill or run from the project root:

```bash
# Search for a unit (e.g. "gram", "iu", "milliliter", "%")
Rscript -e "source('extras/hecate_client.R'); print(vocabulary_search('QUERY', vocabulary_id='UCUM', limit=10))"
```

Replace `QUERY` with the source unit string (or a close variant). The result includes `concept_id` and concept details. Use the correct Standard UCUM concept_id for the unit.

## 3. Add mappings

- **Option A:** Fill `target_concept_id` in `unmapped_units.csv` (one row per unit–measurement pair; use the same concept_id for every row that shares the same unit) and use your own process to bulk-update `stg.map_units` or feed a custom script.
- **Option B:** Add rows to `inst/sql/30_vocab/45_map_units.sql` in the `(VALUES ...)` list, e.g. `('your_unit', concept_id)`, then re-run the ETL (or at least the map_units step and downstream measurement/drug load steps).

Units are matched after normalizing: `LOWER(TRIM(REPLACE(REPLACE(unit_source_value, '[', ''), ']', '')))`. Use that same form in the mapping (e.g. `'mg/dl'` not `'mg/dL'` unless you add both).

---

## Unmapped categorical measurement values (value_as_concept_id = 0)

Categorical lab results (e.g. "Positive", "Negative", "Equivocal") are mapped via `stg.map_measurement_value`. Unmapped ones get `value_as_concept_id = 0`.

### Export unmapped measurement values

```r
ETLDelphi::export_unmapped_measurement_values(con, output_path = "unmapped_measurement_values.csv")
```

This writes a CSV with one row per **unmapped value + measurement** combination:

- `source_value`: the result text (e.g. `Positive`, `Equivocal`)
- `measurement_source_value`: the lab test it appears with (e.g. LOINC code)
- `n_occurrences`: how many times that pair appears
- `target_concept_id`: empty for you to fill with the OMOP Meas Value concept_id (e.g. 9191 = Positive, 9189 = Negative, 45877994 = Equivocal)

### Add mappings for categorical values

Add rows to `inst/sql/30_vocab/50_map_measurement_value.sql` in the `(VALUES ...)` list, using the **normalized** form: `LOWER(TRIM(REPLACE(REPLACE(value_source_value, '[', ''), ']', '')))`. For example:

```sql
('your_result_text', concept_id),
```

Then re-run the ETL (or at least the map_measurement_value step and the measurement labs load).

---

## Unmapped drugs (drug_concept_id = 0)

Drugs are mapped by NDC (and optionally drug name). Wildcard or partial NDCs (e.g. `59630*70248`) are not valid full NDCs: the ETL treats them as non-resolvable unless pattern matching returns exactly one concept; multiple matches are ambiguous (drop or map at ingredient level). For unmapped drugs you can use **drug name** for custom mapping at least at the **ingredient level**.

### Export unmapped drugs (name, NDC, record counts)

```r
ETLDelphi::export_unmapped_drugs(con, output_path = "unmapped_drugs.csv")
```

This writes a CSV with one row per **unmapped (drug_name, NDC)** combination:

- `drug_name`: source drug name
- `drug_ndc_normalized`: NDC with hyphens/spaces removed (empty for name-only rows)
- `drug_ndc_raw`: one example of the raw NDC as in source (e.g. with dashes or wildcards)
- `n_occurrences`: number of orders with this name/NDC
- `target_concept_id`: empty for you to fill (e.g. ingredient or clinical drug concept_id)

### Add mappings for unmapped drugs

- **By NDC:** Add rows to `inst/extdata/custom_ndc_mapping.csv` (columns `drug_ndc_normalized`, `drug_concept_id`) for full NDCs you want to override.
- **By drug name:** Add rows to `inst/extdata/custom_concept_mapping.csv` with `source_value` = drug name (trimmed, first 50 chars as used in ETL), `domain` = `drug`, and `concept_id` = your target (e.g. ingredient-level RxNorm concept). The load step uses this when NDC mapping fails.
