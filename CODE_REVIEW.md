# ETLDelphi Code Review Report

**Date**: 2026-02-24
**Scope**: Full codebase — 18 R files, 66 SQL files, tests, configuration

---

## Executive Summary

ETLDelphi is a well-structured R package that transforms Delphi-2M synthetic patient data into OMOP CDM v5.4 using DuckDB. The architecture follows a clean 3-layer pattern (src → stg → cdm) with lexicographically-ordered SQL scripts, deterministic ID generation, and LLM-powered concept mapping improvement.

**Overall quality**: Good for a research ETL. The SQL logic is largely correct and follows OHDSI conventions. The main risks are in execution ordering, missing edge case handling, and the LLM mapping pipeline's robustness.

| Category | Issues Found | Critical | High | Medium | Low |
|----------|-------------|----------|------|--------|-----|
| Bugs | 7 | 1 | 2 | 3 | 1 |
| Architecture | 4 | 0 | 0 | 3 | 1 |
| Dead code | 2 | 0 | 0 | 0 | 2 |
| Missing deps | 2 | 0 | 2 | 0 | 0 |
| Test coverage | 1 | 0 | 0 | 1 | 0 |

---

## 1. Bugs

### BUG-1: NLP SQL runs before cdm.note is populated [CRITICAL]

**Files**: `inst/sql/35_nlp/01_parse_note_sections.sql`, `inst/sql/40_cdm_core/55_load_note.sql`

`35_nlp/01_parse_note_sections.sql` queries `cdm.note`, but `cdm.note` is populated by `40_cdm_core/55_load_note.sql`. Since the SQL runner executes in lexicographic order, the NLP step always runs on an empty `cdm.note` table and produces empty results.

**Fix**: Rename `inst/sql/35_nlp/` to `inst/sql/45_nlp/` so it runs after `40_cdm_core/`.

---

### BUG-2: observation_period missing for persons with no events [HIGH]

**File**: `inst/sql/60_cdm_derived/80_load_observation_period.sql`

The observation_period query derives start/end dates from `MIN/MAX` of all clinical event dates. Persons who exist in `cdm.person` but have zero events (visits, conditions, drugs, measurements, observations, procedures, or death) get **no observation_period row**. The OMOP CDM requires every person to have at least one observation_period.

```sql
-- Current: only persons WITH events get observation_period
SELECT person_id, MIN(dt) AS start_date, MAX(dt) AS end_date
FROM all_dates
GROUP BY person_id
```

**Fix**: Add a `UNION ALL` fallback that creates a 1-day observation_period for persons with no events, using their birth date or a sentinel date.

---

### BUG-3: Missing DESCRIPTION dependencies [HIGH]

**File**: `R/init_vocabulary_db.R` lines 24–27, 114–115

Uses `checkmate::assertCharacter`, `checkmate::assertChoice`, `stringr::str_subset`, and `stringr::str_replace_all` but neither `checkmate` nor `stringr` appears in DESCRIPTION `Imports` or `Suggests`. The package will fail `R CMD check` and may error at runtime if these packages aren't already installed.

**Fix**: Add to DESCRIPTION Imports:
```
checkmate,
stringr
```

---

### BUG-4: condition_occurrence_id non-deterministic across runs [MEDIUM]

**File**: `inst/sql/50_cdm_clinical/60_load_condition_occurrence.sql` lines 8–11

```sql
ROW_NUMBER() OVER (ORDER BY member_id, problem_code, onset_date,
    encounter_id, problem_description, provider_id, resolution_date) AS rn
```

The ROW_NUMBER is ordered by all columns, but this ordering is non-deterministic for ties (rows with identical values in all listed columns). If source data order changes between runs, the same condition record may get different `rn` values, generating different `condition_occurrence_id`s. Combined with the `NOT EXISTS` idempotency check, this could insert duplicates.

**Fix**: Add a tie-breaking column (e.g., `ctid` or a hash of all columns) to ensure deterministic ordering, or use `DENSE_RANK` with a natural key partition.

---

### BUG-5: drug_era gap_days can go negative with overlapping sub-exposures [MEDIUM]

**File**: `inst/sql/60_cdm_derived/82_load_drug_era.sql` line 203

```sql
DATE_DIFF('day', MIN(drug_sub_exposure_start_date), drug_era_end_date)
  - SUM(days_exposed) AS gap_days
```

If sub-exposures overlap, `SUM(days_exposed)` double-counts the overlapping portion, causing `gap_days` to go negative. This matches the standard OHDSI reference implementation behavior (it has the same limitation), so it may be acceptable, but should be documented.

**Fix**: Add a comment noting this is expected OHDSI behavior, or cap at 0: `GREATEST(..., 0) AS gap_days`.

---

### BUG-6: Fulfillment drug mapping joins on drug_name from orders [MEDIUM]

**File**: `inst/sql/50_cdm_clinical/62_load_drug_exposure_fulfillment.sql` lines 16–22

Fulfillment records join to `medication_orders` by `order_id` to get `drug_name` and `drug_ndc_normalized`, then join to `map_drug_order` on those columns. This works correctly when the order exists, but:

1. When `medication_orders` has no matching order, `drug_name` is NULL and the drug mapping falls through to concept_id = 0
2. A reject table captures orphaned fulfillments (lines 44–47) but the rows are still loaded with concept_id = 0

This is acceptable design (load what we can, track rejects), but should be documented.

---

### BUG-7: `Sys.sleep(0.5)` hardcoded rate limiting [LOW]

**File**: `R/improve_mappings.R` line 296

A fixed 0.5-second sleep between API calls. This is fragile — too slow for generous rate limits, too fast for strict ones. Not a correctness bug but degrades user experience.

**Fix**: Make configurable via parameter or use adaptive backoff based on response headers.

---

## 2. Architecture Issues

### ARCH-1: Duplicated API client patterns [MEDIUM]

**Files**: `R/hecate_client.R`, `R/omophub_client.R`

Both clients have identical error-handling, retry logic, and HTTP request patterns. A shared `api_request()` utility would reduce duplication by ~40 lines and ensure consistent behavior.

---

### ARCH-2: Configuration inconsistency [MEDIUM]

Configuration flows through three different mechanisms:
- `default_etl_config()` list (for SQL runner)
- Environment variables (`OPENAI_API_KEY`, `HECATE_BASE_URL`, etc.)
- Function parameters with hardcoded defaults

This makes it hard to know what's configurable and how. Consider a unified config approach (e.g., one config list with env-var fallbacks for all settings).

---

### ARCH-3: SQL schema substitution is regex-based [MEDIUM]

**File**: `R/sql_runner.R` lines 82–87

```r
sql <- gsub("\\bcdm\\.", paste0(cdm, "."), sql)
```

This regex replaces all occurrences of `cdm.` with the configured schema, but could match inside string literals or comments. The comment-stripping on line 132 helps, but string literals containing `cdm.` would still be affected. In practice this hasn't caused issues because the SQL files don't contain such strings, but it's brittle.

---

### ARCH-4: Export functions follow repetitive pattern [LOW]

**Files**: `R/export_unmapped_units.R`, `R/export_unmapped_drugs.R`, `R/export_unmapped_measurement_values.R`

All three follow the same structure: query → data.frame → write.csv. Could be refactored into a single parameterized function with a query and output path.

---

## 3. Dead Code

### DEAD-1: Empty line in empty_log_df() [LOW]

**File**: `R/improve_mappings.R` line 346

A stray blank line was introduced between the function name and body during the repair_mapping_log refactor. Cosmetic only.

### DEAD-2: `split_sql_statements` documented but not exported [LOW]

**File**: `R/sql_runner.R` lines 181–187, `man/split_sql_statements.Rd`

Has a `.Rd` documentation file but is not in NAMESPACE exports. Either remove the doc or export the function. Currently it's an internal helper, so removing the `.Rd` is appropriate.

---

## 4. ETL Logic Quality

### What's done well

- **3-layer architecture** (src → stg → cdm) is clean and traceable
- **Deterministic ID generation** with offset ranges (300M for measurements, 700M for drug orders, 800M for conditions, etc.) prevents collisions across domains
- **Idempotency via NOT EXISTS** — most CDM load statements check before inserting
- **Reject tables** — failed records are captured (e.g., `stg.reject_condition_missing_person`, `stg.reject_fulfillment_no_order`)
- **Custom mapping fallback** — all CDM loads check `stg.custom_concept_mapping` as a last resort before defaulting to concept_id = 0
- **QC suite** — 6 QC scripts check row counts, mapping coverage, referential integrity, parse failures, required fields, and domain conformance
- **Era tables** — standard OHDSI algorithm correctly converted to DuckDB syntax
- **Pattern-based measurement value mapping** — common screening phrases (no abnormal, negative for) get mapped to Normal/Negative concepts
- **NLP extraction** — SOAP note parsing and entity extraction is well-designed for the Delphi data format

### Areas for improvement

| Area | Current State | Recommendation |
|------|--------------|----------------|
| **Vocabulary validation** | Type concept IDs (32818, 38000177, etc.) are hardcoded | Validate against cdm.concept at ETL start |
| **Drug mapping coverage** | NDC → RxNorm via vocabulary, name fallback via Hecate CSV | Consider adding RxNorm Approximate Match as additional fallback |
| **Measurement value mapping** | Pattern-based + custom + LLM-improved | The 11 LIKE patterns cover ~300K records; comprehensive for Delphi data |
| **Error recovery** | from_step/to_step for partial re-runs | Works well; add checkpointing for the LLM mapping loop |
| **Logging** | etl_run_log + etl_step_log tables | Good coverage; consider adding row-level audit trail |

---

## 5. Test Coverage

### Current state

| Component | Tests? | Coverage |
|-----------|--------|----------|
| `run_etl()` | Yes | Good — config, dry_run, from/to_step |
| `sql_runner.R` | Yes | Good — split_sql_statements, run_sql_scripts |
| `analyze_mapping_quality()` | Partial | Requires external DB |
| `improve_mappings()` | No | Complex function, needs mocked API tests |
| `extract_note_nlp()` | No | Needs mocked API tests |
| API clients | No | Need mocked HTTP tests |
| NDC utilities | No | Need unit tests for normalization edge cases |
| Export functions | No | Need integration tests |
| SQL logic | No | Would benefit from dbt-style test fixtures |

### Recommendations

1. **Add mocked LLM tests** for `improve_mappings()` using `httptest2` or `webmockr`
2. **Add NDC normalization tests** — edge cases like 10-digit vs 11-digit, asterisk handling
3. **Add SQL integration tests** — create small fixture data, run ETL, verify output counts and key values
4. **Remove external DB dependency** from `test-analyze_mapping_quality.R`

---

## 6. Suggested Improvements

### Quick wins (< 1 hour each)

1. **Rename `35_nlp/` → `45_nlp/`** — fixes the execution order bug
2. **Add `checkmate` and `stringr` to DESCRIPTION** — fixes R CMD check
3. **Remove `man/split_sql_statements.Rd`** — or add to NAMESPACE if intended public
4. **Add `GREATEST(..., 0)` to gap_days** in drug_era SQL
5. **Add observation_period fallback** for persons with no events

### Medium effort (1–4 hours each)

6. **Extract shared API client utility** from hecate/omophub clients
7. **Add adaptive rate limiting** to improve_mappings (use Retry-After header)
8. **Parameterize the 500-row LIMIT** in analyze_mapping_quality queries
9. **Add NDC normalization unit tests** covering edge cases

### Larger effort (1+ day)

10. **Create SQL integration test suite** with fixture data
11. **Add mocked LLM integration tests** for improve_mappings
12. **Unify configuration** into a single config-with-env-fallback system
13. **Add drug mapping coverage analysis** to identify which drugs are failing at each mapping stage (NDC → vocabulary → name → custom → LLM)

---

## 7. Security Notes

- **SQL injection risk is low** — the SQL files use static queries with schema substitution only. Dynamic SQL in `improve_mappings.R` uses manual quote escaping (`gsub("'", "''", ...)`), which is adequate for the controlled input (source values from the database itself, not user input).
- **API keys** are read from environment variables, not hardcoded. Good practice.
- **No sensitive data** is logged to the mapping improvement log (only source codes, concept IDs, and reasoning text).

---

## 8. Summary of Recommended Actions

### Must fix before production use
1. ~~BUG-1~~: Rename `35_nlp/` → `45_nlp/` (execution order) — **FIXED**
2. ~~BUG-3~~: Add `checkmate`, `stringr` to DESCRIPTION — **FIXED**
3. ~~BUG-2~~: Add observation_period for event-less persons — **FIXED**

### Should fix
4. BUG-4: Make condition_occurrence_id deterministic
5. ~~BUG-5~~: Document or cap gap_days — **FIXED** (capped with `GREATEST(..., 0)`)
6. ~~ARCH-1~~: Extract shared API client utility — **FIXED** (`api_utils.R`)
7. ARCH-2: Unify configuration approach — partial fix: `resolve_schema()` helper added
8. Add test coverage for core mapping functions

### Nice to have
9. ~~DEAD-2~~: Clean up split_sql_statements docs — **FIXED** (removed orphaned `.Rd`)
10. ~~ARCH-4~~: Refactor repetitive export functions — **FIXED** (`resolve_schema()` + removed `:::` calls)
11. ~~BUG-7~~: Adaptive rate limiting — **FIXED** (configurable `delay` parameter)
12. SQL integration test suite
