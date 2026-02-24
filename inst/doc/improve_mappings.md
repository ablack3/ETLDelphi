# Improving Unmapped Concept Mappings

After running the ETL, some source values will have `concept_id = 0` (unmapped).
ETLDelphi provides two approaches to fix these: an automated LLM-powered loop and
manual mapping functions. Both write to the same CSV files that the ETL consumes on
the next run.

## Quick Start

```r
library(ETLDelphi)
con <- DBI::dbConnect(duckdb::duckdb(), "delphi.duckdb")

# Run the ETL first
run_etl(con)

# See what's unmapped (dry run)
improve_mappings(con, dry_run = TRUE, limit = 20)

# Run automated mapping on a small batch
improve_mappings(con, limit = 10, confidence_threshold = 0.7)

# Or add mappings manually
add_custom_mapping("dexamethasone", "drug", 1518254, ndc_code = "47202252901")

# Re-run ETL from vocab step to pick up new mappings
run_etl(con, from_step = "30_vocab")
```

## Automated Approach: `improve_mappings()`

The `improve_mappings()` function queries staging tables for unmapped source values,
sends each one to GPT-4 with Hecate (semantic vocabulary search) and OMOPHub (NDC
code lookup) as tool-calling functions, and writes high-confidence results to the
custom mapping CSVs.

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `con` | required | DBI connection to DuckDB (post-ETL) |
| `domains` | all 5 | Which domains to process: `"condition"`, `"drug"`, `"measurement"`, `"procedure"`, `"observation"` |
| `limit` | 50 | Max unmapped values per domain (top N by record count) |
| `confidence_threshold` | 0.7 | Minimum LLM confidence to write to production CSV |
| `force_retry` | `FALSE` | Retry control (see below) |
| `dry_run` | `FALSE` | Show what would be processed without calling LLM |
| `model` | `"gpt-4o"` | OpenAI model (or env var `OPENAI_MODEL`) |
| `log_path` | `"mapping_improvement_log.csv"` | Path to detailed log |

### Environment Variables

```bash
OPENAI_API_KEY=sk-...          # Required
OPENAI_MODEL=gpt-4o            # Optional, default gpt-4o
HECATE_API_KEY=...              # Required for vocabulary search
OMOPHUB_API_KEY=oh_...          # Optional, enables NDC code lookup for drugs
```

### Retry Strategies

The improvement log tracks every processed value. On re-run, already-logged values
are skipped automatically. The `force_retry` parameter controls this:

```r
# Default: skip all logged values, process only new ones
improve_mappings(con, limit = 50)

# Retry values that failed or had low confidence
improve_mappings(con, limit = 50, force_retry = "failed")

# Retry everything (ignore log entirely)
improve_mappings(con, limit = 50, force_retry = TRUE)
```

Successive runs with the default `force_retry = FALSE` will progress through
unmapped values in order of record count — the second run picks up where the
first left off.

### Rate Limits

If the OpenAI API returns HTTP 429 (rate limit), the loop stops immediately with a
clear message. Any mappings found before the limit are still saved. Run again later
to continue from where you left off.

### What Gets Written

| File | Content | When |
|------|---------|------|
| `mapping_improvement_log.csv` | Every processed value with confidence, reasoning, context | Always (incremental) |
| `inst/extdata/custom_concept_mapping.csv` | `source_value, domain, concept_id` | Confidence >= threshold |
| `inst/extdata/custom_ndc_mapping.csv` | `drug_ndc_normalized, drug_concept_id` | Drug NDC mappings only |

### How the LLM Uses Context

For each unmapped value, the LLM receives rich context from staging tables:

- **Drug**: drug name + NDC code (both). Cross-checks NDC lookup against the name.
- **Condition**: problem code + text description. Searches by ICD/SNOMED code, verifies with description.
- **Measurement**: LOINC code + test name. Searches by code, verifies with name.
- **Procedure**: procedure code + name + source vocabulary. Searches by code in the appropriate vocabulary.
- **Observation**: allergen name + drug code + vocabulary. Searches SNOMED for allergy concepts.

## Manual Approach: `add_custom_mapping()`

For known mappings or when the LLM gets it wrong, add mappings directly:

```r
# Single mapping
add_custom_mapping("dexamethasone", "drug", 1518254)

# Drug with NDC (writes to both CSVs)
add_custom_mapping("dexamethasone", "drug", 1518254, ndc_code = "47202252901")

# Batch
add_custom_mapping(
  source_value = c("dexamethasone", "valproic acid", "nifedipine"),
  domain       = "drug",
  concept_id   = c(1518254, 789578, 1318137),
  ndc_code     = c("47202252901", "00093063201", "00047007824")
)

# Other domains
add_custom_mapping("J06.9", "condition", 260139)
add_custom_mapping("Penicillin", "observation", 4084167)
```

### Important: Drug source_value

For drugs, `source_value` should be the **drug name** (not the NDC code). This is
because the CDM load SQL joins `custom_concept_mapping` on `drug_name`, not on
`drug_source_value` (which is `COALESCE(ndc, name)`).

If you have the NDC code, pass it as `ndc_code` — it gets written to
`custom_ndc_mapping.csv` separately, which the drug mapping SQL applies as an
override.

## Manual CSV Editing

You can also edit the CSV files directly:

### `inst/extdata/custom_concept_mapping.csv`

```csv
source_value,domain,concept_id
dexamethasone,drug,1518254
J06.9,condition,260139
```

### `inst/extdata/custom_ndc_mapping.csv`

```csv
drug_ndc_normalized,drug_concept_id
47202252901,1518254
00093063201,789578
```

After editing, re-run the ETL from the vocab step:

```r
run_etl(con, from_step = "30_vocab")
```

## Reviewing the Log

The improvement log (`mapping_improvement_log.csv`) contains full details for every
processed value:

```r
log <- read.csv("mapping_improvement_log.csv")

# High-confidence successes
log[log$confidence >= 0.7 & !is.na(log$concept_id) & log$concept_id > 0, ]

# Failures worth manual review
log[log$confidence < 0.5 & log$record_count > 100, c("source_value", "domain", "source_name", "record_count", "reasoning")]

# Drug NDC mappings
log[log$source_is_ndc == TRUE, c("source_value", "source_name", "ndc_normalized", "concept_name", "confidence")]
```

## Finding OMOP Concept IDs

To find the correct concept_id for manual mappings:

- **Athena**: https://athena.ohdsi.org/ — search by name or code
- **Hecate**: Use the search tool in R:
  ```r
  hc <- hecate_client()
  hecate_search("dexamethasone", domain_id = "Drug", standard_concept = "S", client = hc)
  ```

## Full Workflow Example

```r
library(ETLDelphi)
con <- DBI::dbConnect(duckdb::duckdb(), "delphi.duckdb")

# 1. Run ETL
run_etl(con)

# 2. Check mapping coverage
analyze_mapping_quality(con)

# 3. See what's unmapped
improve_mappings(con, dry_run = TRUE, limit = 20, domains = "drug")

# 4. Run automated loop
improve_mappings(con, limit = 20, domains = "drug")

# 5. Review log, fix any bad mappings manually
log <- read.csv("mapping_improvement_log.csv")
# ... review ...

# 6. Add manual mappings for values the LLM missed
add_custom_mapping("OMS", "drug", 1110410, ndc_code = "33413005004")

# 7. Re-run ETL to pick up all new mappings
run_etl(con, from_step = "30_vocab")

# 8. Check improved coverage
analyze_mapping_quality(con)
```
