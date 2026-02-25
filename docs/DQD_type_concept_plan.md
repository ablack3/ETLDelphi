# DQD type concept ID plan

## Summary

ETL type concept IDs (e.g. `note_type_concept_id`, `visit_type_concept_id`, `drug_type_concept_id`) are now **config-driven**. Placeholders like `{visit_type_concept_id}` in SQL are replaced at run time from `config$type_concept_ids`. You can override defaults via `run_etl(con, config = list(type_concept_ids = list(...)))` or a config YAML file.

## DQD failures addressed

| DQD check | Table / field | Fix |
|----------|----------------|-----|
| fkDomain / isStandardValidConcept | NOTE (note_type, note_class, encoding) | Set `note_type_concept_id`, `note_class_concept_id`, `encoding_concept_id` in config to **standard** concepts in the **Type Concept** domain from your CONCEPT table. |
| fkDomain / isStandardValidConcept | VISIT_OCCURRENCE (visit_concept_id, visit_type_concept_id) | Set `default_visit_concept_id` (Visit domain) and `visit_type_concept_id` (Type Concept domain) in config. |
| isStandardValidConcept | DRUG_EXPOSURE (drug_type_concept_id) | Defaults: orders 38000177, dispensed 38000230, immunization 38000280. Override if not in your vocabulary. |
| isStandardValidConcept | PROCEDURE_OCCURRENCE (procedure_type_concept_id) | Default 38000268. Override in config if needed. |
| isStandardValidConcept | OBSERVATION (observation_type_concept_id) | Default 32859 (allergy). Override in config if needed. |

## Not in scope (per your preference)

- **Implausible measurement units** – left as-is; real data often has mixed units.
- Other DQD failures (e.g. condition era dates &lt; 1950, drug_source_concept_id 0, small UNIT_CONCEPT_ID issues) can be handled later.

## How to fix remaining type-concept DQD failures

1. Query your CONCEPT table for standard, valid concepts in the right domain, for example:
   - `SELECT concept_id, concept_name, domain_id FROM concept WHERE domain_id = 'Type Concept' AND standard_concept = 'S' AND invalid_reason IS NULL;`
   - For visit_concept_id: `WHERE domain_id = 'Visit' AND standard_concept = 'S'`.
2. Override in config, e.g. in R:
   ```r
   config <- ETLDelphi::default_etl_config()
   config$type_concept_ids$note_type_concept_id <- 32818L   # example: EHR progress note
   config$type_concept_ids$note_class_concept_id <- 44814639L
   config$type_concept_ids$encoding_concept_id <- 44815386L
   ETLDelphi::run_etl(con, config = config)
   ```
3. Or use a YAML config file and pass `config_path` to `run_etl()`.

## Config keys (type_concept_ids)

All of these are substituted into SQL when present in `config$type_concept_ids`:

- `visit_type_concept_id`, `default_visit_concept_id`
- `condition_type_concept_id`
- `drug_type_orders`, `drug_type_dispensed`, `drug_type_immunization`
- `measurement_type_vitals`, `measurement_type_labs`
- `observation_type_allergy`
- `note_type_concept_id`, `note_class_concept_id`, `encoding_concept_id`, `language_concept_id`
- `procedure_type_concept_id`
- `period_type_concept_id`, `death_type_concept_id`
