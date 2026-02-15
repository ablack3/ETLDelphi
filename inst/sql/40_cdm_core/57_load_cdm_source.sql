-- Populate cdm_source from package vignette content (etl.Rmd, how_to_run.Rmd, run_etl_steps.Rmd).
-- Single row describing this CDM instance and the ETLDelphi transformation.
INSERT INTO cdm.cdm_source (
    cdm_source_name,
    cdm_source_abbreviation,
    cdm_holder,
    source_description,
    source_documentation_reference,
    cdm_etl_reference,
    source_release_date,
    cdm_release_date,
    cdm_version,
    cdm_version_concept_id,
    vocabulary_version
)
SELECT
    'Delphi/Delfi to OMOP CDM v5.4 (ETLDelphi)' AS cdm_source_name,
    'ETLDelphi' AS cdm_source_abbreviation,
    'ETLDelphi' AS cdm_holder,
    'ETL transforms Delphi/Delfi source tables (enrollment, encounter, provider, death, problem, medication_orders, medication_fulfillment, current_medications, immunization, lab_orders, lab_results, vital_sign, allergy, therapy_orders, therapy_actions) into OMOP CDM v5.4 in DuckDB. Three-layer pattern: src (raw) -> stg (typed, normalized) -> cdm (OMOP). Deterministic integer IDs; unmapped concepts = 0; reject and QC tables for observability.' AS source_description,
    'ETLDelphi package vignettes: etl.Rmd, how_to_run.Rmd, run_etl_steps.Rmd' AS source_documentation_reference,
    'ETLDelphi R package - inst/sql/ (00_admin through 90_qc)' AS cdm_etl_reference,
    CURRENT_DATE AS source_release_date,
    CURRENT_DATE AS cdm_release_date,
    '5.4' AS cdm_version,
    COALESCE(
        (SELECT concept_id FROM cdm.concept WHERE vocabulary_id = 'CDM' AND concept_class_id = 'CDM' AND concept_code = '5.4' LIMIT 1),
        (SELECT concept_id FROM cdm.concept WHERE vocabulary_id = 'CDM' LIMIT 1)
    ) AS cdm_version_concept_id,
    COALESCE((SELECT vocabulary_version FROM cdm.vocabulary WHERE vocabulary_id = 'None' LIMIT 1), 'Unknown') AS vocabulary_version
WHERE NOT EXISTS (SELECT 1 FROM cdm.cdm_source LIMIT 1);
