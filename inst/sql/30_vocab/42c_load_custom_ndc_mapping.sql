-- Load custom NDC -> drug_concept_id overrides from CSV (drug_ndc_normalized, drug_concept_id).
-- Use when vocabulary NDC mapping is missing or wrong. Path from config custom_ndc_mapping_path; default inst/extdata/custom_ndc_mapping.csv.
CREATE OR REPLACE TABLE stg.custom_ndc_mapping (
    drug_ndc_normalized VARCHAR,
    drug_concept_id INTEGER
);
INSERT INTO stg.custom_ndc_mapping
SELECT TRIM(CAST(drug_ndc_normalized AS VARCHAR)) AS drug_ndc_normalized, TRY_CAST(drug_concept_id AS INTEGER) AS drug_concept_id
FROM read_csv('@customNdcMappingPath', header = true, auto_detect = true)
WHERE TRY_CAST(drug_concept_id AS INTEGER) IS NOT NULL AND TRY_CAST(drug_concept_id AS INTEGER) > 0;
