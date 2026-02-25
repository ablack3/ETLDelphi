-- Load custom NDC -> drug_concept_id overrides from CSV (drug_ndc_normalized, drug_concept_id).
-- Normalize NDC: strip dashes, spaces, and '*' to match concept_code (digits-only) in NDC vocabulary.
-- One row per normalized NDC (last concept_id wins if CSV has duplicates).
-- Use when vocabulary NDC mapping is missing or wrong. Path from config custom_ndc_mapping_path; default inst/extdata/custom_ndc_mapping.csv.
CREATE OR REPLACE TABLE stg.custom_ndc_mapping (
    drug_ndc_normalized VARCHAR,
    drug_concept_id INTEGER
);
INSERT INTO stg.custom_ndc_mapping
SELECT normalized AS drug_ndc_normalized, drug_concept_id
FROM (
  SELECT
    REPLACE(REPLACE(REPLACE(TRIM(CAST(drug_ndc_normalized AS VARCHAR)), '-', ''), ' ', ''), '*', '') AS normalized,
    TRY_CAST(drug_concept_id AS INTEGER) AS drug_concept_id,
    ROW_NUMBER() OVER (PARTITION BY REPLACE(REPLACE(REPLACE(TRIM(CAST(drug_ndc_normalized AS VARCHAR)), '-', ''), ' ', ''), '*', '') ORDER BY TRY_CAST(drug_concept_id AS INTEGER) DESC NULLS LAST) AS rn
  FROM read_csv('@customNdcMappingPath', header = true, auto_detect = true)
  WHERE TRY_CAST(drug_concept_id AS INTEGER) IS NOT NULL AND TRY_CAST(drug_concept_id AS INTEGER) > 0
) sub
WHERE rn = 1;
