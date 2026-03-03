-- Load custom NDC -> drug_concept_id overrides from CSV (drug_ndc_normalized, drug_concept_id).
-- Normalize NDC: strip dashes, spaces, and '*' to match concept_code (digits-only) in NDC vocabulary.
-- One row per normalized NDC (last CSV row wins if duplicates).
-- Ignore overrides that do not point to a standard, valid Drug concept in the loaded vocabulary.
-- Use when vocabulary NDC mapping is missing or wrong. Path from config custom_ndc_mapping_path; default inst/extdata/custom_ndc_mapping.csv.
CREATE OR REPLACE TABLE stg.custom_ndc_mapping (
    drug_ndc_normalized VARCHAR,
    drug_concept_id INTEGER
);
INSERT INTO stg.custom_ndc_mapping
SELECT normalized AS drug_ndc_normalized, drug_concept_id
FROM (
  SELECT
    raw.normalized,
    raw.drug_concept_id,
    ROW_NUMBER() OVER (PARTITION BY raw.normalized ORDER BY raw.file_row_number DESC) AS rn
  FROM (
    SELECT
      REPLACE(REPLACE(REPLACE(TRIM(CAST(src.drug_ndc_normalized AS VARCHAR)), '-', ''), ' ', ''), '*', '') AS normalized,
      TRY_CAST(src.drug_concept_id AS INTEGER) AS drug_concept_id,
      ROW_NUMBER() OVER () AS file_row_number
    FROM read_csv('@customNdcMappingPath', header = true, auto_detect = true) AS src
  ) raw
  JOIN cdm.concept c
    ON c.concept_id = raw.drug_concept_id
   AND c.standard_concept = 'S'
   AND c.invalid_reason IS NULL
   AND c.domain_id = 'Drug'
  WHERE raw.drug_concept_id IS NOT NULL AND raw.drug_concept_id > 0
) sub
WHERE rn = 1;
