-- LOINC mapping for labs: Test_LOINC -> measurement_concept_id (cdm.concept vocabulary LOINC).
-- If concept in Measurement domain use it; else store source and target 0. For simplicity we use concept_id as measurement_concept_id when found.
CREATE OR REPLACE TABLE stg.map_loinc_measurement AS
SELECT DISTINCT
    lr.test_loinc AS loinc_code,
    COALESCE(c.concept_id, 0) AS measurement_concept_id,
    COALESCE(c.concept_id, 0) AS measurement_source_concept_id
FROM (SELECT DISTINCT test_loinc FROM stg.lab_results WHERE test_loinc IS NOT NULL AND TRIM(test_loinc) <> '') lr
LEFT JOIN cdm.concept c ON c.vocabulary_id = 'LOINC' AND c.concept_code = lr.test_loinc AND c.invalid_reason IS NULL;
