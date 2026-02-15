-- LOINC mapping for labs: Test_LOINC -> source concept (LOINC), then concept_relationship 'Maps to' for standard measurement_concept_id.
-- measurement_source_concept_id = LOINC concept; measurement_concept_id = standard concept (concept_id_2 from Maps to).
CREATE OR REPLACE TABLE stg.map_loinc_measurement AS
SELECT DISTINCT
    lr.test_loinc AS loinc_code,
    COALESCE(std.concept_id, 0) AS measurement_concept_id,
    COALESCE(src.concept_id, 0) AS measurement_source_concept_id
FROM (SELECT DISTINCT test_loinc FROM stg.lab_results WHERE test_loinc IS NOT NULL AND TRIM(test_loinc) <> '') lr
LEFT JOIN cdm.concept src
    ON src.vocabulary_id = 'LOINC' AND src.concept_code = lr.test_loinc AND src.invalid_reason IS NULL
LEFT JOIN cdm.concept_relationship cr
    ON cr.concept_id_1 = src.concept_id
   AND cr.relationship_id = 'Maps to'
   AND cr.invalid_reason IS NULL
LEFT JOIN cdm.concept std ON std.concept_id = cr.concept_id_2 AND std.standard_concept = 'S';
