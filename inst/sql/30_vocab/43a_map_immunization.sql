-- Vaccine mapping: vaccine_cvx -> concept_code (vocabulary_id CVX) -> source_concept_id.
-- Use concept_relationship 'Maps to' to get standard concept_id.
CREATE OR REPLACE TABLE stg.map_immunization AS
SELECT DISTINCT
    i.vaccine_cvx,
    COALESCE(std.concept_id, 0) AS drug_concept_id,
    COALESCE(src.concept_id, 0) AS drug_source_concept_id
FROM (
    SELECT DISTINCT TRIM(vaccine_cvx) AS vaccine_cvx
    FROM stg.immunization
    WHERE vaccine_cvx IS NOT NULL AND TRIM(vaccine_cvx) <> ''
) i
LEFT JOIN cdm.concept src
    ON src.concept_code = i.vaccine_cvx
   AND src.vocabulary_id = 'CVX'
   AND src.invalid_reason IS NULL
LEFT JOIN cdm.concept_relationship cr
    ON cr.concept_id_1 = src.concept_id
   AND cr.relationship_id = 'Maps to'
   AND cr.invalid_reason IS NULL
LEFT JOIN cdm.concept std ON std.concept_id = cr.concept_id_2 AND std.standard_concept = 'S';
