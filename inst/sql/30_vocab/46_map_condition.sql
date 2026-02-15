-- Condition mapping: Problem_Code is ICD-9-CM. Join to concept on source value (concept_code),
-- vocabulary_id = ICD9CM, then concept_relationship 'Maps to' for standard concept_id.
-- condition_source_value = Problem_Code. Unmapped -> 0.
CREATE OR REPLACE TABLE stg.map_condition AS
SELECT DISTINCT
    p.problem_code,
    COALESCE(std.concept_id, 0) AS condition_concept_id,
    COALESCE(src.concept_id, 0) AS condition_source_concept_id
FROM (SELECT DISTINCT problem_code FROM stg.problem WHERE problem_code IS NOT NULL AND TRIM(problem_code) <> '') p
LEFT JOIN cdm.concept src
    ON src.concept_code = TRIM(p.problem_code)
   AND src.vocabulary_id = 'ICD9CM'
   AND src.invalid_reason IS NULL
LEFT JOIN cdm.concept_relationship cr
    ON cr.concept_id_1 = src.concept_id
   AND cr.relationship_id = 'Maps to'
   AND cr.invalid_reason IS NULL
LEFT JOIN cdm.concept std ON std.concept_id = cr.concept_id_2 AND std.standard_concept = 'S';
