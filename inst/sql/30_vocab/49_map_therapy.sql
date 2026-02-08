-- Therapy (procedure): Code + Vocabulary -> procedure_concept_id. CPT/HCPCS/SNOMED via cdm.concept. Else 0.
CREATE OR REPLACE TABLE stg.map_therapy AS
SELECT DISTINCT
    t.code,
    t.vocabulary,
    COALESCE(std.concept_id, 0) AS procedure_concept_id,
    COALESCE(src.concept_id, 0) AS procedure_source_concept_id
FROM (
    SELECT code, vocabulary FROM stg.therapy_orders WHERE code IS NOT NULL AND TRIM(code) <> ''
    UNION
    SELECT code, vocabulary FROM stg.therapy_actions WHERE code IS NOT NULL AND TRIM(code) <> ''
) t
LEFT JOIN cdm.concept src
    ON src.concept_code = t.code
   AND src.vocabulary_id IN ('CPT4', 'HCPCS', 'SNOMED', 'ICD10PCS')
   AND src.invalid_reason IS NULL
LEFT JOIN cdm.concept_relationship cr
    ON cr.concept_id_1 = src.concept_id AND cr.relationship_id = 'Maps to' AND cr.invalid_reason IS NULL
LEFT JOIN cdm.concept std ON std.concept_id = cr.concept_id_2 AND std.standard_concept = 'S';
