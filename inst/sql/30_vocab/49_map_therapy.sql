-- Therapy (procedure): Code + Vocabulary -> procedure_concept_id. CPT/HCPCS/SNOMED via cdm.concept. Else 0.
-- Deduplicate: one (code, vocabulary) may map to multiple concepts via "Maps to"; pick one per (code, vocabulary).
CREATE OR REPLACE TABLE stg.map_therapy AS
SELECT code, vocabulary, procedure_concept_id, procedure_source_concept_id
FROM (
    SELECT
        t.code,
        t.vocabulary,
        COALESCE(std.concept_id, 0) AS procedure_concept_id,
        COALESCE(src.concept_id, 0) AS procedure_source_concept_id,
        ROW_NUMBER() OVER (
            PARTITION BY t.code, t.vocabulary
            ORDER BY
                CASE WHEN std.concept_id IS NULL THEN 1 ELSE 0 END,
                std.concept_id NULLS LAST,
                src.concept_id NULLS LAST
        ) AS rn
    FROM (
        SELECT
            code,
            vocabulary,
            CASE
                WHEN vocabulary IS NULL OR TRIM(vocabulary) = '' THEN NULL
                WHEN UPPER(TRIM(vocabulary)) IN ('CPT', 'CPT4') THEN 'CPT4'
                WHEN UPPER(TRIM(vocabulary)) = 'HCPCS' THEN 'HCPCS'
                WHEN UPPER(TRIM(vocabulary)) IN ('SNOMED', 'SNOMEDCT', 'SNOMED CT') THEN 'SNOMED'
                WHEN UPPER(TRIM(vocabulary)) IN ('ICD10PCS', 'ICD-10-PCS', 'ICD10-PCS') THEN 'ICD10PCS'
                WHEN UPPER(TRIM(vocabulary)) IN ('ICD9PROC', 'ICD9PROCEDURE', 'ICD-9-PROC', 'ICD-9-PROCEDURE', 'ICD9CMPROC', 'ICD-9-CM VOL 3') THEN 'ICD9Proc'
                ELSE NULL
            END AS vocabulary_id
        FROM stg.therapy_orders
        WHERE code IS NOT NULL AND TRIM(code) <> ''
        UNION
        SELECT
            code,
            vocabulary,
            CASE
                WHEN vocabulary IS NULL OR TRIM(vocabulary) = '' THEN NULL
                WHEN UPPER(TRIM(vocabulary)) IN ('CPT', 'CPT4') THEN 'CPT4'
                WHEN UPPER(TRIM(vocabulary)) = 'HCPCS' THEN 'HCPCS'
                WHEN UPPER(TRIM(vocabulary)) IN ('SNOMED', 'SNOMEDCT', 'SNOMED CT') THEN 'SNOMED'
                WHEN UPPER(TRIM(vocabulary)) IN ('ICD10PCS', 'ICD-10-PCS', 'ICD10-PCS') THEN 'ICD10PCS'
                WHEN UPPER(TRIM(vocabulary)) IN ('ICD9PROC', 'ICD9PROCEDURE', 'ICD-9-PROC', 'ICD-9-PROCEDURE', 'ICD9CMPROC', 'ICD-9-CM VOL 3') THEN 'ICD9Proc'
                ELSE NULL
            END AS vocabulary_id
        FROM stg.therapy_actions
        WHERE code IS NOT NULL AND TRIM(code) <> ''
    ) t
    LEFT JOIN cdm.concept src
        ON src.concept_code = t.code
       AND src.vocabulary_id IN ('CPT4', 'HCPCS', 'SNOMED', 'ICD10PCS', 'ICD9Proc')
       AND (t.vocabulary_id IS NULL OR src.vocabulary_id = t.vocabulary_id)
       AND src.invalid_reason IS NULL
    LEFT JOIN cdm.concept_relationship cr
        ON cr.concept_id_1 = src.concept_id AND cr.relationship_id = 'Maps to' AND cr.invalid_reason IS NULL
    LEFT JOIN cdm.concept std ON std.concept_id = cr.concept_id_2 AND std.standard_concept = 'S'
) sub
WHERE rn = 1;
