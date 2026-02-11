-- Allergy: map by drug_code (CVX, NDC) via concept_code, or by text (RxNorm concept_name, drug_name_to_concept).
-- map_allergy_code: drug_code + drug_vocab -> concept via concept_code, "Maps to" for standard.
-- map_allergy: source_value (text) -> concept via concept_name or drug_name_to_concept.
CREATE OR REPLACE TABLE stg.map_allergy_code AS
SELECT DISTINCT
    c.drug_code,
    c.drug_vocab,
    COALESCE(std.concept_id, src.concept_id, 0) AS observation_concept_id
FROM (
    SELECT
        TRIM(drug_code) AS drug_code,
        TRIM(UPPER(drug_vocab)) AS drug_vocab,
        CASE WHEN TRIM(UPPER(drug_vocab)) = 'NDC'
             THEN REPLACE(REPLACE(TRIM(drug_code), '-', ''), ' ', '')
             ELSE TRIM(drug_code)
        END AS drug_code_normalized
    FROM stg.allergy
    WHERE drug_code IS NOT NULL AND TRIM(drug_code) <> ''
      AND drug_vocab IS NOT NULL AND TRIM(UPPER(drug_vocab)) IN ('CVX', 'NDC')
) c
LEFT JOIN cdm.concept src
    ON src.concept_code = c.drug_code_normalized
   AND src.vocabulary_id = c.drug_vocab
   AND src.invalid_reason IS NULL
LEFT JOIN cdm.concept_relationship cr
    ON cr.concept_id_1 = src.concept_id AND cr.relationship_id = 'Maps to' AND cr.invalid_reason IS NULL
LEFT JOIN cdm.concept std ON std.concept_id = cr.concept_id_2 AND std.standard_concept = 'S';

CREATE OR REPLACE TABLE stg.map_allergy AS
SELECT source_value, observation_concept_id
FROM (
    SELECT
        a.source_value,
        COALESCE(std_rx.concept_id, src_rx.concept_id, std_cvx.concept_id, src_cvx.concept_id, dnc.concept_id, 0) AS observation_concept_id,
        ROW_NUMBER() OVER (PARTITION BY a.source_value ORDER BY
            CASE WHEN std_rx.concept_id IS NOT NULL THEN 0 WHEN std_cvx.concept_id IS NOT NULL THEN 1 WHEN src_rx.concept_id IS NOT NULL THEN 2 WHEN src_cvx.concept_id IS NOT NULL THEN 3 WHEN dnc.concept_id IS NOT NULL THEN 4 ELSE 5 END,
            COALESCE(std_rx.concept_id, src_rx.concept_id, std_cvx.concept_id, src_cvx.concept_id, dnc.concept_id, 0) DESC
        ) AS rn
    FROM (
        SELECT DISTINCT TRIM(COALESCE(allergen, drug_code)) AS source_value
        FROM stg.allergy
        WHERE (allergen IS NOT NULL AND TRIM(allergen) <> '') OR (drug_code IS NOT NULL AND TRIM(drug_code) <> '')
    ) a
    LEFT JOIN cdm.concept src_rx
        ON LOWER(TRIM(src_rx.concept_name)) = LOWER(a.source_value)
       AND src_rx.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
       AND src_rx.concept_class_id IN ('Ingredient', 'Clinical Drug', 'Branded Drug')
       AND src_rx.invalid_reason IS NULL
    LEFT JOIN cdm.concept_relationship cr_rx
        ON cr_rx.concept_id_1 = src_rx.concept_id AND cr_rx.relationship_id = 'Maps to' AND cr_rx.invalid_reason IS NULL
    LEFT JOIN cdm.concept std_rx ON std_rx.concept_id = cr_rx.concept_id_2 AND std_rx.standard_concept = 'S'
    LEFT JOIN cdm.concept src_cvx
        ON LOWER(TRIM(src_cvx.concept_name)) = LOWER(a.source_value)
       AND src_cvx.vocabulary_id = 'CVX'
       AND src_cvx.invalid_reason IS NULL
    LEFT JOIN cdm.concept_relationship cr_cvx
        ON cr_cvx.concept_id_1 = src_cvx.concept_id AND cr_cvx.relationship_id = 'Maps to' AND cr_cvx.invalid_reason IS NULL
    LEFT JOIN cdm.concept std_cvx ON std_cvx.concept_id = cr_cvx.concept_id_2 AND std_cvx.standard_concept = 'S'
    LEFT JOIN stg.drug_name_to_concept dnc ON LOWER(TRIM(dnc.drug_name)) = LOWER(a.source_value)
) sub
WHERE rn = 1;
