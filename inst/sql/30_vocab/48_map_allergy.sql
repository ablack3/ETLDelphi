-- Allergy: v1 observation_concept_id=0, observation_source_value = Allergen or Drug_Code. Optional drug mapping later.
CREATE OR REPLACE TABLE stg.map_allergy AS
SELECT DISTINCT
    COALESCE(allergen, drug_code) AS source_value,
    0 AS observation_concept_id
FROM stg.allergy
WHERE (allergen IS NOT NULL AND TRIM(allergen) <> '') OR (drug_code IS NOT NULL AND TRIM(drug_code) <> '');
