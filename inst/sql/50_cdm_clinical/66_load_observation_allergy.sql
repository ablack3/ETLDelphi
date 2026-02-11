-- Load observation from stg.allergy. observation_concept_id from map_allergy (0 in v1). observation_type_concept_id 32859.
INSERT INTO cdm.observation (
    observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id,
    value_as_string, qualifier_source_value, observation_source_value, visit_occurrence_id
)
WITH a AS (
    SELECT
        al.*,
        200000000 + ROW_NUMBER() OVER (ORDER BY al.member_id, al.allergen, al.drug_code, al.onset_date) AS observation_id
    FROM stg.allergy al
)
SELECT
    a.observation_id,
    mp.person_id,
    0,
    a.onset_date,
    32859,
    SUBSTR(a.reaction, 1, 60),
    SUBSTR(a.severity_description, 1, 50),
    SUBSTR(COALESCE(a.allergen, a.drug_code), 1, 50),
    NULL
FROM a
JOIN stg.map_person mp ON mp.member_id = a.member_id
WHERE a.onset_date IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM cdm.observation o WHERE o.observation_id = a.observation_id);

CREATE OR REPLACE TABLE stg.reject_observation_allergy_missing AS
SELECT member_id, allergen, drug_code, onset_date
FROM stg.allergy
WHERE onset_date IS NULL
   OR NOT EXISTS (SELECT 1 FROM stg.map_person mp WHERE mp.member_id = stg.allergy.member_id);
