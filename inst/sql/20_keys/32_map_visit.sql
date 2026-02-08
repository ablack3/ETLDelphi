-- Deterministic visit_occurrence_id from Encounter_ID.
CREATE OR REPLACE TABLE stg.map_visit AS
SELECT
    encounter_id_source,
    ROW_NUMBER() OVER (ORDER BY encounter_id_source) AS visit_occurrence_id
FROM (
    SELECT DISTINCT encounter_id AS encounter_id_source
    FROM stg.encounter
    WHERE encounter_id IS NOT NULL AND TRIM(encounter_id) <> ''
) t;
