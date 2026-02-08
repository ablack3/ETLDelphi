-- Deterministic person_id from distinct member_id across all source tables.
-- Enrollment may not contain all members; union Member_ID from every table that has it.
WITH all_members AS (
    SELECT DISTINCT member_id FROM stg.enrollment WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.encounter WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.death WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.problem WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.medication_orders WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.immunization WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.lab_results WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.vital_sign WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.allergy WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.therapy_orders WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.therapy_actions WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
    UNION
    SELECT DISTINCT member_id FROM stg.current_medications WHERE member_id IS NOT NULL AND TRIM(member_id) <> ''
)
CREATE OR REPLACE TABLE stg.map_person AS
SELECT
    member_id,
    ROW_NUMBER() OVER (ORDER BY member_id) AS person_id
FROM (SELECT DISTINCT member_id FROM all_members) t;
