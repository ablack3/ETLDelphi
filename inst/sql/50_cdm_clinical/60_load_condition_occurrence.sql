-- Load cdm.condition_occurrence from stg.problem. condition_occurrence_id deterministic ROW_NUMBER over (Member_ID, Problem_Code, Onset_Date, Encounter_ID).
-- condition_start_date = onset_date; fallback to encounter_date if onset missing (via join to stg.encounter). condition_type_concept_id 32818.
WITH cond AS (
    SELECT
        p.member_id,
        p.problem_code,
        p.problem_description,
        p.onset_date,
        p.resolution_date,
        p.provider_id,
        p.encounter_id,
        e.encounter_date AS fallback_start_date
    FROM stg.problem p
    LEFT JOIN stg.encounter e ON e.encounter_id = p.encounter_id
),
ranked AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (ORDER BY c.member_id, c.problem_code, COALESCE(c.onset_date, c.fallback_start_date), c.encounter_id) + 800000000 AS condition_occurrence_id
    FROM cond c
)
INSERT INTO cdm.condition_occurrence (
    condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime,
    condition_end_date, condition_type_concept_id, provider_id, visit_occurrence_id,
    condition_source_value, condition_source_concept_id
)
SELECT
    r.condition_occurrence_id,
    mp.person_id,
    COALESCE(mc.condition_concept_id, 0),
    COALESCE(r.onset_date, r.fallback_start_date),
    NULL,
    r.resolution_date,
    32818,
    mpr.provider_id,
    mv.visit_occurrence_id,
    SUBSTR(COALESCE(r.problem_code, r.problem_description), 1, 50),
    COALESCE(mc.condition_source_concept_id, 0)
FROM ranked r
JOIN stg.map_person mp ON mp.member_id = r.member_id
LEFT JOIN stg.map_condition mc ON mc.problem_code = r.problem_code
LEFT JOIN stg.map_provider mpr ON mpr.provider_id_source = r.provider_id
LEFT JOIN stg.map_visit mv ON mv.encounter_id_source = r.encounter_id
WHERE COALESCE(r.onset_date, r.fallback_start_date) IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM cdm.condition_occurrence o WHERE o.condition_occurrence_id = r.condition_occurrence_id);

CREATE OR REPLACE TABLE stg.reject_condition_missing_person AS
SELECT p.member_id, p.problem_code
FROM stg.problem p
WHERE NOT EXISTS (SELECT 1 FROM stg.map_person mp WHERE mp.member_id = p.member_id);
