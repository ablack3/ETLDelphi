-- Load cdm.visit_occurrence from stg.encounter + map_visit + map_person + map_visit_concept. visit_type_concept_id = 32817 (EHR).
-- Reject encounters whose Member_ID not in map_person.
INSERT INTO cdm.visit_occurrence (
    visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_datetime,
    visit_end_date, visit_end_datetime, visit_type_concept_id, provider_id, care_site_id, visit_source_value
)
SELECT
    mv.visit_occurrence_id,
    mp.person_id,
    COALESCE(vc.visit_concept_id, 9202),
    e.encounter_date,
    e.encounter_datetime,
    COALESCE(e.encounter_date, e.encounter_datetime::DATE),
    e.encounter_datetime,
    32817,
    mpr.provider_id,
    cs.care_site_id,
    e.encounter_id
FROM stg.encounter e
JOIN stg.map_visit mv ON mv.encounter_id_source = e.encounter_id
JOIN stg.map_person mp ON mp.member_id = e.member_id
LEFT JOIN stg.map_visit_concept vc ON (vc.appt_type = LOWER(TRIM(e.appt_type)) OR (vc.appt_type IS NULL AND e.appt_type IS NULL)) AND (vc.clinic_type = LOWER(TRIM(e.clinic_type)) OR (vc.clinic_type IS NULL AND e.clinic_type IS NULL))
LEFT JOIN stg.map_provider mpr ON mpr.provider_id_source = e.provider_id
LEFT JOIN stg.map_care_site cs ON cs.care_site_key = TRIM(e.clinic_id)
WHERE e.encounter_date IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM cdm.visit_occurrence c WHERE c.visit_occurrence_id = mv.visit_occurrence_id);

CREATE OR REPLACE TABLE stg.reject_visit_missing_person AS
SELECT e.encounter_id, e.member_id
FROM stg.encounter e
WHERE e.member_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM stg.map_person mp WHERE mp.member_id = e.member_id);
