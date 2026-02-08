-- Load cdm.person from stg.map_person + stg.enrollment. year_of_birth NOT NULL: only insert when DOB parseable.
-- Reject persons missing DOB (log to stg.reject_person_missing_dob or skip). Use enrollment for demographics; fallback encounter.Patient_DOB not in stg.
INSERT INTO cdm.person (
    person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth, birth_datetime,
    race_concept_id, ethnicity_concept_id, location_id, person_source_value, gender_source_value, race_source_value
)
SELECT
    mp.person_id,
    COALESCE(g.gender_concept_id, 0),
    EXTRACT(YEAR FROM e.dob_date)::INTEGER,
    EXTRACT(MONTH FROM e.dob_date)::INTEGER,
    EXTRACT(DAY FROM e.dob_date)::INTEGER,
    e.dob_datetime,
    COALESCE(r.race_concept_id, 0),
    0,
    loc.location_id,
    mp.member_id,
    e.gender_clean,
    e.race_clean
FROM stg.map_person mp
JOIN stg.enrollment e ON e.member_id = mp.member_id AND e.dob_date IS NOT NULL
LEFT JOIN stg.map_gender g ON g.gender_source_value = e.gender_clean
LEFT JOIN stg.map_race r ON r.race_source_value = e.race_clean
LEFT JOIN stg.map_location loc ON loc.location_key = (COALESCE(TRIM(e.address_line_1), '') || '|' || COALESCE(TRIM(e.address_line_2), '') || '|' || COALESCE(TRIM(e.city), '') || '|' || COALESCE(TRIM(e.state), '') || '|' || COALESCE(TRIM(e.zip_code), ''))
WHERE NOT EXISTS (SELECT 1 FROM cdm.person c WHERE c.person_id = mp.person_id);

-- Reject: members in map_person with no enrollment or null DOB (cannot satisfy year_of_birth NOT NULL)
CREATE OR REPLACE TABLE stg.reject_person_missing_dob AS
SELECT mp.member_id, mp.person_id
FROM stg.map_person mp
LEFT JOIN stg.enrollment e ON e.member_id = mp.member_id AND e.dob_date IS NOT NULL
WHERE e.member_id IS NULL;
