-- Load cdm.death from stg.death. person_id via map_person. death_type_concept_id configurable (e.g. 32817).
INSERT INTO cdm.death (person_id, death_date, death_datetime, death_type_concept_id)
SELECT mp.person_id, d.death_date, d.death_datetime, {death_type_concept_id}
FROM stg.death d
JOIN stg.map_person mp ON mp.member_id = d.member_id
WHERE d.death_date IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM cdm.death c WHERE c.person_id = mp.person_id);

CREATE OR REPLACE TABLE stg.reject_death_load AS
SELECT d.member_id, d.death_date
FROM stg.death d
WHERE d.death_date IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM stg.map_person mp WHERE mp.member_id = d.member_id);
