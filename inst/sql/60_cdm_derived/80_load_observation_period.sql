-- Derive observation_period per person: min/max of all event dates (visits, conditions, drugs, measurements, observations, procedures). period_type_concept_id 32821.
-- Persons with no clinical events get a 1-day observation_period on their birth date (OMOP requires every person to have at least one observation_period).
INSERT INTO cdm.observation_period (observation_period_id, person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id)
WITH all_dates AS (
    SELECT person_id, visit_start_date AS dt FROM cdm.visit_occurrence
    UNION ALL
    SELECT person_id, condition_start_date FROM cdm.condition_occurrence
    UNION ALL
    SELECT person_id, drug_exposure_start_date FROM cdm.drug_exposure
    UNION ALL
    SELECT person_id, measurement_date FROM cdm.measurement
    UNION ALL
    SELECT person_id, observation_date FROM cdm.observation
    UNION ALL
    SELECT person_id, procedure_date FROM cdm.procedure_occurrence
    UNION ALL
    SELECT person_id, death_date FROM cdm.death WHERE death_date IS NOT NULL
),
bounds AS (
    SELECT person_id, MIN(dt) AS start_date, MAX(dt) AS end_date
    FROM all_dates
    WHERE dt IS NOT NULL
    GROUP BY person_id
),
-- Include persons with no events: use birth_datetime date or Jan 1 of birth_year
all_persons AS (
    SELECT
        p.person_id,
        COALESCE(b.start_date, CAST(p.birth_datetime AS DATE), MAKE_DATE(p.year_of_birth, 1, 1)) AS start_date,
        COALESCE(b.end_date,   CAST(p.birth_datetime AS DATE), MAKE_DATE(p.year_of_birth, 1, 1)) AS end_date
    FROM cdm.person p
    LEFT JOIN bounds b ON b.person_id = p.person_id
),
with_id AS (
    SELECT person_id, start_date, end_date,
           COALESCE((SELECT MAX(observation_period_id) FROM cdm.observation_period), 899999999) + ROW_NUMBER() OVER (ORDER BY person_id) AS observation_period_id
    FROM all_persons
    WHERE start_date IS NOT NULL
)
SELECT w.observation_period_id, w.person_id, w.start_date, w.end_date, 32821
FROM with_id w
WHERE NOT EXISTS (SELECT 1 FROM cdm.observation_period o WHERE o.person_id = w.person_id);
