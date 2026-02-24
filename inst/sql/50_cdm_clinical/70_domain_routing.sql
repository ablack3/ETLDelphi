-- Domain routing: move records to the correct CDM table based on concept.domain_id.
-- Runs after all CDM loads (60-67). Records with concept_id = 0 stay in their home table.
-- ID ranges for routed records: 91x-95x million (avoids collision with 100M-800M load ranges).

-- ============================================================================
-- Step 0: Log routing summary BEFORE moving records
-- ============================================================================
CREATE OR REPLACE TABLE stg.domain_routing_log AS
WITH mismatches AS (
    SELECT 'condition_occurrence' AS from_table, c.domain_id AS to_domain, COUNT(*) AS record_count
    FROM cdm.condition_occurrence co
    JOIN cdm.concept c ON c.concept_id = co.condition_concept_id
    WHERE co.condition_concept_id <> 0 AND c.domain_id <> 'Condition'
    GROUP BY c.domain_id
    UNION ALL
    SELECT 'drug_exposure', c.domain_id, COUNT(*)
    FROM cdm.drug_exposure de
    JOIN cdm.concept c ON c.concept_id = de.drug_concept_id
    WHERE de.drug_concept_id <> 0 AND c.domain_id <> 'Drug'
    GROUP BY c.domain_id
    UNION ALL
    SELECT 'measurement', c.domain_id, COUNT(*)
    FROM cdm.measurement m
    JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id
    WHERE m.measurement_concept_id <> 0 AND c.domain_id <> 'Measurement'
    GROUP BY c.domain_id
    UNION ALL
    SELECT 'observation', c.domain_id, COUNT(*)
    FROM cdm.observation o
    JOIN cdm.concept c ON c.concept_id = o.observation_concept_id
    WHERE o.observation_concept_id <> 0 AND c.domain_id <> 'Observation'
    GROUP BY c.domain_id
    UNION ALL
    SELECT 'procedure_occurrence', c.domain_id, COUNT(*)
    FROM cdm.procedure_occurrence po
    JOIN cdm.concept c ON c.concept_id = po.procedure_concept_id
    WHERE po.procedure_concept_id <> 0 AND c.domain_id <> 'Procedure'
    GROUP BY c.domain_id
)
SELECT from_table, to_domain, record_count
FROM mismatches
WHERE record_count > 0
ORDER BY from_table, to_domain;

-- ============================================================================
-- Step 1: Route TO condition_occurrence from other tables
-- ============================================================================
-- Remove previously routed rows so this step is idempotent on re-run
DELETE FROM cdm.condition_occurrence
WHERE condition_occurrence_id >= 910000000 AND condition_occurrence_id < 920000000;

INSERT INTO cdm.condition_occurrence (
    condition_occurrence_id, person_id, condition_concept_id, condition_start_date,
    condition_type_concept_id, visit_occurrence_id, condition_source_value
)
WITH to_condition AS (
    SELECT person_id, drug_concept_id AS concept_id, drug_exposure_start_date AS start_date,
           drug_type_concept_id AS type_concept_id, visit_occurrence_id, drug_source_value AS source_value
    FROM cdm.drug_exposure de
    JOIN cdm.concept c ON c.concept_id = de.drug_concept_id
    WHERE de.drug_concept_id <> 0 AND c.domain_id = 'Condition'
    UNION ALL
    SELECT person_id, measurement_concept_id, measurement_date,
           measurement_type_concept_id, visit_occurrence_id, measurement_source_value
    FROM cdm.measurement m
    JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id
    WHERE m.measurement_concept_id <> 0 AND c.domain_id = 'Condition'
    UNION ALL
    SELECT person_id, observation_concept_id, observation_date,
           observation_type_concept_id, visit_occurrence_id, observation_source_value
    FROM cdm.observation o
    JOIN cdm.concept c ON c.concept_id = o.observation_concept_id
    WHERE o.observation_concept_id <> 0 AND c.domain_id = 'Condition'
    UNION ALL
    SELECT person_id, procedure_concept_id, procedure_date,
           procedure_type_concept_id, visit_occurrence_id, procedure_source_value
    FROM cdm.procedure_occurrence po
    JOIN cdm.concept c ON c.concept_id = po.procedure_concept_id
    WHERE po.procedure_concept_id <> 0 AND c.domain_id = 'Condition'
)
SELECT 910000000 + ROW_NUMBER() OVER (ORDER BY person_id, start_date, concept_id),
       person_id, concept_id, start_date, type_concept_id, visit_occurrence_id, source_value
FROM to_condition;

-- ============================================================================
-- Step 2: Route TO drug_exposure from other tables
-- ============================================================================
-- Remove previously routed rows so this step is idempotent on re-run
DELETE FROM cdm.drug_exposure
WHERE drug_exposure_id >= 920000000 AND drug_exposure_id < 930000000;

INSERT INTO cdm.drug_exposure (
    drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date,
    drug_type_concept_id, visit_occurrence_id, drug_source_value
)
WITH to_drug AS (
    SELECT person_id, condition_concept_id AS concept_id, condition_start_date AS start_date,
           condition_type_concept_id AS type_concept_id, visit_occurrence_id, condition_source_value AS source_value
    FROM cdm.condition_occurrence co
    JOIN cdm.concept c ON c.concept_id = co.condition_concept_id
    WHERE co.condition_concept_id <> 0 AND c.domain_id = 'Drug'
    UNION ALL
    SELECT person_id, measurement_concept_id, measurement_date,
           measurement_type_concept_id, visit_occurrence_id, measurement_source_value
    FROM cdm.measurement m
    JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id
    WHERE m.measurement_concept_id <> 0 AND c.domain_id = 'Drug'
    UNION ALL
    SELECT person_id, observation_concept_id, observation_date,
           observation_type_concept_id, visit_occurrence_id, observation_source_value
    FROM cdm.observation o
    JOIN cdm.concept c ON c.concept_id = o.observation_concept_id
    WHERE o.observation_concept_id <> 0 AND c.domain_id = 'Drug'
    UNION ALL
    SELECT person_id, procedure_concept_id, procedure_date,
           procedure_type_concept_id, visit_occurrence_id, procedure_source_value
    FROM cdm.procedure_occurrence po
    JOIN cdm.concept c ON c.concept_id = po.procedure_concept_id
    WHERE po.procedure_concept_id <> 0 AND c.domain_id = 'Drug'
)
SELECT 920000000 + ROW_NUMBER() OVER (ORDER BY person_id, start_date, concept_id),
       person_id, concept_id, start_date, start_date, type_concept_id, visit_occurrence_id, source_value
FROM to_drug;

-- ============================================================================
-- Step 3: Route TO measurement from other tables
-- ============================================================================
-- Remove previously routed rows so this step is idempotent on re-run
DELETE FROM cdm.measurement
WHERE measurement_id >= 930000000 AND measurement_id < 940000000;

INSERT INTO cdm.measurement (
    measurement_id, person_id, measurement_concept_id, measurement_date,
    measurement_type_concept_id, visit_occurrence_id, measurement_source_value
)
WITH to_measurement AS (
    SELECT person_id, condition_concept_id AS concept_id, condition_start_date AS start_date,
           condition_type_concept_id AS type_concept_id, visit_occurrence_id, condition_source_value AS source_value
    FROM cdm.condition_occurrence co
    JOIN cdm.concept c ON c.concept_id = co.condition_concept_id
    WHERE co.condition_concept_id <> 0 AND c.domain_id = 'Measurement'
    UNION ALL
    SELECT person_id, drug_concept_id, drug_exposure_start_date,
           drug_type_concept_id, visit_occurrence_id, drug_source_value
    FROM cdm.drug_exposure de
    JOIN cdm.concept c ON c.concept_id = de.drug_concept_id
    WHERE de.drug_concept_id <> 0 AND c.domain_id = 'Measurement'
    UNION ALL
    SELECT person_id, observation_concept_id, observation_date,
           observation_type_concept_id, visit_occurrence_id, observation_source_value
    FROM cdm.observation o
    JOIN cdm.concept c ON c.concept_id = o.observation_concept_id
    WHERE o.observation_concept_id <> 0 AND c.domain_id = 'Measurement'
    UNION ALL
    SELECT person_id, procedure_concept_id, procedure_date,
           procedure_type_concept_id, visit_occurrence_id, procedure_source_value
    FROM cdm.procedure_occurrence po
    JOIN cdm.concept c ON c.concept_id = po.procedure_concept_id
    WHERE po.procedure_concept_id <> 0 AND c.domain_id = 'Measurement'
)
SELECT 930000000 + ROW_NUMBER() OVER (ORDER BY person_id, start_date, concept_id),
       person_id, concept_id, start_date, type_concept_id, visit_occurrence_id, source_value
FROM to_measurement;

-- ============================================================================
-- Step 4: Route TO observation from other tables
-- ============================================================================
-- Remove previously routed rows so this step is idempotent on re-run
DELETE FROM cdm.observation
WHERE observation_id >= 940000000 AND observation_id < 950000000;

INSERT INTO cdm.observation (
    observation_id, person_id, observation_concept_id, observation_date,
    observation_type_concept_id, visit_occurrence_id, observation_source_value
)
WITH to_observation AS (
    SELECT person_id, condition_concept_id AS concept_id, condition_start_date AS start_date,
           condition_type_concept_id AS type_concept_id, visit_occurrence_id, condition_source_value AS source_value
    FROM cdm.condition_occurrence co
    JOIN cdm.concept c ON c.concept_id = co.condition_concept_id
    WHERE co.condition_concept_id <> 0 AND c.domain_id = 'Observation'
    UNION ALL
    SELECT person_id, drug_concept_id, drug_exposure_start_date,
           drug_type_concept_id, visit_occurrence_id, drug_source_value
    FROM cdm.drug_exposure de
    JOIN cdm.concept c ON c.concept_id = de.drug_concept_id
    WHERE de.drug_concept_id <> 0 AND c.domain_id = 'Observation'
    UNION ALL
    SELECT person_id, measurement_concept_id, measurement_date,
           measurement_type_concept_id, visit_occurrence_id, measurement_source_value
    FROM cdm.measurement m
    JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id
    WHERE m.measurement_concept_id <> 0 AND c.domain_id = 'Observation'
    UNION ALL
    SELECT person_id, procedure_concept_id, procedure_date,
           procedure_type_concept_id, visit_occurrence_id, procedure_source_value
    FROM cdm.procedure_occurrence po
    JOIN cdm.concept c ON c.concept_id = po.procedure_concept_id
    WHERE po.procedure_concept_id <> 0 AND c.domain_id = 'Observation'
)
SELECT 940000000 + ROW_NUMBER() OVER (ORDER BY person_id, start_date, concept_id),
       person_id, concept_id, start_date, type_concept_id, visit_occurrence_id, source_value
FROM to_observation;

-- ============================================================================
-- Step 5: Route TO procedure_occurrence from other tables
-- ============================================================================
-- Remove previously routed rows so this step is idempotent on re-run
DELETE FROM cdm.procedure_occurrence
WHERE procedure_occurrence_id >= 950000000 AND procedure_occurrence_id < 960000000;

INSERT INTO cdm.procedure_occurrence (
    procedure_occurrence_id, person_id, procedure_concept_id, procedure_date,
    procedure_type_concept_id, visit_occurrence_id, procedure_source_value
)
WITH to_procedure AS (
    SELECT person_id, condition_concept_id AS concept_id, condition_start_date AS start_date,
           condition_type_concept_id AS type_concept_id, visit_occurrence_id, condition_source_value AS source_value
    FROM cdm.condition_occurrence co
    JOIN cdm.concept c ON c.concept_id = co.condition_concept_id
    WHERE co.condition_concept_id <> 0 AND c.domain_id = 'Procedure'
    UNION ALL
    SELECT person_id, drug_concept_id, drug_exposure_start_date,
           drug_type_concept_id, visit_occurrence_id, drug_source_value
    FROM cdm.drug_exposure de
    JOIN cdm.concept c ON c.concept_id = de.drug_concept_id
    WHERE de.drug_concept_id <> 0 AND c.domain_id = 'Procedure'
    UNION ALL
    SELECT person_id, measurement_concept_id, measurement_date,
           measurement_type_concept_id, visit_occurrence_id, measurement_source_value
    FROM cdm.measurement m
    JOIN cdm.concept c ON c.concept_id = m.measurement_concept_id
    WHERE m.measurement_concept_id <> 0 AND c.domain_id = 'Procedure'
    UNION ALL
    SELECT person_id, observation_concept_id, observation_date,
           observation_type_concept_id, visit_occurrence_id, observation_source_value
    FROM cdm.observation o
    JOIN cdm.concept c ON c.concept_id = o.observation_concept_id
    WHERE o.observation_concept_id <> 0 AND c.domain_id = 'Procedure'
)
SELECT 950000000 + ROW_NUMBER() OVER (ORDER BY person_id, start_date, concept_id),
       person_id, concept_id, start_date, type_concept_id, visit_occurrence_id, source_value
FROM to_procedure;

-- ============================================================================
-- Step 6: Delete misrouted records from source tables
-- ============================================================================
DELETE FROM cdm.condition_occurrence
WHERE condition_concept_id <> 0
  AND EXISTS (
    SELECT 1 FROM cdm.concept c
    WHERE c.concept_id = condition_occurrence.condition_concept_id
      AND c.domain_id <> 'Condition'
  );

DELETE FROM cdm.drug_exposure
WHERE drug_concept_id <> 0
  AND EXISTS (
    SELECT 1 FROM cdm.concept c
    WHERE c.concept_id = drug_exposure.drug_concept_id
      AND c.domain_id <> 'Drug'
  );

DELETE FROM cdm.measurement
WHERE measurement_concept_id <> 0
  AND EXISTS (
    SELECT 1 FROM cdm.concept c
    WHERE c.concept_id = measurement.measurement_concept_id
      AND c.domain_id <> 'Measurement'
  );

DELETE FROM cdm.observation
WHERE observation_concept_id <> 0
  AND EXISTS (
    SELECT 1 FROM cdm.concept c
    WHERE c.concept_id = observation.observation_concept_id
      AND c.domain_id <> 'Observation'
  );

DELETE FROM cdm.procedure_occurrence
WHERE procedure_concept_id <> 0
  AND EXISTS (
    SELECT 1 FROM cdm.concept c
    WHERE c.concept_id = procedure_occurrence.procedure_concept_id
      AND c.domain_id <> 'Procedure'
  );
