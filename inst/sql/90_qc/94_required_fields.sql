-- QC: required NOT NULL fields per CDM. Store violations in stg.qc_required_fields. ETL runner may error if any violations.
CREATE TABLE IF NOT EXISTS stg.qc_required_fields (
    table_name VARCHAR(64),
    field_name VARCHAR(64),
    violation_count INTEGER
);

DELETE FROM stg.qc_required_fields WHERE 1=1;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'person', 'year_of_birth', COUNT(*) FROM cdm.person WHERE year_of_birth IS NULL;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'person', 'gender_concept_id', COUNT(*) FROM cdm.person WHERE gender_concept_id IS NULL;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'person', 'race_concept_id', COUNT(*) FROM cdm.person WHERE race_concept_id IS NULL;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'visit_occurrence', 'visit_start_date', COUNT(*) FROM cdm.visit_occurrence WHERE visit_start_date IS NULL;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'visit_occurrence', 'visit_end_date', COUNT(*) FROM cdm.visit_occurrence WHERE visit_end_date IS NULL;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'condition_occurrence', 'condition_start_date', COUNT(*) FROM cdm.condition_occurrence WHERE condition_start_date IS NULL;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'drug_exposure', 'drug_exposure_start_date', COUNT(*) FROM cdm.drug_exposure WHERE drug_exposure_start_date IS NULL;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'measurement', 'measurement_date', COUNT(*) FROM cdm.measurement WHERE measurement_date IS NULL;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'observation', 'observation_date', COUNT(*) FROM cdm.observation WHERE observation_date IS NULL;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'procedure_occurrence', 'procedure_date', COUNT(*) FROM cdm.procedure_occurrence WHERE procedure_date IS NULL;

INSERT INTO stg.qc_required_fields (table_name, field_name, violation_count)
SELECT 'death', 'death_date', COUNT(*) FROM cdm.death WHERE death_date IS NULL;
