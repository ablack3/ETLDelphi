-- QC: row counts for stg.* and cdm.*. Store in stg.qc_row_counts (run_id optional; use current timestamp).
CREATE TABLE IF NOT EXISTS stg.qc_row_counts (run_id INTEGER, table_name VARCHAR(128), row_count INTEGER);

INSERT INTO stg.qc_row_counts (run_id, table_name, row_count)
SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.enrollment', COUNT(*) FROM stg.enrollment
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.provider', COUNT(*) FROM stg.provider
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.encounter', COUNT(*) FROM stg.encounter
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.death', COUNT(*) FROM stg.death
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.problem', COUNT(*) FROM stg.problem
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.medication_orders', COUNT(*) FROM stg.medication_orders
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.medication_fulfillment', COUNT(*) FROM stg.medication_fulfillment
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.immunization', COUNT(*) FROM stg.immunization
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.lab_results', COUNT(*) FROM stg.lab_results
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.vital_sign', COUNT(*) FROM stg.vital_sign
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'stg.allergy', COUNT(*) FROM stg.allergy
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'cdm.person', COUNT(*) FROM cdm.person
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'cdm.visit_occurrence', COUNT(*) FROM cdm.visit_occurrence
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'cdm.condition_occurrence', COUNT(*) FROM cdm.condition_occurrence
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'cdm.drug_exposure', COUNT(*) FROM cdm.drug_exposure
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'cdm.measurement', COUNT(*) FROM cdm.measurement
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'cdm.observation', COUNT(*) FROM cdm.observation
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'cdm.procedure_occurrence', COUNT(*) FROM cdm.procedure_occurrence
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'cdm.death', COUNT(*) FROM cdm.death
UNION ALL SELECT (SELECT MAX(run_id) FROM stg.etl_run_log), 'cdm.observation_period', COUNT(*) FROM cdm.observation_period;
