-- QC: count rows in each stg.reject_* table. Optionally sample stored in qc table.
CREATE TABLE IF NOT EXISTS stg.qc_parse_failures (reject_table VARCHAR(128), row_count INTEGER);

DELETE FROM stg.qc_parse_failures WHERE 1=1;

INSERT INTO stg.qc_parse_failures SELECT 'reject_enrollment_dates', COUNT(*) FROM stg.reject_enrollment_dates
UNION ALL SELECT 'reject_provider', COUNT(*) FROM stg.reject_provider
UNION ALL SELECT 'reject_encounter', COUNT(*) FROM stg.reject_encounter
UNION ALL SELECT 'reject_death', COUNT(*) FROM stg.reject_death
UNION ALL SELECT 'reject_problem', COUNT(*) FROM stg.reject_problem
UNION ALL SELECT 'reject_med_orders', COUNT(*) FROM stg.reject_med_orders
UNION ALL SELECT 'reject_med_fulfillment', COUNT(*) FROM stg.reject_med_fulfillment
UNION ALL SELECT 'reject_current_meds', COUNT(*) FROM stg.reject_current_meds
UNION ALL SELECT 'reject_immunization', COUNT(*) FROM stg.reject_immunization
UNION ALL SELECT 'reject_lab_orders', COUNT(*) FROM stg.reject_lab_orders
UNION ALL SELECT 'reject_lab_results', COUNT(*) FROM stg.reject_lab_results
UNION ALL SELECT 'reject_vital_sign', COUNT(*) FROM stg.reject_vital_sign
UNION ALL SELECT 'reject_allergy', COUNT(*) FROM stg.reject_allergy
UNION ALL SELECT 'reject_visit_missing_person', COUNT(*) FROM stg.reject_visit_missing_person
UNION ALL SELECT 'reject_fulfillment_no_order', COUNT(*) FROM stg.reject_fulfillment_no_order
UNION ALL SELECT 'reject_person_missing_dob', COUNT(*) FROM stg.reject_person_missing_dob
UNION ALL SELECT 'reject_condition_missing_person', COUNT(*) FROM stg.reject_condition_missing_person
UNION ALL SELECT 'reject_measurement_labs_missing', COUNT(*) FROM stg.reject_measurement_labs_missing
UNION ALL SELECT 'reject_observation_allergy_missing', COUNT(*) FROM stg.reject_observation_allergy_missing
UNION ALL SELECT 'reject_procedure_missing', COUNT(*) FROM stg.reject_procedure_missing
UNION ALL SELECT 'reject_death_load', COUNT(*) FROM stg.reject_death_load;
