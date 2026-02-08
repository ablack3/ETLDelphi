-- Load measurement from stg.lab_results. measurement_type_concept_id 32827 (Lab).
WITH lab AS (
    SELECT
        lr.*,
        300000000 + ROW_NUMBER() OVER (ORDER BY lr.member_id, lr.test_loinc, lr.date_resulted, lr.order_id) AS measurement_id
    FROM stg.lab_results lr
    WHERE (lr.date_resulted IS NOT NULL OR lr.date_collected IS NOT NULL)
)
INSERT INTO cdm.measurement (
    measurement_id, person_id, measurement_concept_id, measurement_date, measurement_type_concept_id,
    value_as_number, unit_concept_id, unit_source_value, visit_occurrence_id, provider_id,
    measurement_source_value, value_source_value
)
SELECT
    l.measurement_id,
    mp.person_id,
    COALESCE(lm.measurement_concept_id, 0),
    COALESCE(l.date_resulted, l.date_collected),
    32827,
    l.numeric_result,
    u.unit_concept_id,
    SUBSTR(l.units, 1, 50),
    mv.visit_occurrence_id,
    mpr.provider_id,
    SUBSTR(COALESCE(l.test_loinc, l.test_name), 1, 50),
    SUBSTR(l.result_description, 1, 50)
FROM lab l
JOIN stg.map_person mp ON mp.member_id = l.member_id
LEFT JOIN stg.map_loinc_measurement lm ON lm.loinc_code = l.test_loinc
LEFT JOIN stg.map_units u ON u.unit_source_value = LOWER(TRIM(l.units))
LEFT JOIN stg.map_visit mv ON mv.encounter_id_source = l.encounter_id
LEFT JOIN stg.map_provider mpr ON mpr.provider_id_source = l.provider_id
WHERE NOT EXISTS (SELECT 1 FROM cdm.measurement m WHERE m.measurement_id = l.measurement_id);

CREATE OR REPLACE TABLE stg.reject_measurement_labs_missing AS
SELECT member_id, test_loinc, date_resulted, date_collected
FROM stg.lab_results
WHERE date_resulted IS NULL AND date_collected IS NULL
   OR NOT EXISTS (SELECT 1 FROM stg.map_person mp WHERE mp.member_id = stg.lab_results.member_id);
