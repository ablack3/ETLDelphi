-- Load measurement from stg.vital_sign: one row per non-null vital (Height, Weight, SBP, DBP, Pulse, Respiration, Temperature). UNPIVOT via UNION ALL.
INSERT INTO cdm.measurement (
    measurement_id, person_id, measurement_concept_id, measurement_date, measurement_datetime, measurement_type_concept_id,
    value_as_number, unit_concept_id, unit_source_value, visit_occurrence_id, measurement_source_value, value_source_value
)
WITH vitals AS (
    SELECT member_id, encounter_id, encounter_date, encounter_datetime, 'Height' AS vital_name, height AS value_num, height_units AS unit_src FROM stg.vital_sign WHERE height IS NOT NULL
    UNION ALL
    SELECT member_id, encounter_id, encounter_date, encounter_datetime, 'Weight', weight, weight_units FROM stg.vital_sign WHERE weight IS NOT NULL
    UNION ALL
    SELECT member_id, encounter_id, encounter_date, encounter_datetime, 'SystolicBP', systolic_bp, NULL FROM stg.vital_sign WHERE systolic_bp IS NOT NULL
    UNION ALL
    SELECT member_id, encounter_id, encounter_date, encounter_datetime, 'DiastolicBP', diastolic_bp, NULL FROM stg.vital_sign WHERE diastolic_bp IS NOT NULL
    UNION ALL
    SELECT member_id, encounter_id, encounter_date, encounter_datetime, 'Pulse', pulse, NULL FROM stg.vital_sign WHERE pulse IS NOT NULL
    UNION ALL
    SELECT member_id, encounter_id, encounter_date, encounter_datetime, 'Respiration', respiration, NULL FROM stg.vital_sign WHERE respiration IS NOT NULL
    UNION ALL
    SELECT member_id, encounter_id, encounter_date, encounter_datetime, 'Temperature', temperature, temperature_units FROM stg.vital_sign WHERE temperature IS NOT NULL
),
with_id AS (
    SELECT v.*, 400000000 + ROW_NUMBER() OVER (ORDER BY v.member_id, v.encounter_id, v.encounter_date, v.vital_name) AS measurement_id
    FROM vitals v
)
SELECT
    w.measurement_id,
    mp.person_id,
    COALESCE(NULLIF(mv.measurement_concept_id, 0), cust.concept_id, 0),
    w.encounter_date,
    w.encounter_datetime,
    {measurement_type_vitals},
    w.value_num,
    COALESCE(u.unit_concept_id, mv.default_unit_concept_id),
    COALESCE(w.unit_src, mv.default_unit_source_value),
    mapv.visit_occurrence_id,
    w.vital_name,
    CAST(w.value_num AS VARCHAR)
FROM with_id w
JOIN stg.map_person mp ON mp.member_id = w.member_id
LEFT JOIN stg.map_vitals mv ON mv.vital_name = w.vital_name
LEFT JOIN stg.custom_concept_mapping cust ON cust.source_value = w.vital_name AND cust.domain = 'measurement'
LEFT JOIN stg.map_units u ON u.unit_source_value = LOWER(TRIM(w.unit_src))
LEFT JOIN stg.map_visit mapv ON mapv.encounter_id_source = w.encounter_id
WHERE w.encounter_date IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM cdm.measurement m WHERE m.measurement_id = w.measurement_id);
