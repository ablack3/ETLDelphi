-- Load measurement from stg.lab_results. measurement_type_concept_id should be a standard Lab/EHR concept (default 32856).
-- value_as_concept_id for categorical results (Positive, Negative, etc.) from map_measurement_value.
-- range_low, range_high from parsed reference_range.
INSERT INTO cdm.measurement (
    measurement_id, person_id, measurement_concept_id, measurement_source_concept_id, measurement_date, measurement_datetime, measurement_type_concept_id,
    value_as_number, value_as_concept_id, unit_concept_id, unit_source_value, range_low, range_high,
    visit_occurrence_id, provider_id, measurement_source_value, value_source_value
)
WITH raw_lab AS (
    SELECT
        lr.*,
        NULLIF(TRIM(REPLACE(REPLACE(COALESCE(lr.units, ''), '[', ''), ']', '')), '') AS units_clean,
        NULLIF(TRIM(REPLACE(REPLACE(COALESCE(lr.result_description, ''), '[', ''), ']', '')), '') AS result_description_clean,
        300000000 + ROW_NUMBER() OVER (ORDER BY lr.member_id, lr.test_loinc, lr.date_resulted, lr.order_id) AS measurement_id
    FROM stg.lab_results lr
    WHERE (lr.date_resulted IS NOT NULL OR lr.date_collected IS NOT NULL)
),
normalized_lab AS (
    SELECT
        rl.*,
        CASE
            WHEN rl.result_description_clean IS NOT NULL
            THEN REGEXP_REPLACE(
                REGEXP_REPLACE(LOWER(TRIM(rl.result_description_clean)), '[.,;:]+$', ''),
                '\\s+',
                ' '
            )
            ELSE NULL
        END AS result_description_normalized,
        CASE
            WHEN rl.numeric_result IS NULL
             AND rl.result_description_clean IS NOT NULL
             AND REGEXP_MATCHES(rl.result_description_clean, '^[+-]?[0-9]+(?:,[0-9]{3})*(?:[.][0-9]+)?$')
            THEN TRY_CAST(REPLACE(rl.result_description_clean, ',', '') AS DOUBLE)
            ELSE NULL
        END AS parsed_numeric_result
    FROM raw_lab rl
),
lab AS (
    SELECT
        nl.*,
        CASE
            WHEN nl.result_description_normalized IS NOT NULL
             AND REGEXP_MATCHES(nl.result_description_normalized, '^[a-z]+(?:[ /-][a-z]+){0,3}$')
            THEN TRUE
            ELSE FALSE
        END AS is_simple_categorical
    FROM normalized_lab nl
)
SELECT
    l.measurement_id,
    mp.person_id,
    COALESCE(NULLIF(lm.measurement_concept_id, 0), cust.concept_id, 0),
    CASE WHEN lm.loinc_code IS NOT NULL THEN COALESCE(lm.measurement_source_concept_id, 0) ELSE NULL END,
    COALESCE(l.date_resulted, l.date_collected),
    COALESCE(l.date_resulted_datetime, l.date_collected_datetime),
    {measurement_type_labs},
    COALESCE(l.numeric_result, l.parsed_numeric_result),
    CASE
         WHEN COALESCE(l.numeric_result, l.parsed_numeric_result) IS NULL
          AND l.result_description_normalized IS NOT NULL
          AND l.is_simple_categorical
         THEN COALESCE(
           NULLIF(mval.value_as_concept_id, 0),
           cust_val.concept_id,
           -- Pattern-based fallback for common screening/exam result phrases
           CASE
             WHEN l.result_description_normalized LIKE '%no abnormal%'  THEN 4069590  -- Normal
             WHEN l.result_description_normalized LIKE '%no lumps%'     THEN 4069590  -- Normal
             WHEN l.result_description_normalized LIKE '%no lump %'     THEN 4069590  -- Normal
             WHEN l.result_description_normalized LIKE '%no polyps%'    THEN 4069590  -- Normal
             WHEN l.result_description_normalized LIKE '%no growth%'    THEN 4069590  -- Normal
             WHEN l.result_description_normalized LIKE '%no pouches%'   THEN 4069590  -- Normal
             WHEN l.result_description_normalized LIKE '%no murmurs%'   THEN 4069590  -- Normal
             WHEN l.result_description_normalized LIKE '%no nasal%'     THEN 4069590  -- Normal
             WHEN l.result_description_normalized LIKE '%are normal%'   THEN 4069590  -- Normal
             WHEN l.result_description_normalized LIKE '%negative for%' THEN 9189     -- Negative
             WHEN l.result_description_normalized LIKE '%negative %'    THEN 9189     -- Negative
             ELSE NULL
           END,
           0
         )
         ELSE NULL
    END AS value_as_concept_id,
    CASE WHEN l.units_clean IS NOT NULL THEN COALESCE(u.unit_concept_id, 0) ELSE NULL END AS unit_concept_id,
    SUBSTR(l.units_clean, 1, 50),
    l.range_low,
    l.range_high,
    mv.visit_occurrence_id,
    mpr.provider_id,
    SUBSTR(COALESCE(l.test_loinc, l.test_name), 1, 50),
    SUBSTR(l.result_description_clean, 1, 50)
FROM lab l
JOIN stg.map_person mp ON mp.member_id = l.member_id
LEFT JOIN stg.map_loinc_measurement lm ON lm.loinc_code = l.test_loinc
LEFT JOIN stg.custom_concept_mapping cust ON cust.source_value = TRIM(SUBSTR(COALESCE(l.test_loinc, l.test_name), 1, 50)) AND cust.domain = 'measurement'
LEFT JOIN stg.map_units u ON u.unit_source_value = LOWER(l.units_clean)
LEFT JOIN stg.map_visit mv ON mv.encounter_id_source = l.encounter_id
LEFT JOIN stg.map_provider mpr ON mpr.provider_id_source = l.provider_id
LEFT JOIN stg.map_measurement_value mval
  ON mval.result_source_value = l.result_description_normalized
LEFT JOIN stg.custom_concept_mapping cust_val
  ON cust_val.domain = 'measurement_value'
  AND l.result_description_normalized
   = REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(cust_val.source_value)), '[.,;:]+$', ''), '\\s+', ' ')
WHERE NOT EXISTS (SELECT 1 FROM cdm.measurement m WHERE m.measurement_id = l.measurement_id);

-- Update existing measurement rows that have value_as_concept_id = 0 but now match custom mapping (idempotent after adding mappings)
UPDATE cdm.measurement m
SET value_as_concept_id = cust.concept_id
FROM stg.custom_concept_mapping cust
WHERE cust.domain = 'measurement_value'
  AND m.value_as_concept_id = 0
  AND m.value_source_value IS NOT NULL
  AND TRIM(m.value_source_value) <> ''
  AND REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(REPLACE(REPLACE(COALESCE(m.value_source_value, ''), '[', ''), ']', ''))), '[.,;:]+$', ''), '\\s+', ' ')
   = REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(cust.source_value)), '[.,;:]+$', ''), '\\s+', ' ');

CREATE OR REPLACE TABLE stg.reject_measurement_labs_missing AS
SELECT member_id, test_loinc, date_resulted, date_collected
FROM stg.lab_results
WHERE date_resulted IS NULL AND date_collected IS NULL
   OR NOT EXISTS (SELECT 1 FROM stg.map_person mp WHERE mp.member_id = stg.lab_results.member_id);
