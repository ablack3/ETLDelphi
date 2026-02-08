-- Unit mapping: common unit strings -> unit_concept_id (OMOP UCUM where applicable). Unmapped -> 0.
-- 8576 = mg, 8647 = mL, 8510 = cm, 9529 = kg, 8739 = beats/min, 8817 = Celsius, 8859 = g, etc.
CREATE OR REPLACE TABLE stg.map_units AS
SELECT unit_source_value, unit_concept_id FROM (VALUES
    ('mg', 8576),
    ('mg/ml', 8576),
    ('ml', 8647),
    ('cm', 8510),
    ('kg', 9529),
    ('lb', 8739),
    ('beats/min', 8739),
    ('c', 8817),
    ('f', 8816),
    ('g', 8859),
    ('l', 8840),
    ('mmol/l', 8753),
    ('mmhg', 8876)
) AS t(unit_source_value, unit_concept_id);

-- Add distinct units from staging not already mapped -> 0
INSERT INTO stg.map_units
SELECT DISTINCT LOWER(TRIM(u)), 0
FROM (
    SELECT units AS u FROM stg.lab_results WHERE units IS NOT NULL
    UNION SELECT height_units FROM stg.vital_sign WHERE height_units IS NOT NULL
    UNION SELECT weight_units FROM stg.vital_sign WHERE weight_units IS NOT NULL
    UNION SELECT temperature_units FROM stg.vital_sign WHERE temperature_units IS NOT NULL
    UNION SELECT dose_units FROM stg.medication_orders WHERE dose_units IS NOT NULL
) x
WHERE TRIM(u) <> '' AND NOT EXISTS (SELECT 1 FROM stg.map_units m WHERE m.unit_source_value = LOWER(TRIM(x.u)));
