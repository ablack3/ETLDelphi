-- Unit mapping: common unit strings -> unit_concept_id (OMOP UCUM where applicable). Unmapped -> 0.
-- Normalize: LOWER, TRIM, strip brackets []. Lab units from source may have trailing ].
-- UCUM IDs: 8840=mg/dL, 8713=g/dL, 8842=ng/mL, 8845=pg/mL, 8753=mmol/L, 9557=meq/L, 8923=IU/L, 8554=%, 8647=/uL
-- 8751=mg/L, 8583=fL, 8564=pg, 8588=mm, 8555=s, 8550=min, 8795=mL/min, 8862=mosm/kg, 8784=cells/uL, 8785=/mm3
-- 8889=cells/HPF, 8786=/HPF, 9257=/mL, 8815=million/uL, 8940=mIU/L, 8714=U/mL
CREATE OR REPLACE TABLE stg.map_units AS
SELECT unit_source_value, unit_concept_id FROM (VALUES
    ('mg', 8576),
    ('mg/ml', 8576),
    ('mg/dl', 8840),
    ('mg/dL', 8840),
    ('mg/l', 8751),
    ('mg/d', 8751),
    ('ml', 8647),
    ('cm', 8510),
    ('kg', 9529),
    ('lb', 8739),
    ('beats/min', 8739),
    ('c', 8817),
    ('f', 8816),
    ('g', 8859),
    ('l', 8519),
    ('mmol/l', 8753),
    ('nmol/l', 8753),
    ('nmole/ml', 8753),
    ('meq/l', 9557),
    ('mmhg', 8876),
    ('mm', 8588),
    ('u/l', 8923),
    ('iu/l', 8923),
    ('ng/ml', 8842),
    ('pg/ml', 8845),
    ('ng/dl', 8842),
    ('mcg/dl', 9655),
    ('microgram/dl', 9655),
    ('ug/dl', 9655),
    ('g/dl', 8713),
    ('%', 8554),
    ('/ul', 8647),
    ('/uL', 8647),
    ('/mm3', 8785),
    ('/mm³', 8785),
    ('fl', 8583),
    ('pg', 8564),
    ('s', 8555),
    ('min', 8550),
    ('ml/min', 8795),
    ('mm/hr', 8588),
    ('mosm/kg', 8862),
    ('miu/l', 8940),
    ('miu/ml', 8714),
    ('microu/ml', 44777583),
    ('u/ml', 8714),
    ('mu/l', 8923),
    ('m/ul', 8647),
    ('/hpf', 8889),
    ('/lpf', 8786),
    ('%(at-room-air)', 8554),
    ('mg/24hr', 8751),
    ('g/24hr', 8587),
    ('ug/l', 9655),
    ('/ml', 9257),
    ('units', 8510)
) AS t(unit_source_value, unit_concept_id);

-- Add distinct units from staging (normalized: LOWER, TRIM, strip []) not already mapped -> 0
INSERT INTO stg.map_units
SELECT DISTINCT LOWER(TRIM(REPLACE(REPLACE(u, '[', ''), ']', ''))), 0
FROM (
    SELECT units AS u FROM stg.lab_results WHERE units IS NOT NULL
    UNION SELECT height_units FROM stg.vital_sign WHERE height_units IS NOT NULL
    UNION SELECT weight_units FROM stg.vital_sign WHERE weight_units IS NOT NULL
    UNION SELECT temperature_units FROM stg.vital_sign WHERE temperature_units IS NOT NULL
    UNION SELECT dose_units FROM stg.medication_orders WHERE dose_units IS NOT NULL
) x
WHERE TRIM(u) <> '' AND NOT EXISTS (SELECT 1 FROM stg.map_units m WHERE m.unit_source_value = LOWER(TRIM(REPLACE(REPLACE(x.u, '[', ''), ']', ''))));
