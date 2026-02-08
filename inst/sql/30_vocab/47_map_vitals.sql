-- Vitals: vital name -> measurement_concept_id and default unit. Standard OMOP/LOINC concept placeholders.
-- 3036277 Body height, 3013762 Body weight, 3004249 SBP, 3012888 DBP, 3027018 Heart rate, 3024171 Respiration rate, 3020891 Body temperature
CREATE OR REPLACE TABLE stg.map_vitals AS
SELECT * FROM (VALUES
    ('Height', 3036277, 8510, 'cm'),
    ('Weight', 3013762, 9529, 'kg'),
    ('SystolicBP', 3004249, 8876, 'mmHg'),
    ('DiastolicBP', 3012888, 8876, 'mmHg'),
    ('Pulse', 3027018, 8541, 'beats/min'),
    ('Respiration', 3024171, 8541, '/min'),
    ('Temperature', 3020891, 8817, 'C')
) AS t(vital_name, measurement_concept_id, default_unit_concept_id, default_unit_source_value);
