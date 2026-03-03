-- Map categorical lab result values to OMOP Meas Value concepts.
-- 9189 = Negative, 9191 = Positive, 9192 = Trace, 9190 = Non-reactive, 9188 = Final
-- 4069590 = Normal, 4135493 = Abnormal, 4267416 = Low, 4328749 = High
-- 4132135 = Absent, 4181412 = Present, 45877994 = Equivocal/Indeterminate
CREATE OR REPLACE TABLE stg.map_measurement_value AS
SELECT result_source_value, value_as_concept_id FROM (VALUES
    ('negative', 9189),
    ('positive', 9191),
    ('trace', 9192),
    ('non-reactive', 9190),
    ('nonreactive', 9190),
    ('reactive', 9191),
    ('final', 9188),
    ('normal', 4069590),
    ('abnormal', 4135493),
    ('low', 4267416),
    ('high', 4328749),
    ('absent', 4132135),
    ('present', 4181412),
    ('detected', 9191),
    ('not detected', 9189),
    ('undetected', 9189),
    ('equivocal', 45877994),
    ('indeterminate', 45877994),
    ('pass', 4181412),
    ('fail', 4135493),
    ('clear', 4069590),
    ('straw', 763957),
    ('negative-trace', 9192),
    ('trace-negative', 9192)
) AS t(result_source_value, value_as_concept_id);
