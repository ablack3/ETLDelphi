-- Visit concept: (appt_type, clinic_type) -> visit_concept_id. Default 44813942 (Outpatient).
CREATE OR REPLACE TABLE stg.map_visit_concept AS
WITH known AS (
    SELECT * FROM (VALUES
        ('inpatient', NULL, 9201),
        ('emergency', NULL, 9203),
        ('outpatient', NULL, 44813942)
    ) AS t(apt, cln, concept_id)
),
distinct_enc AS (
    SELECT DISTINCT
        LOWER(TRIM(appt_type)) AS appt_type,
        LOWER(TRIM(clinic_type)) AS clinic_type
    FROM stg.encounter
)
SELECT
    e.appt_type,
    e.clinic_type,
    COALESCE(k.concept_id, 44813942) AS visit_concept_id,
    COALESCE(e.appt_type, e.clinic_type, 'outpatient') AS visit_source_value
FROM distinct_enc e
LEFT JOIN known k ON (k.apt = e.appt_type AND (k.cln = e.clinic_type OR (k.cln IS NULL AND e.clinic_type IS NULL)));
