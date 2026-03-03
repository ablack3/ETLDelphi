-- Visit concept: (appt_type, clinic_type) -> visit_concept_id.
-- Rules with NULL clinic_type act as wildcards; unmatched rows fall back to {default_visit_concept_id}.
CREATE OR REPLACE TABLE stg.map_visit_concept AS
WITH known AS (
    SELECT * FROM (VALUES
        ('inpatient', CAST(NULL AS VARCHAR), 9201),
        ('emergency', CAST(NULL AS VARCHAR), 9203),
        ('outpatient', CAST(NULL AS VARCHAR), 9202)
    ) AS t(apt, cln, concept_id)
),
distinct_enc AS (
    SELECT DISTINCT
        LOWER(TRIM(appt_type)) AS appt_type,
        LOWER(TRIM(clinic_type)) AS clinic_type
    FROM stg.encounter
),
ranked AS (
    SELECT
        e.appt_type,
        e.clinic_type,
        k.concept_id,
        ROW_NUMBER() OVER (
            PARTITION BY e.appt_type, e.clinic_type
            ORDER BY CASE
                WHEN k.cln IS NOT NULL AND k.cln = e.clinic_type THEN 0
                WHEN k.cln IS NULL THEN 1
                ELSE 2
            END
        ) AS rn
    FROM distinct_enc e
    LEFT JOIN known k
        ON k.apt = e.appt_type
       AND (k.cln = e.clinic_type OR k.cln IS NULL)
)
SELECT
    r.appt_type,
    r.clinic_type,
    COALESCE(r.concept_id, {default_visit_concept_id}) AS visit_concept_id,
    COALESCE(r.appt_type, r.clinic_type, 'outpatient') AS visit_source_value
FROM ranked r
WHERE r.rn = 1;
