-- ============================================================================
-- Create one SPECIMEN record per person for DNA extraction (blood specimen).
-- specimen_id = 700000000 + person_id (deterministic, avoids collision with
-- any non-genomic specimens).
-- Requires: stg.snp_reference (from simulateGenomicData) for existence check.
-- ============================================================================

INSERT INTO cdm.specimen (
    specimen_id,
    person_id,
    specimen_concept_id,
    specimen_type_concept_id,
    specimen_date,
    specimen_datetime,
    quantity,
    unit_concept_id,
    anatomic_site_concept_id,
    disease_status_concept_id,
    specimen_source_id,
    specimen_source_value,
    unit_source_value,
    anatomic_site_source_value,
    disease_status_source_value
)
SELECT
    700000000 + p.person_id   AS specimen_id,
    p.person_id,
    4001225                   AS specimen_concept_id,          -- Blood specimen (SNOMED 119297000)
    32817                     AS specimen_type_concept_id,     -- EHR
    CASE
        WHEN vo.visit_start_date IS NOT NULL THEN vo.visit_start_date
        ELSE MAKE_DATE(p.year_of_birth + 40, 1, 1)
    END                       AS specimen_date,
    NULL                      AS specimen_datetime,
    NULL                      AS quantity,
    0                         AS unit_concept_id,
    0                         AS anatomic_site_concept_id,
    0                         AS disease_status_concept_id,
    NULL                      AS specimen_source_id,
    'Simulated blood specimen for genotyping' AS specimen_source_value,
    NULL                      AS unit_source_value,
    NULL                      AS anatomic_site_source_value,
    NULL                      AS disease_status_source_value
FROM cdm.person p
LEFT JOIN (
    SELECT person_id, MIN(visit_start_date) AS visit_start_date
    FROM cdm.visit_occurrence
    GROUP BY person_id
) vo ON vo.person_id = p.person_id;
