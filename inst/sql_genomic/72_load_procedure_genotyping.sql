-- ============================================================================
-- Create one PROCEDURE_OCCURRENCE record per person for the genotyping assay.
-- procedure_occurrence_id = 800000000 + person_id (deterministic).
-- Concept: 4019097 = Molecular genetics procedure (SNOMED 116148004)
-- ============================================================================

INSERT INTO cdm.procedure_occurrence (
    procedure_occurrence_id,
    person_id,
    procedure_concept_id,
    procedure_date,
    procedure_datetime,
    procedure_end_date,
    procedure_end_datetime,
    procedure_type_concept_id,
    modifier_concept_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    procedure_source_value,
    procedure_source_concept_id,
    modifier_source_value
)
SELECT
    800000000 + p.person_id   AS procedure_occurrence_id,
    p.person_id,
    4019097                   AS procedure_concept_id,          -- Molecular genetics procedure
    s.specimen_date           AS procedure_date,               -- Same date as specimen
    NULL                      AS procedure_datetime,
    NULL                      AS procedure_end_date,
    NULL                      AS procedure_end_datetime,
    32817                     AS procedure_type_concept_id,     -- EHR
    0                         AS modifier_concept_id,
    NULL                      AS quantity,
    NULL                      AS provider_id,
    NULL                      AS visit_occurrence_id,
    NULL                      AS visit_detail_id,
    'Simulated GWAS genotyping' AS procedure_source_value,
    0                         AS procedure_source_concept_id,
    NULL                      AS modifier_source_value
FROM cdm.person p
JOIN cdm.specimen s ON s.specimen_id = 700000000 + p.person_id;
