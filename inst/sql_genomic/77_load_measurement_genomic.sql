-- ============================================================================
-- MEASUREMENT: map genomic variants to OMOP Genomic vocabulary concepts.
--
-- Each person × SNP with genotype > 0 becomes one MEASUREMENT row:
--   measurement_concept_id  = OMOP Genomic gene variant measurement concept
--                             (joined via HGNC numeric ID → concept_code)
--   value_as_concept_id     = 45880311 (Heterozygous) or 45878253 (Homozygous)
--   value_as_number         = genotype dosage (1 or 2)
--   measurement_source_value = rs_id (e.g. "rs2228145")
--   value_source_value      = "heterozygous" or "homozygous"
--
-- Only SNPs with a valid hgnc_id mapping are included.
-- Null SNPs (no gene annotation) are excluded.
-- ============================================================================

INSERT INTO cdm.measurement (
    measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_time,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    unit_source_concept_id,
    value_source_value,
    measurement_event_id,
    meas_event_field_concept_id
)
SELECT
    -- Deterministic ID: 900000000 + row number
    900000000 + ROW_NUMBER() OVER (ORDER BY gv.person_id, sr.snp_id)
                                          AS measurement_id,
    gv.person_id,
    c.concept_id                          AS measurement_concept_id,
    s.specimen_date                       AS measurement_date,
    NULL                                  AS measurement_datetime,
    NULL                                  AS measurement_time,
    32817                                 AS measurement_type_concept_id,  -- EHR
    0                                     AS operator_concept_id,
    CAST(gv.genotype AS DOUBLE)           AS value_as_number,             -- dosage: 1 or 2
    CASE gv.genotype
        WHEN 1 THEN 45880311             -- Heterozygous (LOINC LA6706-1)
        WHEN 2 THEN 45878253             -- Homozygous   (LOINC LA6705-3)
    END                                   AS value_as_concept_id,
    0                                     AS unit_concept_id,
    NULL                                  AS range_low,
    NULL                                  AS range_high,
    NULL                                  AS provider_id,
    NULL                                  AS visit_occurrence_id,
    NULL                                  AS visit_detail_id,
    sr.snp_id                             AS measurement_source_value,     -- e.g. "rs2228145"
    0                                     AS measurement_source_concept_id,
    NULL                                  AS unit_source_value,
    0                                     AS unit_source_concept_id,
    CASE gv.genotype
        WHEN 1 THEN 'heterozygous'
        WHEN 2 THEN 'homozygous'
    END                                   AS value_source_value,
    NULL                                  AS measurement_event_id,
    0                                     AS meas_event_field_concept_id
FROM stg.genomic_variants gv
JOIN stg.snp_reference sr
    ON sr.snp_id = gv.snp_id
-- Map HGNC numeric ID → OMOP Genomic concept
JOIN cdm.concept c
    ON  c.vocabulary_id = 'OMOP Genomic'
    AND c.concept_code  = REPLACE(sr.hgnc_id, 'HGNC:', '')
    AND c.standard_concept = 'S'
    AND c.domain_id = 'Measurement'
-- Get specimen date for measurement_date
JOIN cdm.specimen s
    ON s.specimen_id = 700000000 + gv.person_id
WHERE gv.genotype > 0
  AND sr.hgnc_id IS NOT NULL;
