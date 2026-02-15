-- Drug mapping: use NDC vocabulary and Drug_NDC (source) -> join concept on source value (concept_code)
-- to get NDC concept_id, then concept_relationship 'Maps to' to get standard drug concept_id for all drug records.
-- Source: stg.medication_orders.drug_ndc_normalized (from Drug_NDC, dashes/spaces stripped in 15_stage_med_orders).
-- Key by (drug_ndc_normalized, drug_name). If no NDC or unmapped: drug_concept_id=0, preserve drug_source_value.
CREATE OR REPLACE TABLE stg.map_drug_order AS
WITH source_ndc AS (
    SELECT DISTINCT drug_ndc_normalized, TRIM(drug_name) AS drug_name
    FROM stg.medication_orders
    WHERE (drug_ndc_normalized IS NOT NULL AND TRIM(drug_ndc_normalized) <> '')
       OR (drug_name IS NOT NULL AND TRIM(drug_name) <> '')
),
-- Join concept table on source value (concept_code) for NDC vocabulary -> NDC concept_id
ndc_concepts AS (
    SELECT
        c.concept_id AS ndc_concept_id,
        c.concept_code AS source_value
    FROM cdm.concept c
    WHERE c.vocabulary_id = 'NDC'
      AND c.invalid_reason IS NULL
),
-- Standard concept_id via concept_relationship 'Maps to'
ndc_to_standard AS (
    SELECT
        cr.concept_id_1 AS ndc_concept_id,
        cr.concept_id_2 AS standard_concept_id
    FROM cdm.concept_relationship cr
    WHERE cr.relationship_id = 'Maps to'
      AND cr.invalid_reason IS NULL
)
SELECT DISTINCT
    m.drug_ndc_normalized,
    m.drug_name,
    COALESCE(t.concept_id, 0) AS drug_concept_id,
    COALESCE(nc.ndc_concept_id, 0) AS drug_source_concept_id
FROM source_ndc m
LEFT JOIN ndc_concepts nc
    ON nc.source_value = m.drug_ndc_normalized
LEFT JOIN ndc_to_standard std
    ON std.ndc_concept_id = nc.ndc_concept_id
LEFT JOIN cdm.concept t
    ON t.concept_id = std.standard_concept_id AND t.standard_concept = 'S';

-- Rows with only drug_name (no NDC): add with concept_id from drug_name_to_concept (Hecate) or 0
INSERT INTO stg.map_drug_order
SELECT DISTINCT
    NULL,
    TRIM(mo.drug_name),
    COALESCE(dnc.concept_id, 0),
    0
FROM stg.medication_orders mo
LEFT JOIN stg.drug_name_to_concept dnc ON dnc.drug_name = TRIM(mo.drug_name)
WHERE (mo.drug_ndc_normalized IS NULL OR TRIM(mo.drug_ndc_normalized) = '')
  AND mo.drug_name IS NOT NULL AND TRIM(mo.drug_name) <> ''
  AND NOT EXISTS (SELECT 1 FROM stg.map_drug_order d WHERE d.drug_name = TRIM(mo.drug_name) AND d.drug_ndc_normalized IS NULL);
