-- Drug mapping: NDC -> standard RxNorm concept_id via cdm.concept (vocabulary NDC) and concept_relationship 'Maps to'.
-- Key by (drug_ndc_normalized, drug_name). If no NDC or unmapped: drug_concept_id=0, preserve drug_source_value.
CREATE OR REPLACE TABLE stg.map_drug_order AS
SELECT DISTINCT
    m.drug_ndc_normalized,
    m.drug_name,
    COALESCE(t.concept_id, 0) AS drug_concept_id,
    COALESCE(ndc.concept_id, 0) AS drug_source_concept_id
FROM (
    SELECT DISTINCT drug_ndc_normalized, TRIM(drug_name) AS drug_name
    FROM stg.medication_orders
    WHERE (drug_ndc_normalized IS NOT NULL AND TRIM(drug_ndc_normalized) <> '')
       OR (drug_name IS NOT NULL AND TRIM(drug_name) <> '')
) m
LEFT JOIN cdm.concept ndc
    ON ndc.vocabulary_id = 'NDC'
   AND ndc.concept_code = m.drug_ndc_normalized
   AND ndc.invalid_reason IS NULL
LEFT JOIN cdm.concept_relationship cr
    ON cr.concept_id_1 = ndc.concept_id
   AND cr.relationship_id = 'Maps to'
   AND cr.invalid_reason IS NULL
LEFT JOIN cdm.concept t ON t.concept_id = cr.concept_id_2 AND t.standard_concept = 'S';

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
