-- Backfill cdm.drug_exposure rows that still have drug_concept_id = 0 using current
-- stg.map_drug_order (e.g. after adding custom NDC or fixing vocabulary). Aligns
-- CDM with mapping so Shiny / analyze_mapping_quality show the same unmapped set as
-- export_unmapped_drugs. Match on normalized drug_source_value (strip *) to NDC or drug_name.
UPDATE cdm.drug_exposure e
SET drug_concept_id = m.drug_concept_id,
    drug_source_concept_id = COALESCE(m.drug_source_concept_id, 0)
FROM stg.map_drug_order m
WHERE e.drug_concept_id = 0
  AND e.drug_source_value IS NOT NULL
  AND TRIM(e.drug_source_value) <> ''
  AND (
    REPLACE(TRIM(e.drug_source_value), '*', '') = m.drug_ndc_normalized
    OR TRIM(e.drug_source_value) = m.drug_name
  )
  AND m.drug_concept_id <> 0;
