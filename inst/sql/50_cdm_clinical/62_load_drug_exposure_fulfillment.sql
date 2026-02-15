-- Load drug_exposure from stg.medication_fulfillment. Join to medication_orders for Member_ID and drug. drug_type_concept_id 38000230.
-- Reject fulfillment rows with Order_ID not in medication_orders.
INSERT INTO cdm.drug_exposure (
    drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date,
    drug_type_concept_id, quantity, days_supply, provider_id, visit_occurrence_id,
    drug_source_value, drug_source_concept_id
)
WITH f AS (
    SELECT
        mf.*,
        mo.member_id,
        mo.drug_name,
        mo.drug_ndc_normalized,
        600000000 + ROW_NUMBER() OVER (ORDER BY mf.order_id, mf.dispense_date, mf.fill_no) AS drug_exposure_id
    FROM stg.medication_fulfillment mf
    LEFT JOIN stg.medication_orders mo ON mo.order_id = mf.order_id
),
mapped AS (
    SELECT f.*, d.drug_concept_id, d.drug_source_concept_id, cust.concept_id AS cust_concept_id
    FROM f
    LEFT JOIN stg.map_drug_order d ON (d.drug_ndc_normalized = f.drug_ndc_normalized OR (d.drug_ndc_normalized IS NULL AND f.drug_ndc_normalized IS NULL)) AND d.drug_name = f.drug_name
    LEFT JOIN stg.custom_concept_mapping cust ON cust.source_value = TRIM(SUBSTR(f.drug_name, 1, 50)) AND cust.domain = 'drug'
)
SELECT
    m.drug_exposure_id,
    mp.person_id,
    COALESCE(m.drug_concept_id, m.cust_concept_id, 0),
    m.dispense_date,
    CASE WHEN m.days_of_supply IS NOT NULL AND m.days_of_supply > 0 THEN m.dispense_date + (m.days_of_supply - 1) ELSE m.dispense_date END,
    38000230,
    m.dispense_qty,
    m.days_of_supply,
    NULL,
    mv.visit_occurrence_id,
    SUBSTR(COALESCE(NULLIF(TRIM(m.drug_ndc_normalized), ''), m.drug_name), 1, 50),
    COALESCE(m.drug_source_concept_id, 0)
FROM mapped m
JOIN stg.map_person mp ON mp.member_id = m.member_id
LEFT JOIN stg.map_visit mv ON mv.encounter_id_source = m.encounter_id
WHERE m.member_id IS NOT NULL
  AND m.dispense_date IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM cdm.drug_exposure x WHERE x.drug_exposure_id = m.drug_exposure_id);

CREATE OR REPLACE TABLE stg.reject_fulfillment_no_order AS
SELECT mf.order_id, mf.dispense_date
FROM stg.medication_fulfillment mf
WHERE NOT EXISTS (SELECT 1 FROM stg.medication_orders mo WHERE mo.order_id = mf.order_id);
