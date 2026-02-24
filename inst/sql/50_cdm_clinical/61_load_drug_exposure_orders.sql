-- Load drug_exposure from stg.medication_orders. drug_type_concept_id 38000177 (EHR order).
INSERT INTO cdm.drug_exposure (
    drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date,
    drug_type_concept_id, refills, quantity, sig, provider_id, visit_occurrence_id,
    drug_source_value, drug_source_concept_id, route_source_value, dose_unit_source_value
)
WITH ord AS (
    SELECT
        mo.*,
        700000000 + ROW_NUMBER() OVER (ORDER BY mo.order_id, mo.member_id, mo.order_date) AS drug_exposure_id
    FROM stg.medication_orders mo
    WHERE mo.order_date IS NOT NULL
)
SELECT
    o.drug_exposure_id,
    mp.person_id,
    COALESCE(NULLIF(d.drug_concept_id, 0), cust.concept_id, 0),
    o.order_date,
    o.order_date,
    38000177,
    o.refills,
    o.qty_ordered,
    o.sig,
    mpr.provider_id,
    mv.visit_occurrence_id,
    SUBSTR(COALESCE(NULLIF(TRIM(o.drug_ndc_normalized), ''), o.drug_name), 1, 50),
    COALESCE(d.drug_source_concept_id, 0),
    SUBSTR(o.route, 1, 50),
    SUBSTR(o.dose_units, 1, 50)
FROM ord o
JOIN stg.map_person mp ON mp.member_id = o.member_id
LEFT JOIN stg.map_drug_order d ON (d.drug_ndc_normalized = o.drug_ndc_normalized OR (d.drug_ndc_normalized IS NULL AND o.drug_ndc_normalized IS NULL)) AND d.drug_name = o.drug_name
LEFT JOIN stg.custom_concept_mapping cust ON cust.source_value = TRIM(SUBSTR(COALESCE(NULLIF(TRIM(o.drug_ndc_normalized), ''), o.drug_name), 1, 50)) AND cust.domain = 'drug'
LEFT JOIN stg.map_provider mpr ON mpr.provider_id_source = o.order_provider_id
LEFT JOIN stg.map_visit mv ON mv.encounter_id_source = o.encounter_id
WHERE NOT EXISTS (SELECT 1 FROM cdm.drug_exposure x WHERE x.drug_exposure_id = o.drug_exposure_id);
