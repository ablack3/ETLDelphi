-- Load drug_exposure from stg.immunization. drug_type_concept_id 32818 (EHR administration record).
-- drug_concept_id and drug_source_concept_id from map_immunization (CVX -> concept via Maps to).
INSERT INTO cdm.drug_exposure (
    drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date,
    drug_type_concept_id, lot_number, provider_id, visit_occurrence_id,
    drug_source_value, drug_source_concept_id, route_source_value, dose_unit_source_value
)
WITH imm AS (
    SELECT
        i.*,
        500000000 + ROW_NUMBER() OVER (ORDER BY i.member_id, i.vaccination_date, i.vaccine_cvx, i.vaccine_name) AS drug_exposure_id
    FROM stg.immunization i
    WHERE i.vaccination_date IS NOT NULL
)
SELECT
    i.drug_exposure_id,
    mp.person_id,
    COALESCE(NULLIF(mi.drug_concept_id, 0), cust.concept_id, 0),
    i.vaccination_date,
    i.vaccination_date,
    32818,
    SUBSTR(i.lot_number, 1, 50),
    mpr.provider_id,
    mv.visit_occurrence_id,
    SUBSTR(COALESCE(i.vaccine_name, i.vaccine_cvx), 1, 50),
    COALESCE(mi.drug_source_concept_id, 0),
    SUBSTR(i.route, 1, 50),
    SUBSTR(i.units, 1, 50)
FROM imm i
JOIN stg.map_person mp ON mp.member_id = i.member_id
LEFT JOIN stg.map_immunization mi ON mi.vaccine_cvx = TRIM(i.vaccine_cvx)
LEFT JOIN stg.custom_concept_mapping cust ON cust.source_value = TRIM(SUBSTR(COALESCE(i.vaccine_name, i.vaccine_cvx), 1, 50)) AND cust.domain = 'drug'
LEFT JOIN stg.map_provider mpr ON mpr.provider_id_source = i.provider_id
LEFT JOIN stg.map_visit mv ON mv.encounter_id_source = i.encounter_id
WHERE NOT EXISTS (SELECT 1 FROM cdm.drug_exposure x WHERE x.drug_exposure_id = i.drug_exposure_id);
