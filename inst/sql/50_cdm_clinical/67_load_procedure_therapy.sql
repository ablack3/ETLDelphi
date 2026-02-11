-- Load procedure_occurrence from stg.therapy_orders and stg.therapy_actions. procedure_date from encounter via Encounter_ID.
INSERT INTO cdm.procedure_occurrence (
    procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id,
    visit_occurrence_id, procedure_source_value, modifier_source_value
)
WITH therapy_union AS (
    SELECT member_id, order_id, code, name, target_area, vocabulary, encounter_id, 'orders' AS table_source FROM stg.therapy_orders
    UNION ALL
    SELECT member_id, order_id, code, name, target_area, vocabulary, encounter_id, 'actions' FROM stg.therapy_actions
),
with_date AS (
    SELECT t.*, e.encounter_date
    FROM therapy_union t
    LEFT JOIN stg.encounter e ON e.encounter_id = t.encounter_id
),
with_id AS (
    SELECT *, 100000000 + ROW_NUMBER() OVER (ORDER BY member_id, order_id, code, encounter_id, table_source) AS procedure_occurrence_id
    FROM with_date
    WHERE encounter_date IS NOT NULL
)
SELECT
    w.procedure_occurrence_id,
    mp.person_id,
    COALESCE(mt.procedure_concept_id, cust.concept_id, 0),
    w.encounter_date,
    38000268,
    mv.visit_occurrence_id,
    SUBSTR(COALESCE(w.code, w.name), 1, 50),
    SUBSTR(w.target_area, 1, 50)
FROM with_id w
JOIN stg.map_person mp ON mp.member_id = w.member_id
LEFT JOIN stg.map_therapy mt ON mt.code = w.code AND (mt.vocabulary = w.vocabulary OR (mt.vocabulary IS NULL AND w.vocabulary IS NULL))
LEFT JOIN stg.custom_concept_mapping cust ON cust.source_value = TRIM(SUBSTR(COALESCE(w.code, w.name), 1, 50)) AND cust.domain = 'procedure'
LEFT JOIN stg.map_visit mv ON mv.encounter_id_source = w.encounter_id
WHERE NOT EXISTS (SELECT 1 FROM cdm.procedure_occurrence p WHERE p.procedure_occurrence_id = w.procedure_occurrence_id);

CREATE OR REPLACE TABLE stg.reject_procedure_missing AS
SELECT t.member_id, t.order_id, t.code, t.encounter_id FROM (
    SELECT member_id, order_id, code, encounter_id FROM stg.therapy_orders
    UNION ALL
    SELECT member_id, order_id, code, encounter_id FROM stg.therapy_actions
) t
LEFT JOIN stg.encounter e ON e.encounter_id = t.encounter_id
WHERE t.encounter_id IS NOT NULL AND e.encounter_date IS NULL
   OR NOT EXISTS (SELECT 1 FROM stg.map_person mp WHERE mp.member_id = t.member_id);
