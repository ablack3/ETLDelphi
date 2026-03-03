-- Load cdm.note from stg.encounter SOAP_Note where not null/empty. note_id deterministic by Encounter_ID.
INSERT INTO cdm.note (
    note_id, person_id, note_date, note_datetime, note_type_concept_id, note_class_concept_id,
    note_text, encoding_concept_id, language_concept_id, visit_occurrence_id, note_source_value
)
SELECT
    900000000 + ROW_NUMBER() OVER (ORDER BY e.encounter_id) AS note_id,
    mp.person_id,
    e.encounter_date,
    e.encounter_datetime,
    {note_type_concept_id},
    {note_class_concept_id},
    COALESCE(TRIM(e.soap_note), ' '),
    {encoding_concept_id},
    {language_concept_id},
    mv.visit_occurrence_id,
    'SOAP_Note'
FROM stg.encounter e
JOIN stg.map_person mp ON mp.member_id = e.member_id
JOIN stg.map_visit mv ON mv.encounter_id_source = e.encounter_id
WHERE e.encounter_date IS NOT NULL
  AND e.soap_note IS NOT NULL AND TRIM(e.soap_note) <> ''
  AND NOT EXISTS (SELECT 1 FROM cdm.note n WHERE n.note_source_value = 'SOAP_Note' AND n.visit_occurrence_id = mv.visit_occurrence_id);

-- Reject: notes missing person or visit (already excluded by JOINs; table for audit)
CREATE OR REPLACE TABLE stg.reject_note_missing_link AS
SELECT e.encounter_id, e.member_id
FROM stg.encounter e
WHERE e.soap_note IS NOT NULL AND TRIM(e.soap_note) <> ''
  AND (NOT EXISTS (SELECT 1 FROM stg.map_person mp WHERE mp.member_id = e.member_id)
       OR NOT EXISTS (SELECT 1 FROM stg.map_visit mv WHERE mv.encounter_id_source = e.encounter_id));
