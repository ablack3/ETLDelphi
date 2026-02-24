-- Parse SOAP note sections from cdm.note into stg.note_parsed.
-- Notes follow template format: s:<subjective> o:<objective> a:<assessment> p:<plan>
-- Section markers: 's:' at start, ' o:', ' a:', ' p:' with leading space.

CREATE OR REPLACE TABLE stg.note_parsed AS
WITH positions AS (
    SELECT
        n.note_id,
        n.person_id,
        n.note_date,
        n.visit_occurrence_id,
        n.note_text,
        -- s: always starts at position 1
        CASE WHEN n.note_text LIKE 's:%' THEN 3 ELSE NULL END AS start_s,
        -- Find section boundaries (space + marker)
        CASE WHEN STRPOS(n.note_text, ' o:') > 0 THEN STRPOS(n.note_text, ' o:') ELSE NULL END AS pos_o,
        CASE WHEN STRPOS(n.note_text, ' a:') > 0 THEN STRPOS(n.note_text, ' a:') ELSE NULL END AS pos_a,
        CASE WHEN STRPOS(n.note_text, ' p:') > 0 THEN STRPOS(n.note_text, ' p:') ELSE NULL END AS pos_p
    FROM cdm.note n
    WHERE n.note_text IS NOT NULL
      AND LENGTH(TRIM(n.note_text)) > 10
)
SELECT
    note_id,
    person_id,
    note_date,
    visit_occurrence_id,
    -- Subjective: from position 3 to before ' o:'
    CASE
        WHEN start_s IS NOT NULL AND pos_o IS NOT NULL
        THEN TRIM(SUBSTR(note_text, start_s, pos_o - start_s))
        WHEN start_s IS NOT NULL AND pos_o IS NULL AND pos_a IS NOT NULL
        THEN TRIM(SUBSTR(note_text, start_s, pos_a - start_s))
        ELSE NULL
    END AS section_subjective,
    -- Objective: from after ' o:' to before ' a:'
    CASE
        WHEN pos_o IS NOT NULL AND pos_a IS NOT NULL AND pos_a > pos_o
        THEN TRIM(SUBSTR(note_text, pos_o + 3, pos_a - pos_o - 3))
        WHEN pos_o IS NOT NULL AND pos_a IS NULL AND pos_p IS NOT NULL
        THEN TRIM(SUBSTR(note_text, pos_o + 3, pos_p - pos_o - 3))
        ELSE NULL
    END AS section_objective,
    -- Assessment: from after ' a:' to before ' p:'
    CASE
        WHEN pos_a IS NOT NULL AND pos_p IS NOT NULL AND pos_p > pos_a
        THEN TRIM(SUBSTR(note_text, pos_a + 3, pos_p - pos_a - 3))
        WHEN pos_a IS NOT NULL AND pos_p IS NULL
        THEN TRIM(SUBSTR(note_text, pos_a + 3))
        ELSE NULL
    END AS section_assessment,
    -- Plan: from after ' p:' to end
    CASE
        WHEN pos_p IS NOT NULL
        THEN TRIM(SUBSTR(note_text, pos_p + 3))
        ELSE NULL
    END AS section_plan
FROM positions;
