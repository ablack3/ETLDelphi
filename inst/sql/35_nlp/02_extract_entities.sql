-- Extract structured entities from parsed SOAP note sections.
-- Creates staging tables: nlp_conditions, nlp_prescriptions, nlp_procedures, nlp_screenings.
-- Depends on: stg.note_parsed (from 01_parse_note_sections.sql).

-- ============================================================================
-- 2a. CONDITIONS from Assessment section
-- The assessment contains the diagnosis (e.g., "Lung Cancer", "Leukemia").
-- Some assessments include immunization info: "no current issues, administered immunizations: ..."
-- We extract the diagnosis part and strip immunization-only assessments.
-- ============================================================================

CREATE OR REPLACE TABLE stg.nlp_conditions AS
WITH raw AS (
    SELECT
        note_id,
        person_id,
        note_date,
        visit_occurrence_id,
        -- Strip immunization suffix: "no current issues, administered immunizations: X"
        -- Keep the assessment part before ", administered"
        CASE
            WHEN STRPOS(LOWER(section_assessment), ', administered') > 0
            THEN TRIM(SUBSTR(section_assessment, 1, STRPOS(LOWER(section_assessment), ', administered') - 1))
            ELSE TRIM(section_assessment)
        END AS assessment_text
    FROM stg.note_parsed
    WHERE section_assessment IS NOT NULL
      AND TRIM(section_assessment) != ''
)
SELECT
    note_id,
    person_id,
    note_date,
    visit_occurrence_id,
    assessment_text AS lexical_variant,
    'Y' AS term_exists,
    'current' AS term_temporal
FROM raw
WHERE assessment_text IS NOT NULL
  AND assessment_text != ''
  AND LOWER(assessment_text) != 'no current issues';


-- ============================================================================
-- 2b. PRESCRIPTIONS from Plan section
-- Pattern: "prescribe <drug1> - <dose1>,<drug2> - <dose2>"
-- May be followed by " and order <proc>" or " and schedule <proc>"
-- ============================================================================

CREATE OR REPLACE TABLE stg.nlp_prescriptions AS
WITH rx_blocks AS (
    SELECT
        note_id,
        person_id,
        note_date,
        visit_occurrence_id,
        -- Extract the prescribe block: text after "prescribe " up to " and order"/" and schedule"/end
        CASE
            WHEN section_plan LIKE '%prescribe %' AND STRPOS(section_plan, ' and order') > STRPOS(section_plan, 'prescribe ')
            THEN TRIM(SUBSTR(section_plan,
                STRPOS(section_plan, 'prescribe ') + 10,
                STRPOS(section_plan, ' and order') - STRPOS(section_plan, 'prescribe ') - 10))
            WHEN section_plan LIKE '%prescribe %' AND STRPOS(section_plan, ' and schedule') > STRPOS(section_plan, 'prescribe ')
            THEN TRIM(SUBSTR(section_plan,
                STRPOS(section_plan, 'prescribe ') + 10,
                STRPOS(section_plan, ' and schedule') - STRPOS(section_plan, 'prescribe ') - 10))
            WHEN section_plan LIKE '%prescribe %'
            THEN TRIM(SUBSTR(section_plan, STRPOS(section_plan, 'prescribe ') + 10))
            ELSE NULL
        END AS rx_block
    FROM stg.note_parsed
    WHERE section_plan LIKE '%prescribe %'
)
SELECT
    b.note_id,
    b.person_id,
    b.note_date,
    b.visit_occurrence_id,
    TRIM(SPLIT_PART(t.drug_dose, ' - ', 1)) AS drug_name,
    TRIM(SUBSTR(t.drug_dose, STRPOS(t.drug_dose, ' - ') + 3)) AS dose,
    'plan' AS section
FROM rx_blocks b,
    UNNEST(string_split(b.rx_block, ',')) AS t(drug_dose)
WHERE b.rx_block IS NOT NULL
  AND TRIM(t.drug_dose) != ''
  AND STRPOS(t.drug_dose, ' - ') > 0;


-- ============================================================================
-- 2c. PROCEDURES from Plan section
-- Patterns: "order <procedure>" and "schedule <procedure> - <body_part>"
-- ============================================================================

CREATE OR REPLACE TABLE stg.nlp_procedures AS
-- Orders: "order <procedure_name>"
SELECT
    note_id,
    person_id,
    note_date,
    visit_occurrence_id,
    CASE
        WHEN STRPOS(section_plan, ' and prescribe') > STRPOS(section_plan, 'order ')
        THEN TRIM(SUBSTR(section_plan,
            STRPOS(section_plan, 'order ') + 6,
            STRPOS(section_plan, ' and prescribe') - STRPOS(section_plan, 'order ') - 6))
        WHEN STRPOS(section_plan, ' and schedule') > STRPOS(section_plan, 'order ')
        THEN TRIM(SUBSTR(section_plan,
            STRPOS(section_plan, 'order ') + 6,
            STRPOS(section_plan, ' and schedule') - STRPOS(section_plan, 'order ') - 6))
        ELSE TRIM(SUBSTR(section_plan, STRPOS(section_plan, 'order ') + 6))
    END AS procedure_name,
    'order' AS action_type,
    'plan' AS section
FROM stg.note_parsed
WHERE section_plan LIKE '%order %'
  AND section_plan NOT LIKE '%sooner for new symptoms%'

UNION ALL

-- Schedules: "schedule <procedure_name> - <body_part>"
SELECT
    note_id,
    person_id,
    note_date,
    visit_occurrence_id,
    CASE
        WHEN STRPOS(section_plan, ' and prescribe') > STRPOS(section_plan, 'schedule ')
        THEN TRIM(SUBSTR(section_plan,
            STRPOS(section_plan, 'schedule ') + 9,
            STRPOS(section_plan, ' and prescribe') - STRPOS(section_plan, 'schedule ') - 9))
        WHEN STRPOS(section_plan, ' and order') > STRPOS(section_plan, 'schedule ')
        THEN TRIM(SUBSTR(section_plan,
            STRPOS(section_plan, 'schedule ') + 9,
            STRPOS(section_plan, ' and order') - STRPOS(section_plan, 'schedule ') - 9))
        ELSE TRIM(SUBSTR(section_plan, STRPOS(section_plan, 'schedule ') + 9))
    END AS procedure_name,
    'schedule' AS action_type,
    'plan' AS section
FROM stg.note_parsed
WHERE section_plan LIKE '%schedule %'
  AND section_plan NOT LIKE '%well child%';


-- ============================================================================
-- 2d. SCREENING RESULTS from Objective section
-- Objective contains vitals + screening/exam results, comma-separated.
-- Filter out vitals (Height, Weight, Temperature, Pulse, BP, Respiration).
-- Remaining items are screening results and physical exam findings.
-- ============================================================================

CREATE OR REPLACE TABLE stg.nlp_screenings AS
SELECT
    np.note_id,
    np.person_id,
    np.note_date,
    np.visit_occurrence_id,
    TRIM(t.item) AS lexical_variant,
    CASE
        WHEN LOWER(TRIM(t.item)) LIKE '%no abnormal%' THEN 'N'
        WHEN LOWER(TRIM(t.item)) LIKE '%no polyps%' THEN 'N'
        WHEN LOWER(TRIM(t.item)) LIKE '%no growth%' THEN 'N'
        WHEN LOWER(TRIM(t.item)) LIKE '%no pouches%' THEN 'N'
        WHEN LOWER(TRIM(t.item)) LIKE '%no lump%' THEN 'N'
        WHEN LOWER(TRIM(t.item)) LIKE '%no nasal%' THEN 'N'
        WHEN LOWER(TRIM(t.item)) LIKE '%no murmurs%' THEN 'N'
        WHEN LOWER(TRIM(t.item)) LIKE '%negative%' THEN 'N'
        WHEN LOWER(TRIM(t.item)) LIKE '%pass%' THEN 'N'
        WHEN LOWER(TRIM(t.item)) LIKE '%normal%' THEN 'N'
        ELSE 'Y'
    END AS term_exists
FROM stg.note_parsed np,
    UNNEST(string_split(np.section_objective, ',')) AS t(item)
WHERE np.section_objective IS NOT NULL
  -- Filter out vitals
  AND TRIM(t.item) != ''
  AND TRIM(t.item) NOT LIKE 'Height %'
  AND TRIM(t.item) NOT LIKE 'Weight %'
  AND TRIM(t.item) NOT LIKE 'Temperature %'
  AND TRIM(t.item) NOT LIKE 'Pulse %'
  AND TRIM(t.item) NOT LIKE 'SystolicBP %'
  AND TRIM(t.item) NOT LIKE 'DiastolicBP %'
  AND TRIM(t.item) NOT LIKE 'Respiration %'
  -- Filter out items that are just numbers or single words
  AND LENGTH(TRIM(t.item)) > 5;
