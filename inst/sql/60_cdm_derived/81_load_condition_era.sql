-- Build condition_era from condition_occurrence using 30-day persistence window.
-- Standard OHDSI era-building algorithm converted to DuckDB.
-- Consecutive condition records with <= 30 days gap are merged into one era.
-- Only mapped conditions (condition_concept_id != 0) are included.

DELETE FROM cdm.condition_era WHERE 1=1;

INSERT INTO cdm.condition_era (
    condition_era_id,
    person_id,
    condition_concept_id,
    condition_era_start_date,
    condition_era_end_date,
    condition_occurrence_count
)
WITH
-- 1. Normalize: ensure every record has a valid end date
cte_condition_target AS (
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date,
        COALESCE(condition_end_date, condition_start_date + INTERVAL '1 day') AS condition_end_date
    FROM cdm.condition_occurrence
    WHERE condition_concept_id != 0
      AND condition_start_date IS NOT NULL
),

-- 2. Build event stream: starts (type -1) and padded ends (type +1)
--    Then find era boundaries using start_ordinal vs overall_ord
cte_end_dates AS (
    SELECT
        person_id,
        condition_concept_id,
        event_date - INTERVAL '30 days' AS end_date  -- unpad
    FROM (
        SELECT
            e1.person_id,
            e1.condition_concept_id,
            e1.event_date,
            COALESCE(e1.start_ordinal, MAX(e2.start_ordinal)) AS start_ordinal,
            e1.overall_ord
        FROM (
            SELECT
                person_id,
                condition_concept_id,
                event_date,
                event_type,
                start_ordinal,
                ROW_NUMBER() OVER (
                    PARTITION BY person_id, condition_concept_id
                    ORDER BY event_date, event_type
                ) AS overall_ord
            FROM (
                -- Start dates
                SELECT
                    person_id,
                    condition_concept_id,
                    condition_start_date AS event_date,
                    -1 AS event_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY person_id, condition_concept_id
                        ORDER BY condition_start_date
                    ) AS start_ordinal
                FROM cte_condition_target

                UNION ALL

                -- Padded end dates (+30 days)
                SELECT
                    person_id,
                    condition_concept_id,
                    condition_end_date + INTERVAL '30 days' AS event_date,
                    1 AS event_type,
                    NULL AS start_ordinal
                FROM cte_condition_target
            ) rawdata
        ) e1
        INNER JOIN (
            SELECT
                person_id,
                condition_concept_id,
                condition_start_date AS event_date,
                ROW_NUMBER() OVER (
                    PARTITION BY person_id, condition_concept_id
                    ORDER BY condition_start_date
                ) AS start_ordinal
            FROM cte_condition_target
        ) e2
            ON e1.person_id = e2.person_id
            AND e1.condition_concept_id = e2.condition_concept_id
            AND e2.event_date <= e1.event_date
        GROUP BY
            e1.person_id,
            e1.condition_concept_id,
            e1.event_date,
            e1.start_ordinal,
            e1.overall_ord
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),

-- 3. Match each condition start to its nearest era end date
cte_condition_ends AS (
    SELECT
        c.person_id,
        c.condition_concept_id,
        c.condition_start_date,
        MIN(e.end_date) AS era_end_date
    FROM cte_condition_target c
    INNER JOIN cte_end_dates e
        ON c.person_id = e.person_id
        AND c.condition_concept_id = e.condition_concept_id
        AND e.end_date >= c.condition_start_date
    GROUP BY
        c.person_id,
        c.condition_concept_id,
        c.condition_start_date
)

-- 4. Aggregate into eras
SELECT
    ROW_NUMBER() OVER (ORDER BY person_id, condition_concept_id, era_end_date) AS condition_era_id,
    person_id,
    condition_concept_id,
    MIN(condition_start_date) AS condition_era_start_date,
    era_end_date AS condition_era_end_date,
    COUNT(*) AS condition_occurrence_count
FROM cte_condition_ends
GROUP BY
    person_id,
    condition_concept_id,
    era_end_date;
