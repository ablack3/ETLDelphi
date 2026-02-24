-- Build drug_era from drug_exposure using 30-day persistence window.
-- Standard OHDSI era-building algorithm converted to DuckDB.
-- Drug exposures are rolled up to RxNorm Ingredient level via concept_ancestor.
-- Consecutive exposures to the same ingredient with <= 30 day gaps are merged.
-- Only mapped drugs (drug_concept_id != 0) are included.
-- gap_days = total drug-free days within the era (non-stockpile assumption).

DELETE FROM cdm.drug_era WHERE 1=1;

INSERT INTO cdm.drug_era (
    drug_era_id,
    person_id,
    drug_concept_id,
    drug_era_start_date,
    drug_era_end_date,
    drug_exposure_count,
    gap_days
)
WITH
-- 1. Roll up to ingredient level and normalize end dates
cte_drug_target AS (
    SELECT
        d.drug_exposure_id,
        d.person_id,
        c.concept_id AS ingredient_concept_id,
        d.drug_exposure_start_date,
        COALESCE(
            NULLIF(d.drug_exposure_end_date, d.drug_exposure_start_date),
            d.drug_exposure_start_date + (COALESCE(NULLIF(d.days_supply, 0), 1) * INTERVAL '1 day')
        ) AS drug_exposure_end_date
    FROM cdm.drug_exposure d
    JOIN cdm.concept_ancestor ca
        ON ca.descendant_concept_id = d.drug_concept_id
    JOIN cdm.concept c
        ON ca.ancestor_concept_id = c.concept_id
        AND c.vocabulary_id = 'RxNorm'
        AND c.concept_class_id = 'Ingredient'
    WHERE d.drug_concept_id != 0
      AND d.drug_exposure_start_date IS NOT NULL
      AND COALESCE(d.days_supply, 0) >= 0
),

-- 2. First pass: collapse overlapping exposures into sub-exposures (no gap window yet)
cte_sub_end_dates AS (
    SELECT
        person_id,
        ingredient_concept_id,
        event_date AS end_date
    FROM (
        SELECT
            person_id,
            ingredient_concept_id,
            event_date,
            event_type,
            MAX(start_ordinal) OVER (
                PARTITION BY person_id, ingredient_concept_id
                ORDER BY event_date, event_type
                ROWS UNBOUNDED PRECEDING
            ) AS start_ordinal,
            ROW_NUMBER() OVER (
                PARTITION BY person_id, ingredient_concept_id
                ORDER BY event_date, event_type
            ) AS overall_ord
        FROM (
            SELECT
                person_id,
                ingredient_concept_id,
                drug_exposure_start_date AS event_date,
                -1 AS event_type,
                ROW_NUMBER() OVER (
                    PARTITION BY person_id, ingredient_concept_id
                    ORDER BY drug_exposure_start_date
                ) AS start_ordinal
            FROM cte_drug_target

            UNION ALL

            SELECT
                person_id,
                ingredient_concept_id,
                drug_exposure_end_date AS event_date,
                1 AS event_type,
                NULL AS start_ordinal
            FROM cte_drug_target
        ) rawdata
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),

-- 3. Match each exposure to its sub-exposure end date
cte_drug_exposure_ends AS (
    SELECT
        dt.person_id,
        dt.ingredient_concept_id,
        dt.drug_exposure_start_date,
        MIN(e.end_date) AS drug_sub_exposure_end_date
    FROM cte_drug_target dt
    JOIN cte_sub_end_dates e
        ON dt.person_id = e.person_id
        AND dt.ingredient_concept_id = e.ingredient_concept_id
        AND e.end_date >= dt.drug_exposure_start_date
    GROUP BY
        dt.drug_exposure_id,
        dt.person_id,
        dt.ingredient_concept_id,
        dt.drug_exposure_start_date
),

-- 4. Aggregate sub-exposures
cte_sub_exposures AS (
    SELECT
        person_id,
        ingredient_concept_id,
        MIN(drug_exposure_start_date) AS drug_sub_exposure_start_date,
        drug_sub_exposure_end_date,
        COUNT(*) AS drug_exposure_count,
        DATE_DIFF('day', MIN(drug_exposure_start_date), drug_sub_exposure_end_date) AS days_exposed
    FROM cte_drug_exposure_ends
    GROUP BY
        person_id,
        ingredient_concept_id,
        drug_sub_exposure_end_date
),

-- 5. Second pass: apply 30-day persistence window to sub-exposures
cte_era_end_dates AS (
    SELECT
        person_id,
        ingredient_concept_id,
        event_date - INTERVAL '30 days' AS end_date  -- unpad
    FROM (
        SELECT
            person_id,
            ingredient_concept_id,
            event_date,
            event_type,
            MAX(start_ordinal) OVER (
                PARTITION BY person_id, ingredient_concept_id
                ORDER BY event_date, event_type
                ROWS UNBOUNDED PRECEDING
            ) AS start_ordinal,
            ROW_NUMBER() OVER (
                PARTITION BY person_id, ingredient_concept_id
                ORDER BY event_date, event_type
            ) AS overall_ord
        FROM (
            SELECT
                person_id,
                ingredient_concept_id,
                drug_sub_exposure_start_date AS event_date,
                -1 AS event_type,
                ROW_NUMBER() OVER (
                    PARTITION BY person_id, ingredient_concept_id
                    ORDER BY drug_sub_exposure_start_date
                ) AS start_ordinal
            FROM cte_sub_exposures

            UNION ALL

            -- Pad end dates by 30 days
            SELECT
                person_id,
                ingredient_concept_id,
                drug_sub_exposure_end_date + INTERVAL '30 days' AS event_date,
                1 AS event_type,
                NULL AS start_ordinal
            FROM cte_sub_exposures
        ) rawdata
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),

-- 6. Match sub-exposures to era end dates
cte_drug_era_ends AS (
    SELECT
        se.person_id,
        se.ingredient_concept_id,
        se.drug_sub_exposure_start_date,
        MIN(e.end_date) AS drug_era_end_date,
        se.drug_exposure_count,
        se.days_exposed
    FROM cte_sub_exposures se
    JOIN cte_era_end_dates e
        ON se.person_id = e.person_id
        AND se.ingredient_concept_id = e.ingredient_concept_id
        AND e.end_date >= se.drug_sub_exposure_start_date
    GROUP BY
        se.person_id,
        se.ingredient_concept_id,
        se.drug_sub_exposure_start_date,
        se.drug_exposure_count,
        se.days_exposed
)

-- 7. Final aggregation into drug eras
SELECT
    ROW_NUMBER() OVER (ORDER BY person_id, ingredient_concept_id, drug_era_end_date) AS drug_era_id,
    person_id,
    ingredient_concept_id AS drug_concept_id,
    MIN(drug_sub_exposure_start_date) AS drug_era_start_date,
    drug_era_end_date,
    SUM(drug_exposure_count) AS drug_exposure_count,
    DATE_DIFF('day', MIN(drug_sub_exposure_start_date), drug_era_end_date) - SUM(days_exposed) AS gap_days
FROM cte_drug_era_ends
GROUP BY
    person_id,
    ingredient_concept_id,
    drug_era_end_date;
