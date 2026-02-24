-- Build dose_era from drug_exposure + drug_strength using 30-day persistence window.
-- A dose era is a continuous span where a person receives the same ingredient at the same dose.
-- Requires cdm.drug_strength to compute daily dose per ingredient.
-- Dose form changes do NOT break an era (only dose value changes do).
-- Only mapped drugs (drug_concept_id != 0) with known strength are included.

DELETE FROM cdm.dose_era WHERE 1=1;

INSERT INTO cdm.dose_era (
    dose_era_id,
    person_id,
    drug_concept_id,
    unit_concept_id,
    dose_value,
    dose_era_start_date,
    dose_era_end_date
)
WITH
-- 1. Compute daily dose per drug exposure using drug_strength
--    amount_value is for fixed-amount drugs (e.g. 500mg tablet)
--    numerator/denominator is for concentration-based drugs (e.g. 100mg/mL)
cte_dose_target AS (
    SELECT
        d.person_id,
        ds.ingredient_concept_id,
        d.drug_exposure_start_date,
        COALESCE(
            NULLIF(d.drug_exposure_end_date, d.drug_exposure_start_date),
            d.drug_exposure_start_date + (COALESCE(NULLIF(d.days_supply, 0), 1) * INTERVAL '1 day')
        ) AS drug_exposure_end_date,
        COALESCE(
            ds.amount_value,
            CASE WHEN ds.denominator_value IS NOT NULL AND ds.denominator_value > 0
                 THEN ds.numerator_value / ds.denominator_value
                 ELSE ds.numerator_value
            END
        ) AS dose_value,
        COALESCE(
            NULLIF(ds.amount_unit_concept_id, 0),
            NULLIF(ds.numerator_unit_concept_id, 0),
            0
        ) AS unit_concept_id
    FROM cdm.drug_exposure d
    JOIN cdm.drug_strength ds
        ON ds.drug_concept_id = d.drug_concept_id
    WHERE d.drug_concept_id != 0
      AND d.drug_exposure_start_date IS NOT NULL
      AND COALESCE(d.days_supply, 0) >= 0
      -- Must have a computable dose
      AND (ds.amount_value IS NOT NULL OR ds.numerator_value IS NOT NULL)
),

-- 2. Build event stream partitioned by person + ingredient + unit + dose
--    (dose changes create separate eras)
cte_end_dates AS (
    SELECT
        person_id,
        ingredient_concept_id,
        unit_concept_id,
        dose_value,
        event_date - INTERVAL '30 days' AS end_date  -- unpad
    FROM (
        SELECT
            person_id,
            ingredient_concept_id,
            unit_concept_id,
            dose_value,
            event_date,
            event_type,
            MAX(start_ordinal) OVER (
                PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value
                ORDER BY event_date, event_type
                ROWS UNBOUNDED PRECEDING
            ) AS start_ordinal,
            ROW_NUMBER() OVER (
                PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value
                ORDER BY event_date, event_type
            ) AS overall_ord
        FROM (
            -- Start dates
            SELECT
                person_id,
                ingredient_concept_id,
                unit_concept_id,
                dose_value,
                drug_exposure_start_date AS event_date,
                -1 AS event_type,
                ROW_NUMBER() OVER (
                    PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value
                    ORDER BY drug_exposure_start_date
                ) AS start_ordinal
            FROM cte_dose_target

            UNION ALL

            -- Padded end dates (+30 days)
            SELECT
                person_id,
                ingredient_concept_id,
                unit_concept_id,
                dose_value,
                drug_exposure_end_date + INTERVAL '30 days' AS event_date,
                1 AS event_type,
                NULL AS start_ordinal
            FROM cte_dose_target
        ) rawdata
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),

-- 3. Match each exposure to its era end date
cte_dose_ends AS (
    SELECT
        dt.person_id,
        dt.ingredient_concept_id,
        dt.unit_concept_id,
        dt.dose_value,
        dt.drug_exposure_start_date,
        MIN(e.end_date) AS era_end_date
    FROM cte_dose_target dt
    JOIN cte_end_dates e
        ON dt.person_id = e.person_id
        AND dt.ingredient_concept_id = e.ingredient_concept_id
        AND dt.unit_concept_id = e.unit_concept_id
        AND dt.dose_value = e.dose_value
        AND e.end_date >= dt.drug_exposure_start_date
    GROUP BY
        dt.person_id,
        dt.ingredient_concept_id,
        dt.unit_concept_id,
        dt.dose_value,
        dt.drug_exposure_start_date
)

-- 4. Aggregate into dose eras
SELECT
    ROW_NUMBER() OVER (ORDER BY person_id, ingredient_concept_id, unit_concept_id, dose_value, era_end_date) AS dose_era_id,
    person_id,
    ingredient_concept_id AS drug_concept_id,
    unit_concept_id,
    dose_value,
    MIN(drug_exposure_start_date) AS dose_era_start_date,
    era_end_date AS dose_era_end_date
FROM cte_dose_ends
GROUP BY
    person_id,
    ingredient_concept_id,
    unit_concept_id,
    dose_value,
    era_end_date;
