-- Stage lab_results: parse Date_Collected, Date_Resulted; TRY_CAST Numeric_Result; normalize LOINC fields.
CREATE OR REPLACE TABLE stg.lab_results AS
SELECT
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("Order_ID" AS VARCHAR)) AS order_id,
    TRIM(CAST("Test_LOINC" AS VARCHAR)) AS test_loinc,
    TRIM(CAST("Test_Name" AS VARCHAR)) AS test_name,
    TRIM(CAST("Date_Collected" AS VARCHAR)) AS date_collected_raw,
    TRIM(CAST("Date_Resulted" AS VARCHAR)) AS date_resulted_raw,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Date_Collected" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Date_Collected" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Date_Collected" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Date_Collected" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) AS date_collected_datetime,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Date_Collected" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Date_Collected" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Date_Collected" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Date_Collected" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    )::DATE AS date_collected,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Date_Resulted" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Date_Resulted" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Date_Resulted" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Date_Resulted" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) AS date_resulted_datetime,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Date_Resulted" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Date_Resulted" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Date_Resulted" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Date_Resulted" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    )::DATE AS date_resulted,
    try_cast(TRIM(CAST("Numeric_Result" AS VARCHAR)) AS DOUBLE) AS numeric_result,
    TRIM(CAST("Units" AS VARCHAR)) AS units,
    TRIM(CAST("Result_Description" AS VARCHAR)) AS result_description,
    TRIM(CAST("Reference_Range" AS VARCHAR)) AS reference_range,
    -- Parse range_low, range_high from reference_range. Formats: "4.5-5.5", "4.5 - 5.5", "<5", ">10", "3 to 5"
    -- "<5" => range_high=5; ">10" => range_low=10; "4.5-5.5" => range_low=4.5, range_high=5.5
    CASE
        WHEN regexp_matches(TRIM(CAST("Reference_Range" AS VARCHAR)), '^>[0-9]') THEN try_cast(regexp_extract(TRIM(CAST("Reference_Range" AS VARCHAR)), '^>([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)
        ELSE COALESCE(
            try_cast(regexp_extract(TRIM(CAST("Reference_Range" AS VARCHAR)), '([0-9]+\\.?[0-9]*)\\s*[-]\\s*([0-9]+\\.?[0-9]*)', 1) AS DOUBLE),
            try_cast(regexp_extract(TRIM(CAST("Reference_Range" AS VARCHAR)), '([0-9]+\\.?[0-9]*)\\s+to\\s+([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)
        )
    END AS range_low,
    CASE
        WHEN regexp_matches(TRIM(CAST("Reference_Range" AS VARCHAR)), '^<[0-9]') THEN try_cast(regexp_extract(TRIM(CAST("Reference_Range" AS VARCHAR)), '^<([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)
        ELSE COALESCE(
            try_cast(regexp_extract(TRIM(CAST("Reference_Range" AS VARCHAR)), '([0-9]+\\.?[0-9]*)\\s*[-]\\s*([0-9]+\\.?[0-9]*)', 2) AS DOUBLE),
            try_cast(regexp_extract(TRIM(CAST("Reference_Range" AS VARCHAR)), '([0-9]+\\.?[0-9]*)\\s+to\\s+([0-9]+\\.?[0-9]*)', 2) AS DOUBLE)
        )
    END AS range_high,
    TRIM(CAST("Provider_ID" AS VARCHAR)) AS provider_id,
    TRIM(CAST("Encounter_ID" AS VARCHAR)) AS encounter_id
FROM src.lab_results;

CREATE OR REPLACE TABLE stg.reject_lab_results AS
SELECT *
FROM src.lab_results
WHERE (TRIM(CAST("Date_Collected" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("Date_Collected" AS VARCHAR)) <> '' AND COALESCE(try_strptime(SUBSTR(TRIM(CAST("Date_Collected" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'), try_strptime(TRIM(CAST("Date_Collected" AS VARCHAR)), '%Y-%m-%d'), try_strptime(TRIM(CAST("Date_Collected" AS VARCHAR)), '%m/%d/%Y')) IS NULL)
   OR (TRIM(CAST("Date_Resulted" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("Date_Resulted" AS VARCHAR)) <> '' AND COALESCE(try_strptime(SUBSTR(TRIM(CAST("Date_Resulted" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'), try_strptime(TRIM(CAST("Date_Resulted" AS VARCHAR)), '%Y-%m-%d'), try_strptime(TRIM(CAST("Date_Resulted" AS VARCHAR)), '%m/%d/%Y')) IS NULL);
