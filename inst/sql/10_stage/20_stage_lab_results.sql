-- Stage lab_results: parse Date_Collected, Date_Resulted; TRY_CAST Numeric_Result; normalize LOINC fields.
CREATE OR REPLACE TABLE stg.lab_results AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("Order_ID") AS order_id,
    TRIM("Test_LOINC") AS test_loinc,
    TRIM("Test_Name") AS test_name,
    TRIM("Date_Collected") AS date_collected_raw,
    TRIM("Date_Resulted") AS date_resulted_raw,
    COALESCE(
        try_strptime(TRIM("Date_Collected"), '%Y-%m-%d'),
        try_strptime(TRIM("Date_Collected"), '%m/%d/%Y')
    )::DATE AS date_collected,
    COALESCE(
        try_strptime(TRIM("Date_Resulted"), '%Y-%m-%d'),
        try_strptime(TRIM("Date_Resulted"), '%m/%d/%Y')
    )::DATE AS date_resulted,
    try_cast(TRIM("Numeric_Result") AS DOUBLE) AS numeric_result,
    TRIM("Units") AS units,
    TRIM("Result_Description") AS result_description,
    TRIM("Reference_Range") AS reference_range,
    TRIM("Provider_ID") AS provider_id,
    TRIM("Encounter_ID") AS encounter_id
FROM src.lab_results;

CREATE OR REPLACE TABLE stg.reject_lab_results AS
SELECT *
FROM src.lab_results
WHERE (TRIM("Date_Collected") IS NOT NULL AND TRIM("Date_Collected") <> '' AND COALESCE(try_strptime(TRIM("Date_Collected"), '%Y-%m-%d'), try_strptime(TRIM("Date_Collected"), '%m/%d/%Y')) IS NULL)
   OR (TRIM("Date_Resulted") IS NOT NULL AND TRIM("Date_Resulted") <> '' AND COALESCE(try_strptime(TRIM("Date_Resulted"), '%Y-%m-%d'), try_strptime(TRIM("Date_Resulted"), '%m/%d/%Y')) IS NULL);
