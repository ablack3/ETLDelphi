-- Stage lab_orders: parse Order_Date; normalize Test_LOINC (trim).
CREATE OR REPLACE TABLE stg.lab_orders AS
SELECT
    TRIM(CAST("Order_ID" AS VARCHAR)) AS order_id,
    TRIM(CAST("Order_Date" AS VARCHAR)) AS order_date_raw,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Order_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    )::DATE AS order_date,
    TRIM(CAST("Patient_ID" AS VARCHAR)) AS patient_id,
    TRIM(CAST("Test_LOINC" AS VARCHAR)) AS test_loinc,
    TRIM(CAST("Test_Name" AS VARCHAR)) AS test_name,
    TRIM(CAST("Encounter_ID" AS VARCHAR)) AS encounter_id
FROM src.lab_orders;

CREATE OR REPLACE TABLE stg.reject_lab_orders AS
SELECT *
FROM src.lab_orders
WHERE TRIM(CAST("Order_Date" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("Order_Date" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Order_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) IS NULL;
