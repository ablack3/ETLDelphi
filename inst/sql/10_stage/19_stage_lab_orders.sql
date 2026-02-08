-- Stage lab_orders: parse Order_Date; normalize Test_LOINC (trim).
CREATE OR REPLACE TABLE stg.lab_orders AS
SELECT
    TRIM("Order_ID") AS order_id,
    TRIM("Order_Date") AS order_date_raw,
    COALESCE(
        try_strptime(TRIM("Order_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Order_Date"), '%m/%d/%Y')
    )::DATE AS order_date,
    TRIM("Patient_ID") AS patient_id,
    TRIM("Test_LOINC") AS test_loinc,
    TRIM("Test_Name") AS test_name,
    TRIM("Encounter_ID") AS encounter_id
FROM src.lab_orders;

CREATE OR REPLACE TABLE stg.reject_lab_orders AS
SELECT *
FROM src.lab_orders
WHERE TRIM("Order_Date") IS NOT NULL AND TRIM("Order_Date") <> ''
  AND COALESCE(
        try_strptime(TRIM("Order_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Order_Date"), '%m/%d/%Y')
    ) IS NULL;
