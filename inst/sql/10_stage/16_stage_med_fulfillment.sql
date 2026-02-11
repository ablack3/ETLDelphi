-- Stage medication_fulfillment: parse Dispense_Date; TRY_CAST Dispense_Qty, Days_Of_Supply, Fill_No.
CREATE OR REPLACE TABLE stg.medication_fulfillment AS
SELECT
    TRIM(CAST("Order_ID" AS VARCHAR)) AS order_id,
    TRIM(CAST("Dispense_Date" AS VARCHAR)) AS dispense_date_raw,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Dispense_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Dispense_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Dispense_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Dispense_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    )::DATE AS dispense_date,
    try_cast(TRIM(CAST("Dispense_Qty" AS VARCHAR)) AS DOUBLE) AS dispense_qty,
    try_cast(TRIM(CAST("Days_Of_Supply" AS VARCHAR)) AS INTEGER) AS days_of_supply,
    try_cast(TRIM(CAST("Fill_No" AS VARCHAR)) AS INTEGER) AS fill_no,
    TRIM(CAST("Encounter_ID" AS VARCHAR)) AS encounter_id
FROM src.medication_fulfillment;

CREATE OR REPLACE TABLE stg.reject_med_fulfillment AS
SELECT *
FROM src.medication_fulfillment
WHERE TRIM(CAST("Dispense_Date" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("Dispense_Date" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Dispense_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Dispense_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Dispense_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Dispense_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) IS NULL;
