-- Stage medication_fulfillment: parse Dispense_Date; TRY_CAST Dispense_Qty, Days_Of_Supply, Fill_No.
CREATE OR REPLACE TABLE stg.medication_fulfillment AS
SELECT
    TRIM("Order_ID") AS order_id,
    TRIM("Dispense_Date") AS dispense_date_raw,
    COALESCE(
        try_strptime(TRIM("Dispense_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Dispense_Date"), '%m/%d/%Y')
    )::DATE AS dispense_date,
    try_cast(TRIM("Dispense_Qty") AS DOUBLE) AS dispense_qty,
    try_cast(TRIM("Days_Of_Supply") AS INTEGER) AS days_of_supply,
    try_cast(TRIM("Fill_No") AS INTEGER) AS fill_no,
    TRIM("Encounter_ID") AS encounter_id
FROM src.medication_fulfillment;

CREATE OR REPLACE TABLE stg.reject_med_fulfillment AS
SELECT *
FROM src.medication_fulfillment
WHERE TRIM("Dispense_Date") IS NOT NULL AND TRIM("Dispense_Date") <> ''
  AND COALESCE(
        try_strptime(TRIM("Dispense_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Dispense_Date"), '%m/%d/%Y')
    ) IS NULL;
