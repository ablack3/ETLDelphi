-- Stage current_medications: parse Last_Filled_Date; TRY_CAST Refills, Days_Of_Supply.
CREATE OR REPLACE TABLE stg.current_medications AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("Last_Filled_Date") AS last_filled_date_raw,
    COALESCE(
        try_strptime(TRIM("Last_Filled_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Last_Filled_Date"), '%m/%d/%Y')
    )::DATE AS last_filled_date,
    TRIM("Drug_Name") AS drug_name,
    TRIM("Sig") AS sig,
    try_cast(TRIM("Refills") AS INTEGER) AS refills,
    try_cast(TRIM("Days_Of_Supply") AS INTEGER) AS days_of_supply,
    TRIM("Order_ID") AS order_id,
    TRIM("Encounter_ID") AS encounter_id
FROM src.current_medications;

CREATE OR REPLACE TABLE stg.reject_current_meds AS
SELECT *
FROM src.current_medications
WHERE TRIM("Last_Filled_Date") IS NOT NULL AND TRIM("Last_Filled_Date") <> ''
  AND COALESCE(
        try_strptime(TRIM("Last_Filled_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Last_Filled_Date"), '%m/%d/%Y')
    ) IS NULL;
