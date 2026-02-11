-- Stage current_medications: parse Last_Filled_Date; TRY_CAST Refills, Days_Of_Supply.
CREATE OR REPLACE TABLE stg.current_medications AS
SELECT
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("Last_Filled_Date" AS VARCHAR)) AS last_filled_date_raw,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Last_Filled_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Last_Filled_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Last_Filled_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Last_Filled_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    )::DATE AS last_filled_date,
    TRIM(CAST("Drug_Name" AS VARCHAR)) AS drug_name,
    TRIM(CAST("Sig" AS VARCHAR)) AS sig,
    try_cast(TRIM(CAST("Refills" AS VARCHAR)) AS INTEGER) AS refills,
    try_cast(TRIM(CAST("Days_Of_Supply" AS VARCHAR)) AS INTEGER) AS days_of_supply,
    TRIM(CAST("Order_ID" AS VARCHAR)) AS order_id,
    TRIM(CAST("Encounter_ID" AS VARCHAR)) AS encounter_id
FROM src.current_medications;

CREATE OR REPLACE TABLE stg.reject_current_meds AS
SELECT *
FROM src.current_medications
WHERE TRIM(CAST("Last_Filled_Date" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("Last_Filled_Date" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Last_Filled_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Last_Filled_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Last_Filled_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Last_Filled_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) IS NULL;
