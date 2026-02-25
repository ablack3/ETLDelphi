-- Stage medication_orders: parse Order_Date, Last_Filled_Date; TRY_CAST Dose, Qty_Ordered, Refills, Days_Of_Supply.
-- Drug_NDC: strip dashes, spaces, and '*' for mapping (concept_code in NDC vocabulary is digits-only). Drug_Name trimmed.
CREATE OR REPLACE TABLE stg.medication_orders AS
SELECT
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("Order_ID" AS VARCHAR)) AS order_id,
    TRIM(CAST("Drug_Name" AS VARCHAR)) AS drug_name,
    REPLACE(REPLACE(REPLACE(TRIM(CAST("Drug_NDC" AS VARCHAR)), '-', ''), ' ', ''), '*', '') AS drug_ndc_normalized,
    TRIM(CAST("Drug_NDC" AS VARCHAR)) AS drug_ndc_raw,
    TRIM(CAST("Order_Date" AS VARCHAR)) AS order_date_raw,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Order_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    )::DATE AS order_date,
    TRIM(CAST("Last_Filled_Date" AS VARCHAR)) AS last_filled_date_raw,
    COALESCE(
        try_strptime(TRIM(CAST("Last_Filled_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Last_Filled_Date" AS VARCHAR)), '%m/%d/%Y')
    )::DATE AS last_filled_date,
    try_cast(TRIM(CAST("Dose" AS VARCHAR)) AS DOUBLE) AS dose,
    try_cast(TRIM(CAST("Qty_Ordered" AS VARCHAR)) AS DOUBLE) AS qty_ordered,
    try_cast(TRIM(CAST("Refills" AS VARCHAR)) AS INTEGER) AS refills,
    TRIM(CAST("Sig" AS VARCHAR)) AS sig,
    TRIM(CAST("Route" AS VARCHAR)) AS route,
    TRIM(CAST("Units" AS VARCHAR)) AS dose_units,
    TRIM(CAST("Order_Provider_ID" AS VARCHAR)) AS order_provider_id,
    TRIM(CAST("Encounter_ID" AS VARCHAR)) AS encounter_id
FROM src.medication_orders;

-- Days_Of_Supply not in medication_orders DDL; add if present in your source
-- Reject: date or numeric parse failures (optional; here we only reject order_date)
CREATE OR REPLACE TABLE stg.reject_med_orders AS
SELECT *
FROM src.medication_orders
WHERE (TRIM(CAST("Order_Date" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("Order_Date" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Order_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Order_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) IS NULL);
