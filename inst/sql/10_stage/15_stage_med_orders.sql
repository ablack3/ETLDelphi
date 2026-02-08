-- Stage medication_orders: parse Order_Date, Last_Filled_Date; TRY_CAST Dose, Qty_Ordered, Refills, Days_Of_Supply.
-- Drug_NDC: strip dashes for mapping. Drug_Name trimmed.
CREATE OR REPLACE TABLE stg.medication_orders AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("Order_ID") AS order_id,
    TRIM("Drug_Name") AS drug_name,
    REPLACE(REPLACE(TRIM("Drug_NDC"), '-', ''), ' ', '') AS drug_ndc_normalized,
    TRIM("Drug_NDC") AS drug_ndc_raw,
    TRIM("Order_Date") AS order_date_raw,
    COALESCE(
        try_strptime(TRIM("Order_Date"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("Order_Date"), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM("Order_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Order_Date"), '%m/%d/%Y')
    )::DATE AS order_date,
    TRIM("Last_Filled_Date") AS last_filled_date_raw,
    COALESCE(
        try_strptime(TRIM("Last_Filled_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Last_Filled_Date"), '%m/%d/%Y')
    )::DATE AS last_filled_date,
    try_cast(TRIM("Dose") AS DOUBLE) AS dose,
    try_cast(TRIM("Qty_Ordered") AS DOUBLE) AS qty_ordered,
    try_cast(TRIM("Refills") AS INTEGER) AS refills,
    TRIM("Sig") AS sig,
    TRIM("Route") AS route,
    TRIM("Units") AS dose_units,
    TRIM("Order_Provider_ID") AS order_provider_id,
    TRIM("Encounter_ID") AS encounter_id
FROM src.medication_orders;

-- Days_Of_Supply not in medication_orders DDL; add if present in your source
-- Reject: date or numeric parse failures (optional; here we only reject order_date)
CREATE OR REPLACE TABLE stg.reject_med_orders AS
SELECT *
FROM src.medication_orders
WHERE (TRIM("Order_Date") IS NOT NULL AND TRIM("Order_Date") <> ''
  AND COALESCE(
        try_strptime(TRIM("Order_Date"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("Order_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Order_Date"), '%m/%d/%Y')
    ) IS NULL);
