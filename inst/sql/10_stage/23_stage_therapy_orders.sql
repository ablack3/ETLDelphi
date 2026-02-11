-- Stage therapy_orders: normalize Code, Vocabulary. No dates in source; link via Encounter_ID, Member_ID.
CREATE OR REPLACE TABLE stg.therapy_orders AS
SELECT
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("Order_ID" AS VARCHAR)) AS order_id,
    TRIM(CAST("Code" AS VARCHAR)) AS code,
    TRIM(CAST("Name" AS VARCHAR)) AS name,
    TRIM(CAST("Target_Area" AS VARCHAR)) AS target_area,
    TRIM(CAST("Vocabulary" AS VARCHAR)) AS vocabulary,
    TRIM(CAST("Encounter_ID" AS VARCHAR)) AS encounter_id
FROM src.therapy_orders;
