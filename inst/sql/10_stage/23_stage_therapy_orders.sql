-- Stage therapy_orders: normalize Code, Vocabulary. No dates in source; link via Encounter_ID, Member_ID.
CREATE OR REPLACE TABLE stg.therapy_orders AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("Order_ID") AS order_id,
    TRIM("Code") AS code,
    TRIM("Name") AS name,
    TRIM("Target_Area") AS target_area,
    TRIM("Vocabulary") AS vocabulary,
    TRIM("Encounter_ID") AS encounter_id
FROM src.therapy_orders;
