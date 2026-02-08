-- Stage therapy_actions: normalize Code, Vocabulary. Link via Encounter_ID, Member_ID.
CREATE OR REPLACE TABLE stg.therapy_actions AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("Order_ID") AS order_id,
    TRIM("Code") AS code,
    TRIM("Name") AS name,
    TRIM("Result") AS result,
    TRIM("Target_Area") AS target_area,
    TRIM("Vocabulary") AS vocabulary,
    TRIM("Encounter_ID") AS encounter_id
FROM src.therapy_actions;
