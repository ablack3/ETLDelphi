-- Stage vital_sign: parse Encounter_Date; TRY_CAST Height, Weight, SystolicBP, DiastolicBP, Pulse, Respiration, Temperature.
CREATE OR REPLACE TABLE stg.vital_sign AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("Encounter_ID") AS encounter_id,
    TRIM("Encounter_Date") AS encounter_date_raw,
    COALESCE(
        try_strptime(TRIM("Encounter_Date"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("Encounter_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Encounter_Date"), '%m/%d/%Y')
    )::DATE AS encounter_date,
    try_cast(TRIM("Height") AS DOUBLE) AS height,
    TRIM("Height_Units") AS height_units,
    try_cast(TRIM("Weight") AS DOUBLE) AS weight,
    TRIM("Weight_Units") AS weight_units,
    try_cast(TRIM("SystolicBP") AS DOUBLE) AS systolic_bp,
    try_cast(TRIM("DiastolicBP") AS DOUBLE) AS diastolic_bp,
    try_cast(TRIM("Pulse") AS DOUBLE) AS pulse,
    try_cast(TRIM("Respiration") AS DOUBLE) AS respiration,
    try_cast(TRIM("Temperature") AS DOUBLE) AS temperature,
    TRIM("Temperature_Units") AS temperature_units
FROM src.vital_sign;

CREATE OR REPLACE TABLE stg.reject_vital_sign AS
SELECT *
FROM src.vital_sign
WHERE TRIM("Encounter_Date") IS NOT NULL AND TRIM("Encounter_Date") <> ''
  AND COALESCE(
        try_strptime(TRIM("Encounter_Date"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("Encounter_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Encounter_Date"), '%m/%d/%Y')
    ) IS NULL;
