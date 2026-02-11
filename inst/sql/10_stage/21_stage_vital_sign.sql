-- Stage vital_sign: parse Encounter_Date; TRY_CAST Height, Weight, SystolicBP, DiastolicBP, Pulse, Respiration, Temperature.
CREATE OR REPLACE TABLE stg.vital_sign AS
SELECT
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("Encounter_ID" AS VARCHAR)) AS encounter_id,
    TRIM(CAST("Encounter_Date" AS VARCHAR)) AS encounter_date_raw,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Encounter_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) AS encounter_datetime,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Encounter_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    )::DATE AS encounter_date,
    try_cast(TRIM(CAST("Height" AS VARCHAR)) AS DOUBLE) AS height,
    TRIM(CAST("Height_Units" AS VARCHAR)) AS height_units,
    try_cast(TRIM(CAST("Weight" AS VARCHAR)) AS DOUBLE) AS weight,
    TRIM(CAST("Weight_Units" AS VARCHAR)) AS weight_units,
    try_cast(TRIM(CAST("SystolicBP" AS VARCHAR)) AS DOUBLE) AS systolic_bp,
    try_cast(TRIM(CAST("DiastolicBP" AS VARCHAR)) AS DOUBLE) AS diastolic_bp,
    try_cast(TRIM(CAST("Pulse" AS VARCHAR)) AS DOUBLE) AS pulse,
    try_cast(TRIM(CAST("Respiration" AS VARCHAR)) AS DOUBLE) AS respiration,
    try_cast(TRIM(CAST("Temperature" AS VARCHAR)) AS DOUBLE) AS temperature,
    TRIM(CAST("Temperature_Units" AS VARCHAR)) AS temperature_units
FROM src.vital_sign;

CREATE OR REPLACE TABLE stg.reject_vital_sign AS
SELECT *
FROM src.vital_sign
WHERE TRIM(CAST("Encounter_Date" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("Encounter_Date" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Encounter_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Encounter_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) IS NULL;
