-- Stage immunization: parse Vaccination_Date; TRY_CAST Dose. Normalize CVX (trim).
CREATE OR REPLACE TABLE stg.immunization AS
SELECT
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("Vaccine_CVX" AS VARCHAR)) AS vaccine_cvx,
    TRIM(CAST("Vaccine_Name" AS VARCHAR)) AS vaccine_name,
    TRIM(CAST("Vaccination_Date" AS VARCHAR)) AS vaccination_date_raw,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Vaccination_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Vaccination_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Vaccination_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Vaccination_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    )::DATE AS vaccination_date,
    try_cast(TRIM(CAST("Dose" AS VARCHAR)) AS DOUBLE) AS dose,
    TRIM(CAST("Units" AS VARCHAR)) AS units,
    TRIM(CAST("Route" AS VARCHAR)) AS route,
    TRIM(CAST("Lot_Number" AS VARCHAR)) AS lot_number,
    TRIM(CAST("Provider_ID" AS VARCHAR)) AS provider_id,
    TRIM(CAST("Encounter_ID" AS VARCHAR)) AS encounter_id
FROM src.immunization;

CREATE OR REPLACE TABLE stg.reject_immunization AS
SELECT *
FROM src.immunization
WHERE TRIM(CAST("Vaccination_Date" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("Vaccination_Date" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Vaccination_Date" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Vaccination_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Vaccination_Date" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Vaccination_Date" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) IS NULL;
