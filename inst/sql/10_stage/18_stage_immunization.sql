-- Stage immunization: parse Vaccination_Date; TRY_CAST Dose. Normalize CVX (trim).
CREATE OR REPLACE TABLE stg.immunization AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("Vaccine_CVX") AS vaccine_cvx,
    TRIM("Vaccine_Name") AS vaccine_name,
    TRIM("Vaccination_Date") AS vaccination_date_raw,
    COALESCE(
        try_strptime(TRIM("Vaccination_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Vaccination_Date"), '%m/%d/%Y')
    )::DATE AS vaccination_date,
    try_cast(TRIM("Dose") AS DOUBLE) AS dose,
    TRIM("Units") AS units,
    TRIM("Route") AS route,
    TRIM("Lot_Number") AS lot_number,
    TRIM("Provider_ID") AS provider_id,
    TRIM("Encounter_ID") AS encounter_id
FROM src.immunization;

CREATE OR REPLACE TABLE stg.reject_immunization AS
SELECT *
FROM src.immunization
WHERE TRIM("Vaccination_Date") IS NOT NULL AND TRIM("Vaccination_Date") <> ''
  AND COALESCE(
        try_strptime(TRIM("Vaccination_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Vaccination_Date"), '%m/%d/%Y')
    ) IS NULL;
