-- Stage provider: typed DOB, cleaned sex/specialty. Reject DOB parse failures.
CREATE OR REPLACE TABLE stg.provider AS
SELECT
    TRIM("Provider_ID") AS provider_id,
    TRIM("NPI") AS npi,
    TRIM("Name") AS name,
    TRIM("Specialty") AS specialty,
    TRIM("DOB") AS dob_raw,
    COALESCE(
        try_strptime(TRIM("DOB"), '%Y-%m-%d'),
        try_strptime(TRIM("DOB"), '%m/%d/%Y')
    )::DATE AS dob_date,
    LOWER(TRIM("Sex")) AS sex_clean,
    TRIM("Facility_Name") AS facility_name,
    TRIM("Location") AS location
FROM src.provider;

CREATE OR REPLACE TABLE stg.reject_provider AS
SELECT *
FROM src.provider
WHERE TRIM("DOB") IS NOT NULL AND TRIM("DOB") <> ''
  AND COALESCE(
        try_strptime(TRIM("DOB"), '%Y-%m-%d'),
        try_strptime(TRIM("DOB"), '%m/%d/%Y')
    ) IS NULL;
