-- Stage provider: typed DOB, cleaned sex/specialty. Reject DOB parse failures.
CREATE OR REPLACE TABLE stg.provider AS
SELECT
    TRIM(CAST("Provider_ID" AS VARCHAR)) AS provider_id,
    TRIM(CAST("NPI" AS VARCHAR)) AS npi,
    TRIM(CAST("Name" AS VARCHAR)) AS name,
    TRIM(CAST("Specialty" AS VARCHAR)) AS specialty,
    TRIM(CAST("DOB" AS VARCHAR)) AS dob_raw,
    COALESCE(
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%m/%d/%Y')
    )::DATE AS dob_date,
    LOWER(TRIM(CAST("Sex" AS VARCHAR))) AS sex_clean,
    TRIM(CAST("Facility_Name" AS VARCHAR)) AS facility_name,
    TRIM(CAST("Location" AS VARCHAR)) AS location
FROM src.provider;

CREATE OR REPLACE TABLE stg.reject_provider AS
SELECT *
FROM src.provider
WHERE TRIM(CAST("DOB" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("DOB" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%m/%d/%Y')
    ) IS NULL;
