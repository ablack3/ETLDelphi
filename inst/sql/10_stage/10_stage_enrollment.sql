-- Stage enrollment: typed DOB, cleaned gender/race/address. Reject rows where DOB non-null but parse fails.
-- Assumes src.enrollment exists with varchar columns (Member_ID, DOB, Gender, Race, Address_Line_1, etc.)

-- Cast to VARCHAR so TRIM works whether CSV loaded columns as varchar or numeric
CREATE OR REPLACE TABLE stg.enrollment AS
SELECT
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("Member_SSN" AS VARCHAR)) AS member_ssn,
    TRIM(CAST("Name_First" AS VARCHAR)) AS name_first,
    TRIM(CAST("MI" AS VARCHAR)) AS mi,
    TRIM(CAST("Name_Last" AS VARCHAR)) AS name_last,
    TRIM(CAST("Title" AS VARCHAR)) AS title,
    TRIM(CAST("DOB" AS VARCHAR)) AS dob_raw,
    COALESCE(
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%m-%d-%Y')
    )::DATE AS dob_date,
    COALESCE(
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%m-%d-%Y')
    ) AS dob_datetime,
    LOWER(TRIM(CAST("Gender" AS VARCHAR))) AS gender_clean,
    LOWER(TRIM(CAST("Race" AS VARCHAR))) AS race_clean,
    TRIM(CAST("Address_Line_1" AS VARCHAR)) AS address_line_1,
    TRIM(CAST("Address_Line_2" AS VARCHAR)) AS address_line_2,
    TRIM(CAST("City" AS VARCHAR)) AS city,
    TRIM(CAST("State" AS VARCHAR)) AS state,
    TRIM(CAST("Zip_Code" AS VARCHAR)) AS zip_code
FROM src.enrollment;

-- Rows with non-null DOB that failed to parse
CREATE OR REPLACE TABLE stg.reject_enrollment_dates AS
SELECT *
FROM src.enrollment
WHERE TRIM(CAST("DOB" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("DOB" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("DOB" AS VARCHAR)), '%m-%d-%Y')
    ) IS NULL;
