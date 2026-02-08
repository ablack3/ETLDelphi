-- Stage enrollment: typed DOB, cleaned gender/race/address. Reject rows where DOB non-null but parse fails.
-- Assumes src.enrollment exists with varchar columns (Member_ID, DOB, Gender, Race, Address_Line_1, etc.)

CREATE OR REPLACE TABLE stg.enrollment AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("Member_SSN") AS member_ssn,
    TRIM("Name_First") AS name_first,
    TRIM("MI") AS mi,
    TRIM("Name_Last") AS name_last,
    TRIM("Title") AS title,
    TRIM("DOB") AS dob_raw,
    COALESCE(
        try_strptime(TRIM("DOB"), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM("DOB"), '%Y-%m-%d'),
        try_strptime(TRIM("DOB"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("DOB"), '%m/%d/%Y'),
        try_strptime(TRIM("DOB"), '%m-%d-%Y')
    )::DATE AS dob_date,
    COALESCE(
        try_strptime(TRIM("DOB"), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM("DOB"), '%Y-%m-%d'),
        try_strptime(TRIM("DOB"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("DOB"), '%m/%d/%Y'),
        try_strptime(TRIM("DOB"), '%m-%d-%Y')
    ) AS dob_datetime,
    LOWER(TRIM("Gender")) AS gender_clean,
    LOWER(TRIM("Race")) AS race_clean,
    TRIM("Address_Line_1") AS address_line_1,
    TRIM("Address_Line_2") AS address_line_2,
    TRIM("City") AS city,
    TRIM("State") AS state,
    TRIM("Zip_Code") AS zip_code
FROM src.enrollment;

-- Rows with non-null DOB that failed to parse
CREATE OR REPLACE TABLE stg.reject_enrollment_dates AS
SELECT *
FROM src.enrollment
WHERE TRIM("DOB") IS NOT NULL AND TRIM("DOB") <> ''
  AND COALESCE(
        try_strptime(TRIM("DOB"), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM("DOB"), '%Y-%m-%d'),
        try_strptime(TRIM("DOB"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("DOB"), '%m/%d/%Y'),
        try_strptime(TRIM("DOB"), '%m-%d-%Y')
    ) IS NULL;
