-- Stage death: parse DOD to death_date and death_datetime. Reject parse failures.
CREATE OR REPLACE TABLE stg.death AS
SELECT
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("DOD" AS VARCHAR)) AS dod_raw,
    COALESCE(
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%m/%d/%Y')
    )::DATE AS death_date,
    COALESCE(
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%m/%d/%Y')
    ) AS death_datetime
FROM src.death;

CREATE OR REPLACE TABLE stg.reject_death AS
SELECT *
FROM src.death
WHERE TRIM(CAST("DOD" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("DOD" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("DOD" AS VARCHAR)), '%m/%d/%Y')
    ) IS NULL;
