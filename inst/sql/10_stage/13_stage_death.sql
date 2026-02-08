-- Stage death: parse DOD to death_date and death_datetime. Reject parse failures.
CREATE OR REPLACE TABLE stg.death AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("DOD") AS dod_raw,
    COALESCE(
        try_strptime(TRIM("DOD"), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM("DOD"), '%Y-%m-%d'),
        try_strptime(TRIM("DOD"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("DOD"), '%m/%d/%Y')
    )::DATE AS death_date,
    COALESCE(
        try_strptime(TRIM("DOD"), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM("DOD"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("DOD"), '%Y-%m-%d'),
        try_strptime(TRIM("DOD"), '%m/%d/%Y')
    ) AS death_datetime
FROM src.death;

CREATE OR REPLACE TABLE stg.reject_death AS
SELECT *
FROM src.death
WHERE TRIM("DOD") IS NOT NULL AND TRIM("DOD") <> ''
  AND COALESCE(
        try_strptime(TRIM("DOD"), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM("DOD"), '%Y-%m-%d'),
        try_strptime(TRIM("DOD"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("DOD"), '%m/%d/%Y')
    ) IS NULL;
