-- Stage problem: parse Onset_Date, Resolution_Date; normalize Problem_Code, Problem_Type.
CREATE OR REPLACE TABLE stg.problem AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("Problem_Code") AS problem_code,
    TRIM("Problem_Description") AS problem_description,
    TRIM("Problem_Type") AS problem_type,
    TRIM("Onset_Date") AS onset_date_raw,
    TRIM("Resolution_Date") AS resolution_date_raw,
    COALESCE(
        try_strptime(TRIM("Onset_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Onset_Date"), '%m/%d/%Y')
    )::DATE AS onset_date,
    COALESCE(
        try_strptime(TRIM("Resolution_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Resolution_Date"), '%m/%d/%Y')
    )::DATE AS resolution_date,
    TRIM("Provider_ID") AS provider_id,
    TRIM("Encounter_ID") AS encounter_id
FROM src.problem;

CREATE OR REPLACE TABLE stg.reject_problem AS
SELECT *
FROM src.problem p
WHERE (TRIM(p."Onset_Date") IS NOT NULL AND TRIM(p."Onset_Date") <> '' AND COALESCE(try_strptime(TRIM(p."Onset_Date"), '%Y-%m-%d'), try_strptime(TRIM(p."Onset_Date"), '%m/%d/%Y')) IS NULL)
   OR (TRIM(p."Resolution_Date") IS NOT NULL AND TRIM(p."Resolution_Date") <> '' AND COALESCE(try_strptime(TRIM(p."Resolution_Date"), '%Y-%m-%d'), try_strptime(TRIM(p."Resolution_Date"), '%m/%d/%Y')) IS NULL);
