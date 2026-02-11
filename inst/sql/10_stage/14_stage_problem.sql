-- Stage problem: parse Onset_Date, Resolution_Date; normalize Problem_Code, Problem_Type.
CREATE OR REPLACE TABLE stg.problem AS
SELECT
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("Problem_Code" AS VARCHAR)) AS problem_code,
    TRIM(CAST("Problem_Description" AS VARCHAR)) AS problem_description,
    TRIM(CAST("Problem_Type" AS VARCHAR)) AS problem_type,
    TRIM(CAST("Onset_Date" AS VARCHAR)) AS onset_date_raw,
    TRIM(CAST("Resolution_Date" AS VARCHAR)) AS resolution_date_raw,
    COALESCE(
        try_strptime(TRIM(CAST("Onset_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Onset_Date" AS VARCHAR)), '%m/%d/%Y')
    )::DATE AS onset_date,
    COALESCE(
        try_strptime(TRIM(CAST("Resolution_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Resolution_Date" AS VARCHAR)), '%m/%d/%Y')
    )::DATE AS resolution_date,
    TRIM(CAST("Provider_ID" AS VARCHAR)) AS provider_id,
    TRIM(CAST("Encounter_ID" AS VARCHAR)) AS encounter_id
FROM src.problem;

CREATE OR REPLACE TABLE stg.reject_problem AS
SELECT *
FROM src.problem p
WHERE (TRIM(CAST(p."Onset_Date" AS VARCHAR)) IS NOT NULL AND TRIM(CAST(p."Onset_Date" AS VARCHAR)) <> '' AND COALESCE(try_strptime(TRIM(CAST(p."Onset_Date" AS VARCHAR)), '%Y-%m-%d'), try_strptime(TRIM(CAST(p."Onset_Date" AS VARCHAR)), '%m/%d/%Y')) IS NULL)
   OR (TRIM(CAST(p."Resolution_Date" AS VARCHAR)) IS NOT NULL AND TRIM(CAST(p."Resolution_Date" AS VARCHAR)) <> '' AND COALESCE(try_strptime(TRIM(CAST(p."Resolution_Date" AS VARCHAR)), '%Y-%m-%d'), try_strptime(TRIM(CAST(p."Resolution_Date" AS VARCHAR)), '%m/%d/%Y')) IS NULL);
