-- Stage encounter: parse Encounter_DateTime, normalize appt_type/clinic_type. Reject datetime parse failures.
-- Normalize: strip timezone suffix (e.g. +00) that DuckDB adds when CAST(timestamp AS VARCHAR)
CREATE OR REPLACE TABLE stg.encounter AS
SELECT
    TRIM(CAST("Encounter_ID" AS VARCHAR)) AS encounter_id,
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("Appt_Type" AS VARCHAR)) AS appt_type,
    TRIM(CAST("Provider_ID" AS VARCHAR)) AS provider_id,
    TRIM(CAST("Clinic_ID" AS VARCHAR)) AS clinic_id,
    TRIM(CAST("Encounter_DateTime" AS VARCHAR)) AS encounter_datetime_raw,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) AS encounter_datetime,
    COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    )::DATE AS encounter_date,
    TRIM(CAST("Clinic_Type" AS VARCHAR)) AS clinic_type,
    TRIM(CAST("SOAP_Note" AS VARCHAR)) AS soap_note
FROM src.encounter;

CREATE OR REPLACE TABLE stg.reject_encounter AS
SELECT *
FROM src.encounter
WHERE TRIM(CAST("Encounter_DateTime" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("Encounter_DateTime" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(SUBSTR(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), 1, 19), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%m/%d/%Y'),
        try_strptime(TRIM(CAST("Encounter_DateTime" AS VARCHAR)), '%m/%d/%Y %H:%M:%S')
    ) IS NULL;
