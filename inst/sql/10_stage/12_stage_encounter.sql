-- Stage encounter: parse Encounter_DateTime, normalize appt_type/clinic_type. Reject datetime parse failures.
CREATE OR REPLACE TABLE stg.encounter AS
SELECT
    TRIM("Encounter_ID") AS encounter_id,
    TRIM("Member_ID") AS member_id,
    TRIM("Appt_Type") AS appt_type,
    TRIM("Provider_ID") AS provider_id,
    TRIM("Clinic_ID") AS clinic_id,
    TRIM("Encounter_DateTime") AS encounter_datetime_raw,
    COALESCE(
        try_strptime(TRIM("Encounter_DateTime"), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM("Encounter_DateTime"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("Encounter_DateTime"), '%Y-%m-%d'),
        try_strptime(TRIM("Encounter_DateTime"), '%m/%d/%Y')
    ) AS encounter_datetime,
    COALESCE(
        try_strptime(TRIM("Encounter_DateTime"), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM("Encounter_DateTime"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("Encounter_DateTime"), '%Y-%m-%d'),
        try_strptime(TRIM("Encounter_DateTime"), '%m/%d/%Y')
    )::DATE AS encounter_date,
    TRIM("Clinic_Type") AS clinic_type,
    TRIM("SOAP_Note") AS soap_note
FROM src.encounter;

CREATE OR REPLACE TABLE stg.reject_encounter AS
SELECT *
FROM src.encounter
WHERE TRIM("Encounter_DateTime") IS NOT NULL AND TRIM("Encounter_DateTime") <> ''
  AND COALESCE(
        try_strptime(TRIM("Encounter_DateTime"), '%Y-%m-%d %H:%M:%S'),
        try_strptime(TRIM("Encounter_DateTime"), '%Y-%m-%dT%H:%M:%SZ'),
        try_strptime(TRIM("Encounter_DateTime"), '%Y-%m-%d'),
        try_strptime(TRIM("Encounter_DateTime"), '%m/%d/%Y')
    ) IS NULL;
