-- Stage allergy: parse Onset_Date; normalize Drug_Code, Drug_Vocab, Allergen, Allergy_Type, Severity_Description.
CREATE OR REPLACE TABLE stg.allergy AS
SELECT
    TRIM(CAST("Member_ID" AS VARCHAR)) AS member_id,
    TRIM(CAST("Allergen" AS VARCHAR)) AS allergen,
    TRIM(CAST("Drug_Code" AS VARCHAR)) AS drug_code,
    TRIM(CAST("Drug_Vocab" AS VARCHAR)) AS drug_vocab,
    TRIM(CAST("Allergy_Type" AS VARCHAR)) AS allergy_type,
    TRIM(CAST("Onset_Date" AS VARCHAR)) AS onset_date_raw,
    COALESCE(
        try_strptime(TRIM(CAST("Onset_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Onset_Date" AS VARCHAR)), '%m/%d/%Y')
    )::DATE AS onset_date,
    TRIM(CAST("Reaction" AS VARCHAR)) AS reaction,
    TRIM(CAST("Severity_Description" AS VARCHAR)) AS severity_description
FROM src.allergy;

CREATE OR REPLACE TABLE stg.reject_allergy AS
SELECT *
FROM src.allergy
WHERE TRIM(CAST("Onset_Date" AS VARCHAR)) IS NOT NULL AND TRIM(CAST("Onset_Date" AS VARCHAR)) <> ''
  AND COALESCE(
        try_strptime(TRIM(CAST("Onset_Date" AS VARCHAR)), '%Y-%m-%d'),
        try_strptime(TRIM(CAST("Onset_Date" AS VARCHAR)), '%m/%d/%Y')
    ) IS NULL;
