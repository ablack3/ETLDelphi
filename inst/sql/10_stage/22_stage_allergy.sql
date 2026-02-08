-- Stage allergy: parse Onset_Date; normalize Drug_Code, Drug_Vocab, Allergen, Allergy_Type, Severity_Description.
CREATE OR REPLACE TABLE stg.allergy AS
SELECT
    TRIM("Member_ID") AS member_id,
    TRIM("Allergen") AS allergen,
    TRIM("Drug_Code") AS drug_code,
    TRIM("Drug_Vocab") AS drug_vocab,
    TRIM("Allergy_Type") AS allergy_type,
    TRIM("Onset_Date") AS onset_date_raw,
    COALESCE(
        try_strptime(TRIM("Onset_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Onset_Date"), '%m/%d/%Y')
    )::DATE AS onset_date,
    TRIM("Reaction") AS reaction,
    TRIM("Severity_Description") AS severity_description
FROM src.allergy;

CREATE OR REPLACE TABLE stg.reject_allergy AS
SELECT *
FROM src.allergy
WHERE TRIM("Onset_Date") IS NOT NULL AND TRIM("Onset_Date") <> ''
  AND COALESCE(
        try_strptime(TRIM("Onset_Date"), '%Y-%m-%d'),
        try_strptime(TRIM("Onset_Date"), '%m/%d/%Y')
    ) IS NULL;
