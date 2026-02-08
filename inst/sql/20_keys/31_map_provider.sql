-- Deterministic provider_id from Provider_ID. Include providers from provider table and from encounter/orders/labs/immunization.
WITH all_providers AS (
    SELECT DISTINCT provider_id AS provider_id_source FROM stg.provider WHERE provider_id IS NOT NULL AND TRIM(provider_id) <> ''
    UNION
    SELECT DISTINCT provider_id FROM stg.encounter WHERE provider_id IS NOT NULL AND TRIM(provider_id) <> ''
    UNION
    SELECT DISTINCT order_provider_id FROM stg.medication_orders WHERE order_provider_id IS NOT NULL AND TRIM(order_provider_id) <> ''
    UNION
    SELECT DISTINCT provider_id FROM stg.immunization WHERE provider_id IS NOT NULL AND TRIM(provider_id) <> ''
    UNION
    SELECT DISTINCT provider_id FROM stg.lab_results WHERE provider_id IS NOT NULL AND TRIM(provider_id) <> ''
    UNION
    SELECT DISTINCT provider_id FROM stg.problem WHERE provider_id IS NOT NULL AND TRIM(provider_id) <> ''
)
CREATE OR REPLACE TABLE stg.map_provider AS
SELECT
    provider_id_source,
    ROW_NUMBER() OVER (ORDER BY provider_id_source) AS provider_id
FROM (SELECT DISTINCT provider_id_source FROM all_providers) t;
