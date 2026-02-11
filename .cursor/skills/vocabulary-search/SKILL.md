---
name: vocabulary-search
description: Searches OMOP vocabulary via Hecate API for concept lookup. Use when resolving unmapped units, drugs, conditions, or any OMOP concept mapping question (e.g. unmapped_units.csv, source_to_concept_map, vocabulary_id).
---

# Vocabulary Search

When helping with OMOP concept mapping (unmapped units, drugs, conditions, etc.), search the Hecate API for standardized concepts.

## How to Search

From the project root, run:

```bash
Rscript -e "source('extras/hecate_client.R'); print(vocabulary_search('QUERY'))"
```

For units specifically, use UCUM vocabulary:

```bash
Rscript -e "source('extras/hecate_client.R'); print(vocabulary_search('QUERY', vocabulary_id='UCUM', limit=10))"
```

Replace `QUERY` with the term to search (e.g. `gram`, `iu`, `milliliter`, `%`).

## Parameters

- `vocabulary_id`: `"UCUM"` for units, `"RxNorm"` for drugs, `"LOINC"` for labs, etc.
- `domain_id`: `"Unit"` for unit concepts
- `limit`: Max results (default 20)

## Examples

```bash
# Search for "gram" (unit)
Rscript -e "source('extras/hecate_client.R'); vocabulary_search('gram', vocabulary_id='UCUM')"

# Search for "iu" (international unit)
Rscript -e "source('extras/hecate_client.R'); vocabulary_search('iu', vocabulary_id='UCUM')"

# Search for "milliliter"
Rscript -e "source('extras/hecate_client.R'); vocabulary_search('milliliter', vocabulary_id='UCUM')"
```

## Requirements

- `httr2` and `jsonlite` R packages
- `HECATE_API_KEY` env var if the API requires auth
- Network access to Hecate (e.g. https://hecate.pantheon-hds.com/api)
