# Update Single Airtable Record

Updates specific fields in one record.

## Usage

``` r
update_airtable_record(base_id, table_name, token, record_id, updates)
```

## Arguments

- base_id:

  Character string. The Airtable base ID.

- table_name:

  Character string. Name of the table.

- token:

  Character string. Airtable API token.

- record_id:

  Character string. ID of the record to update.

- updates:

  Named list. Fields and values to update.

## Value

httr2 response object.
