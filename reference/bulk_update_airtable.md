# Bulk Update Multiple Airtable Records

Updates multiple records in batches of 10.

## Usage

``` r
bulk_update_airtable(base_id, table_name, token, updates_df)
```

## Arguments

- base_id:

  Character string. The Airtable base ID.

- table_name:

  Character string. Name of the table.

- token:

  Character string. Airtable API token.

- updates_df:

  Data frame with 'airtable_id' column and fields to update.

## Value

List of response objects.
