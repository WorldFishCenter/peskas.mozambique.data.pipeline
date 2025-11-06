# Get All Records from Airtable with Pagination

Retrieves ALL records from an Airtable table, handling pagination
automatically.

## Usage

``` r
airtable_to_df(base_id, table_name, token, list_handler = "collapse")
```

## Arguments

- base_id:

  Character string. The Airtable base ID.

- table_name:

  Character string. The name of the table to retrieve.

- token:

  Character string. Airtable API token for authentication.

- list_handler:

  Character string. "collapse" (default) or "count" for list fields.

## Value

A tibble with all records and an 'airtable_id' column.
