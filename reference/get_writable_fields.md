# Get Writable Fields from Airtable Table

Returns which fields can be updated (excludes computed fields).

## Usage

``` r
get_writable_fields(base_id, token, table_name)
```

## Arguments

- base_id:

  Character string. The Airtable base ID.

- token:

  Character string. Airtable API token.

- table_name:

  Character string. Name of the table.

## Value

Character vector of writable field names.
