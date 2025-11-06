# Create New Airtable Records

Creates new records in batches of 10.

## Usage

``` r
df_to_airtable(df, base_id, table_name, token)
```

## Arguments

- df:

  Data frame containing data to create.

- base_id:

  Character string. The Airtable base ID.

- table_name:

  Character string. Name of the table.

- token:

  Character string. Airtable API token.

## Value

List of response objects.
