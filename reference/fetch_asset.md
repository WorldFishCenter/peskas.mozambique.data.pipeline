# Fetch and Filter Asset Data from Airtable

Retrieves data from a specified Airtable table and filters it based on
form ID. Handles cases where form_id column contains multiple
comma-separated IDs.

## Usage

``` r
fetch_asset(table_name = NULL, select_cols = NULL, form = NULL, conf = NULL)
```

## Arguments

- table_name:

  Character. Name of the Airtable table to fetch.

- select_cols:

  Character vector. Column names to select from the table.

- form:

  Character. Form ID to filter by.

- conf:

  Configuration object from read_config().

## Value

A filtered and selected data frame from Airtable containing only rows
where the form_id field contains the specified form ID.

## Details

This function uses string detection to handle multi-valued form_id
fields that may contain comma-separated lists of form IDs.
