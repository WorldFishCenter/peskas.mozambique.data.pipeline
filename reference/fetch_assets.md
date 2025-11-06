# Fetch Multiple Asset Tables from Airtable

Fetches taxa, gear, vessels, and landing sites data from Airtable
filtered by the specified form ID. Returns distinct records for each
table.

## Usage

``` r
fetch_assets(form_id = NULL, conf = NULL)
```

## Arguments

- form_id:

  Character. Form ID to filter assets by. This is passed to each
  individual fetch_asset call.

- conf:

  Configuration object from read_config().

## Value

A named list containing four data frames:

- `taxa`: Contains survey_label, alpha3_code, and scientific_name
  columns

- `gear`: Contains survey_label and standard_name columns

- `vessels`: Contains survey_label and standard_name columns

- `sites`: Contains site and site_code columns

## Details

Each table is fetched separately using
[`fetch_asset()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/fetch_asset.md)
and filtered to return only distinct rows to avoid duplicates in the
mapping tables.
