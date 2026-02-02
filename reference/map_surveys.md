# Map Survey Labels to Standardized Taxa, Gear, and Vessel Names

Converts local species, gear, and vessel labels from surveys to
standardized names using Airtable reference tables. Replaces catch_taxon
with scientific_name and alpha3_code, and replaces local gear and vessel
names with standardized types.

## Usage

``` r
map_surveys(
  data = NULL,
  taxa_mapping = NULL,
  gear_mapping = NULL,
  vessels_mapping = NULL,
  sites_mapping = NULL,
  geo_mapping = NULL
)
```

## Arguments

- data:

  A data frame with preprocessed survey data containing catch_taxon,
  gear, vessel_type, and landing_site columns.

- taxa_mapping:

  A data frame from Airtable taxa table with survey_label, alpha3_code,
  and scientific_name columns.

- gear_mapping:

  A data frame from Airtable gears table with survey_label and
  standard_name columns.

- vessels_mapping:

  A data frame from Airtable vessels table with survey_label and
  standard_name columns.

- sites_mapping:

  A data frame from Airtable landing_sites table with site_code and site
  columns.

- geo_mapping:

  A data frame from Airtable geo table with GAUL codes and names

## Value

A tibble with catch_taxon replaced by scientific_name and alpha3_code,
gear and vessel_type replaced by standardized names, and landing_site
replaced by the full site name. Records without matches will have NA
values.

## Details

This function is called within `preprocess_landings()` after processing
raw survey data. The mapping tables are retrieved from Airtable frame
base and filtered by form ID before being passed to this function.
