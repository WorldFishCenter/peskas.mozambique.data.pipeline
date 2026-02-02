# Export Raw API-Ready Trip Data

Processes WorldFish preprocessed survey data into a simplified
API-friendly format and exports it to cloud storage for external
consumption. This function exports the **raw/preprocessed** version of
trip data without validation filters. For validated API exports, see the
companion function that will process validated surveys.

## Usage

``` r
export_api_raw(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging level threshold for the logger package (e.g., DEBUG,
  INFO). See
  [`logger::log_levels`](https://daroczig.github.io/logger/reference/log_levels.html)
  for available options. Default is logger::DEBUG.

## Value

NULL (invisible). The function uploads data to cloud storage as a side
effect.

## Details

The function performs the following operations:

- Downloads **preprocessed** (not validated) WF survey data from cloud
  storage

- Generates unique trip IDs using xxhash64 algorithm

- Transforms nested survey structure to flat API format

- Joins with standardized lookup tables (districts, gear types, vessel
  types)

- Exports to the **raw** cloud storage path (before validation)

**Data Pipeline Context**: This function exports raw preprocessed data
and is part of a two-stage API export pipeline:

1.  `export_api_raw()` - Exports raw/preprocessed data (this function)

2.  (Future)
    [`export_api_validated()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_api_validated.md) -
    Will export quality-controlled validated data

**Output Schema**: The exported dataset includes the following fields:

- `trip_id`: Unique identifier (TRIP_xxxxxxxxxxxx format)

- `landing_date`: Date of landing

- `gaul_2_name`: Standardized district name (GAUL level 2)

- `n_fishers`: Total number of fishers (men + women + children)

- `trip_duration_hrs`: Duration in hours

- `gear`: Standardized gear type

- `vessel_type`: Standardized vessel type

- `catch_habitat`: Habitat where catch occurred

- `catch_outcome`: Outcome of catch (landed, sold, etc.)

- `n_catch`: Number of individual catch items

- `catch_taxon`: Species or taxonomic group

- `length_cm`: Length measurement in centimeters

- `catch_kg`: Weight in kilograms

- `catch_price`: Price in local currency

**Cloud Storage Location**: Files are uploaded to the path specified in
`conf$api$trips$raw$cloud_path` (e.g., `mozambique/raw/`) with versioned
filenames following the pattern:
`{file_prefix}__{timestamp}_{git_sha}__.parquet`

## Examples

``` r
if (FALSE) { # \dontrun{
# Export raw API trip data with default debug logging
export_api_raw()

# Export with info-level logging only
export_api_raw(logger::INFO)
} # }

```
