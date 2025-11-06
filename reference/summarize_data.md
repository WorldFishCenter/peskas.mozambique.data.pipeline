# Summarize WorldFish Survey Data

Processes validated survey data from WorldFish sources, filtering out
flagged submissions and generating summary datasets for various
dimensions:

- Monthly summaries with aggregated catch metrics

- Taxa summaries with species-specific information

- District summaries with submission and effort metrics

- Gear summaries with gear-specific performance metrics

- Grid summaries from vessel tracking data

## Usage

``` r
summarize_data(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging level threshold for the logger package (e.g., DEBUG, INFO)
  See
  [`logger::log_levels`](https://daroczig.github.io/logger/reference/log_levels.html)
  for available options.

## Value

NULL (invisible). The function uploads summary files to cloud storage as
a side effect.

## Details

The function performs the following operations:

- Retrieves validated WF survey data

- Filters for approved validation status

- Creates multiple summary datasets:

  - Monthly summaries: Average catch, price, CPUE, and RPUE by district
    and month

  - Taxa summaries: Catch metrics by species, district, and month

  - District summaries: Submission counts and effort metrics by district

  - Gear summaries: Performance metrics by gear type

  - Grid summaries: Downloaded from cloud storage

- Uploads all summaries to cloud storage as versioned parquet files

The metrics calculated include:

- Total and mean catch weight

- Price per kg of catch

- CPUE (Catch Per Unit Effort) - both hourly and daily

- RPUE (Revenue Per Unit Effort) - both hourly and daily

- Number of submissions and fishers

- Trip duration

## See also

- [`get_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_validation_status.md)
  for retrieving validation information

- [`upload_cloud_file()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/upload_cloud_file.md)
  for uploading results to cloud storage

- [`download_parquet_from_cloud()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/download_parquet_from_cloud.md)
  for retrieving validated surveys and grid summaries

## Examples

``` r
if (FALSE) { # \dontrun{
# Summarize WF data with default debug logging
summarize_data()

# Summarize with info-level logging only
summarize_data(logger::INFO)
} # }
```
