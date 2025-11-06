# Validate Lurio Survey Data

This function validates preprocessed fisheries survey data using a
comprehensive approach adapted from the Peskas Zanzibar pipeline. It
performs both basic data quality checks and composite economic indicator
validation to ensure data integrity.

## Usage

``` r
validate_surveys_lurio(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG)

## Value

This function does not return a value. Instead, it processes the data
and uploads both the validated results and validation flags to cloud
storage.

## Details

The validation process follows a two-stage approach:

**Stage 1: Basic Data Quality Checks (Flags 1-7)**

1.  **Form completeness**: Catch outcome is "1" but catch_taxon is
    missing

2.  **Catch info completeness**: Catch taxon exists but no weight or
    individuals

3.  **Length validation**: Fish length below species minimum

4.  **Length validation**: Fish length above species 75th percentile
    maximum

5.  **Bucket weight**: Weight per bucket exceeds 50kg

6.  **Bucket count**: Number of buckets exceeds 300

7.  **Individual count**: Number of individuals exceeds 200 per record

**Stage 2: Composite Economic Indicators (Flags 8-10)**

1.  **Price per kg**: Exceeds 1875 MZN/kg (~30 EUR/kg, following
    Zanzibar thresholds)

2.  **CPUE**: Catch per unit effort exceeds 30 kg/fisher/day

3.  **RPUE**: Revenue per unit effort exceeds 1875 MZN/fisher/day

Submissions with any validation flags are excluded from the final
validated dataset but the flags are preserved for data quality
monitoring.

## Note

This function requires a configuration file accessible via
[`read_config()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/read_config.md)
providing cloud storage connection details.

## Examples

``` r
if (FALSE) { # \dontrun{
validate_surveys_lurio()
} # }
```
