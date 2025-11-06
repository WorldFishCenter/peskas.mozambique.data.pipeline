# Preprocess Lurio Landings Data

This function preprocesses raw Lurio survey data from Google Cloud
Storage. It performs data cleaning, transformation, catch weight
calculations using length-weight relationships, and uploads processed
data back to GCS.

## Usage

``` r
preprocess_landings_lurio(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG)

## Value

Invisible NULL. Function processes data and uploads to Google Cloud
Storage.

## Details

The function performs the following main operations:

1.  Downloads ASFIS species data and Airtable form assets from GCS

2.  Downloads raw survey data from GCS as Parquet file

3.  Extracts and processes general trip information (dates, location,
    GPS)

4.  Extracts and processes trip details (vessel, gear, fishers,
    duration)

5.  Processes catch data using
    [`process_species_group()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_species_group.md)
    to reshape from wide to long format

6.  Calculates catch weights via
    [`calculate_catch_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/calculate_catch_lurio.md)
    using length-weight coefficients

7.  Processes market information (catch use and price)

8.  Joins all datasets and filters for active surveys

9.  Maps survey codes to standardized names using Airtable assets

10. Uploads preprocessed data as versioned Parquet file to GCS

## Examples

``` r
if (FALSE) { # \dontrun{
preprocess_landings_lurio()
} # }
```
