# Preprocess ADNAP Landings Data

This function preprocesses raw ADNAP survey data from Google Cloud
Storage. It performs data cleaning, transformation, catch weight
calculations using length-weight relationships, and uploads processed
data back to GCS.

## Usage

``` r
preprocess_landings_adnap(log_threshold = logger::DEBUG)
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

3.  Processes general trip information using
    [`preprocess_general_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_general_adnap.md)

4.  Processes catch data using
    [`preprocess_catch()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_catch.md)
    which handles survey version detection

5.  Calculates catch weights via
    [`calculate_catch_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/calculate_catch_adnap.md)
    using length-weight coefficients

6.  Joins trip and catch data

7.  Maps survey codes to standardized names using Airtable assets

8.  Uploads preprocessed data as versioned Parquet file to GCS

## Examples

``` r
if (FALSE) { # \dontrun{
preprocess_landings_adnap()
} # }
```
