# Download Parquet File from Cloud Storage

This function downloads a Parquet file from cloud storage and loads it
as a data frame.

## Usage

``` r
download_parquet_from_cloud(prefix, provider, options)
```

## Arguments

- prefix:

  A character string specifying the file prefix path in cloud storage.

- provider:

  A character string specifying the cloud storage provider key.

- options:

  A named list of cloud storage provider options.

## Value

A tibble containing the data from the Parquet file.

## Examples

``` r
if (FALSE) { # \dontrun{
# Download survey data
data <- download_parquet_from_cloud(
  prefix = "raw-data/survey-data",
  provider = conf$storage$google$key,
  options = conf$storage$google$options
)
} # }
```
