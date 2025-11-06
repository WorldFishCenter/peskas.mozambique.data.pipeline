# Upload Data as Parquet File to Cloud Storage

This function uploads a data frame as a Parquet file to cloud storage
with versioning support.

## Usage

``` r
upload_parquet_to_cloud(
  data,
  prefix,
  provider,
  options,
  compression = "lz4",
  compression_level = 12
)
```

## Arguments

- data:

  A data frame containing the data to be uploaded.

- prefix:

  A character string specifying the file prefix path.

- provider:

  A character string specifying the cloud storage provider key.

- options:

  A named list of cloud storage provider options.

- compression:

  A character string specifying compression type. Default is "lz4".

- compression_level:

  An integer specifying compression level. Default is 12.

## Value

NULL (called for side effects)

## Examples

``` r
if (FALSE) { # \dontrun{
# Upload survey data
upload_parquet_to_cloud(
  data = survey_data,
  prefix = "raw-data/survey-data",
  provider = conf$storage$google$key,
  options = conf$storage$google$options
)
} # }
```
