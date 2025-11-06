# Download File from Cloud Storage

This function downloads one or more files from cloud storage.

## Usage

``` r
download_cloud_file(name, provider, options, file = name)
```

## Arguments

- name:

  A character vector of object names in cloud storage.

- provider:

  A character string specifying the cloud storage provider.

- options:

  A named list of cloud storage provider options.

- file:

  A character vector of local file paths. Defaults to object names.

## Value

A character vector of local file paths.

## Examples

``` r
if (FALSE) { # \dontrun{
download_cloud_file(
  name = "data/processed.parquet",
  provider = "gcs",
  options = list(bucket = "my-bucket"),
  file = "local_data.parquet"
)
} # }
```
