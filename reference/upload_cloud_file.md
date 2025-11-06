# Upload File to Cloud Storage

This function uploads one or more files to cloud storage.

## Usage

``` r
upload_cloud_file(file, provider, options, name = file)
```

## Arguments

- file:

  A character vector of file paths to upload.

- provider:

  A character string specifying the cloud storage provider.

- options:

  A named list of cloud storage provider options.

- name:

  A character vector of names to assign files in cloud storage. Defaults
  to local filenames.

## Value

A list of upload metadata.

## Examples

``` r
if (FALSE) { # \dontrun{
upload_cloud_file(
  file = "data.parquet",
  provider = "gcs",
  options = list(bucket = "my-bucket"),
  name = "data/processed.parquet"
)
} # }
```
