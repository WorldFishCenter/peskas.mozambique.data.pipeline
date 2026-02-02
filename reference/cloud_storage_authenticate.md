# Authenticate to a Cloud Storage Provider

This function is primarily used internally by other functions to
establish authentication with specified cloud providers such as Google
Cloud Services (GCS) or Amazon Web Services (AWS).

## Usage

``` r
cloud_storage_authenticate(provider, options)
```

## Arguments

- provider:

  A character string specifying the cloud provider ("gcs" or "aws").

- options:

  A named list of options specific to the cloud provider (see details).

## Details

For GCS, the options list must include:

- `service_account_key`: The contents of the authentication JSON file
  from your Google Project.

This function wraps
[`googleCloudStorageR::gcs_auth()`](https://cloudyr.github.io/googleCloudStorageR//reference/gcs_auth.html)
to handle GCS authentication.

## Examples

``` r
if (FALSE) { # \dontrun{
authentication_details <- readLines("path/to/json_file.json")
cloud_storage_authenticate("gcs", list(service_account_key = authentication_details))
#'
} # }
```
