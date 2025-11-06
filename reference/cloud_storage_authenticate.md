# Authenticate to a Cloud Storage Provider

This function authenticates to a cloud storage provider using the
specified authentication method. Currently supports Google Cloud Storage
(GCS) and Amazon Web Services (AWS).

## Usage

``` r
cloud_storage_authenticate(provider, options)
```

## Arguments

- provider:

  A character string specifying the cloud provider. Currently supports
  "gcs" for Google Cloud Storage or "aws" for Amazon Web Services.

- options:

  A named list of options specific to the cloud provider. For GCS, this
  should include `service_account_key` with the contents of the
  authentication JSON file from your Google Project.

## Value

NULL (called for side effects)

## Examples

``` r
if (FALSE) { # \dontrun{
# Authenticate with Google Cloud Storage
authentication_details <- readLines("path/to/json_file.json")
cloud_storage_authenticate("gcs", list(service_account_key = authentication_details))
} # }
```
