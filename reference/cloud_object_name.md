# Generate Cloud Object Name

This function generates the full object name for a file in cloud
storage, finding the latest version if not specified.

## Usage

``` r
cloud_object_name(
  prefix,
  version = "latest",
  extension = "",
  provider,
  exact_match = FALSE,
  options
)
```

## Arguments

- prefix:

  A character string specifying the file prefix.

- version:

  A character string specifying the version. Default is "latest".

- extension:

  A character string specifying the file extension.

- provider:

  A character string specifying the cloud storage provider.

- exact_match:

  A logical value indicating whether to match prefix exactly.

- options:

  A named list of cloud storage provider options.

## Value

A character string with the full object name.
