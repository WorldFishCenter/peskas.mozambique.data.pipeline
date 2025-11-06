# Get metadata tables

Get Metadata tables from Google sheets. This function downloads the
tables include information about the fishery.

## Usage

``` r
get_metadata(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging threshold level. Default is logger::DEBUG.

## Details

The parameters needed in `conf.yml` are:

    storage:
      storage_name:
        key:
        options:
          project:
          bucket:
          service_account_key:

## Examples

``` r
if (FALSE) { # \dontrun{
# Ensure you have the necessary configuration in conf.yml
metadata_tables <- get_metadata()
} # }
```
