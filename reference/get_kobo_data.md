# Retrieve Data from Kobotoolbox API

This function retrieves survey data from Kobotoolbox API for a specific
asset. It supports pagination and handles both JSON and XML formats.

## Usage

``` r
get_kobo_data(
  assetid,
  url = "eu.kobotoolbox.org",
  uname = NULL,
  pwd = NULL,
  encoding = "UTF-8",
  format = "json"
)
```

## Arguments

- assetid:

  The asset ID of the Kobotoolbox form.

- url:

  The URL of Kobotoolbox (default is "eu.kobotoolbox.org").

- uname:

  Username for Kobotoolbox account.

- pwd:

  Password for Kobotoolbox account.

- encoding:

  Encoding to be used for data retrieval (default is "UTF-8").

- format:

  Format of the data to retrieve, either "json" or "xml" (default is
  "json").

## Value

A list containing all retrieved survey results.

## Details

The function uses pagination to retrieve large datasets, with a limit of
30,000 records per request. It continues to fetch data until all records
are retrieved or an error occurs.

## Examples

``` r
if (FALSE) { # \dontrun{
kobo_data <- get_kobo_data(
  assetid = "your_asset_id",
  uname = "your_username",
  pwd = "your_password"
)
} # }
```
