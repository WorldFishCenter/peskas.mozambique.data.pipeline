# Download and Process ADNAP Surveys from Kobotoolbox

This function retrieves ADNAP survey data from Kobotoolbox, processes
it, and uploads the raw data to Google Cloud Storage as Parquet files.
It uses the `get_kobo_data` function to retrieve survey submissions via
the Kobotoolbox API.

## Usage

``` r
ingest_landings_adnap()
```

## Value

Invisible NULL. Function downloads data, processes it, and uploads to
Google Cloud Storage.

## Details

The function performs the following steps:

1.  Reads configuration settings from config.yml

2.  Downloads survey data from Kobotoolbox using `get_kobo_data`

3.  Checks for uniqueness of submissions

4.  Flattens nested JSON data to tabular format

5.  Uploads raw data as versioned Parquet file to Google Cloud Storage

## Note

The function uses configuration values from config.yml:

- Hardcoded URL: "eu.kobotoolbox.org"

- Hardcoded encoding: "UTF-8"

- Configuration values for: asset_id, username, password (shared with
  Lurio)

- GCS bucket and credentials from configuration

## Examples

``` r
if (FALSE) { # \dontrun{
ingest_landings_adnap()
} # }
```
