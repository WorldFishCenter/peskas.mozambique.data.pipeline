# Ingest Pelagic Data Systems (PDS) Trip Data

This function handles the automated ingestion of GPS boat trip data from
Pelagic Data Systems (PDS). It performs the following operations:

1.  Retrieves device metadata from the configured source

2.  Downloads trip data from PDS API using device IMEIs

3.  Converts the data to parquet format

4.  Uploads the processed file to configured cloud storage

## Usage

``` r
ingest_pds_trips(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging threshold to use. Default is logger::DEBUG. See
  [`logger::log_levels`](https://daroczig.github.io/logger/reference/log_levels.html)
  for available options.

## Value

None (invisible). The function performs its operations for side effects:

- Creates a parquet file locally with trip data

- Uploads file to configured cloud storage

- Generates logs of the process

## Details

The function requires specific configuration in the `conf.yml` file with
the following structure:

    pds:
      token: "your_pds_token"               # PDS API token
      secret: "your_pds_secret"             # PDS API secret
      pds_trips:
        file_prefix: "pds_trips"            # Prefix for output files
    storage:
      google:                               # Storage provider name
        key: "google"                       # Storage provider identifier
        options:
          project: "project-id"             # Cloud project ID
          bucket: "bucket-name"             # Storage bucket name
          service_account_key: "path/to/key.json"

The function processes trips sequentially:

- Retrieves device metadata using
  [`get_metadata()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_metadata.md)

- Downloads trip data using the
  [`get_trips()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_trips.md)
  function

- Converts the data to parquet format

- Uploads the resulting file to configured storage provider

## See also

- [`get_trips()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_trips.md)
  for details on the PDS trip data retrieval process

- [`get_metadata()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_metadata.md)
  for details on the device metadata retrieval

- [`upload_cloud_file()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/upload_cloud_file.md)
  for details on the cloud upload process

## Examples

``` r
if (FALSE) { # \dontrun{
# Run with default debug logging
ingest_pds_trips()

# Run with info-level logging only
ingest_pds_trips(logger::INFO)
} # }
```
