# Ingest Pelagic Data Systems (PDS) Track Data

This function handles the automated ingestion of GPS boat track data
from Pelagic Data Systems (PDS). It downloads and stores only new tracks
that haven't been previously uploaded to Google Cloud Storage. Uses
parallel processing for improved performance.

## Usage

``` r
ingest_pds_tracks(log_threshold = logger::DEBUG, batch_size = NULL)
```

## Arguments

- log_threshold:

  The logging threshold to use. Default is logger::DEBUG.

- batch_size:

  Optional number of tracks to process. If NULL, processes all new
  tracks.

## Value

None (invisible). The function performs its operations for side effects.
