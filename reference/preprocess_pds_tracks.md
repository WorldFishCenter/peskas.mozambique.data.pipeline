# Preprocess Pelagic Data Systems (PDS) Track Data

Downloads raw GPS tracks and creates a gridded summary of fishing
activity.

## Usage

``` r
preprocess_pds_tracks(log_threshold = logger::DEBUG, grid_size = 500)
```

## Arguments

- log_threshold:

  The logging threshold to use. Default is logger::DEBUG.

- grid_size:

  Numeric. Size of grid cells in meters (100, 250, 500, or 1000).

## Value

None (invisible). Creates and uploads preprocessed files.
