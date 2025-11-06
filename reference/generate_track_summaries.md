# Generate Grid Summaries for Track Data

Processes GPS track data into 1km grid summaries for visualization and
analysis.

## Usage

``` r
generate_track_summaries(data, min_hours = 0.15, max_hours = 15)
```

## Arguments

- data:

  Preprocessed track data

- min_hours:

  Minimum hours threshold for filtering (default: 0.15)

- max_hours:

  Maximum hours threshold for filtering (default: 10)

## Value

A dataframe with grid summary statistics
