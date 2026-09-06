# Process Version Data Helper Function

Internal helper function to process catch and general info for a
specific survey version

## Usage

``` r
process_version_data(catch_info = NULL, asfis = NULL, conf = read_config())
```

## Arguments

- catch_info:

  Processed catch information

- asfis:

  ASFIS species data

- conf:

  Pipeline configuration, as returned by
  [`read_config()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/read_config.md).
  Supplies the pinned FishBase/SeaLifeBase releases under
  `metadata:fishbase`.

## Value

Combined and processed survey data
