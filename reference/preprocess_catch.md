# Preprocess Catch Data from Survey Forms

Processes catch data from KoBoToolbox survey forms, handling different
field structures that result from various survey form configurations.
Automatically normalizes species field variations and processes length
group data.

## Usage

``` r
preprocess_catch(data = NULL)
```

## Arguments

- data:

  A data frame containing raw survey data with species groups

## Value

A data frame with processed catch data including: submission_id,
n_catch, count_method, catch_taxon, n_buckets, weight_bucket,
individuals, length, catch_weight

## Details

The function uses
[`reshape_catch_data()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/reshape_catch_data.md)
internally which automatically handles:

- Multiple species field formats (species_TL, species_RF, species_SH,
  etc.)

- Separate length group structures for fish over 100cm

- Length range conversion to midpoint values

- Species code standardization (e.g., TUN→TUS, SKH→CVX)
