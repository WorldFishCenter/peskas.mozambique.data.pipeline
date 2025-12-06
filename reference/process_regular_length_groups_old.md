# Process Regular Length Groups (Under 100cm) - DEPRECATED

This function is kept for reference but is no longer used. Use
expand_length_frequency() instead for simpler row-by-row processing.

## Usage

``` r
process_regular_length_groups_old(species_long = NULL)
```

## Arguments

- species_long:

  A data frame with species groups already reshaped to long format

## Value

A data frame with columns: submission_id, n_catch, length_range, count
Returns NULL if no regular length group data is found
