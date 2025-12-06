# Process Length Groups for Fish Over 100cm

Extracts and processes length data for large fish (over 100cm) from
KoBoToolbox survey data. These are stored in separate repeated group
structures outside the main species_group.

## Usage

``` r
process_over100_length_groups(df = NULL, species_long = NULL)
```

## Arguments

- df:

  The original raw data frame containing separate length group columns

- species_long:

  A data frame with species groups already reshaped to long format

## Value

A data frame with columns: submission_id, n_catch, length_range,
length_over, count Returns NULL if no over-100cm length group data is
found

## Details

Length groups for fish over 100cm are stored separately from the main
species_group structure, in columns following the pattern:
species_group/no_fish_by_length_group_100/fish_length_over100

The function:

1.  Identifies columns matching the over-100cm pattern

2.  Pivots them to long format

3.  For fish_length_over100 entries, extracts the actual length value

4.  Links this data back to the corresponding species group via
    submission_id

5.  Assigns new n_catch values to avoid conflicts with existing species
    groups
