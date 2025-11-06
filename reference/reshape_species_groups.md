# Reshape Species Groups from Wide to Long Format

This function converts a data frame containing repeated species group
columns (species_group.0, species_group.1, etc.) into a long format
where each species group is represented as a separate row, with a column
indicating which group it belongs to.

## Usage

``` r
reshape_species_groups(df = NULL)
```

## Arguments

- df:

  A data frame containing species group data in wide format

## Value

A data frame in long format with each row representing a single species
group record

## Details

The function identifies columns that follow the pattern
"species_group.X" and restructures the data so that each species group
from a submission is represented as a separate row. It removes the
position prefix from column names and adds an n_catch column to track
the group number (1-based indexing). Rows that contain only NA values
are filtered out.

## Examples

``` r
if (FALSE) { # \dontrun{
long_species_data <- reshape_species_groups(catch_info)
} # }
```
