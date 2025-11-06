# Process Species Length and Catch Data

Processes raw survey data to extract and organize species information,
length measurements, and catch estimates. Handles both empty and
non-empty submissions, converting wide-format survey data into a
structured list of tibbles.

## Usage

``` r
process_species_group(data = NULL)
```

## Arguments

- data:

  A data frame containing raw survey data with required columns:

  - submission_id: Unique identifier for each submission

  - group_species\_\*: Species information columns

  - Optional: no_individuals\_*, fish_length_over*, catch_estimate
    columns

## Value

A list of tibbles, one per submission_id, containing:

- Basic information: submission_id, species data, catch estimates

- Length data: When available, includes length classes and measurements

- Empty submissions: Preserved with NA values

## Examples

``` r
if (FALSE) { # \dontrun{
results <- process_species_group(raw_dat)
submission_data <- results[["1234"]]
} # }
```
