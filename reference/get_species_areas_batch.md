# Get FAO Areas for Species (Batch Version)

Efficiently retrieves FAO areas for multiple species by processing them
in batches by database source, reducing API calls and processing time.

## Usage

``` r
get_species_areas_batch(matched_species)
```

## Arguments

- matched_species:

  Data frame from match_species_from_taxa()

## Value

A data frame with columns:

- a3_code: FAO 3-alpha code

- species: Scientific name

- area_code: FAO area code

- database: Source database

## Examples

``` r
if (FALSE) { # \dontrun{
species_areas <- get_species_areas_batch(matched_species)
# Filter for specific FAO area
area_51_species <- species_areas %>%
  dplyr::filter(area_code == 51)
} # }
```
