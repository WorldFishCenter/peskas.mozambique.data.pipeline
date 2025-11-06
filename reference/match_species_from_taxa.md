# Match Species from Taxa Databases

Matches species between FAO codes and database records, handling
different taxonomic levels (species, genus, family, order)
appropriately.

## Usage

``` r
match_species_from_taxa(species_list, taxa_data)
```

## Arguments

- species_list:

  Processed species list from process_species_list()

- taxa_data:

  Taxa data from load_taxa_databases()

## Value

A data frame with columns:

- a3_code: FAO 3-alpha code

- species: Scientific name

- database: Source database

## Examples

``` r
if (FALSE) { # \dontrun{
taxa_data <- load_taxa_databases()
species_list <- process_species_list(fao_codes, asfis)
matches <- match_species_from_taxa(species_list, taxa_data)
} # }
```
