# Process Species List with Taxonomic Information

Processes a list of species by assigning database sources and taxonomic
ranks. Determines whether species should be looked up in FishBase or
SeaLifeBase based on their ISSCAAP group.

## Usage

``` r
process_species_list(fao_codes, asfis_list)
```

## Arguments

- fao_codes:

  Vector of FAO 3-alpha codes

- asfis_list:

  ASFIS list data frame containing taxonomic information

## Value

A data frame with columns:

- a3_code: FAO 3-alpha code

- scientific_name: Scientific name (cleaned)

- database: "fishbase" or "sealifebase"

- rank: Taxonomic rank ("Genus", "Family", "Order", "Species")

- ... (other taxonomic fields)

## Note

ISSCAAP groups 57, 45, 43, 42, 56 are assigned to SeaLifeBase; all
others to FishBase

## Examples

``` r
if (FALSE) { # \dontrun{
species_list <- process_species_list(c("TUN", "PEZ"), asfis_data)
} # }
```
