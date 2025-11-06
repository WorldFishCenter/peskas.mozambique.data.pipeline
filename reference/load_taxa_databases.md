# Load Taxa Data from FishBase and SeaLifeBase

Retrieves taxonomic data from both FishBase and SeaLifeBase databases in
a single function call. This is typically the first step in species
identification and classification.

## Usage

``` r
load_taxa_databases()
```

## Value

A list with two elements:

- fishbase: Data frame containing FishBase taxonomic data

- sealifebase: Data frame containing SeaLifeBase taxonomic data

## Examples

``` r
if (FALSE) { # \dontrun{
taxa_data <- load_taxa_databases()
fishbase_taxa <- taxa_data$fishbase
sealifebase_taxa <- taxa_data$sealifebase
} # }
```
