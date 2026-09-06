# Load Taxa Data from FishBase and SeaLifeBase

Retrieves taxonomic data from both FishBase and SeaLifeBase databases in
a single function call. This is typically the first step in species
identification and classification.

## Usage

``` r
load_taxa_databases(fb_version = "latest", slb_version = "latest")
```

## Arguments

- fb_version:

  FishBase release to read, e.g. `"25.04"`. `"latest"` lets the
  installed `rfishbase` choose, which is what broke the pipeline.

- slb_version:

  SeaLifeBase release to read, e.g. `"24.07"`.

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
