# Get Length-Weight Coefficients and Length Limits for Species

Retrieves and summarizes length-weight relationship coefficients from
FishBase and SeaLifeBase, and the length limits alerts 3 and 4 check
against.

## Usage

``` r
getLWCoeffs(
  taxa_list = NULL,
  asfis_list = NULL,
  fb_version = "latest",
  slb_version = "latest",
  fao_areas = 51,
  taxa_lengths = NULL
)
```

## Arguments

- taxa_list:

  Character vector of FAO 3-alpha codes

- asfis_list:

  ASFIS list data frame

- fb_version:

  FishBase release to read, e.g. `"25.04"`. Pinned in `inst/config.yml`
  under `metadata:fishbase`; `"latest"` is unsafe.

- slb_version:

  SeaLifeBase release to read, e.g. `"24.07"`.

- fao_areas:

  FAO major fishing areas to keep species from. Zanzibar, Kenya and
  Mozambique are all area 51 (Western Indian Ocean); set it from
  `metadata:fishbase:fao_areas` in config when porting to another
  country.

- taxa_lengths:

  The hub table of species lengths written by
  [`coasts::enrich_taxa()`](https://rdrr.io/pkg/coasts/man/enrich_taxa.html).

## Value

A list with two elements:

- lw - A data frame with length-weight coefficients:

  - catch_taxon - FAO 3-alpha code

  - n - Number of (a, b) records aggregated

  - lw_a - Geometric mean of parameter 'a' across studies

  - lw_b - Arithmetic mean of parameter 'b' across studies

- ml - The length limits, one row per `catch_taxon`: `min_length` and
  `max_length_75` in cm, from
  [`coasts::get_length_limits()`](https://rdrr.io/pkg/coasts/man/get_length_limits.html)
  for fish and hand-set for the shellfish the shared rule leaves out.

## Examples

``` r
if (FALSE) { # \dontrun{
# Get coefficients and length limits
results <- getLWCoeffs(taxa_list, asfis_list, taxa_lengths = taxa_lengths)

# Access length-weight coefficients
lw_coeffs <- results$lw

# Access length limits
length_limits <- results$ml
} # }
```
