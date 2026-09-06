# Get Length-Weight Coefficients and Morphological Data for Species

Retrieves and summarizes length-weight relationship coefficients and
morphological data by handling both FishBase and SeaLifeBase data in a
single workflow.

## Usage

``` r
getLWCoeffs(
  taxa_list = NULL,
  asfis_list = NULL,
  fb_version = "latest",
  slb_version = "latest",
  fao_areas = 51
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

## Value

A list with two elements:

- lw - A data frame with length-weight coefficients:

  - catch_taxon - FAO 3-alpha code

  - n - Number of (a, b) records aggregated

  - lw_a - Geometric mean of parameter 'a' across studies

  - lw_b - Arithmetic mean of parameter 'b' across studies

- ml - A data frame with morphological data:

  - catch_taxon - FAO 3-alpha code

  - n - Number of measurements

  - max_length_75 - 75th percentile of maximum length

  - max_weightkg_75 - 75th percentile of maximum weight in kg

## Examples

``` r
if (FALSE) { # \dontrun{
# Get coefficients and morphological data
results <- getLWCoeffs(taxa_list, asfis_list)

# Access length-weight coefficients
lw_coeffs <- results$lw

# Access morphological data
morph_data <- results$ml
} # }
```
