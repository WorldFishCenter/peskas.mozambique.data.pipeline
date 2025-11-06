# Get Length-Weight Coefficients and Morphological Data for Species

Retrieves and summarizes length-weight relationship coefficients and
morphological data by handling both FishBase and SeaLifeBase data in a
single workflow.

## Usage

``` r
getLWCoeffs(taxa_list = NULL, asfis_list = NULL)
```

## Arguments

- taxa_list:

  Character vector of FAO 3-alpha codes

- asfis_list:

  ASFIS list data frame

## Value

A list with two elements:

- lw - A data frame with length-weight coefficients:

  - catch_taxon - FAO 3-alpha code

  - n - Number of measurements

  - a_6 - 60th percentile of parameter 'a'

  - b_6 - 60th percentile of parameter 'b'

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
