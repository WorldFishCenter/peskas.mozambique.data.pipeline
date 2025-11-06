# Get Length-Weight and Morphological Parameters for Species (Batch Version)

Retrieves length-weight relationship parameters and optional
morphological data for multiple species efficiently by processing them
in batches. Handles both fish and non-fish species appropriately.

## Usage

``` r
get_length_weight_batch(species_areas_filtered, include_morphology = FALSE)
```

## Arguments

- species_areas_filtered:

  Data frame with filtered species

- include_morphology:

  Logical, whether to include morphological data (Length, CommonLength,
  Weight). Default is FALSE.

## Value

If include_morphology is FALSE (default), a data frame with columns:

- a3_code: FAO 3-alpha code

- species: Scientific name

- area_code: FAO area code

- database: Source database

- type: Measurement type (e.g., "TL" for total length)

- a: Length-weight parameter a

- b: Length-weight parameter b

If include_morphology is TRUE, a list with two elements:

- length_weight: Data frame as described above

- morphology: Data frame with columns:

  - a3_code: FAO 3-alpha code

  - species: Scientific name

  - area_code: FAO area code

  - database: Source database

  - Length: Maximum recorded length

  - CommonLength: Common length

  - Weight: Maximum weight

## Note

- For FishBase species, only total length (TL) measurements are used

- Questionable estimates (EsQ = "yes") are excluded

## Examples

``` r
if (FALSE) { # \dontrun{
# Get just length-weight parameters
lw_data <- get_length_weight_batch(species_areas_filtered)

# Get both length-weight and morphological data
results <- get_length_weight_batch(species_areas_filtered, include_morphology = TRUE)
lw_data <- results$length_weight
morph_data <- results$morphology
} # }
```
