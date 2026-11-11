# Get Length-Weight Parameters for Species (Batch Version)

Retrieves length-weight relationship parameters for multiple species
efficiently by processing them in batches. Handles both fish and
non-fish species appropriately.

## Usage

``` r
get_length_weight_batch(
  species_areas_filtered,
  fb_version = "latest",
  slb_version = "latest"
)
```

## Arguments

- species_areas_filtered:

  Data frame with filtered species

- fb_version:

  FishBase release to read, e.g. `"25.04"`. Pinned in `inst/config.yml`
  under `metadata:fishbase`; `"latest"` is unsafe.

- slb_version:

  SeaLifeBase release to read, e.g. `"24.07"`.

## Value

A data frame with columns:

- a3_code: FAO 3-alpha code

- species: Scientific name

- area_code: FAO area code

- database: Source database

- type: Measurement type (e.g., "TL" for total length)

- a: Length-weight parameter a

- b: Length-weight parameter b

## Note

- For FishBase species, only total length (TL) measurements are used

- Questionable estimates (EsQ = "yes") are excluded

## Examples

``` r
if (FALSE) { # \dontrun{
# Get just length-weight parameters
lw_data <- get_length_weight_batch(species_areas_filtered)
} # }
```
