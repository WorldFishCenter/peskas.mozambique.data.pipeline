# Reshape Catch Data with Length Groupings

This function takes KoBoToolbox survey data with species catch
information and reshapes it into long format while properly handling
nested length group information. It supports both regular length groups
(under 100cm) and separate structures for fish over 100cm.

## Usage

``` r
reshape_catch_data(df = NULL)
```

## Arguments

- df:

  A data frame containing catch data with species groups and length
  information. Expected to have columns following the pattern:

  - species_group.X columns (where X is a position number)

  - Multiple species fields within each group (species_TL, species_RF,
    species_SH, etc.)

  - Optional regular length group columns
    (species_group.X.species_group/no_fish_by_length_group/no_individuals_Y_Z)

  - Optional over-100cm length group columns
    (species_group/no_fish_by_length_group_100/)

## Value

A data frame in long format with each row representing a species catch
record. The output includes columns: - submission_id: Unique survey
submission identifier - n_catch: Catch record number within submission
(1-based) - counting_method: Method used to count/measure catch -
species: FAO 3-alpha species code (normalized from multiple species\_\*
fields) - n_buckets, weight_bucket, catch_weight: Bucket-based
measurements - length_range: Length category (e.g., "5_10", "over100") -
length_over: Specific length for fish over 100cm - count: Number of
individuals in that length range

## Details

The function performs the following steps:

1.  Reshapes species groups from wide to long using
    [`reshape_species_groups()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/reshape_species_groups.md)

2.  Normalizes multiple species fields (species_TL for teleosts,
    species_RF for rays/fish, species_SH for sharks, species_FSH for
    finfish, species_CRB for crabs, species_CE for cephalopods,
    species_LO for lobster, species_CR for crustacea, species_MA for
    marine animals, species_OY for oysters, species_FI for fish,
    species_FFI for flatfish, species_RA for rays, species_SHK for
    sharks, species_MZZ for miscellaneous) into a single 'species'
    column

3.  Processes each species row individually using
    [`expand_length_frequency()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/expand_length_frequency.md)
    to:

    - Check if length frequency data exists for that row

    - If yes: expand to multiple rows (one per length bin)

    - If no: keep single row with NA for length fields

    - Always preserve all metadata (counting_method, species, etc.)

4.  Removes length group columns after pivoting

5.  Sorts by submission_id and n_catch

This approach is simpler and more robust than the old extract-and-join
method, as it processes each species row individually and preserves all
metadata throughout.

## Examples

``` r
if (FALSE) { # \dontrun{
# Reshape catch data from raw survey
catch_long <- reshape_catch_data(raw_survey_data)

# Analyze counts by length range
catch_long |>
  dplyr::filter(!is.na(count)) |>
  dplyr::group_by(species, length_range) |>
  dplyr::summarize(total_count = sum(as.numeric(count), na.rm = TRUE))

# View fish over 100cm with their specific lengths
catch_long |>
  dplyr::filter(length_range == "over100") |>
  dplyr::select(submission_id, species, length_over, count)
} # }
```
