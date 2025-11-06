# Calculate Catch Weight from Length-Weight Relationships or Bucket Measurements

Calculates total catch weight using either length-weight relationships
or bucket measurements. The function prioritizes length-based
calculations when available, falling back to bucket-based measurements
when length data is missing. For Octopus (OCZ), the function converts
total length (TL) to mantle length (ML) by dividing TL by 5.5 before
applying the length-weight formula. This accounts for species-specific
differences in body morphology.

## Usage

``` r
calculate_catch_lurio(catch_data = NULL, lwcoeffs = NULL)
```

## Arguments

- catch_data:

  A data frame containing catch information with columns:

  - submission_id - Unique identifier for the catch

  - n_catch - Number of catch events

  - catch_taxon - FAO 3-alpha code

  - individuals - Number of individuals (for length-based calculations)

  - length - Length measurement in cm

  - n_buckets - Number of buckets

  - weight_bucket - Weight per bucket in kg

- lwcoeffs:

  A data frame containing length-weight coefficients with columns:

  - catch_taxon - FAO 3-alpha code

  - a_6 - 60th percentile of parameter 'a'

  - b_6 - 60th percentile of parameter 'b'

## Value

A tibble with the following columns:

- submission_id - Unique identifier for the catch

- n_catch - Number of catch events

- catch_taxon - FAO 3-alpha code

- individuals - Number of individuals

- length - Length measurement in cm

- n_buckets - Number of buckets

- weight_bucket - Weight per bucket in kg

- catch_kg - Total catch weight in kg

## Details

The function calculates catch weight using two methods:

1.  Length-based calculation: W = a \* L^b \* N / 1000 Where:

    - W is total weight in kg

    - a and b are length-weight relationship coefficients (75th
      percentile)

    - L is length in cm

    - N is number of individuals

2.  Bucket-based calculation: W = n_buckets \* weight_bucket Where:

    - W is total weight in kg

    - n_buckets is number of buckets

    - weight_bucket is weight per bucket in kg

The final catch_kg uses length-based calculation when available, falling
back to bucket-based calculation when length data is missing.

## Note

- Length-based calculations use 75th percentile of length-weight
  coefficients

- All weights are returned in kilograms

- NA values are returned when neither calculation method is possible

## Examples

``` r
if (FALSE) { # \dontrun{
# Calculate catch weights
catch_weights <- calculate_catch_lurio(
  catch_data = catch_data,
  lwcoeffs = length_weight_coeffs
)
} # }
```
