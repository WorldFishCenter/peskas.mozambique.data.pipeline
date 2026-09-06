# Calculate Catch Weight from Length-Weight Relationships or Bucket Measurements

Calculates total catch weight using either length-weight relationships
or bucket measurements. The function prioritizes length-based
calculations when available, falling back to bucket-based measurements
when length data is missing. For octopus (`OCZ` and `OQC`), the function
converts the recorded arm-span to mantle length by dividing by 5.5
before applying the length-weight formula, because the published
coefficients for both are fitted on mantle length.

## Usage

``` r
calculate_catch_adnap(catch_data = NULL, lwcoeffs = NULL)
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

  - lw_a - geometric mean of parameter 'a' across studies

  - lw_b - arithmetic mean of parameter 'b' across studies

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

    - a and b are length-weight relationship coefficients aggregated
      across studies (geometric mean of a, arithmetic mean of b; cf.
      Froese 2006)

    - L is length in cm

    - N is number of individuals

2.  Bucket-based calculation: W = n_buckets \* weight_bucket Where:

    - W is total weight in kg

    - n_buckets is number of buckets

    - weight_bucket is weight per bucket in kg

The final catch_kg uses length-based calculation when available, falling
back to bucket-based calculation when length data is missing.

## Note

- Length-based calculations aggregate study-level (a, b) pairs as a =
  exp(mean(log(a))) (geometric mean), b = mean(b) (arithmetic mean).
  This preserves the log-linear nature of the length-weight
  relationship.

- All weights are returned in kilograms

- NA values are returned when neither calculation method is possible

## Examples

``` r
if (FALSE) { # \dontrun{
# Calculate catch weights
catch_weights <- calculate_catch_adnap(
  catch_data = catch_data,
  lwcoeffs = length_weight_coeffs
)
} # }
```
