# Validate Total Catch

Aggregates `catch_kg` to a total per
`submission_id, landing_site, gear`, computes outlier bounds by landing
site + gear, and flags outlier total catches. Outliers are assigned a
numeric `flag_value` and optionally set to `NA`.

## Usage

``` r
validate_total_catch(data, k = 3, flag_value = 5)
```

## Arguments

- data:

  A data frame containing `submission_id`, `landing_site`, `gear`, and
  `catch_kg`.

- k:

  A numeric parameter passed to
  [`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html), used
  for bounding outliers in
  [`get_total_catch_bounds`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_total_catch_bounds.md).

- flag_value:

  A numeric value to assign to records whose total catch is above the
  upper bound. Default is `5`.

## Value

A data frame with columns:

- `submission_id`

- `total_catch_kg` (potentially set to `NA` if outlier)

- `alert_total` (the alert code or `NA_real_`)

## Details

1.  Groups `data` by `submission_id, landing_site, gear` and sums
    `catch_kg` into `total_catch_kg`.

2.  Retrieves outlier bounds from
    [`get_total_catch_bounds`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_total_catch_bounds.md).

3.  Flags any `total_catch_kg` above `upper.up` with `alert_total`. Sets
    those outlier values to `NA_real_`.

## See also

[`get_total_catch_bounds`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_total_catch_bounds.md),
[`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)

## Examples

``` r
if (FALSE) { # \dontrun{
total_catch_alert <- validate_total_catch(data, k = 3, flag_value = 5)
head(total_catch_alert)
} # }
```
