# Outlier Alert for Numeric Vectors

This helper function identifies numeric outliers based on the bounds
computed by
[`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html). It
optionally applies a log transform within `LocScaleB` (through the `...`
argument) and flags values outside the computed lower or upper bounds.
Values below the lower bound receive the alert code `alert_if_smaller`,
values above the upper bound receive `alert_if_larger`, and in-range
values receive `no_alert_value`.

## Usage

``` r
alert_outlier(
  x,
  no_alert_value = NA_real_,
  alert_if_larger = no_alert_value,
  alert_if_smaller = no_alert_value,
  ...
)
```

## Arguments

- x:

  A numeric vector in which to detect outliers.

- no_alert_value:

  A numeric code (default `NA_real_`) to assign to non-outlier values.

- alert_if_larger:

  A numeric code to assign if `x > upper bound`.

- alert_if_smaller:

  A numeric code to assign if `x < lower bound`.

- ...:

  Additional arguments passed to
  [`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html), such
  as `logt` or `k`.

## Value

A numeric vector of the same length as `x`, containing the alert codes
for each element (`alert_if_smaller`, `alert_if_larger`, or
`no_alert_value`).

## Details

- The function checks if all `x` values are `NA` or zero, or if the MAD
  (median absolute deviation) is zero. In these cases, it returns `NA`
  for all elements, since meaningful bounds cannot be computed.

- Otherwise, it calls `univOutl::LocScaleB(x, ...)` to compute `bounds`.

- If a log transform was used (`logt = TRUE`), `LocScaleB` returns the
  log-scale bounds; typically, you would back-transform them to compare
  with the raw values.

## See also

[`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html) for the
underlying outlier detection algorithm.

## Examples

``` r
if (FALSE) { # \dontrun{
x <- c(1, 2, 3, 100)
alert_outlier(x, no_alert_value = NA, alert_if_larger = 9, logt = TRUE, k = 3)
} # }
```
