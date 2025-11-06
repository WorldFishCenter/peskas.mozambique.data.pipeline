# Validate Catch Price

Detects outliers in `catch_price` by `gear + catch_taxon`, assigning a
numeric alert code to records exceeding the computed upper bound.
Outliers can be replaced with `NA`.

## Usage

``` r
validate_price(data, k = 3, flag_value = 6)
```

## Arguments

- data:

  A data frame with `submission_id`, `gear`, `catch_taxon`, and
  `catch_price`.

- k:

  A numeric parameter passed to
  [`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html), used
  in
  [`get_price_bounds`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_price_bounds.md).

- flag_value:

  A numeric code (default `6`) assigned to outlier prices.

## Value

A data frame with columns:

- `submission_id`:

  Carried from the input.

- `catch_taxon`:

  Carried from the input.

- `catch_price`:

  Potentially set to `NA` if outlier.

- `alert_price`:

  The numeric alert code, or `NA`.

## Details

1.  Calls
    [`get_price_bounds`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_price_bounds.md)
    to compute outlier thresholds.

2.  Joins thresholds back to `data` via `gear` and `catch_taxon`.

3.  Rowwise, flags prices above the `upper.up` bound with `alert_price`,
    and replaces them with `NA_real_`.

## See also

[`get_price_bounds`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_price_bounds.md),
[`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)

## Examples

``` r
if (FALSE) { # \dontrun{
price_alert <- validate_price(data, k = 3, flag_value = 6)
head(price_alert)
} # }
```
