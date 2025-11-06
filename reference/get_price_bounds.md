# Get Price Bounds by Gear + Taxon

Computes outlier bounds for `catch_price` grouped by the interaction of
`gear` and `catch_taxon`. By default, it applies `logt = TRUE` in
[`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html) and
exponentiates the resulting upper bound.

## Usage

``` r
get_price_bounds(data, k)
```

## Arguments

- data:

  A data frame containing `gear`, `catch_taxon`, and `catch_price`.

- k:

  A numeric parameter passed to
  [`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html).

## Value

A data frame with columns `gear`, `catch_taxon`, and `upper.up`
representing the exponentiated upper bound. The `lower.low` column is
dropped.

## Details

Similar to
[`get_catch_bounds_taxon`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_catch_bounds_taxon.md),
but operating on `catch_price` rather than `catch_kg`. Splits the data
by gear + taxon, uses `LocScaleB` to find outlier bounds, and
exponentiates the upper bound.

## See also

[`validate_price`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_price.md)
for usage in a validation step.

## Examples

``` r
if (FALSE) { # \dontrun{
price_bounds <- get_price_bounds(data, k = 3)
head(price_bounds)
} # }
```
