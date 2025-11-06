# Get Catch Bounds by Gear + Taxon

Computes upper/lower outlier bounds for catch data (`catch_kg`) grouped
by the interaction of `gear` and `catch_taxon`. This function uses
[`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html) with a
(default) log transform to find outlier thresholds for each
`gear-taxont` group, then exponentiates the upper bound.

## Usage

``` r
get_catch_bounds_taxon(data, k)
```

## Arguments

- data:

  A data frame that includes the columns `gear`, `catch_taxon`, and
  `catch_kg`.

- k:

  A numeric parameter passed to
  [`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html),
  controlling how "wide" the outlier threshold will be (often `k = 3` or
  `k = 2`).

## Value

A data frame with columns `gear`, `catch_taxon`, `upper.up`, and the
bounds from `LocScaleB`. The `lower.low` column is dropped.

## Details

1.  Filters to columns `gear`, `catch_taxon`, and `catch_kg`.

2.  Splits the data by the interaction of `gear` and `catch_taxon`.

3.  Runs [`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)
    on each subset (with `logt = TRUE`), retrieving the `bounds`.

4.  Binds the subset results, exponentiates the `upper.up` bound, then
    separates `gear_taxon` back into `gear` and `catch_taxon`.

## See also

[`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html) for
outlier detection,
[`validate_catch_taxa`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_catch_taxa.md)
for usage of these bounds in a validation step.

## Examples

``` r
if (FALSE) { # \dontrun{
data_bounds <- get_catch_bounds_taxon(data, k = 3)
head(data_bounds)
} # }
```
