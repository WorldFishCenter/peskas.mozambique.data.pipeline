# Validate Catch at Taxon Level

Flags outliers in `catch_kg` by comparing values to upper bounds
computed per `gear + catch_taxon` group (via
[`get_catch_bounds_taxon`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_catch_bounds_taxon.md)).
Rows with `catch_kg` above the bound receive a numeric alert code and
optionally have their `catch_kg` set to `NA`.

## Usage

``` r
validate_catch_taxa(data, k = 3, flag_value = 4)
```

## Arguments

- data:

  A data frame containing (at least) `submission_id`, `gear`,
  `catch_taxon`, and `catch_kg`.

- k:

  A numeric parameter passed to
  [`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html); used
  for computing outlier bounds in
  [`get_catch_bounds_taxon`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_catch_bounds_taxon.md).

- flag_value:

  A numeric value to assign when `catch_kg` exceeds the upper bound.
  Default is `4`.

## Value

A data frame with the columns:

- `submission_id`:

  Carried from the input data.

- `catch_taxon`:

  Carried from the input data.

- `catch_kg`:

  Potentially modified if outlier.

- `alert_catch`:

  The alert code `flag_value` for outliers, or `NA_real_` if no outlier.

## Details

1.  Calls
    [`get_catch_bounds_taxon`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_catch_bounds_taxon.md)
    to retrieve outlier bounds for each `gear + catch_taxon` group.

2.  Joins these bounds to `data` on `gear` and `catch_taxon`.

3.  Rowwise, checks if `catch_kg` is greater than the bound `upper.up`.
    If so, sets `alert_catch = flag_value` and optionally replaces
    `catch_kg` with `NA_real_`.

## See also

[`get_catch_bounds_taxon`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_catch_bounds_taxon.md),
[`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)

## Examples

``` r
if (FALSE) { # \dontrun{
catch_alert <- validate_catch_taxa(data, k = 3, flag_value = 4)
head(catch_alert)
} # }
```
