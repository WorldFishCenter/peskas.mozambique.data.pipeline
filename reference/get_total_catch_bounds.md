# Get Total Catch Bounds by Landing Site and Gear

Computes outlier bounds for total catch (`total_catch_kg`) grouped by
the combination of `landing_site` and `gear`. Internally, it first
aggregates `catch_kg` to get a single `total_catch_kg` per submission,
then applies
[`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html) (with a
log transform) within each site-gear group.

## Usage

``` r
get_total_catch_bounds(data, k)
```

## Arguments

- data:

  A data frame containing `submission_id`, `landing_site`, `gear`, and
  `catch_kg`.

- k:

  A numeric parameter passed to
  [`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html).

## Value

A data frame with columns:

- `landing_site`

- `gear`

- `upper.up` (the exponentiated upper bound)

## Details

1.  Groups by `submission_id, landing_site, gear` and sums `catch_kg`
    into `total_catch_kg`.

2.  Splits the results by site-gear combination.

3.  Calls [`LocScaleB`](https://rdrr.io/pkg/univOutl/man/LocScaleB.html)
    on each group, exponentiates the `upper.up` bound, then returns a
    data frame with `landing_site`, `gear`, and `upper.up`.

## See also

[`validate_total_catch`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_total_catch.md)
for usage of these bounds in validation.

## Examples

``` r
if (FALSE) { # \dontrun{
total_bounds <- get_total_catch_bounds(data, k = 3)
head(total_bounds)
} # }
```
