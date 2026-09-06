# Restate a length-weight pair on a total-length basis

Given `W = a * L_type^b` and `L_type ~= ratio * TL`, substitution gives
`W = a * ratio^b * TL^b`. So `b` is unchanged and only `a` is rescaled.

## Usage

``` r
convert_lw_to_tl(lw, conversions)
```

## Arguments

- lw:

  A tibble of length-weight rows carrying `species`, `Type`, `a` and
  `b`.

- conversions:

  Output of
  [`get_length_conversions()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_length_conversions.md).

## Value

`lw` with `a` restated on a TL basis and `Type` set to `"TL"`. Rows with
no usable conversion are dropped.
