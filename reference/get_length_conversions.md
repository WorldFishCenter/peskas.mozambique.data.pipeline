# Length-type conversion ratios from FishBase POPLL

FishBase tags every published length-weight pair with the length type
the original study measured. For tunas, billfish and several carangids
that is fork length, because FL is the standard measurement in those
fisheries – not because anything is wrong with the record. Mozambique's
enumerators measure total length, so an FL-fitted `(a, b)` cannot be
applied directly: a 200 cm TL swordfish is 186.5 cm FL, and feeding the
TL straight into the FL relationship weighs it 105.7 kg against 83.7 kg,
26% too heavy.

The conversions are published data, in FishBase's POPLL table. This
reads them and reduces each to a single scaling factor `ratio` such that
`L_type ~= ratio * TL`, which is what
[`convert_lw_to_tl()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/convert_lw_to_tl.md)
needs.

## Usage

``` r
get_length_conversions(species, version = "latest", max_intercept = 1)
```

## Arguments

- species:

  Character vector of scientific names (FishBase only).

- version:

  FishBase release to read.

- max_intercept:

  Largest absolute POPLL intercept, in cm, still treated as
  proportional.

## Value

A tibble of `species`, `type` and `ratio`, or `NULL` when no usable
conversion exists.

## Details

POPLL stores a linear fit, `Length2 = a + b * Length1`. Treating it as
proportional (dropping the intercept) is what makes a power-law
conversion possible, and it is well supported: of the 23,921 TL-to-FL/SL
rows in release 25.04, 21,029 have an intercept of exactly zero and
23,222 are below 1 cm. Rows with a larger intercept are not proportional
and are discarded rather than approximated. Where several fits exist for
the same species and type, the median ratio is used.

The resulting ratios are physically sensible – median FL/TL 0.962, SL/TL
0.831 – and validate against the 632 species that carry both a native TL
pair and an FL one: converting halves the median error in predicted
weight (15.6% against 27.5% for using the FL pair as-is), and on the
1,186 species with both TL and SL pairs it cuts it fourfold (16.8%
against 70.6%). The residual is the scatter between independent
published studies, not conversion error.
