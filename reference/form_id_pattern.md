# Build a Form-ID Match Pattern for Airtable Asset Tables

Builds the regular expression used to select the rows of an Airtable
asset table (`taxa`, `gear`, `vessels`, `sites`, `geo`) that belong to
one or more forms.

## Usage

``` r
form_id_pattern(form_ids)
```

## Arguments

- form_ids:

  Character vector of Airtable form record IDs, e.g. the output of one
  or more
  [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_airtable_form_id.md)
  calls.

## Value

A length-1 character string: a regex matching any of `form_ids` as a
whole element of a comma-separated list.

## Details

`form_id` in the assets snapshot is a *linked-record* field.
[`airtable_to_df()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/airtable_to_df.md)
collapses it with `paste(collapse = ", ")`, so a record shared by three
forms arrives as a single string `"recA, recB, recC"`. Selecting a
form's records therefore needs a whole-element match rather than a
substring test.

Both degenerate inputs are rejected rather than tolerated, because both
fail silently downstream: a zero-length id makes
[`paste0()`](https://rdrr.io/r/base/paste.html) recycle to a pattern
that matches only empty strings (every asset table comes back empty and
every join yields `NA`), and a multi-element pattern makes
[`stringr::str_detect()`](https://stringr.tidyverse.org/reference/str_detect.html)
recycle element-wise against the data instead of testing alternatives.

## Examples

``` r
form_id_pattern("recAAAAAAAAAAAAAA")
#> [1] "(^|,\\s*)(recAAAAAAAAAAAAAA)(\\s*,|$)"
form_id_pattern(c("recAAAAAAAAAAAAAA", "recBBBBBBBBBBBBBB"))
#> [1] "(^|,\\s*)(recAAAAAAAAAAAAAA|recBBBBBBBBBBBBBB)(\\s*,|$)"
```
