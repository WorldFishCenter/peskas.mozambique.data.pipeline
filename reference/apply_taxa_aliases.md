# Apply the search-name aliases to a processed species list

Replaces the ASFIS name and rank for any code in
[`taxa_search_aliases()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/taxa_search_aliases.md),
leaving every other row untouched. A code with several aliases expands
to one row per alias, so all of them are searched and their coefficients
pooled.

`rank` is taken from the table rather than re-derived from the name: the
suffix rules in
[`process_species_list()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_species_list.md)
cannot recognise a bare genus like *Osteomugil*, which has no space and
no `-idae` or `-formes` ending.

## Usage

``` r
apply_taxa_aliases(species_list, aliases = taxa_search_aliases())
```

## Arguments

- species_list:

  Output of
  [`process_species_list()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_species_list.md)
  before aliasing.

- aliases:

  Alias table, defaulting to
  [`taxa_search_aliases()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/taxa_search_aliases.md).

## Value

`species_list` with aliased rows substituted.
