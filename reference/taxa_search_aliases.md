# Search names that override the ASFIS reference name

A few ASFIS reference names match nothing in the taxonomic backbone, so
the taxon is dropped, gets no coefficients, and every catch row of it
weighs `NA` – which sums to zero. This table substitutes a name that
does match. Each entry is a correction to the *reference data*, not to
FishBase.

## Usage

``` r
taxa_search_aliases()
```

## Value

A tibble of `a3_code`, `scientific_name` and `rank`. Several rows may
share an `a3_code`; all of them are searched.

## Details

Every row was derived by looking the ASFIS name up in the FishBase or
SeaLifeBase synonym table for the pinned release and taking the accepted
name it points to. Three groups:

- Synonyms:

  Names valid when ASFIS was written and since moved to another genus.
  The carangids account for most of them – *Carangoides* was split
  across *Ferdauia*, *Platycaranx*, *Atropus* and *Turrum* – and the
  cuttlefish for the rest, *Sepia* having been split across
  *Rhombosepion*, *Ascarosepion* and *Acanthosepion*. `VMX` is
  *Valamugil*, a genus the backbone no longer carries at all; its
  species were split across *Osteomugil* and *Moolgarda* (6 species each
  at release 25.04), so both are searched. *Crenimugil* also absorbed
  some but carries 0 species in the backbone, so listing it would only
  produce a standing unmatched-name warning.

- Spellings:

  `ESR`, `PKT`, `RPO`, `SYQ`, `ZEV` and `LGE` differ from the accepted
  name by an epithet ending or a doubled consonant.

- Broken ASFIS strings:

  `HES` is truncated in the reference table –
  `Herklotsichthys quadrimaculat.` – and `GRX` carries a parenthetical,
  `Haemulidae (=Pomadasyidae)`, whose embedded space makes
  [`process_species_list()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_species_list.md)
  read a family name as a species. Both are substituted with the string
  the backbone actually holds; `Pomadasyidae` carries 0 species against
  138 for `Haemulidae`.

`GQV` is *Plectorhinchus orientalis*, which the synonym table points at
two species: it is a `synonym` of *P. vittatus* and a `misapplied name`
for *P. picus*. Only the former is used.

Two codes are deliberately absent, because choosing a target means
deciding which families Mozambique lands rather than reading a synonym
off a table. Both are a rank
[`match_species_from_taxa()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/match_species_from_taxa.md)
cannot search: `CRA` ("marine crabs nei") is the infraorder *Brachyura*,
and SeaLifeBase carries no rank between order *Decapoda* and family;
`CUX` ("sea cucumbers nei") is the class *Holothuroidea*, whose 1,133
species span 8 orders and 24 families. They stay in the
[`assert_taxa_coverage()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/assert_taxa_coverage.md)
baseline instead.

## Porting

Country-specific. The mechanism transfers unchanged; the rows do not.
Rebuild the table for each country's own taxa list.
