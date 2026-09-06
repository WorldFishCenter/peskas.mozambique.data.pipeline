# Fail the run when a taxon loses its length-weight coefficients

The FishBase read is a live network read of a remote parquet dataset. A
new release therefore reaches the pipeline the moment a container is
rebuilt, with no code change. Release 26.06 dissolved `Caesionidae` into
`Lutjanidae` and `Scaridae` into `Labridae`; both family names survive
with **zero species** in them, so any taxon whose reference name is one
of those families expands to nothing and gets no coefficients.

Nothing fails on its own when that happens:
[`calculate_catch_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/calculate_catch_adnap.md)
left-joins the coefficients, so a taxon with no `(lw_a, lw_b)` pair
yields `NA` weight, and `NA` sums to zero. The taxon disappears from the
portal and the run stays green. This turns that silence into a failed
job.

## Usage

``` r
assert_taxa_coverage(
  taxa_list,
  lw,
  exempt = c("MZZ", "CRA", "CUX", "AND", "NAI", "ADT", "CJV", "CWC", "ECG", "EFZ", "EJX",
    "GQT", "GQV", "ICZ", "NUH", "OCN", "OIC", "PEJ", "PKF", "RDR", "TCI", "TEC", "UVG",
    "YFK", "EFB", "EFN", "HMP", "KAK", "PKV", "QCY", "RMB")
)
```

## Arguments

- taxa_list:

  Character vector of FAO 3-alpha codes requested.

- lw:

  The `lw` table from
  [`getLWCoeffs()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/getLWCoeffs.md),
  after any manually curated coefficients have been bound on.

- exempt:

  Codes that carry no coefficients today. This is a **baseline, not a
  whitelist**: it records the taxa that were already uncovered when the
  check was introduced (measured 2026-09-06 against FishBase 25.04 /
  SeaLifeBase 24.07 over the live KoBo data for both forms, 257 of 288
  codes resolving), so that any *new* loss fails the run. `CJX`
  (*Caesionidae*) and `PWT` (*Scaridae*) are deliberately absent — they
  resolve at 25.04 and are the two codes that break at 26.06, so a
  release move fails here. Shrinking this list is follow-up work; each
  group below is a separate fix.

  Not a taxon

  :   `MZZ` (*Actinopterygii*, "marine fishes nei") is dropped by
      [`get_fao_groups()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_fao_groups.md)
      before the search runs.

  A rank the matcher cannot search

  :   [`match_species_from_taxa()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/match_species_from_taxa.md)
      handles species, genus, family and order. `CRA` ("marine crabs
      nei") is the infraorder *Brachyura*, and SeaLifeBase carries no
      rank between order *Decapoda* and family; `CUX` ("sea cucumbers
      nei") is the class *Holothuroidea*, whose 1,133 species span 8
      orders and 24 families. Aliasing either means deciding which
      families Mozambique lands, so both are left here rather than
      guessed at in
      [`taxa_search_aliases()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/taxa_search_aliases.md).
      `CUX` is the largest single loss in the list, at 972 Lurio rows.

  Wrong reference name

  :   The ASFIS name does not describe the animal landed in Mozambique,
      so the area 51 filter correctly removes it. `AND` (*Tylosurus
      acus*) and `NAI` (*Naso lituratus*) both name species absent from
      FAO 51.

  No published coefficients

  :   The species resolves and does occur in FAO 51, but FishBase or
      SeaLifeBase carries no length-weight pair for it in any length
      type. There is nothing to convert and nothing to alias; the
      measurement does not exist. `ADT`, `CJV`, `CWC`, `ECG`, `EFZ`,
      `EJX`, `GQT`, `GQV`, `ICZ`, `NUH`, `OCN`, `OIC`, `PEJ`, `PKF`,
      `RDR`, `TCI`, `TEC`, `UVG` and `YFK` — 19 codes, and the bulk of
      this baseline. All but `EJX` and `OCN` are FishBase.

  No usable length type

  :   Published (a, b) pairs exist, but in a length type
      [`get_length_conversions()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_length_conversions.md)
      cannot restate on a total-length basis, because FishBase's POPLL
      table carries no proportional length-length fit for that species.
      `HMP` is standard length, `PKV` and `QCY` fork length, `RMB` disc
      width (a manta, which POPLL does not relate to TL at all), and
      `EFB`, `EFN` and `KAK` carry a pair with **no length type
      recorded**, which cannot be converted from. The conversion did
      recover 18 other codes here, so this group is what is left after
      it.

## Value

`lw`, invisibly.

## Porting

The check transfers unchanged, but `exempt` is country-specific. Run
once against the country's own taxa list and record whatever it reports
as the starting baseline.
