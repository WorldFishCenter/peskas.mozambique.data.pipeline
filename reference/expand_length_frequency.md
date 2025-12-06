# Expand Length Frequency Data for a Single Species Row

Takes a single species row and expands it into multiple rows if length
frequency data is present. Preserves all metadata (counting_method,
species, etc.) throughout.

## Usage

``` r
expand_length_frequency(species_row)
```

## Arguments

- species_row:

  A single-row data frame containing one species record

## Value

A data frame with one or more rows:

- If length frequency data exists: multiple rows (one per length bin)

- If no length frequency data: single row with NA for length fields

## Details

This function processes length frequency data in a row-by-row manner,
which is simpler and more robust than extracting all length data first
and then joining.

The function:

1.  Identifies length group columns in the row

2.  If found: pivots them to create multiple rows (one per length bin)

3.  If not found: returns the row as-is with NA length fields

4.  Always preserves all metadata columns
