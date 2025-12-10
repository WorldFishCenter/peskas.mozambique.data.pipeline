# Standardize Enumerator Names

Cleans and standardizes enumerator names by removing special characters,
fixing typos, and matching similar names using string distance.

## Usage

``` r
standardize_enumerator_names(data = NULL, max_distance = 3)
```

## Arguments

- data:

  A data frame with columns 'submission_id' and 'enumerator_name'

- max_distance:

  Maximum Levenshtein distance for matching similar names. Lower values
  are stricter. Default is 3.

## Value

A data frame with two columns: 'submission_id' and
'enumerator_name_clean'

## Details

The function:

- Removes numbers and special characters

- Converts to lowercase

- Removes extra whitespace

- Marks single-word entries as "undefined"

- Matches similar names (e.g., "john smith" and "jhon smith")

- Returns the shorter/alphabetically first variant as the standard name

## Examples

``` r
if (FALSE) { # \dontrun{
clean_names <- standardize_enumerator_names(raw_dat, max_distance = 2)
} # }
```
