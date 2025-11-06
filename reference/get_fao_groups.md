# Extract and Format FAO Taxonomic Groups

Filters and formats taxonomic information from the FAO ASFIS list for
specified FAO 3-alpha codes, excluding miscellaneous ("MZZ") and unknown
("UNKN") categories.

## Usage

``` r
get_fao_groups(fao_codes = NULL, asfis_list = NULL)
```

## Arguments

- fao_codes:

  A character vector of FAO 3-alpha codes to extract. If NULL, returns
  empty dataset.

- asfis_list:

  A data frame containing the FAO ASFIS list with required columns:

  - Alpha3_Code - FAO 3-alpha code

  - Scientific_Name - Scientific name of taxon

  - English_name - Common name in English

  - Family - Family name

  - Order - Order name

  - ISSCAAP_Group - FAO ISSCAAP group number

## Value

A tibble with standardized column names containing taxonomic
information:

- a3_code - FAO 3-alpha code

- scientific_name - Scientific name

- english_name - Common name in English

- family - Family name

- order - Order name

- taxon_group - ISSCAAP group number

## Details

The function:

1.  Filters ASFIS list for specified FAO codes

2.  Standardizes column names for consistency

3.  Removes miscellaneous ("MZZ") and unknown ("UNKN") categories

4.  Preserves the taxonomic hierarchy information

## Note

- Requires dplyr package

- MZZ (Miscellaneous marine fishes) and UNKN (Unknown) are automatically
  excluded

- Column names are standardized for consistency with other functions

## Examples

``` r
# Example ASFIS data
asfis <- data.frame(
  Alpha3_Code = c("TUN", "MZZ", "RAG"),
  Scientific_Name = c("Thunnini", "Marine fishes nei", "Rastrelliger kanagurta"),
  English_name = c("Tunas", "Marine fishes", "Indian mackerel"),
  Family = c("SCOMBRIDAE", NA, "SCOMBRIDAE"),
  Order = c("PERCIFORMES", NA, "PERCIFORMES"),
  ISSCAAP_Group = c(36, 39, 37)
)

# Get taxonomic information for specific codes
fao_taxa <- get_fao_groups(c("TUN", "RAG"), asfis)
```
