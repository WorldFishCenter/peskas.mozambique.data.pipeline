# Calculate Fishery Metrics

Transforms catch-level data into normalized fishery performance
indicators. Calculates site-level, gear-specific, and species-specific
metrics.

## Usage

``` r
calculate_fishery_metrics(data = NULL)
```

## Arguments

- data:

  A data frame with catch records containing required columns:
  submission_id, landing_date, district, gear, catch_outcome,
  no_men_fishers, no_women_fishers, no_child_fishers, catch_taxon,
  catch_price, catch_kg

## Value

A data frame in normalized long format with columns: landing_site,
year_month, metric_type, metric_value, gear_type, species, rank
