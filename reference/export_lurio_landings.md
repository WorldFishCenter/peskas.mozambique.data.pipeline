# Export Lurio landings data

Processes validated landings data and exports multiple summary datasets
to MongoDB. Calculates fishery performance metrics (CPUE, RPUE),
aggregates data by site and taxa, and creates visualization-ready
formats for the monitoring portal.

## Usage

``` r
export_lurio_landings()
```

## Details

Exports the following collections to MongoDB:

- monthly_metrics: Time series of catch and revenue metrics by district

- sites_stats: Landing site statistics and performance indicators

- taxa_length: Length frequency distributions by species

- taxa_sites: Top 5 taxa composition per landing site

- habitat_gears_list: CPUE/RPUE metrics by habitat and gear type

## Examples

``` r
if (FALSE) { # \dontrun{
export_lurio_landings()
} # }
```
