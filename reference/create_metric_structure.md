# Create metric structure for MongoDB/ApexCharts

Transforms gear and habitat data into the nested JSON structure required
by ApexCharts for visualization. Aggregates metrics by habitat, ranks
gears by performance, and formats as series data.

## Usage

``` r
create_metric_structure(data, metric_col)
```

## Arguments

- data:

  Data frame containing gear, habitat, cpue, and rpue columns

- metric_col:

  Name of the metric column to structure ("cpue" or "rpue")

## Value

List with nested structure: each habitat contains an array of gear/value
pairs
