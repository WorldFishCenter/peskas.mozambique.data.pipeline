# Export Processed Landings Data

Exports validated landings data to a MongoDB collection. This function
filters and transforms preprocessed data, calculates total catch
weights, and exports selected fields to a validated collection.

## Usage

``` r
export_landings()
```

## Value

None (invisible). Data is exported directly to MongoDB.

## Details

The function performs the following steps:

1.  Pulls preprocessed data from MongoDB

2.  Calculates total catch weights per submission

3.  Filters for valid survey activities (survey_activity == 1)

4.  Selects relevant fields for export

5.  Uploads the filtered data to the validated collection

## Note

Only submissions with survey_activity == 1 are included in the export.
Catch weights are summed per submission, with NA values removed.

## Exported Fields

The following fields are included in the export:

- submission_id: Unique identifier for the submission

- landing_date: Date and time of landing

- district: Administrative district

- landing_site: Name of landing site

- catch_outcome: Outcome of catch

- lat: Latitude

- lon: Longitude

- habitat: Fishing habitat

- vessel_type: Type of fishing vessel

- propulsion_gear: Type of propulsion

- trip_duration: Duration of fishing trip

- gear: Fishing gear used

- catch_df: Nested catch data

## See also

[`preprocess_landings_adnap`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_landings_adnap.md)
for data preprocessing
[`calculate_catch_adnap`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/calculate_catch_adnap.md)
for catch weight calculations

## Examples

``` r
if (FALSE) { # \dontrun{
export_landings()
} # }
```
