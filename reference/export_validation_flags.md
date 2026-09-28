# Export Validation Flags to MongoDB

Exports validation flags to MongoDB, keeping the decisions reviewers
made in the Peskas Management Platform or in KoboToolbox (read
beforehand with
[`coasts::review_decisions()`](https://rdrr.io/pkg/coasts/man/review_decisions.html)).

## Usage

``` r
export_validation_flags(
  conf = NULL,
  asset_id = c("adnap", "lurio"),
  all_flags = NULL,
  validation_statuses = NULL
)
```

## Arguments

- conf:

  Configuration object from
  [`read_config()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/read_config.md)
  containing MongoDB connection parameters and survey-specific settings

- asset_id:

  Character string specifying which survey to process. Must be one of
  "adnap" or "lurio". Determines which configuration to use from
  `conf$ingestion$kobo-{asset_id}`. Default is "adnap".

- all_flags:

  Data frame containing all validation flags with columns:
  `submission_id`, `submitted_by`, `submission_date`, `alert_flag`

- validation_statuses:

  Reviewers' decisions from
  [`coasts::review_decisions()`](https://rdrr.io/pkg/coasts/man/review_decisions.html),
  with columns `submission_id`, `validation_status`, `validated_at`,
  `validated_by`

## Value

Invisible NULL. The function pushes data to MongoDB as a side effect.

## Details

The function performs the following steps:

1.  Joins validation flags with KoboToolbox validation statuses

2.  Identifies manual human approvals (excluding system username)

3.  Preserves manual human decisions while updating system-generated
    statuses

4.  Creates both wide and long format datasets for different reporting
    needs

5.  Pushes results directly to MongoDB collections

**Validation Status Logic:**

- If submission has flags AND validated_by is system username: set to
  "not_approved"

- If submission has no flags AND validated_by is system username: set to
  "approved"

- If validated_by is NOT system username: preserve existing status (a
  reviewer's approval or rejection)

## Note

This function is called internally by
[`validate_surveys_adnap()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/validate_surveys_adnap.md)
and should not typically be called directly. It requires:

- Valid configuration with MongoDB connection string

- Survey-specific configuration under `conf$ingestion$kobo-{asset_id}`

- System username configured to identify automated vs. manual
  validations

## MongoDB Collections

The function pushes to two MongoDB collections:

- flags-asset_id:

  Wide format with one row per submission including validation status
  and flags

- enumerators_stats-asset_id:

  Long format with one row per flag per submission for enumerator
  statistics

## See also

- [`validate_surveys_adnap()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/validate_surveys_adnap.md)
  for the main validation workflow

- [`coasts::mdb_collection_push()`](https://rdrr.io/pkg/coasts/man/mdb_collection_push.html)
  for MongoDB operations

## Examples

``` r
if (FALSE) { # \dontrun{
# Called internally by validate_surveys_adnap()
export_validation_flags(
  conf = conf,
  asset_id = "adnap",
  all_flags = flags_combined,
  validation_statuses = validation_statuses
)
} # }
```
