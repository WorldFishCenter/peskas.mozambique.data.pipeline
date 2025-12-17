# Export Validation Flags to MongoDB

Exports validation flags directly to MongoDB without updating
KoboToolbox validation statuses. This function replaces the workflow of
[`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/sync_validation_submissions.md)
to avoid slow API updates to KoboToolbox. Instead, it uses KoboToolbox
validation status queries only to identify manually edited validations
by human reviewers.

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
  [`read_config()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/read_config.md)
  containing MongoDB connection parameters and survey-specific settings

- asset_id:

  Character string specifying which survey to process. Must be one of
  "adnap" or "lurio". Determines which configuration to use from
  `conf$ingestion$kobo-{asset_id}`. Default is "adnap".

- all_flags:

  Data frame containing all validation flags with columns:
  `submission_id`, `submitted_by`, `submission_date`, `alert_flag`

- validation_statuses:

  Data frame from
  [`get_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_validation_status.md)
  with columns: `submission_id`, `validation_status`, `validated_by`,
  `validation_date`

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

**Key Differences from sync_validation_submissions():**

- Does NOT update validation statuses in KoboToolbox (avoids slow API
  calls)

- Uses `validation_statuses` parameter obtained via
  [`get_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_validation_status.md)

- Stores final validation state only in MongoDB

- Respects manual human approvals by preserving their validation status

- System-generated validations are updated based on current flags

**Validation Status Logic:**

- If submission has flags AND validated_by is system username: set to
  "not_approved"

- If submission has no flags AND validated_by is system username: set to
  "approved"

- If validated_by is NOT system username: preserve existing status
  (manual approval)

## Note

This function is called internally by
[`validate_surveys_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_surveys_adnap.md)
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

- [`validate_surveys_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_surveys_adnap.md)
  for the main validation workflow

- [`get_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_validation_status.md)
  for fetching KoboToolbox validation status

- [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/sync_validation_submissions.md)
  for the deprecated approach that updates KoboToolbox

- [`mdb_collection_push()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/mdb_collection_push.md)
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
