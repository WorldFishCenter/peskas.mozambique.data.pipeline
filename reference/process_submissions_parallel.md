# Process Submissions in Parallel with Rate Limiting

Helper function to process a list of submission IDs in parallel with
built-in rate limiting to avoid overwhelming the KoboToolbox API. This
function is used internally by validation and sync functions to apply
operations to submissions while respecting API constraints.

## Usage

``` r
process_submissions_parallel(
  submission_ids,
  process_fn,
  description = "submissions",
  rate_limit = 0.2
)
```

## Arguments

- submission_ids:

  Character vector of submission IDs to process

- process_fn:

  Function to apply to each submission ID. Should accept a single
  submission_id parameter and return a tibble with results.

- description:

  Character string describing the type of submissions being processed
  (used in log messages). Default is "submissions".

- rate_limit:

  Numeric value specifying the delay in seconds between API calls.
  Default is 0.2 seconds. Adjust based on API rate limits.

## Value

A tibble containing the combined results from all processed submissions.
If the process_fn returns an `update_success` column, failures are
logged.

## Details

The function uses parallel processing via furrr with intentional delays
between requests to prevent overwhelming the API server. It includes
progress tracking and optional failure logging if the result includes an
`update_success` column.

Key features:

- Parallel execution using existing future plan

- Rate limiting via Sys.sleep() between requests

- Progress tracking via progressr

- Automatic failure detection and logging

- Returns combined results as a data frame

## Examples

``` r
if (FALSE) { # \dontrun{
# Setup parallel processing first
future::plan(future::multisession, workers = 4)

# Fetch validation status for multiple submissions
results <- process_submissions_parallel(
  submission_ids = c("123", "456", "789"),
  process_fn = function(id) {
    get_validation_status(
      submission_id = id,
      asset_id = "your_asset_id",
      token = "your_token"
    )
  },
  description = "validation statuses",
  rate_limit = 0.1
)

# Update validation status for multiple submissions
update_results <- process_submissions_parallel(
  submission_ids = c("123", "456"),
  process_fn = function(id) {
    update_validation_status(
      submission_id = id,
      status = "validation_status_approved",
      asset_id = "your_asset_id",
      token = "your_token"
    )
  },
  description = "approval updates",
  rate_limit = 0.2
)
} # }
```
