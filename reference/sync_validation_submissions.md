# Synchronize Validation Statuses with KoboToolbox

Synchronizes validation statuses between the local system and
KoboToolbox by processing validation flags and updating submission
statuses accordingly. This function follows the Kenya pipeline pattern
with rate limiting, manual approval respect, and optimized API usage.

## Usage

``` r
sync_validation_submissions(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging level threshold for the logger package (e.g., DEBUG,
  INFO). Default is logger::DEBUG.

## Value

None. The function performs status updates and database operations as
side effects.

## Details

The function follows these steps:

1.  Downloads the current validation flags from cloud storage

2.  Sets up parallel processing with rate limiting

3.  Fetches current validation status from KoboToolbox FIRST (before any
    updates)

4.  Identifies manually approved submissions (excluding system username)

5.  Updates flagged submissions as "not approved" (EXCLUDING manual
    approvals)

6.  Updates clean submissions as "approved" (SKIPPING already-approved
    ones)

7.  Fetches final validation status after updates

8.  Combines results and pushes to MongoDB for record-keeping

Key improvements over previous implementation:

- Rate limiting prevents overwhelming KoboToolbox API

- Manual approvals are respected and never overwritten

- Already-approved submissions are skipped to minimize API calls

- Better error tracking and logging

## Note

This function requires proper configuration in the config file,
including:

- MongoDB connection parameters

- KoboToolbox asset ID and token (configured under
  ingestion\$kobo-adnap)

- KoboToolbox username (to identify system approvals)

- Google cloud storage parameters

## Parallel Processing

The function uses the future and furrr packages for parallel processing,
with the number of workers set to system cores minus 2 to prevent
resource exhaustion. Rate limiting is implemented via Sys.sleep() to
respect API constraints.

## See also

- [`process_submissions_parallel()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_submissions_parallel.md)
  for the helper function with rate limiting

- [`get_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_validation_status.md)
  for fetching KoboToolbox validation status

- [`update_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/update_validation_status.md)
  for updating KoboToolbox validation status

## Examples

``` r
if (FALSE) { # \dontrun{
# Run with default DEBUG logging
sync_validation_submissions()

# Run with INFO level logging
sync_validation_submissions(log_threshold = logger::INFO)
} # }
```
