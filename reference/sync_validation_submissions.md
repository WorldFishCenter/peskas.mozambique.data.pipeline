# Synchronize Validation Statuses with KoboToolbox

Synchronizes validation statuses between the local system and
KoboToolbox by processing validation flags and updating submission
statuses accordingly. This function handles both flagged (not approved)
and clean (approved) submissions in parallel.

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

2.  Sets up parallel processing using the future package

3.  Processes submissions with alert flags (marking them as not approved
    in KoboToolbox)

4.  Processes submissions without alert flags (marking them as approved
    in KoboToolbox)

5.  Retrieves current validation status from KoboToolbox for all
    submissions

6.  Adds KoboToolbox validation status (validation_status, validated_at,
    validated_by) to validation flags

7.  Pushes all validation flags with KoboToolbox status to MongoDB for
    record-keeping

Progress reporting is enabled to track the status of submissions being
processed.

## Note

This function requires proper configuration in the config file,
including:

- MongoDB connection parameters

- KoboToolbox asset ID and token (configured under
  ingestion\$kobo-adnap)

- Google cloud storage parameters

## Parallel Processing

The function uses the future and furrr packages for parallel processing,
with the number of workers set to system cores minus 2 to prevent
resource exhaustion.

## Examples

``` r
if (FALSE) { # \dontrun{
# Run with default DEBUG logging
sync_validation_submissions()

# Run with INFO level logging
sync_validation_submissions(log_threshold = logger::INFO)
} # }
```
