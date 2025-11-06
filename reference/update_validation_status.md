# Update Validation Status in KoboToolbox

Updates the validation status for a specific submission in KoboToolbox.
The function allows setting the status to approved, not approved, or on
hold.

## Usage

``` r
update_validation_status(
  submission_id = NULL,
  asset_id = NULL,
  token = NULL,
  status = "validation_status_approved",
  debug = FALSE
)
```

## Arguments

- submission_id:

  Character string. The ID of the submission to update.

- asset_id:

  Character string. The asset ID from KoboToolbox.

- token:

  Character string. The authorization token for KoboToolbox API.

- status:

  Character string. The validation status to set. Must be one of:
  "validation_status_approved", "validation_status_not_approved", or
  "validation_status_on_hold".

- debug:

  Logical. If TRUE, prints the request object and response. Default is
  FALSE.

## Value

A tibble with one row containing:

- submission_id:

  The ID of the updated submission

- validation_status:

  The new validation status

- validated_at:

  Timestamp of validation as POSIXct

- validated_by:

  Username of the validator

- update_success:

  Logical indicating if the update was successful

## Examples

``` r
if (FALSE) { # \dontrun{
# Update a single submission
update_validation_status(
  submission_id = "1234567",
  asset_id = "your asset id",
  token = "Token YOUR_TOKEN_HERE",
  status = "validation_status_approved"
)

# Update multiple submissions using purrr
submission_ids <- c("1234567", "154267")
submission_ids %>%
  purrr::map_dfr(update_validation_status,
    asset_id = "your asset id",
    token = "Token YOUR_TOKEN_HERE",
    status = "validation_status_approved"
  )
} # }
```
