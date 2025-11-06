# Get Validation Status from KoboToolbox

Retrieves the validation status for a specific submission in
KoboToolbox. The function handles NULL responses and returns a
consistent tibble structure regardless of the API response.

## Usage

``` r
get_validation_status(
  submission_id = NULL,
  asset_id = NULL,
  token = NULL,
  debug = FALSE
)
```

## Arguments

- submission_id:

  Character string. The ID of the submission to check.

- asset_id:

  Character string. The asset ID from KoboToolbox.

- token:

  Character string. The authorization token for KoboToolbox API.

- debug:

  Logical. If TRUE, prints the request object. Default is FALSE.

## Value

A tibble with one row containing:

- submission_id:

  The ID of the checked submission

- validation_status:

  The validation status (e.g., "validation_status_approved" or
  "not_validated")

- validated_at:

  Timestamp of validation as POSIXct

- validated_by:

  Username of the validator

## Examples

``` r
if (FALSE) { # \dontrun{
# Single submission
get_validation_status(
  submission_id = "1234567",
  asset_id = "your asset id",
  token = "Token YOUR_TOKEN_HERE"
)

# Multiple submissions using purrr
submission_ids <- c("1234567", "154267")
submission_ids %>%
  purrr::map_dfr(get_validation_status,
    asset_id = "your asset id",
    token = "Token YOUR_TOKEN_HERE"
  )
} # }
```
