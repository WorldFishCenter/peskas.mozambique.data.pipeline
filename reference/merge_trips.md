# Merge GPS Tracker Trips with Validated Survey Landings

Merges PDS trip data with validated survey landings. Joins only when
there is exactly one trip and one survey per device per day.

## Usage

``` r
merge_trips(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  Logging threshold level (default: logger::DEBUG)

## Value

Invisible NULL. Uploads merged data to Google Cloud Storage.

## Note

Landing date is assumed to be the trip end date from PDS. Records with
multiple trips or surveys per device-day are included but not joined.

## Examples

``` r
if (FALSE) { # \dontrun{
merge_trips()
} # }
```
