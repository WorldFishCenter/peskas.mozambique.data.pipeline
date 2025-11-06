# Get Trip Points from Pelagic Data Systems API

Retrieves trip points data from the Pelagic Data Systems API. The
function can either fetch data for a specific trip ID or for a date
range. The response can be returned as a data frame or written directly
to a file.

## Usage

``` r
get_trip_points(
  token = NULL,
  secret = NULL,
  id = NULL,
  dateFrom = NULL,
  dateTo = NULL,
  path = NULL,
  imeis = NULL,
  deviceInfo = FALSE,
  errant = FALSE,
  withLastSeen = FALSE,
  tags = NULL,
  overwrite = TRUE
)
```

## Arguments

- token:

  Character string. Access token for the PDS API.

- secret:

  Character string. Secret key for the PDS API.

- id:

  Numeric or character. Optional trip ID. If provided, retrieves points
  for specific trip. If NULL, dateFrom and dateTo must be provided.

- dateFrom:

  Character string. Start date for data retrieval in format
  "YYYY-MM-DD". Required if id is NULL.

- dateTo:

  Character string. End date for data retrieval in format "YYYY-MM-DD".
  Required if id is NULL.

- path:

  Character string. Optional path where the CSV file should be saved. If
  provided, the function returns the path instead of the data frame.

- imeis:

  Vector of character or numeric. Optional IMEI numbers to filter the
  data.

- deviceInfo:

  Logical. If TRUE, includes device information in the response. Default
  is FALSE.

- errant:

  Logical. If TRUE, includes errant points in the response. Default is
  FALSE.

- withLastSeen:

  Logical. If TRUE, includes last seen information. Default is FALSE.

- tags:

  Vector of character. Optional tags to filter the data.

- overwrite:

  Logical. If TRUE, will overwrite existing file when path is provided.
  Default is TRUE.

## Value

If path is NULL, returns a tibble containing the trip points data. If
path is provided, returns the file path as a character string.

## Examples

``` r
if (FALSE) { # \dontrun{
# Get data for a specific trip
trip_data <- get_trip_points(
  token = "your_token",
  secret = "your_secret",
  id = "12345",
  deviceInfo = TRUE
)

# Get data for a date range
date_data <- get_trip_points(
  token = "your_token",
  secret = "your_secret",
  dateFrom = "2024-01-01",
  dateTo = "2024-01-31"
)

# Save data directly to file
file_path <- get_trip_points(
  token = "your_token",
  secret = "your_secret",
  id = "12345",
  path = "trip_data.csv"
)
} # }
```
