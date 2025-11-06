# Sync Data with Airtable (Update + Create)

Main function for daily syncing. Updates existing records and creates
new ones.

## Usage

``` r
device_sync(
  boats_df,
  base_id,
  table_name = "pds_devices",
  token,
  key_field = "imei"
)
```

## Arguments

- boats_df:

  Data frame with device data. Must include key_field column.

- base_id:

  Character string. The Airtable base ID.

- table_name:

  Character string. Name of the table (default: "pds_devices").

- token:

  Character string. Airtable API token.

- key_field:

  Character string. Field to match on (default: "imei").

## Value

List with update and create results.
