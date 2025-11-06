# Get Airtable Form ID from KoBoToolbox Asset ID

Retrieves the Airtable record ID for a form based on its KoBoToolbox
asset ID.

## Usage

``` r
get_airtable_form_id(kobo_asset_id = NULL, conf = NULL)
```

## Arguments

- kobo_asset_id:

  Character. The KoBoToolbox asset ID to match.

- conf:

  Configuration object from read_config().

## Value

Character. The Airtable record ID for the matching form.
