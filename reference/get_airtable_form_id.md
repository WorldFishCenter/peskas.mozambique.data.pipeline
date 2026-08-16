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

## Details

Fails loudly when the lookup does not resolve to exactly one record. A
missing environment variable makes `kobo_asset_id` an empty string,
which would otherwise return `character(0)` and silently degrade every
downstream asset filter into one that matches nothing.
