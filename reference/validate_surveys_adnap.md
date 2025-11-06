# Validate ADNAP Survey Data

Validates ADNAP survey data by applying quality control checks and
integrating with KoBoToolbox validation status. The function filters out
submissions that don't meet validation criteria and processes catch
data. Approved submissions in KoBoToolbox bypass automatic validation
flags.

## Usage

``` r
validate_surveys_adnap(log_threshold = logger::DEBUG)
```

## Arguments

- log_threshold:

  The logging level threshold for the logger package (e.g., DEBUG, INFO)

## Value

Invisible NULL. The function uploads two datasets to Google Cloud
Storage:

1.  Validation flags for each submission

2.  Validated survey data with invalid submissions removed

## Details

The validation process follows a two-stage approach:

**Stage 1: Basic Data Quality Checks (Flags 1-7)**

1.  **Form completeness**: Catch outcome is "1" but catch_taxon is
    missing

2.  **Catch info completeness**: Catch taxon exists but no weight or
    individuals

3.  **Length validation**: Fish length below species minimum

4.  **Length validation**: Fish length above species 75th percentile
    maximum

5.  **Bucket weight**: Weight per bucket exceeds 50kg

6.  **Bucket count**: Number of buckets exceeds 300

7.  **Individual count**: Number of individuals exceeds 200 per record

**Stage 2: Composite Economic Indicators (Flags 8-10)**

1.  **Price per kg**: Exceeds 2500 MZN/kg (~30 EUR/kg)

2.  **CPUE**: Catch per unit effort exceeds 30 kg/fisher/hour

3.  **RPUE**: Revenue per unit effort exceeds 2500 MZN/fisher/hour

**KoBoToolbox Integration**: The function queries KoBoToolbox validation
status for each submission. Submissions marked as
"validation_status_approved" in KoBoToolbox have all flags cleared and
are included in the validated dataset regardless of automatic checks.

## Note

- Requires configuration parameters in config.yml with KoBoToolbox
  credentials

- Downloads preprocessed survey data from Google Cloud Storage

- Uses parallel processing to query KoBoToolbox validation status

- Submissions approved in KoBoToolbox bypass all automatic validation
  flags

- Sets catch_kg to 0 when catch_outcome is 0

## Examples

``` r
if (FALSE) { # \dontrun{
validate_surveys_adnap()
} # }
```
