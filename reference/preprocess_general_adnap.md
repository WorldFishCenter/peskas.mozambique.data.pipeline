# Preprocess General Survey Information for ADNAP

Processes general survey information from ADNAP KoBoToolbox forms
including trip details, fisher counts, vessel information, and survey
metadata.

## Usage

``` r
preprocess_general_adnap(data = NULL)
```

## Arguments

- data:

  A data frame containing raw ADNAP survey data

## Value

A data frame with processed general survey information including:
submission_id, dates, location, gear, trip details, fisher counts, etc.
