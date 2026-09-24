# peskas.mozambique.data.pipeline

R package for the Peskas Mozambique pipeline: two KoBo landing-survey
chains (ADNAP and Lurio) plus PDS GPS trips, validated and exported to
GCS, Mongo and the Peskas API. PDS ingestion, portal summaries and
portal export are `coasts::` functions called from the workflow with
`package = "peskas.mozambique.data.pipeline"`. Ecosystem context (other
repos, data flow, cross-repo contracts): see PESKAS.md, loaded via
CLAUDE.local.md.

## Commands

- Load: `Rscript -e 'devtools::load_all()'`
- Docs: `Rscript -e 'devtools::document()'`
- Check: `Rscript -e 'devtools::check()'`
- `tests/testthat/` is empty, so `devtools::test()` proves nothing.
  Verify changes by running the function against the `default` (dev)
  profile, or by pushing a non-main branch.
- Pipeline steps and their order:
  `.github/workflows/data-pipeline.yaml`.

## Architecture

- Two independent chains, one function per step, suffixed `_lurio` /
  `_adnap`: `ingest_landings_*` (R/ingestion.R), `preprocess_landings_*`
  (R/preprocessing-surveys.R), `validate_surveys_*` (R/validation.R).
- **Only ADNAP feeds downstream products**:
  [`merge_trips()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/merge_trips.md)
  (joins PDS trips to validated ADNAP landings),
  [`export_api_raw()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/export_api_raw.md)
  /
  [`export_api_validated()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/export_api_validated.md)
  (R/api.R), and through them
  [`coasts::summarize_data()`](https://rdrr.io/pkg/coasts/man/summarize_data.html)
  and
  [`coasts::export_portal()`](https://rdrr.io/pkg/coasts/man/export_portal.html)
  (Mongo `portal-*`). Lurio ends in
  [`export_lurio_landings()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/export_lurio_landings.md),
  which writes its own summaries to the `mozambique-*` pipeline
  database.
- [`export_landings()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/export_landings.md)
  (R/export.R) is not called by the workflow.
- Catch weight: ADNAP uses
  [`calculate_catch_adnap()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/calculate_catch_adnap.md)
  (R/model-taxa.R), called from
  [`process_version_data()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/process_version_data.md);
  Lurio uses
  [`calculate_catch_lurio()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/calculate_catch_lurio.md)
  (R/preprocessing-surveys.R). Both: length-weight coefficients first,
  bucket count (`n_buckets * weight_bucket`) as fallback. Octopus
  (`OCZ`, `OQC`) lengths are arm-span, divided by 5.5 to mantle length
  before the L-W formula.
- **Fisher-estimate override** (ADNAP only): where `count_method == "3"`
  the fisher’s own weight replaces the calculated `catch_kg` (in
  [`process_version_data()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/process_version_data.md)).
  Lurio carries the same columns but does not apply it.
- Length-weight coefficients come from
  [`getLWCoeffs()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/getLWCoeffs.md)
  (R/model-taxa.R) with FishBase/SeaLifeBase releases pinned in
  `conf$metadata$fishbase`. `FLY` uses a hand-set coefficient that
  replaces the FishBase row.
  [`assert_taxa_coverage()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/assert_taxa_coverage.md)
  stops the run if a taxon has no coefficient.
- Retired KoBo taxon codes are remapped on read in
  [`preprocess_landings_lurio()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/preprocess_landings_lurio.md):
  `TUN -> TUS`, `SKH -> CVX`, `CLP -> ANX`. Old submissions keep the old
  codes, so the remap stays even after the form changes.
- Metadata (“assets”: taxa, gear, vessels, sites, geo) comes from
  Airtable, filtered to the current form via
  [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/get_airtable_form_id.md) +
  [`form_id_pattern()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/form_id_pattern.md),
  and joined by
  [`map_surveys()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/map_surveys.md).
- Validation flags are inline
  [`dplyr::case_when()`](https://dplyr.tidyverse.org/reference/case-and-replace-when.html)
  blocks inside each `validate_surveys_*()`:
  - Stage 1, per catch/trip: flags 1-7 and 12-13, into `alert_flag`.
  - Stage 2, composite: flags 8-11 (price per kg, CPUE, RPUE, zero
    fishers), computed only for submissions with no stage-1 flag.
  - Thresholds are hard-coded at the top of each `validate_surveys_*()`
    (e.g. `price_kg_max`, `max_length_cm`). `conf$validation$k_*` in
    config is read by no code.
  - Any flagged submission is dropped from the validated file; the flags
    are pushed by
    [`export_validation_flags()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/export_validation_flags.md)
    to Mongo `validation-*` as `surveys_flags-<asset_id>` /
    `enumerators_stats-<asset_id>`.
- Config keys: `conf$ingestion$<chain>` (KoBo),
  `conf$surveys$<chain>$<stage>$file_prefix` (GCS prefixes).

## Rules

- Add a validation flag as the next free number (14, 15, …) in both
  [`validate_surveys_lurio()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/validate_surveys_lurio.md)
  and
  [`validate_surveys_adnap()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/validate_surveys_adnap.md)
  where it applies. Existing numbers are read by peskas-validation.
- A change to catch weight or flags usually belongs in both chains;
  check the sibling function and say if you left one alone on purpose.
- Call the coasts version of Airtable and KoBo validation-status
  helpers. R/airtable.R,
  [`fetch_asset()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/fetch_asset.md)
  /
  [`form_id_pattern()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/form_id_pattern.md)
  in R/preprocessing-surveys.R, and
  [`get_validation_status()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/get_validation_status.md)
  /
  [`update_validation_status()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/update_validation_status.md)
  in R/validation-functions.R are local copies of coasts functions; flag
  them when you touch them rather than extending them.
- Keep the taxon remap and the `FLY` override unless the task is to
  change them; both protect published catch.

## Gotchas

- Env vars (see `.env.example` and `inst/config.yml`):
  - KoBo: `KOBO_ASSET_ID_LURIO`, `KOBO_ASSET_ID_ADNAP`, `KOBO_USERNAME`,
    `KOBO_PASSWORD`, `KOBO_TOKEN`.
  - Mongo: `MONGODB_CONNECTION_STRING` (pipeline + portal),
    `MONGODB_CONNECTION_STRING_VALIDATION` (validation db).
  - Other: `GCP_SA_KEY`, `PDS_TOKEN`, `PDS_SECRET`, `AIRTABLE_TOKEN`,
    `AIRTABLE_BASE_ID_FRAME`.
- [`ingest_landings_adnap()`](https://worldfishcenter.github.io/peskas.mozambique.data.pipeline/reference/ingest_landings_adnap.md)
  authenticates with the Lurio username and password
  (`conf$ingestion$lurio`); `conf$ingestion$adnap` has no password.
- `GOOGLE_SHEET_ID` / `metadata$google_sheets` in config is read by no
  code in this repo or in coasts.
- Config profiles are only `default` and `production`.
