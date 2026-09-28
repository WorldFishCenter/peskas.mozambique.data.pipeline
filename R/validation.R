#' Validate Lurio Survey Data
#'
#' This function validates preprocessed fisheries survey data using a comprehensive
#' approach adapted from the Peskas Zanzibar pipeline. It performs both basic data
#' quality checks and composite economic indicator validation to ensure data integrity.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#' @return This function does not return a value. Instead, it processes the data and
#'   uploads both the validated results and validation flags to cloud storage.
#'
#' @details
#' The validation process follows a two-stage approach:
#'
#' \strong{Stage 1: Basic Data Quality Checks (Flags 1-7)}
#' \enumerate{
#'   \item \strong{Form completeness}: Catch outcome is "1" but catch_taxon is missing
#'   \item \strong{Catch info completeness}: Catch taxon exists but no weight or individuals
#'   \item \strong{Length validation}: Fish length below species minimum
#'   \item \strong{Length validation}: Fish length above species 75th percentile maximum
#'   \item \strong{Bucket weight}: Weight per bucket exceeds 50kg
#'   \item \strong{Bucket count}: Number of buckets exceeds 300
#'   \item \strong{Individual count}: Number of individuals exceeds 200 per record
#' }
#'
#' \strong{Stage 2: Composite Economic Indicators (Flags 8-10)}
#' \enumerate{
#'   \item \strong{Price per kg}: Exceeds 1875 MZN/kg (~30 EUR/kg, following Zanzibar thresholds)
#'   \item \strong{CPUE}: Catch per unit effort exceeds 30 kg/fisher/day
#'   \item \strong{RPUE}: Revenue per unit effort exceeds 1875 MZN/fisher/day
#' }
#'
#' Submissions with any validation flags are excluded from the final validated dataset
#' but the flags are preserved for data quality monitoring.
#'
#' @note This function requires a configuration file accessible via \code{read_config()}
#'   providing cloud storage connection details.
#'
#' @examples
#' \dontrun{
#' validate_surveys_lurio()
#' }
#'
#' @keywords workflow validation
#' @export
validate_surveys_lurio <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  # Load preprocessed surveys data
  preprocessed_surveys <-
    coasts::download_parquet_from_cloud(
      prefix = conf$surveys$`lurio`$preprocessed$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  validation_statuses <- survey_review_decisions(conf, "lurio")

  # Validation thresholds
  max_bucket_weight_kg <- 50 # Maximum weight per bucket
  max_n_buckets <- 300 # Maximum number of buckets
  max_n_individuals <- 200 # Maximum individuals per record
  price_kg_max <- 2500 # 30 EUR converted to MZN (81420 TZS * 0.023 MZN/TZS)
  cpue_max <- 30 # Max CPUE kg/fisher/day
  rpue_max <- 2500 # 30 EUR converted to MZN
  max_length_cm <- 500

  # Prepare catch data for validation - adapt to Mozambique structure

  catch_df <-
    preprocessed_surveys |>
    dplyr::filter(
      .data$survey_activity == "1"
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "landing_date",
      "submission_date",
      "catch_outcome",
      "catch_price",
      catch_taxon = "alpha3_code",
      "length",
      "min_length",
      "max_length_75",
      "individuals",
      "n_buckets",
      "weight_bucket",
      "catch_kg",
    )

  # Apply basic validation flags to catch data
  catch_flags <-
    catch_df |>
    dplyr::mutate(
      # Flag 1: Form incomplete - catch outcome is "1" but catch_taxon missing
      alert_form_incomplete = dplyr::case_when(
        .data$catch_outcome == "1" & is.na(.data$catch_taxon) ~ "1",
        TRUE ~ NA_character_
      ),
      # Flag 2: Catch info incomplete - catch_taxon exists but no catch_kg or individuals
      alert_catch_info_incomplete = dplyr::case_when(
        !is.na(.data$catch_taxon) &
          (.data$catch_kg <= 0 | is.na(.data$catch_kg)) &
          (is.na(.data$individuals) | .data$individuals <= 0) ~
          "2",
        TRUE ~ NA_character_
      ),
      # Flag 3: Length below minimum
      alert_min_length = dplyr::case_when(
        !is.na(.data$length) &
          !is.na(.data$min_length) &
          .data$length < .data$min_length ~
          "3",
        TRUE ~ NA_character_
      ),
      # Flag 4: Length above 75th percentile maximum
      alert_max_length = dplyr::case_when(
        !is.na(.data$length) &
          !is.na(.data$max_length_75) &
          .data$length > .data$max_length_75 ~
          "4",
        !is.na(.data$length) & .data$length > max_length_cm ~ "4",
        TRUE ~ NA_character_
      ),
      # Flag 5: Bucket weight exceeds maximum (following Zanzibar exactly)
      alert_bucket_weight = dplyr::case_when(
        !is.na(.data$weight_bucket) &
          .data$weight_bucket > max_bucket_weight_kg ~
          "5",
        TRUE ~ NA_character_
      ),
      # Flag 6: Number of buckets exceeds maximum (following Zanzibar exactly)
      alert_n_buckets = dplyr::case_when(
        !is.na(.data$n_buckets) & .data$n_buckets > max_n_buckets ~ "6",
        TRUE ~ NA_character_
      ),
      # Flag 7: Number of individuals exceeds maximum (following Zanzibar exactly)
      alert_n_individuals = dplyr::case_when(
        !is.na(.data$individuals) &
          .data$individuals > max_n_individuals ~ "7",
        TRUE ~ NA_character_
      ),
    ) |>
    dplyr::select(
      "submission_id",
      "submission_date",
      dplyr::contains("alert_")
    )

  general_flags <-
    preprocessed_surveys |>
    dplyr::mutate(
      alert_duration = dplyr::case_when(
        .data$trip_duration <= 0 | .data$trip_duration >= 60 ~ "12",
        TRUE ~ NA_character_
      ),
      alert_date = dplyr::case_when(
        .data$submission_date < .data$landing_date ~ "13",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(
      "submission_id",
      "submission_date",
      dplyr::contains("alert_")
    ) |>
    dplyr::distinct()

  # Create flags summary per submission (following Zanzibar approach)
  flags_id <-
    catch_flags |>
    dplyr::full_join(
      general_flags,
      by = c("submission_id", "submission_date")
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      alert_flag = paste(
        .data$alert_min_length,
        .data$alert_max_length,
        .data$alert_bucket_weight,
        .data$alert_n_buckets,
        .data$alert_n_individuals,
        .data$alert_form_incomplete,
        .data$alert_catch_info_incomplete,
        .data$alert_date,
        .data$alert_duration,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      ),
      submission_date = lubridate::as_datetime(.data$submission_date)
    ) |>
    dplyr::select(
      "submission_id",
      "submission_date",
      "alert_flag"
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      submission_date = dplyr::first(.data$submission_date),
      alert_flag = if (all(is.na(.data$alert_flag))) {
        NA_character_
      } else {
        paste(.data$alert_flag[!is.na(.data$alert_flag)], collapse = ", ")
      }
    ) %>%
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      )
    )

  # Filter validated catch data (remove flagged submissions)
  catch_df_validated <-
    catch_df |>
    dplyr::left_join(flags_id, by = c("submission_id", "submission_date")) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      submission_alerts = paste(
        unique(.data$alert_flag[!is.na(.data$alert_flag)]),
        collapse = ","
      )
    ) |>
    dplyr::mutate(
      submission_alerts = ifelse(
        .data$submission_alerts == "",
        NA_character_,
        .data$submission_alerts
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(is.na(.data$submission_alerts))

  surveys_basic_validated <-
    preprocessed_surveys |>
    dplyr::left_join(catch_df_validated) |>
    dplyr::select(
      -c(
        "alert_flag",
        "submission_alerts",
        "min_length",
        "max_length_75",
        "n"
      )
    ) |>
    # if catch outcome is 0 catch kg must be set to 0
    dplyr::mutate(
      catch_kg = dplyr::if_else(
        .data$catch_outcome == "0",
        0,
        .data$catch_kg
      ),
      catch_price = dplyr::if_else(
        .data$catch_outcome == "0",
        0,
        .data$catch_price
      )
    ) |>
    dplyr::select(-"catch_taxon") |>
    dplyr::rename(catch_taxon = "alpha3_code") |>
    dplyr::distinct()

  ### get flags for composite indicators ###
  no_flag_ids <-
    flags_id |>
    dplyr::filter(is.na(.data$alert_flag)) |>
    dplyr::select("submission_id") |>
    dplyr::distinct()

  indicators <-
    surveys_basic_validated |>
    dplyr::filter(.data$submission_id %in% no_flag_ids$submission_id) |>
    dplyr::select(
      "submission_id",
      "catch_outcome",
      "landing_date",
      "district",
      "landing_site",
      "gear",
      "trip_duration",
      "vessel_type",
      "n_fishers",
      "catch_taxon",
      "catch_price",
      "catch_kg"
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "catch_outcome",
          "landing_date",
          "district",
          "landing_site",
          "gear",
          "trip_duration",
          "vessel_type",
          "n_fishers",
          "catch_price"
        ),
        ~ dplyr::first(.x)
      ),
      catch_kg = sum(.data$catch_kg)
    ) |>
    dplyr::transmute(
      submission_id = .data$submission_id,
      catch_outcome = .data$catch_outcome,
      n_fishers = .data$n_fishers,
      price_kg = .data$catch_price / .data$catch_kg,
      price_kg_USD = .data$price_kg * 0.016,
      cpue = .data$catch_kg / .data$n_fishers / .data$trip_duration,
      rpue = .data$catch_price / .data$n_fishers / .data$trip_duration,
      rpue_USD = .data$rpue * 0.016
    )

  composite_flags <-
    indicators |>
    dplyr::mutate(
      alert_price_kg = dplyr::case_when(
        .data$price_kg > price_kg_max ~ "8",
        TRUE ~ NA_character_
      ),
      alert_cpue = dplyr::case_when(
        !.data$cpue == Inf & .data$cpue > cpue_max ~ "9",
        TRUE ~ NA_character_
      ),
      alert_rpue = dplyr::case_when(
        !.data$rpue == Inf & .data$rpue > rpue_max ~ "10",
        TRUE ~ NA_character_
      ),
      alert_fishers = dplyr::case_when(
        .data$n_fishers == 0 & .data$catch_outcome == "1" ~ "11",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate(
      alert_flag_composite = paste(
        .data$alert_price_kg,
        .data$alert_cpue,
        .data$alert_rpue,
        .data$alert_fishers,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag_composite = ifelse(
        .data$alert_flag_composite == "",
        NA_character_,
        .data$alert_flag_composite
      )
    ) |>
    dplyr::select("submission_id", "alert_flag_composite")

  # bind new flags to flags dataframe
  flags_combined <-
    flags_id |>
    dplyr::full_join(composite_flags, by = "submission_id") |>
    dplyr::mutate(
      alert_flag = dplyr::case_when(
        # If both are non-NA, combine them
        !is.na(.data$alert_flag) & !is.na(.data$alert_flag_composite) ~
          paste(.data$alert_flag, .data$alert_flag_composite, sep = ", "),
        # If only one is non-NA, use that one
        is.na(.data$alert_flag) ~ .data$alert_flag_composite,
        is.na(.data$alert_flag_composite) ~ .data$alert_flag,
        # If both are NA, keep it NA
        TRUE ~ NA_character_
      )
    ) |>
    # Remove the now redundant alert_flag_composite column
    dplyr::select(-"alert_flag_composite") |>
    dplyr::left_join(
      surveys_basic_validated |>
        dplyr::select(
          "submission_id",
          submitted_by = "enumerator_name_clean"
        ) |>
        dplyr::distinct(),
      by = "submission_id"
    ) |>
    dplyr::relocate("submitted_by", .after = "submission_id") |>
    dplyr::distinct()

  # A reviewer's decision outranks the automatic flags, either way.
  flagged_ids <-
    flags_combined |>
    dplyr::filter(
      (!is.na(.data$alert_flag) &
        !.data$submission_id %in% review_ids(validation_statuses, "approved")) |
        .data$submission_id %in% review_ids(validation_statuses, "not_approved")
    ) |>
    dplyr::pull("submission_id") |>
    unique()

  validated_data <-
    surveys_basic_validated |>
    dplyr::filter(!.data$submission_id %in% flagged_ids)

  coasts::upload_parquet_to_cloud(
    data = validated_data,
    prefix = conf$surveys$`lurio`$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  export_validation_flags(
    conf = conf,
    asset_id = "lurio",
    all_flags = flags_combined,
    validation_statuses = validation_statuses
  )

  invisible(NULL)
}
#' Validate ADNAP Survey Data
#'
#' @description
#' Validates ADNAP survey data by applying quality control checks and integrating
#' with KoBoToolbox validation status. The function filters out submissions that don't
#' meet validation criteria and processes catch data. Approved submissions in KoBoToolbox
#' bypass automatic validation flags.
#'
#' @details
#' The validation process follows a two-stage approach:
#'
#' \strong{Stage 1: Basic Data Quality Checks (Flags 1-7)}
#' \enumerate{
#'   \item \strong{Form completeness}: Catch outcome is "1" but catch_taxon is missing
#'   \item \strong{Catch info completeness}: Catch taxon exists but no weight or individuals
#'   \item \strong{Length validation}: Fish length below species minimum
#'   \item \strong{Length validation}: Fish length above species 75th percentile maximum
#'   \item \strong{Bucket weight}: Weight per bucket exceeds 50kg
#'   \item \strong{Bucket count}: Number of buckets exceeds 300
#'   \item \strong{Individual count}: Number of individuals exceeds 200 per record
#' }
#'
#' \strong{Stage 2: Composite Economic Indicators (Flags 8-10)}
#' \enumerate{
#'   \item \strong{Price per kg}: Exceeds 2500 MZN/kg (~30 EUR/kg)
#'   \item \strong{CPUE}: Catch per unit effort exceeds 30 kg/fisher/hour
#'   \item \strong{RPUE}: Revenue per unit effort exceeds 2500 MZN/fisher/hour
#' }
#'
#' \strong{KoBoToolbox Integration}:
#' The function queries KoBoToolbox validation status for each submission.
#' Submissions marked as "validation_status_approved" in KoBoToolbox have all
#' flags cleared and are included in the validated dataset regardless of automatic checks.
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#'
#' @return Invisible NULL. The function uploads two datasets to Google Cloud Storage:
#' \enumerate{
#'   \item Validation flags for each submission
#'   \item Validated survey data with invalid submissions removed
#' }
#'
#' @note
#' - Requires configuration parameters in config.yml with KoBoToolbox credentials
#' - Downloads preprocessed survey data from Google Cloud Storage
#' - Uses parallel processing to query KoBoToolbox validation status
#' - Submissions approved in KoBoToolbox bypass all automatic validation flags
#' - Sets catch_kg to 0 when catch_outcome is 0
#'
#' @examples
#' \dontrun{
#' validate_surveys_adnap()
#' }
#'
#' @keywords workflow validation
#' @export
validate_surveys_adnap <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  # Load and preprocess survey data
  preprocessed_surveys <-
    coasts::download_parquet_from_cloud(
      prefix = conf$surveys$`adnap`$preprocessed$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  validation_statuses <- survey_review_decisions(conf, "adnap")

  max_bucket_weight_kg <- 50
  max_n_buckets <- 250
  max_n_individuals <- 500
  price_kg_max <- 2500 # Mozambican metical -> 30 eur
  cpue_max <- 30
  rpue_max <- 2500
  # Absolute backstop for alert 4; see the note in validate_surveys_lurio().
  max_length_cm <- 500

  catch_df <-
    preprocessed_surveys |>
    dplyr::filter(
      .data$survey_activity == "1" &
        .data$collect_data_today == "1"
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      # dplyr::ends_with("fishers"),
      "catch_outcome",
      "catch_price",
      catch_taxon = "alpha3_code",
      "length",
      "min_length",
      "max_length_75",
      "individuals",
      "n_buckets",
      "weight_bucket",
      "catch_kg"
    )
  # dplyr::mutate(n_fishers = rowSums(across(c("no_men_fishers", "no_women_fishers", "no_child_fishers")),
  #                                 na.rm = TRUE)) |>
  # dplyr::select(-c("no_men_fishers", "no_women_fishers", "no_child_fishers")) |>
  # dplyr::relocate("n_fishers", .after = "has_boat")

  catch_flags <-
    catch_df |>
    dplyr::mutate(
      alert_form_incomplete = dplyr::case_when(
        .data$catch_outcome == "1" & is.na(.data$catch_taxon) ~ "1",
        TRUE ~ NA_character_
      ),
      alert_catch_info_incomplete = dplyr::case_when(
        !is.na(.data$catch_taxon) &
          is.na(.data$n_buckets) &
          is.na(.data$catch_kg) &
          is.na(.data$individuals) ~
          "2",
        TRUE ~ NA_character_
      ),
      alert_min_length = dplyr::case_when(
        .data$length < .data$min_length ~ "3",
        TRUE ~ NA_character_
      ),
      alert_max_length = dplyr::case_when(
        .data$length > .data$max_length_75 ~ "4",
        # Backstop for a taxon FishBase gives no bound for; see max_length_cm.
        .data$length > max_length_cm ~ "4",
        TRUE ~ NA_character_
      ),
      alert_bucket_weight = dplyr::case_when(
        !is.na(.data$weight_bucket) &
          .data$weight_bucket > max_bucket_weight_kg ~
          "5",
        TRUE ~ NA_character_
      ),
      alert_n_buckets = dplyr::case_when(
        !is.na(.data$n_buckets) & .data$n_buckets > max_n_buckets ~ "6",
        TRUE ~ NA_character_
      ),
      alert_n_individuals = dplyr::case_when(
        !is.na(.data$individuals) & .data$individuals > max_n_individuals ~ "7",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      dplyr::contains("alert_")
    )

  general_flags <-
    preprocessed_surveys |>
    dplyr::mutate(
      alert_duration = dplyr::case_when(
        .data$trip_duration <= 0 | .data$trip_duration >= 60 ~ "12",
        TRUE ~ NA_character_
      ),
      alert_date = dplyr::case_when(
        .data$submission_date < .data$landing_date ~ "13",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      dplyr::contains("alert_")
    )

  flags_id <-
    catch_flags |>
    dplyr::full_join(
      general_flags,
      by = c("submission_id", "n_catch", "submission_date")
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      alert_flag = paste(
        .data$alert_min_length,
        .data$alert_max_length,
        .data$alert_bucket_weight,
        .data$alert_n_buckets,
        .data$alert_n_individuals,
        .data$alert_form_incomplete,
        .data$alert_date,
        .data$alert_duration,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      ),
      submission_date = lubridate::as_datetime(.data$submission_date)
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      "alert_flag"
    ) |>
    dplyr::group_by(.data$submission_id) %>%
    # Summarize to get values
    dplyr::summarise(
      submission_date = dplyr::first(.data$submission_date),
      alert_flag = if (all(is.na(.data$alert_flag))) {
        NA_character_
      } else {
        paste(.data$alert_flag[!is.na(.data$alert_flag)], collapse = ", ")
      }
    ) %>%
    # Clean up empty strings
    dplyr::mutate(
      alert_flag = ifelse(
        .data$alert_flag == "",
        NA_character_,
        .data$alert_flag
      )
    )

  catch_df_validated <-
    catch_df |>
    dplyr::full_join(flags_id, by = c("submission_id", "submission_date")) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      submission_alerts = paste(
        unique(.data$alert_flag[!is.na(.data$alert_flag)]),
        collapse = ","
      )
    ) |>
    dplyr::mutate(
      submission_alerts = ifelse(
        .data$submission_alerts == "",
        NA_character_,
        .data$submission_alerts
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::filter(is.na(.data$submission_alerts))

  surveys_basic_validated <-
    preprocessed_surveys |>
    dplyr::left_join(catch_df_validated) |>
    dplyr::select(
      -c("alert_flag", "submission_alerts", "min_length", "max_length_75", "n")
    ) |>
    # if catch outcome is 0 catch kg must be set to 0
    dplyr::mutate(
      catch_kg = dplyr::if_else(.data$catch_outcome == "0", 0, .data$catch_kg),
      catch_price = dplyr::if_else(
        .data$catch_outcome == "0",
        0,
        .data$catch_price
      )
    )

  ### get flags for composite indicators ###
  no_flag_ids <-
    flags_id |>
    dplyr::filter(is.na(.data$alert_flag)) |>
    dplyr::select("submission_id") |>
    dplyr::distinct()

  indicators <-
    surveys_basic_validated |>
    dplyr::filter(.data$submission_id %in% no_flag_ids$submission_id) |>
    dplyr::mutate(
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "submission_id",
      "catch_outcome",
      "landing_date",
      "district",
      "landing_site",
      "gear",
      "trip_duration",
      "vessel_type",
      "n_fishers",
      "catch_taxon",
      "catch_price",
      "catch_kg"
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "catch_outcome",
          "landing_date",
          "district",
          "landing_site",
          "gear",
          "trip_duration",
          "vessel_type",
          "n_fishers",
          "catch_price"
        ),
        ~ dplyr::first(.x)
      ),
      catch_kg = sum(.data$catch_kg)
    ) |>
    dplyr::transmute(
      submission_id = .data$submission_id,
      catch_outcome = .data$catch_outcome,
      n_fishers = .data$n_fishers,
      price_kg = .data$catch_price / .data$catch_kg,
      price_kg_USD = .data$price_kg * 0.016,
      cpue = .data$catch_kg / .data$n_fishers / .data$trip_duration,
      rpue = .data$catch_price / .data$n_fishers / .data$trip_duration,
      rpue_USD = .data$rpue * 0.016
    )

  composite_flags <-
    indicators |>
    dplyr::mutate(
      alert_price_kg = dplyr::case_when(
        .data$price_kg > price_kg_max ~ "8",
        TRUE ~ NA_character_
      ),
      alert_cpue = dplyr::case_when(
        !.data$cpue == Inf & .data$cpue > cpue_max ~ "9",
        TRUE ~ NA_character_
      ),
      alert_rpue = dplyr::case_when(
        !.data$rpue == Inf & .data$rpue > rpue_max ~ "10",
        TRUE ~ NA_character_
      ),
      # A trip with nobody on it did not happen: a zero here is the
      # enumerator's untouched default, not a count. These submissions record
      # survey_activity = 1, a fishing_start, a fishing_end and a habitat, so
      # the trip is real and the crew was simply never entered. The catch
      # outcome used to narrow this, which let the same defect through on
      # no-catch trips and published n_fishers = 0 against a schema whose
      # minimum is 1, with every per-fisher metric dividing into Inf.
      alert_fishers = dplyr::case_when(
        .data$n_fishers == 0 ~ "11",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate(
      alert_flag_composite = paste(
        .data$alert_price_kg,
        .data$alert_cpue,
        .data$alert_rpue,
        .data$alert_fishers,
        sep = ","
      ) |>
        stringr::str_remove_all("NA,") |>
        stringr::str_remove_all(",NA") |>
        stringr::str_remove_all("^NA$")
    ) |>
    dplyr::mutate(
      alert_flag_composite = ifelse(
        .data$alert_flag_composite == "",
        NA_character_,
        .data$alert_flag_composite
      )
    ) |>
    dplyr::select("submission_id", "alert_flag_composite")

  # bind new flags to flags dataframe
  flags_combined <-
    flags_id |>
    dplyr::full_join(composite_flags, by = "submission_id") |>
    dplyr::mutate(
      alert_flag = dplyr::case_when(
        # If both are non-NA, combine them
        !is.na(.data$alert_flag) & !is.na(.data$alert_flag_composite) ~
          paste(.data$alert_flag, .data$alert_flag_composite, sep = ", "),
        # If only one is non-NA, use that one
        is.na(.data$alert_flag) ~ .data$alert_flag_composite,
        is.na(.data$alert_flag_composite) ~ .data$alert_flag,
        # If both are NA, keep it NA
        TRUE ~ NA_character_
      )
    ) |>
    # Remove the now redundant alert_flag_composite column
    dplyr::select(-"alert_flag_composite") |>
    dplyr::left_join(
      surveys_basic_validated |>
        dplyr::select("submission_id", "submitted_by") |>
        dplyr::distinct(),
      by = "submission_id"
    ) |>
    dplyr::relocate("submitted_by", .after = "submission_id") |>
    dplyr::distinct()

  # A reviewer's decision outranks the automatic flags, either way.
  flags_ids <-
    flags_combined |>
    dplyr::filter(
      (!is.na(.data$alert_flag) &
        !.data$submission_id %in% review_ids(validation_statuses, "approved")) |
        .data$submission_id %in% review_ids(validation_statuses, "not_approved")
    ) |>
    dplyr::pull(.data$submission_id) |>
    unique()

  clean_landings <-
    surveys_basic_validated |>
    dplyr::filter(!.data$submission_id %in% flags_ids)

  coasts::upload_parquet_to_cloud(
    data = clean_landings,
    prefix = conf$surveys$`adnap`$validated$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  export_validation_flags(
    conf = conf,
    asset_id = "adnap",
    all_flags = flags_combined,
    validation_statuses = validation_statuses
  )

  invisible(NULL)
}

# Reviewers' decisions on one form, read before this run rewrites its flags
# collection. Both forms are read with the Lurio token, as before.
survey_review_decisions <- function(conf, survey = c("adnap", "lurio")) {
  survey <- match.arg(survey)
  asset_id <- conf$ingestion[[survey]]$asset_id
  coasts::review_decisions(
    flags = coasts::mdb_collection_pull(
      connection_string = conf$storage$mongodb$connection_strings$validation,
      db_name = conf$storage$mongodb$databases$validation$database_name,
      collection_name = paste(
        conf$storage$mongodb$databases$validation$collections$flags,
        asset_id,
        sep = "-"
      )
    ),
    pipeline_users = unique(c(
      conf$ingestion$adnap$username,
      conf$ingestion$lurio$username
    )),
    asset_id = asset_id,
    token = conf$ingestion$lurio$token
  )
}

review_ids <- function(decisions, status = c("approved", "not_approved")) {
  status <- match.arg(status)
  decisions$submission_id[
    decisions$validation_status == paste0("validation_status_", status)
  ]
}

#' Export Validation Flags to MongoDB
#'
#' @description
#' Exports validation flags to MongoDB, keeping the decisions reviewers made in
#' the Peskas Management Platform or in KoboToolbox (read beforehand with
#' `coasts::review_decisions()`).
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Joins validation flags with KoboToolbox validation statuses
#'   \item Identifies manual human approvals (excluding system username)
#'   \item Preserves manual human decisions while updating system-generated statuses
#'   \item Creates both wide and long format datasets for different reporting needs
#'   \item Pushes results directly to MongoDB collections
#' }
#'
#' \strong{Validation Status Logic:}
#' \itemize{
#'   \item If submission has flags AND validated_by is system username: set to "not_approved"
#'   \item If submission has no flags AND validated_by is system username: set to "approved"
#'   \item If validated_by is NOT system username: preserve existing status (a reviewer's approval or rejection)
#' }
#'
#' @param conf Configuration object from `read_config()` containing MongoDB connection
#'   parameters and survey-specific settings
#' @param asset_id Character string specifying which survey to process. Must be one of
#'   "adnap" or "lurio". Determines which configuration to use from
#'   `conf$ingestion$kobo-{asset_id}`. Default is "adnap".
#' @param all_flags Data frame containing all validation flags with columns:
#'   `submission_id`, `submitted_by`, `submission_date`, `alert_flag`
#' @param validation_statuses Reviewers' decisions from `coasts::review_decisions()`,
#'   with columns `submission_id`, `validation_status`, `validated_at`, `validated_by`
#'
#' @return Invisible NULL. The function pushes data to MongoDB as a side effect.
#'
#' @section MongoDB Collections:
#' The function pushes to two MongoDB collections:
#' \describe{
#'   \item{flags-{asset_id}}{Wide format with one row per submission including
#'     validation status and flags}
#'   \item{enumerators_stats-{asset_id}}{Long format with one row per flag per
#'     submission for enumerator statistics}
#' }
#'
#' @note
#' This function is called internally by `validate_surveys_adnap()` and should not
#' typically be called directly. It requires:
#' \itemize{
#'   \item Valid configuration with MongoDB connection string
#'   \item Survey-specific configuration under `conf$ingestion$kobo-{asset_id}`
#'   \item System username configured to identify automated vs. manual validations
#' }
#'
#' @examples
#' \dontrun{
#' # Called internally by validate_surveys_adnap()
#' export_validation_flags(
#'   conf = conf,
#'   asset_id = "adnap",
#'   all_flags = flags_combined,
#'   validation_statuses = validation_statuses
#' )
#' }
#'
#' @seealso
#' \itemize{
#'   \item \code{\link[=validate_surveys_adnap]{validate_surveys_adnap()}} for the main validation workflow
#'   \item \code{\link[coasts:mdb_collection_push]{coasts::mdb_collection_push()}} for MongoDB operations
#' }
#'
#' @keywords validation workflow
#' @export
export_validation_flags <- function(
  conf = NULL,
  asset_id = c("adnap", "lurio"),
  all_flags = NULL,
  validation_statuses = NULL
) {
  asset_id <- match.arg(asset_id)
  config_key <- asset_id
  # Get the survey-specific config
  survey_conf <- conf$surveys[[config_key]]

  # Reviewers' decisions carry integer ids; the flags may hold them as text.
  if (is.character(all_flags$submission_id)) {
    validation_statuses$submission_id <- as.character(
      validation_statuses$submission_id
    )
  }

  validation_flags_with_kobo_status <-
    all_flags |>
    dplyr::full_join(validation_statuses, by = "submission_id") |>
    dplyr::mutate(
      # The pipeline signs an unflagged submission, unless a reviewer decided it.
      validated_by = dplyr::if_else(
        is.na(.data$alert_flag) & is.na(.data$validated_by),
        conf$ingestion$`adnap`$username,
        .data$validated_by
      ),
      validation_status = dplyr::case_when(
        # Preserve existing status if validated by someone else (not pipeline account user and not NA)
        !is.na(.data$validated_by) &
          .data$validated_by !=
            conf$ingestion$`adnap`$username ~ .data$validation_status,
        # Apply new status only if validated_by is NA or matches kobo user
        !is.na(.data$alert_flag) ~ "validation_status_not_approved",
        is.na(.data$alert_flag) ~ "validation_status_approved",
        TRUE ~ .data$validation_status
      ),
    ) |>
    dplyr::filter(!is.na(.data$submitted_by))

  validation_flags_long <- validation_flags_with_kobo_status |>
    dplyr::mutate(alert_flag = as.character(.data$alert_flag)) %>%
    tidyr::separate_rows("alert_flag", sep = ",\\s*") |>
    dplyr::select(-c(dplyr::starts_with("valid")))

  asset_id <- conf$ingestion[[config_key]]$asset_id

  coasts::mdb_collection_push(
    data = validation_flags_with_kobo_status,
    connection_string = conf$storage$mongodb$connection_strings$validation,
    db_name = conf$storage$mongodb$databases$validation$database_name,
    collection_name = paste(
      conf$storage$mongodb$databases$validation$collections$flags,
      asset_id,
      sep = "-"
    )
  )

  coasts::mdb_collection_push(
    data = validation_flags_long,
    connection_string = conf$storage$mongodb$connection_strings$validation,
    db_name = conf$storage$mongodb$databases$validation$database_name,
    collection_name = paste(
      conf$storage$mongodb$databases$validation$collections$enumerators_stats,
      asset_id,
      sep = "-"
    )
  )

  logger::log_info("Validation synchronization completed successfully")
  invisible(NULL)
}
