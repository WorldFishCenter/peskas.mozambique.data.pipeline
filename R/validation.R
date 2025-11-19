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
  preprocessed_landings <-
    download_parquet_from_cloud(
      prefix = conf$ingestion$`kobo-lurio`$preprocessed_surveys$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  # Validation thresholds
  max_bucket_weight_kg <- 50 # Maximum weight per bucket
  max_n_buckets <- 300 # Maximum number of buckets
  max_n_individuals <- 200 # Maximum individuals per record
  price_kg_max <- 1875 # 30 EUR converted to MZN (81420 TZS * 0.023 MZN/TZS)
  cpue_max <- 30 # Max CPUE kg/fisher/day
  rpue_max <- 1875 # 30 EUR converted to MZN

  # Prepare catch data for validation - adapt to Mozambique structure
  catch_df <-
    preprocessed_landings |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "submission_date",
      "district",
      "landing_site",
      "catch_outcome",
      "gear",
      "vessel_type",
      "propulsion_gear",
      "trip_duration",
      "tot_fishers",
      catch_taxon = "alpha3_code",
      "individuals",
      "length",
      "min_length",
      "max_length_75",
      "n_buckets",
      "weight_bucket",
      "catch_kg",
      "catch_price",
      "habitat",
      "lat",
      "lon"
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
        !is.na(.data$individuals) & .data$individuals > max_n_individuals ~ "7",
        TRUE ~ NA_character_
      ),
    )

  # Create flags summary per submission (following Zanzibar approach)
  flags_id <-
    catch_flags |>
    dplyr::select(
      "submission_id",
      "submission_date",
      dplyr::contains("alert_")
    ) |>
    dplyr::mutate(
      alert_flag = paste(
        .data$alert_form_incomplete,
        .data$alert_catch_info_incomplete,
        .data$alert_min_length,
        .data$alert_max_length,
        .data$alert_bucket_weight,
        .data$alert_n_buckets,
        .data$alert_n_individuals,
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
      )
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
      },
      .groups = "drop"
    ) |>
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

  # Create initial validated dataset
  validated_data <-
    catch_df_validated |>
    dplyr::select(
      -c("alert_flag", "submission_alerts", "min_length", "max_length_75")
    ) |>
    # If catch outcome is 0, catch kg and price must be 0
    dplyr::mutate(
      catch_kg = dplyr::if_else(.data$catch_outcome == "0", 0, .data$catch_kg),
      catch_price = dplyr::if_else(
        .data$catch_outcome == "0",
        0,
        .data$catch_price
      )
    )

  ### Composite indicator validation (following Zanzibar approach) ###
  no_flag_ids <-
    flags_id |>
    dplyr::filter(is.na(.data$alert_flag)) |>
    dplyr::select("submission_id") |>
    dplyr::distinct()

  # Calculate indicators for composite validation
  indicators <-
    validated_data |>
    dplyr::filter(.data$submission_id %in% no_flag_ids$submission_id) |>
    dplyr::select(
      "submission_id",
      "catch_outcome",
      "landing_date",
      "district",
      "landing_site",
      "gear",
      "trip_duration",
      "tot_fishers",
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
          "tot_fishers"
        ),
        ~ dplyr::first(.x)
      ),
      total_catch_price = sum(.data$catch_price, na.rm = TRUE),
      total_catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      price_kg = ifelse(
        .data$total_catch_kg > 0,
        .data$total_catch_price / .data$total_catch_kg,
        0
      ),
      price_kg_USD = .data$price_kg * 0.016, # MZN to USD conversion (~0.016)
      cpue = .data$total_catch_kg / .data$tot_fishers / .data$trip_duration,
      rpue = .data$total_catch_price / .data$tot_fishers / .data$trip_duration,
      rpue_USD = .data$rpue * 0.016
    )

  # Apply composite validation flags
  composite_flags <-
    indicators |>
    dplyr::mutate(
      alert_price_kg = dplyr::case_when(
        .data$price_kg > price_kg_max ~ "8",
        TRUE ~ NA_character_
      ),
      alert_cpue = dplyr::case_when(
        .data$cpue > cpue_max ~ "9",
        TRUE ~ NA_character_
      ),
      alert_rpue = dplyr::case_when(
        .data$rpue > rpue_max ~ "10",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate(
      alert_flag_composite = paste(
        .data$alert_price_kg,
        .data$alert_cpue,
        .data$alert_rpue,
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

  # Combine all flags
  flags_combined <-
    flags_id |>
    dplyr::full_join(composite_flags, by = "submission_id") |>
    dplyr::mutate(
      alert_flag = dplyr::case_when(
        !is.na(.data$alert_flag) & !is.na(.data$alert_flag_composite) ~
          paste(.data$alert_flag, .data$alert_flag_composite, sep = ", "),
        is.na(.data$alert_flag) ~ .data$alert_flag_composite,
        is.na(.data$alert_flag_composite) ~ .data$alert_flag,
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(-"alert_flag_composite") |>
    dplyr::distinct()

  final_validated_data <-
    validated_data |>
    dplyr::semi_join(
      flags_combined |>
        dplyr::filter(is.na(.data$alert_flag)) |>
        dplyr::distinct(.data$submission_id),
      by = "submission_id"
    )

  # Upload validation flags
  # upload_parquet_to_cloud(
  #   data = flags_combined,
  #   prefix = paste0(
  #     conf$ingestion$`kobo-v1`$validated_surveys$file_prefix,
  #     "-flags"
  #   ),
  #   provider = conf$storage$google$key,
  #   options = conf$storage$google$options
  # )

  # Upload validated data
  upload_parquet_to_cloud(
    data = final_validated_data,
    prefix = conf$ingestion$`kobo-v1`$validated_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info(
    "Validation completed. {nrow(final_validated_data)} records validated, {sum(!is.na(flags_combined$alert_flag))} submissions flagged"
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

  # 1. Load and preprocess survey data
  preprocessed_surveys <-
    download_parquet_from_cloud(
      prefix = conf$ingestion$`kobo-adnap`$preprocessed_surveys$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  future::plan(
    strategy = future::multisession,
    workers = future::availableCores() - 2
  )

  # Get validation status from KoboToolbox for existing submissions
  submission_ids <- unique(preprocessed_surveys$submission_id)

  # Query validation status from ADNAP asset
  logger::log_info(
    "Querying validation status from ADNAP asset for {length(submission_ids)} submissions"
  )

  validation_results <- submission_ids %>%
    furrr::future_map_dfr(
      get_validation_status,
      asset_id = conf$ingestion$`kobo-adnap`$asset_id,
      token = conf$ingestion$`kobo-lurio`$token,
      .options = furrr::furrr_options(seed = TRUE)
    )

  # Extract manually approved IDs (exclude system-approved)
  # Only human-reviewed approvals should override automatic validation flags
  manual_approved_ids <- validation_results %>%
    dplyr::filter(
      .data$validation_status == "validation_status_approved" &
        !is.na(.data$validated_by) &
        .data$validated_by != "" &
        .data$validated_by != conf$ingestion$`kobo-lurio`$username # Exclude system approvals
    ) %>%
    dplyr::pull(.data$submission_id) %>%
    unique()

  logger::log_info(
    "Found {length(manual_approved_ids)} manually approved submissions in KoboToolbox"
  )

  max_bucket_weight_kg <- 50
  max_n_buckets <- 300
  max_n_individuals <- 200
  price_kg_max <- 2500 # Mozambican metical -> 30 eur
  cpue_max <- 30
  rpue_max <- 2500

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
    )

  flags_id <-
    catch_flags |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "submission_date",
      dplyr::contains("alert_")
    ) |>
    dplyr::mutate(
      alert_flag = paste(
        .data$alert_min_length,
        .data$alert_max_length,
        .data$alert_bucket_weight,
        .data$alert_n_buckets,
        .data$alert_n_individuals,
        .data$alert_form_incomplete,
        .data$alert_catch_info_incomplete,
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
        .data$cpue > cpue_max ~ "9",
        TRUE ~ NA_character_
      ),
      alert_rpue = dplyr::case_when(
        .data$rpue > rpue_max ~ "10",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate(
      alert_flag_composite = paste(
        .data$alert_price_kg,
        .data$alert_cpue,
        .data$alert_rpue,
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
    dplyr::distinct() |>
    # Preserve manual approvals by human reviewers, but re-validate system approvals
    dplyr::mutate(
      alert_flag = dplyr::if_else(
        .data$submission_id %in% manual_approved_ids,
        NA_character_,
        .data$alert_flag
      )
    )

  upload_parquet_to_cloud(
    data = flags_combined,
    prefix = conf$ingestion$`kobo-adnap`$validation$flags$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  upload_parquet_to_cloud(
    data = surveys_basic_validated,
    prefix = conf$ingestion$`kobo-adnap`$validated_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
  invisible(NULL)
}

#' Synchronize Validation Statuses with KoboToolbox
#'
#' @description
#' Synchronizes validation statuses between the local system and KoboToolbox by processing
#' validation flags and updating submission statuses accordingly. This function follows the
#' Kenya pipeline pattern with rate limiting, manual approval respect, and optimized API usage.
#'
#' @details
#' The function follows these steps:
#' 1. Downloads the current validation flags from cloud storage
#' 2. Sets up parallel processing with rate limiting
#' 3. Fetches current validation status from KoboToolbox FIRST (before any updates)
#' 4. Identifies manually approved submissions (excluding system username)
#' 5. Updates flagged submissions as "not approved" (EXCLUDING manual approvals)
#' 6. Updates clean submissions as "approved" (SKIPPING already-approved ones)
#' 7. Fetches final validation status after updates
#' 8. Combines results and pushes to MongoDB for record-keeping
#'
#' Key improvements over previous implementation:
#' - Rate limiting prevents overwhelming KoboToolbox API
#' - Manual approvals are respected and never overwritten
#' - Already-approved submissions are skipped to minimize API calls
#' - Better error tracking and logging
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO).
#'        Default is logger::DEBUG.
#'
#' @return None. The function performs status updates and database operations as side effects.
#'
#' @section Parallel Processing:
#' The function uses the future and furrr packages for parallel processing, with the number
#' of workers set to system cores minus 2 to prevent resource exhaustion. Rate limiting is
#' implemented via Sys.sleep() to respect API constraints.
#'
#' @note
#' This function requires proper configuration in the config file, including:
#' - MongoDB connection parameters
#' - KoboToolbox asset ID and token (configured under ingestion$kobo-adnap)
#' - KoboToolbox username (to identify system approvals)
#' - Google cloud storage parameters
#'
#' @examples
#' \dontrun{
#' # Run with default DEBUG logging
#' sync_validation_submissions()
#'
#' # Run with INFO level logging
#' sync_validation_submissions(log_threshold = logger::INFO)
#' }
#'
#' @seealso
#' * [process_submissions_parallel()] for the helper function with rate limiting
#' * [get_validation_status()] for fetching KoboToolbox validation status
#' * [update_validation_status()] for updating KoboToolbox validation status
#'
#' @importFrom logger log_threshold log_info
#' @importFrom dplyr filter pull
#' @importFrom future plan multisession availableCores
#' @importFrom progressr handlers handler_progress
#'
#' @keywords workflow validation
#' @export
sync_validation_submissions <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)

  conf <- read_config()

  # Download validation flags for ADNAP
  validation_flags <-
    download_parquet_from_cloud(
      prefix = conf$ingestion$`kobo-adnap`$validation$flags$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  all_submission_ids <- unique(validation_flags$submission_id)

  # Setup parallel processing
  future::plan(
    strategy = future::multisession,
    workers = future::availableCores() - 2
  )

  # Enable progress reporting globally
  progressr::handlers(progressr::handler_progress(
    format = "[:bar] :current/:total (:percent) eta: :eta"
  ))

  # STEP 1: Fetch current status FIRST (with rate limiting)
  logger::log_info(
    "Fetching current validation status for {length(all_submission_ids)} submissions"
  )

  current_kobo_status <- process_submissions_parallel(
    submission_ids = all_submission_ids,
    process_fn = function(id) {
      get_validation_status(
        submission_id = id,
        asset_id = conf$ingestion$`kobo-adnap`$asset_id,
        token = conf$ingestion$`kobo-adnap`$token
      )
    },
    description = "current validation statuses",
    rate_limit = 0.1
  )

  # STEP 2: Identify manual approvals (exclude system username)
  # Manual approvals = submissions approved by ANY human user (not the system/API user)
  # These represent human review decisions that should NEVER be overwritten
  # Filter criteria:
  #   - validation_status is "approved"
  #   - validated_by is not empty/NA
  #   - validated_by is NOT the system username (conf$ingestion$`kobo-adnap`$username)
  # Result: Any approval by a human reviewer (e.g., enumerators, supervisors, data managers)
  manual_approved_ids <- current_kobo_status %>%
    dplyr::filter(
      .data$validation_status == "validation_status_approved" &
        !is.na(.data$validated_by) &
        .data$validated_by != "" &
        .data$validated_by != conf$ingestion$`kobo-adnap`$username
    ) %>%
    dplyr::pull(.data$submission_id)

  logger::log_info(
    "Found {length(manual_approved_ids)} manually approved submissions (will not be overwritten)"
  )

  # STEP 3: Identify flagged submissions (EXCLUDING manual approvals)
  flagged_submissions <- validation_flags %>%
    dplyr::filter(!is.na(.data$alert_flag)) %>%
    dplyr::pull(.data$submission_id) %>%
    unique() %>%
    setdiff(manual_approved_ids) # CRITICAL: Don't override human decisions

  # STEP 4: Update flagged submissions (with rate limiting)
  if (length(flagged_submissions) > 0) {
    logger::log_info(
      "Marking {length(flagged_submissions)} flagged submissions as not approved"
    )

    flagged_results <- process_submissions_parallel(
      submission_ids = flagged_submissions,
      process_fn = function(id) {
        update_validation_status(
          submission_id = id,
          status = "validation_status_not_approved",
          asset_id = conf$ingestion$`kobo-adnap`$asset_id,
          token = conf$ingestion$`kobo-adnap`$token
        )
      },
      description = "flagged submissions",
      rate_limit = 0.1
    )
  } else {
    logger::log_info("No flagged submissions to process")
  }

  # STEP 5: Identify clean submissions that need approval
  clean_submissions <- validation_flags %>%
    dplyr::filter(is.na(.data$alert_flag)) %>%
    dplyr::pull(.data$submission_id) %>%
    unique()

  # OPTIMIZATION: Skip already-approved submissions
  clean_to_update <- clean_submissions %>%
    setdiff(
      current_kobo_status %>%
        dplyr::filter(
          .data$validation_status == "validation_status_approved"
        ) %>%
        dplyr::pull(.data$submission_id)
    )

  skipped_count <- length(clean_submissions) - length(clean_to_update)
  if (skipped_count > 0) {
    logger::log_info(
      "Skipping {skipped_count} already-approved clean submissions"
    )
  }

  # STEP 6: Update clean submissions (with rate limiting)
  if (length(clean_to_update) > 0) {
    logger::log_info(
      "Marking {length(clean_to_update)} clean submissions as approved"
    )

    clean_results <- process_submissions_parallel(
      submission_ids = clean_to_update,
      process_fn = function(id) {
        update_validation_status(
          submission_id = id,
          status = "validation_status_approved",
          asset_id = conf$ingestion$`kobo-adnap`$asset_id,
          token = conf$ingestion$`kobo-adnap`$token
        )
      },
      description = "clean submissions",
      rate_limit = 0.2 # Slightly slower for approvals
    )
  } else {
    logger::log_info("No clean submissions need approval updates")
  }

  # STEP 7: Fetch final status after updates
  logger::log_info("Fetching final validation status after updates")

  final_kobo_status <- process_submissions_parallel(
    submission_ids = all_submission_ids,
    process_fn = function(id) {
      get_validation_status(
        submission_id = id,
        asset_id = conf$ingestion$`kobo-adnap`$asset_id,
        token = conf$ingestion$`kobo-adnap`$token
      )
    },
    description = "final validation statuses",
    rate_limit = 0.1
  )

  # STEP 8: Combine with validation flags
  validation_flags_with_kobo_status <- validation_flags %>%
    dplyr::left_join(
      final_kobo_status,
      by = "submission_id",
      suffix = c("", "_kobo")
    )

  # STEP 9: Create long format for enumerator statistics
  validation_flags_long <- validation_flags_with_kobo_status |>
    dplyr::mutate(alert_flag = as.character(.data$alert_flag)) %>%
    tidyr::separate_rows("alert_flag", sep = ",\\s*") |>
    dplyr::select(-c(dplyr::starts_with("valid")))

  # STEP 10: Push to MongoDB
  asset_id <- conf$ingestion$`kobo-adnap`$asset_id

  mdb_collection_push(
    data = validation_flags_with_kobo_status,
    connection_string = conf$storage$mongodb$connection_strings$validation,
    db_name = conf$storage$mongodb$validation$database_name,
    collection_name = paste(
      conf$storage$mongodb$databases$validation$collections$flags,
      asset_id,
      sep = "-"
    )
  )

  mdb_collection_push(
    data = validation_flags_long,
    connection_string = conf$storage$mongodb$connection_strings$validation,
    db_name = conf$storage$mongodb$validation$database_name,
    collection_name = paste(
      conf$storage$mongodb$databases$validation$collections$enumerators_stats,
      asset_id,
      sep = "-"
    )
  )

  logger::log_info("Validation synchronization completed successfully")
  invisible(NULL)
}
