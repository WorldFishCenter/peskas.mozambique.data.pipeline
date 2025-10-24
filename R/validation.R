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
      "catch_taxon",
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

  # get validation table and store approved ids from both assets
  submission_ids <- unique(preprocessed_surveys$submission_id)

  # Query validation status from both survey versions using same credentials
  logger::log_info(
    "Querying validation status from both wf_surveys_v1 and wf_surveys_v2 assets"
  )

  # Query wf_surveys v2 (new asset)
  validation_results <- submission_ids %>%
    furrr::future_map_dfr(
      get_validation_status,
      asset_id = conf$ingestion$`kobo-adnap`$asset_id,
      token = conf$ingestion$`kobo-lurio`$token,
      .options = furrr::furrr_options(seed = TRUE)
    )

  # Combine validation results and extract approved IDs
  approved_ids <- validation_results %>%
    dplyr::bind_rows() %>%
    dplyr::filter(.data$validation_status == "validation_status_approved") %>%
    dplyr::pull(.data$submission_id) %>%
    unique()

  logger::log_info(
    "Found {length(approved_ids)} approved submissions across both assets"
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

  validated_data <-
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
    validated_data |>
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
      validated_data |>
        dplyr::select("submission_id", "submitted_by") |>
        dplyr::distinct(),
      by = "submission_id"
    ) |>
    dplyr::relocate("submitted_by", .after = "submission_id") |>
    dplyr::distinct() |>
    # keep approved submissions untouched
    dplyr::mutate(
      alert_flag = dplyr::if_else(
        .data$submission_id %in% approved_ids,
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
    data = validated_data,
    prefix = conf$ingestion$`kobo-adnap`$validated_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  invisible(NULL)
}
