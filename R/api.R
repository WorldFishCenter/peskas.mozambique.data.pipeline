#' Export Raw API-Ready Trip Data
#'
#' @description
#' Processes WorldFish preprocessed survey data into a simplified API-friendly format
#' and exports it to cloud storage for external consumption. This function exports the
#' **raw/preprocessed** version of trip data without validation filters. For validated
#' API exports, see the companion function that will process validated surveys.
#'
#' @details
#' The function performs the following operations:
#' - Downloads **preprocessed** (not validated) WF survey data from cloud storage
#' - Generates unique trip IDs using xxhash64 algorithm
#' - Transforms nested survey structure to flat API format
#' - Joins with standardized lookup tables (districts, gear types, vessel types)
#' - Exports to the **raw** cloud storage path (before validation)
#'
#' **Data Pipeline Context**:
#' This function exports raw preprocessed data and is part of a two-stage API export pipeline:
#' 1. `export_api_raw()` - Exports raw/preprocessed data (this function)
#' 2. (Future) `export_api_validated()` - Will export quality-controlled validated data
#'
#' **Output Schema**:
#' The exported dataset includes the following fields:
#' - `trip_id`: Unique identifier (TRIP_xxxxxxxxxxxx format)
#' - `landing_date`: Date of landing
#' - `gaul_2_name`: Standardized district name (GAUL level 2)
#' - `n_fishers`: Total number of fishers (men + women + children)
#' - `trip_duration_hrs`: Duration in hours
#' - `gear`: Standardized gear type
#' - `vessel_type`: Standardized vessel type
#' - `catch_habitat`: Habitat where catch occurred
#' - `catch_outcome`: Outcome of catch (landed, sold, etc.)
#' - `n_catch`: Number of individual catch items
#' - `catch_taxon`: Species or taxonomic group
#' - `length_cm`: Length measurement in centimeters
#' - `catch_kg`: Weight in kilograms
#' - `catch_price`: Price in local currency
#'
#' **Cloud Storage Location**:
#' Files are uploaded to the path specified in `conf$api$trips$raw$cloud_path`
#' (e.g., `mozambique/raw/`) with versioned filenames following the pattern:
#' `{file_prefix}__{timestamp}_{git_sha}__.parquet`
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO).
#'   See `logger::log_levels` for available options. Default is logger::DEBUG.
#'
#' @return NULL (invisible). The function uploads data to cloud storage as a side effect.
#'
#' @examples
#' \dontrun{
#' # Export raw API trip data with default debug logging
#' export_api_raw()
#'
#' # Export with info-level logging only
#' export_api_raw(logger::INFO)
#' }
#'
#'
#' @keywords workflow export
#' @export
export_api_raw <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Downloading preprocessed survey data...")
  preprocessed_surveys <- download_parquet_from_cloud(
    prefix = conf$ingestion$`kobo-adnap`$preprocessed_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Transforming surveys to API format...")
  api_raw <- preprocessed_surveys |>
    dplyr::rowwise() |>
    dplyr::mutate(
      trip_id = paste0(
        "TRIP_",
        .data$submission_id
        #substr(digest::digest(.data$submission_id, algo = "xxhash64"), 1, 12)
      ),
      survey_id = conf$ingestion$`kobo-adnap`$asset_id
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      n_catch = as.integer(.data$n_catch),
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "survey_id",
      "trip_id",
      "landing_date",
      "gaul_1_code",
      "gaul_1_name",
      "gaul_2_code",
      "gaul_2_name",
      "n_fishers",
      trip_duration_hrs = "trip_duration",
      "gear",
      "vessel_type",
      catch_habitat = "habitat",
      "catch_outcome",
      "n_catch",
      catch_taxon = "alpha3_code",
      length_cm = "length",
      "catch_kg",
      tot_catch_price = "catch_price"
    ) |>
    dplyr::group_by(.data$trip_id) |>
    dplyr::mutate(
      catch_price = NA_real_,
      tot_catch_kg = sum(catch_kg),
    ) |>
    dplyr::ungroup() |>
    dplyr::relocate(
      c("catch_price", "tot_catch_kg", "tot_catch_price"),
      .after = "catch_price"
    ) |>
    dplyr::distinct()

  logger::log_info(
    "Processed {nrow(api_raw)} records from {length(unique(api_raw$trip_id))} unique trips"
  )

  # Write locally with just the filename
  filename <- conf$api$trips$raw$file_prefix |>
    add_version(extension = "parquet")

  logger::log_info("Writing parquet file locally: {filename}")
  arrow::write_parquet(
    api_raw,
    sink = filename,
    compression = "lz4",
    compression_level = 12
  )
  # Upload to cloud with the full path
  cloud_path <- file.path(
    conf$api$trips$raw$cloud_path,
    filename
  )

  logger::log_info("Uploading to cloud storage: {cloud_path}")
  upload_cloud_file(
    file = filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_api,
    name = cloud_path
  )

  # Clean up local file
  file.remove(filename)
  logger::log_success("API trip data export completed successfully")

  invisible(NULL)
}

#' Export Validated API-Ready Trip Data
#'
#' @description
#' Processes WorldFish preprocessed survey data into a simplified API-friendly format
#' and exports it to cloud storage for external consumption. This function exports the
#' **validated** version of trip data.
#'
#' @details
#' The function performs the following operations:
#' - Downloads **validated** WF survey data from cloud storage
#' - Generates unique trip IDs using xxhash64 algorithm
#' - Transforms nested survey structure to flat API format
#' - Joins with standardized lookup tables (districts, gear types, vessel types)
#' - Exports to the **validated** cloud storage path (before validation)
#'
#' **Data Pipeline Context**:
#' This function exports raw preprocessed data and is part of a two-stage API export pipeline:
#' 1. `raw()` - Exports raw/preprocessed data
#' 1. `export_api_validated()` - Exports validated data (this function)
#'
#' **Output Schema**:
#' The exported dataset includes the following fields:
#' - `trip_id`: Unique identifier (TRIP_xxxxxxxxxxxx format)
#' - `landing_date`: Date of landing
#' - `gaul_2_name`: Standardized district name (GAUL level 2)
#' - `n_fishers`: Total number of fishers (men + women + children)
#' - `trip_duration_hrs`: Duration in hours
#' - `gear`: Standardized gear type
#' - `vessel_type`: Standardized vessel type
#' - `catch_habitat`: Habitat where catch occurred
#' - `catch_outcome`: Outcome of catch (landed, sold, etc.)
#' - `n_catch`: Number of individual catch items
#' - `catch_taxon`: Species or taxonomic group
#' - `length_cm`: Length measurement in centimeters
#' - `catch_kg`: Weight in kilograms
#' - `catch_price`: Price in local currency
#'
#' **Cloud Storage Location**:
#' Files are uploaded to the path specified in `conf$api$trips$validated$cloud_path`
#' (e.g., `mozambique/validated/`) with versioned filenames following the pattern:
#' `{file_prefix}__{timestamp}_{git_sha}__.parquet`
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO).
#'   See `logger::log_levels` for available options. Default is logger::DEBUG.
#'
#' @return NULL (invisible). The function uploads data to cloud storage as a side effect.
#'
#' @examples
#' \dontrun{
#' # Export raw API trip data with default debug logging
#' export_api_validated()
#'
#' # Export with info-level logging only
#' export_api_validated(logger::INFO)
#' }
#'
#'
#' @keywords workflow export
#' @export
export_api_validated <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Downloading preprocessed survey data...")
  validated_surveys <- download_parquet_from_cloud(
    prefix = conf$ingestion$`kobo-adnap`$validated_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  logger::log_info("Transforming surveys to API format...")
  api_validated <- validated_surveys |>
    dplyr::rowwise() |>
    dplyr::mutate(
      trip_id = paste0(
        "TRIP_",
        .data$submission_id
        #substr(digest::digest(.data$submission_id, algo = "xxhash64"), 1, 12)
      ),
      survey_id = conf$ingestion$`kobo-adnap`$asset_id
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      n_catch = as.integer(.data$n_catch),
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "survey_id",
      "trip_id",
      "landing_date",
      "gaul_1_code",
      "gaul_1_name",
      "gaul_2_code",
      "gaul_2_name",
      "n_fishers",
      trip_duration_hrs = "trip_duration",
      "gear",
      "vessel_type",
      catch_habitat = "habitat",
      "catch_outcome",
      "n_catch",
      "catch_taxon",
      length_cm = "length",
      "catch_kg",
      tot_catch_price = "catch_price"
    ) |>
    dplyr::group_by(.data$trip_id) |>
    dplyr::mutate(
      catch_price = NA_real_,
      tot_catch_kg = sum(catch_kg),
    ) |>
    dplyr::ungroup() |>
    dplyr::relocate(
      c("catch_price", "tot_catch_kg", "tot_catch_price"),
      .after = "catch_price"
    ) |>
    dplyr::distinct()

  logger::log_info(
    "Processed {nrow(api_validated)} records from {length(unique(api_validated$trip_id))} unique trips"
  )

  # Write locally with just the filename
  filename <- conf$api$trips$validated$file_prefix |>
    add_version(extension = "parquet")

  logger::log_info("Writing parquet file locally: {filename}")
  arrow::write_parquet(
    api_validated,
    sink = filename,
    compression = "lz4",
    compression_level = 12
  )
  # Upload to cloud with the full path
  cloud_path <- file.path(
    conf$api$trips$validated$cloud_path,
    filename
  )

  logger::log_info("Uploading to cloud storage: {cloud_path}")
  upload_cloud_file(
    file = filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_api,
    name = cloud_path
  )

  # Clean up local file
  file.remove(filename)
  logger::log_success("API trip data export completed successfully")

  invisible(NULL)
}
