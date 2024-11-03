#' Preprocess Landings Data
#'
#' This function preprocesses raw landings data from a MongoDB collection.
#' It performs various data cleaning and transformation operations, including
#' column renaming, data pivoting, and standardization of catch names.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return A tibble containing the preprocessed landings data
#'
#' @details
#' The function performs the following main operations:
#' 1. Pulls raw data from the MongoDB collection
#' 2. Renames columns and selects relevant fields
#' 3. Generates unique survey IDs
#' 4. Cleans and standardizes text fields
#' 5. Pivots catch data from wide to long format
#' 6. Standardizes catch names and separates size information
#' 7. Converts data types and handles cases with no catch data
#' 7. Uploads the processed data to the preprocessed MongoDB collection.
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' preprocessed_data <- preprocess_landings()
#' }
#' @export
preprocess_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  # get raw landings from mongodb
  raw_dat <- mdb_collection_pull(
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$raw,
    db_name = conf$storage$mongodb$database$pipeline$name,
    connection_string = conf$storage$mongodb$connection_string
  ) |>
    dplyr::as_tibble() %>%
    dplyr::mutate_all(as.character)

  general_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) %>%
    dplyr::select("submission_id",
      "landing_date",
      "district",
      landing_site_palma = "group_station/district_palma",
      landing_site_mocimboa = "group_station/district_mocimboa",
      "survey_activity",
      "survey_activity_whynot",
      submission_date = "today",
      gps = "location_coordinates",
    ) %>%
    dplyr::mutate(landing_site = dplyr::coalesce(.data$landing_site_palma, .data$landing_site_mocimboa)) %>%
    tidyr::separate(.data$gps,
      into = c("lat", "lon", "drop1", "drop2"),
      sep = " "
    ) %>%
    dplyr::select(-c("drop1", "drop2", "landing_site_palma", "landing_site_mocimboa")) %>%
    dplyr::relocate("landing_site", .after = "district")


  trip_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) %>%
    dplyr::select(
      "submission_id",
      "has_boat",
      "boat_reg_no",
      "has_PDS",
      tracker_imei = "PDS_IMEI",
      "vessel_type",
      "propulsion_gear",
      "trip_duration",
      "habitat",
      male_fishers = "no_fishers/no_men_fishers",
      female_fishers = "no_fishers/no_women_fishers",
      child_fishers = "no_fishers/no_child_fishers",
      gear = "gear_type",
      "mesh_size",
      "hook_size",
      "hook_size_other"
    )

  species_info <-
    raw_dat %>%
    process_species_group() %>%
    dplyr::bind_rows() %>%
    tidyr::nest(catch_data = -.data$submission_id)

  logger::log_info("Uploading preprocessed data to mongodb")
  # upload preprocessed landings
  mdb_collection_push(
    data = preprocessed_landings,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$ongoing$preprocessed,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}


#' Process Species Length and Catch Data
#'
#' @description
#' Processes raw survey data to extract and organize species information, length measurements,
#' and catch estimates. Handles both empty and non-empty submissions, converting wide-format
#' survey data into a structured list of tibbles.
#'
#' @param data A data frame containing raw survey data with required columns:
#'   * submission_id: Unique identifier for each submission
#'   * group_species_*: Species information columns
#'   * Optional: no_individuals_*, fish_length_over*, catch_estimate columns
#'
#' @return A list of tibbles, one per submission_id, containing:
#'   * Basic information: submission_id, species data, catch estimates
#'   * Length data: When available, includes length classes and measurements
#'   * Empty submissions: Preserved with NA values
#'
#' @keywords preprocessing
#'
#' @examples
#' \dontrun{
#' results <- process_species_group(raw_dat)
#' submission_data <- results[["1234"]]
#' }
#'
#' @export
process_species_group <- function(data = NULL) {
  # ----------------------------
  # Step 0: Convert data to duckplyr data frame for efficient processing
  # ----------------------------
  df_duck <- duckplyr::as_duckplyr_df(data)

  # ----------------------------
  # Step 1: Identify fully empty submissions
  # ----------------------------
  group_species_data <- df_duck %>%
    dplyr::select("submission_id", dplyr::starts_with("group_species")) %>%
    dplyr::collect()

  empty_submissions <- group_species_data %>%
    dplyr::filter(dplyr::if_all(dplyr::starts_with("group_species"), is.na))

  empty_df <- empty_submissions %>%
    dplyr::select("submission_id") %>%
    dplyr::mutate(
      n = NA_character_,
      selected_species = NA_character_,
      length_class = NA_character_,
      value = NA_character_,
      collection_type = NA_character_,
      n_buckets = NA_character_,
      weight_bucket = NA_character_,
      catch_estimate = NA_character_
    )

  non_empty_data <- group_species_data %>%
    dplyr::filter(!.data$submission_id %in% empty_submissions$submission_id)

  if (nrow(non_empty_data) == 0) {
    return(split(empty_df, as.character(empty_df$submission_id)))
  }

  # ----------------------------
  # Step 2: Process and reshape the data for non-empty submissions
  # ----------------------------
  processed_data <- non_empty_data %>%
    dplyr::mutate(dplyr::across(dplyr::everything(), as.character)) %>%
    tidyr::pivot_longer(
      cols = -"submission_id",
      names_to = c("dummy", "n", "var"),
      names_sep = "/"
    ) %>%
    dplyr::mutate(
      var = ifelse(is.na(.data$var), .data$n, .data$var),
      n = stringr::str_extract(.data$dummy, "\\d+")
    ) %>%
    tidyr::pivot_wider(names_from = "var", values_from = "value") %>%
    dplyr::mutate(selected_species = dplyr::coalesce(.data$group_species_001, .data$selected_species)) %>%
    dplyr::filter(!is.na(.data$selected_species)) %>%
    dplyr::select(-"group_species_001", -"dummy")

  # ----------------------------
  # Step 3: Process length class data for non-empty submissions
  # ----------------------------
  no_individuals_cols <- grep("^no_individuals", names(processed_data), value = TRUE)
  over_length_cols <- grep("^fish_length_over", names(processed_data), value = TRUE)

  length_data <- processed_data %>%
    dplyr::select(
      "submission_id", "n", "selected_species",
      tidyselect::all_of(c(no_individuals_cols, over_length_cols))
    ) %>%
    tidyr::pivot_longer(
      cols = tidyselect::all_of(no_individuals_cols),
      names_to = "length_class",
      values_to = "value"
    ) %>%
    dplyr::filter(!is.na(.data$value)) %>%
    dplyr::mutate(
      length_class = stringr::str_replace(.data$length_class, "^no_individuals_", ""),
      length_class = tolower(stringr::str_trim(.data$length_class)),
      length_class = dplyr::case_when(!is.na(.data$fish_length_over60) & length_class == "over60" ~
        .data$fish_length_over60, TRUE ~ .data$length_class),
      length_class = dplyr::case_when(!is.na(.data$fish_length_over35) & length_class == "over35" ~
        .data$fish_length_over35, TRUE ~ .data$length_class)
    ) %>%
    dplyr::select(-tidyselect::all_of(over_length_cols))

  # ----------------------------
  # Step 4: Get bucket-related data and other variables for non-empty submissions
  # ----------------------------
  buckets_data <- processed_data %>%
    dplyr::select(
      "submission_id", "n", "collection_type", "selected_species",
      "n_buckets", "weight_bucket", "catch_estimate"
    )

  # ----------------------------
  # Step 5 & 6: Merge all data
  # ----------------------------
  final_output <- dplyr::full_join(
    length_data,
    buckets_data,
    by = c("submission_id", "n", "selected_species")
  ) %>%
    dplyr::bind_rows(empty_df)

  return(split(final_output, as.character(final_output$submission_id)))
}
