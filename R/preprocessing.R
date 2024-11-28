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

  metadata <- get_metadata()

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
      "catch_outcome",
      submission_date = "today",
      gps = "location_coordinates",
    ) %>%
    dplyr::mutate(landing_code = dplyr::coalesce(.data$landing_site_palma, .data$landing_site_mocimboa)) %>%
    tidyr::separate(.data$gps,
      into = c("lat", "lon", "drop1", "drop2"),
      sep = " "
    ) %>%
    dplyr::relocate("landing_code", .after = "district") %>%
    dplyr::mutate(dplyr::across(c("lat", "lon"), as.numeric),
      landing_date = lubridate::as_datetime(.data$landing_date),
      submission_date = lubridate::as_datetime(.data$submission_date),
      district = stringr::str_to_title(.data$district)
    ) %>%
    dplyr::left_join(metadata$landing_site, by = c("district", "landing_code")) %>%
    dplyr::relocate("submission_date", .after = "landing_date") %>%
    dplyr::relocate("landing_site", .after = "district") %>%
    dplyr::select(-c("drop1", "drop2", "landing_code", "landing_site_palma", "landing_site_mocimboa"))



  trip_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) %>%
    dplyr::select(
      "submission_id",
      "has_boat",
      "boat_reg_no",
      "has_PDS",
      tracker_imei = "PDS_IMEI",
      vessel_code = "vessel_type",
      "propulsion_gear",
      "trip_duration",
      habitat_code = "habitat",
      male_fishers = "no_fishers/no_men_fishers",
      female_fishers = "no_fishers/no_women_fishers",
      child_fishers = "no_fishers/no_child_fishers",
      gear_code = "gear_type",
      "mesh_size",
      "hook_size",
      "hook_size_other"
    ) %>%
    dplyr::mutate(dplyr::across(dplyr::ends_with("fishers"), ~ stringr::str_remove(.x, "_")), # Remove the suffix but need to be fixed
      dplyr::across(c("trip_duration", "mesh_size", "hook_size", "hook_size_other", dplyr::ends_with("fishers")), as.numeric),
      tot_fishers = rowSums(dplyr::across(dplyr::ends_with("fishers")), na.rm = TRUE),
      propulsion_gear = dplyr::case_when(
        .data$propulsion_gear == "1" ~ "Motor",
        .data$propulsion_gear == "2" ~ "Vela",
        .data$propulsion_gear == "3" ~ "Remo",
        TRUE ~ NA_character_
      ),
      hook_size = dplyr::coalesce(.data$hook_size, .data$hook_size_other)
    ) %>%
    dplyr::left_join(metadata$habitat, by = c("habitat_code")) %>%
    dplyr::left_join(metadata$vessel_type, by = c("vessel_code")) %>%
    dplyr::left_join(metadata$gear_type, by = c("gear_code")) %>%
    dplyr::relocate("habitat", .after = "tracker_imei") %>%
    dplyr::relocate("vessel_type", .after = "habitat") %>%
    dplyr::relocate("gear", .after = "vessel_type") %>%
    dplyr::select(-c("habitat_code", "vessel_code", "gear_code", "hook_size_other", "male_fishers", "female_fishers", "child_fishers"))

  species_info <-
    raw_dat %>%
    process_species_group() %>% # Use the duckdb version for faster processing
    dplyr::bind_rows() %>%
    dplyr::mutate(length_class = dplyr::case_when(
      length_class == "5_10" ~ 7.5,
      length_class == "10_15" ~ 12.5,
      length_class == "15_20" ~ 17.5,
      length_class == "20_25" ~ 22.5,
      length_class == "25_30" ~ 27.5,
      length_class == "30_35" ~ 32.5,
      length_class == "35_40" ~ 37.5,
      length_class == "40_45" ~ 42.5,
      length_class == "45_50" ~ 47.5,
      length_class == "50_55" ~ 52.5,
      length_class == "55_60" ~ 57.5,
      TRUE ~ as.numeric(length_class)
    )) %>%
    dplyr::mutate(dplyr::across(c("value", "n_buckets", "weight_bucket", "catch_estimate"), as.numeric)) %>%
    dplyr::rename(
      catch_number = "selected_species",
      counts = "value"
    ) %>%
    dplyr::left_join(metadata$catch_groups, by = c("catch_number"), relationship = "many-to-many") %>%
    dplyr::select("submission_id",
      "n",
      catch_taxon = "catch_name_en",
      "length_class":"catch_estimate"
    ) %>%
    dplyr::left_join(peskas.mozambique.data.pipeline::lw_coeffs, by = "catch_taxon") %>%
    # calculate_catch() %>% # causing bugs ruuning remotely on github actions due rfishbase dependency issues
    tidyr::nest(catch_df = -.data$submission_id)

  market_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_conservation_trading/")) %>%
    dplyr::select(
      "submission_id",
      "catch_use",
      "catch_price",
      "total_catch_value"
    ) %>%
    dplyr::mutate(catch_price = dplyr::coalesce(.data$catch_price, .data$total_catch_value)) %>%
    dplyr::select(-"total_catch_value") %>%
    dplyr::mutate(catch_price = as.numeric(.data$catch_price))


  info_list <- list(general_info, trip_info, species_info, market_info)

  preprocessed_landings <- purrr::reduce(info_list, dplyr::full_join, by = "submission_id")

  logger::log_info("Uploading preprocessed data to mongodb")
  # upload preprocessed landings
  mdb_collection_push(
    data = preprocessed_landings,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$preprocessed,
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
  # Step 1: Identify fully empty submissions
  # ----------------------------
  group_species_cols <- grep("^group_species", names(data), value = TRUE)
  group_species_data <- data[, c("submission_id", group_species_cols)]

  # Find empty submissions (all group_species columns are NA)
  empty_submissions <- group_species_data[apply(group_species_data[, group_species_cols], 1, function(x) all(is.na(x))), ]

  # Create empty dataframe template
  empty_df <- data.frame(
    submission_id = empty_submissions$submission_id,
    n = NA_character_,
    selected_species = NA_character_,
    length_class = NA_character_,
    value = NA_character_,
    collection_type = NA_character_,
    n_buckets = NA_character_,
    weight_bucket = NA_character_,
    catch_estimate = NA_character_,
    stringsAsFactors = FALSE
  )

  # Filter out empty submissions from main data
  non_empty_data <- group_species_data[!group_species_data$submission_id %in% empty_submissions$submission_id, ]

  if (nrow(non_empty_data) == 0) {
    return(split(empty_df, as.character(empty_df$submission_id)))
  }

  # ----------------------------
  # Step 2: Process and reshape the data for non-empty submissions
  # ----------------------------
  # Convert everything to character
  non_empty_data[] <- lapply(non_empty_data, as.character)

  # Reshape from wide to long
  processed_data <- tidyr::pivot_longer(
    non_empty_data,
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
      length_class = dplyr::case_when(
        !is.na(.data$fish_length_over60) & .data$length_class == "over60" ~ .data$fish_length_over60,
        !is.na(.data$fish_length_over35) & .data$length_class == "over35" ~ .data$fish_length_over35,
        TRUE ~ .data$length_class
      )
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

  # Split by submission_id and return
  return(split(final_output, as.character(final_output$submission_id)))
}

