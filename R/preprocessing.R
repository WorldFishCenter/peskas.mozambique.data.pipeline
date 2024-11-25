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
      submission_date = lubridate::as_datetime(.data$submission_date)
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
      gear = "gear_type",
      "mesh_size",
      "hook_size",
      "hook_size_other"
    ) %>%
    dplyr::mutate(dplyr::across(c("trip_duration", "mesh_size", "hook_size", "hook_size_other", dplyr::ends_with("fishers")), as.numeric),
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
    dplyr::relocate("habitat", .after = "tracker_imei") %>%
    dplyr::relocate("vessel_type", .after = "habitat") %>%
    dplyr::select(-c("habitat_code", "vessel_code", "hook_size_other"))

  species_info <-
    raw_dat %>%
    process_species_group() %>%
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
    calculate_catch() %>%
    tidyr::nest(catch_df = -.data$submission_id)


  info_list <- list(general_info, trip_info, species_info)

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


#' Process Species Length and Catch Data using DuckDB
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
process_species_group_db <- function(data = NULL) {
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

#' Calculate Catch Weights Using Length-Weight Relationships
#'
#' @description
#' This function calculates catch weights using length-weight relationships from FishBase.
#' It processes catch data by mapping common names to scientific names, retrieving
#' length-weight coefficients, and applying the length-weight equation to estimate catches.
#'
#' @param catch_df A tibble containing catch data with columns:
#'   \itemize{
#'     \item submission_id: Unique identifier for the submission
#'     \item catch_taxon: Species or group name
#'     \item length_class: Length class in centimeters
#'     \item counts: Number of individuals
#'   }
#'
#' @details
#' The function performs several steps:
#' 1. Groups related species categories (e.g., combining sardines and pilchards)
#' 2. Maps common names to scientific names using FishBase
#' 3. Retrieves length-weight relationships for species in the area (C_Code = 508)
#' 4. Calculates median length-weight coefficients for each species group
#' 5. Estimates catch weights using the length-weight equation: W = a * L^b
#'
#' @return A tibble containing the original catch data plus:
#' \itemize{
#'   \item catch_gr: Estimated catch weight in grams
#' }
#'
#' @note
#' - Length-weight relationships are filtered for area code 508
#' - Median values of length-weight coefficients are used when multiple relationships exist
#' - Only relationships with no quality issues (EsQ is NA) are used
#'
#' @seealso
#' \code{\link{preprocess_landings}} for data preprocessing steps
#'
#' @examples
#' \dontrun{
#' catch_estimates <- calculate_catch(preprocessed_catch_data)
#' }
#'
#' @keywords preprocessing modeling
#' @export
calculate_catch <- function(catch_df = NULL) {
  group_mapping <- list(
    "Sardines/pilchards" = c("sardine", "pilchard"),
    "Jacks/Trevally/Other Scad" = c("jack", "trevally", "scad"),
    "Snapper/seaperch" = c("snapper", "seaperch"),
    "Tuna/Bonito/Other Mackerel" = c("tuna", "bonito", "mackerel"),
    "Javelin/Grunt" = c("javelin", "grunt"),
    "Mojarra/Silverbelly" = c("mojarra", "silverbelly")
  )

  # Get all unique terms
  decomposed_list <- unlist(group_mapping)

  # Create initial sci_names list
  sci_names <-
    stats::na.omit(c(unique(catch_df$catch_taxon), decomposed_list)) %>%
    purrr::set_names() %>%
    purrr::map(rfishbase::common_to_sci) %>%
    purrr::map(~ rfishbase::country(unique(.$Species), fields = c("Species", "SpecCode", "C_Code")))

  # Combine groups and remove individual components
  for (group_name in names(group_mapping)) {
    components <- group_mapping[[group_name]]
    sci_names[[group_name]] <- dplyr::bind_rows(sci_names[components])
    sci_names[components] <- NULL # Remove the individual components
  }


  lw_coeffs <-
    sci_names %>%
    dplyr::bind_rows(.id = "catch_taxon") %>%
    dplyr::filter(.data$C_Code == 508) %>%
    dplyr::select("catch_taxon", "Species") %>%
    dplyr::distinct() %>%
    {
      species_list <- unique(.$Species)
      dplyr::left_join(
        .,
        rfishbase::length_weight(species_list, fields = c("Species", "a", "b", "EsQ")),
        by = "Species",
        relationship = "many-to-many"
      )
    } %>%
    dplyr::filter(is.na(.data$EsQ)) %>%
    dplyr::select(-"EsQ") %>%
    dplyr::group_by(.data$catch_taxon) %>%
    dplyr::summarise(
      a = stats::median(.data$a, na.rm = TRUE),
      b = stats::median(.data$b, na.rm = TRUE)
    )

  catch_df %>%
    dplyr::left_join(lw_coeffs, by = "catch_taxon") %>%
    dplyr::mutate(catch_gr = dplyr::case_when(
      is.na(.data$collection_type) | .data$collection_type == "1" ~ (.data$a * .data$length_class^.data$b) * .data$counts,
      .data$collection_type == "2" ~ (.data$n_buckets * .data$weight_bucket) * 1000,
      TRUE ~ NA_real_
    )) %>%
    dplyr::select(-c("a", "b"))
}
