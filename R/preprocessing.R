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

  asfis <- download_parquet_from_cloud(
    prefix = "asfis",
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  metadata <- get_metadata()

  # get raw landings from cloud storage
  raw_dat <- download_parquet_from_cloud(
    prefix = conf$ingestion$`kobo-v1`$raw_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) %>%
    dplyr::mutate_all(as.character)

  general_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) %>%
    dplyr::select(
      "submission_id",
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
    dplyr::mutate(
      landing_code = dplyr::coalesce(
        .data$landing_site_palma,
        .data$landing_site_mocimboa
      )
    ) %>%
    tidyr::separate(
      .data$gps,
      into = c("lat", "lon", "drop1", "drop2"),
      sep = " "
    ) %>%
    dplyr::relocate("landing_code", .after = "district") %>%
    dplyr::mutate(
      dplyr::across(c("lat", "lon"), as.numeric),
      landing_date = lubridate::as_datetime(.data$landing_date),
      submission_date = lubridate::as_datetime(.data$submission_date),
      district = stringr::str_to_title(.data$district)
    ) %>%
    dplyr::left_join(
      metadata$landing_site,
      by = c("district", "landing_code")
    ) %>%
    dplyr::relocate("submission_date", .after = "landing_date") %>%
    dplyr::relocate("landing_site", .after = "district") %>%
    dplyr::select(
      -c(
        "drop1",
        "drop2",
        "landing_code",
        "landing_site_palma",
        "landing_site_mocimboa"
      )
    )

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
    dplyr::mutate(
      dplyr::across(
        dplyr::ends_with("fishers"),
        ~ stringr::str_remove(.x, "_")
      ), # Remove the suffix but need to be fixed
      dplyr::across(
        c(
          "trip_duration",
          "mesh_size",
          "hook_size",
          "hook_size_other",
          dplyr::ends_with("fishers")
        ),
        as.numeric
      ),
      tot_fishers = rowSums(
        dplyr::across(dplyr::ends_with("fishers")),
        na.rm = TRUE
      ),
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
    dplyr::select(
      -c(
        "habitat_code",
        "vessel_code",
        "gear_code",
        "hook_size_other",
        "male_fishers",
        "female_fishers",
        "child_fishers"
      )
    )

  catch_info <-
    raw_dat %>%
    process_species_group() %>% # Use the duckdb version for faster processing
    dplyr::bind_rows() %>%
    dplyr::mutate(
      length_class = dplyr::case_when(
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
      )
    ) %>%
    dplyr::mutate(dplyr::across(
      c("value", "n_buckets", "weight_bucket", "catch_estimate"),
      as.numeric
    )) %>%
    dplyr::rename(
      catch_number = "selected_species",
      counts = "value"
    ) %>%
    dplyr::left_join(
      metadata$catch_groups,
      by = c("catch_number"),
      relationship = "many-to-many"
    ) %>%
    dplyr::select(
      "submission_id",
      "n",
      catch_taxon = "interagency_code",
      "length_class":"catch_estimate"
    ) %>%
    dplyr::select(
      "submission_id",
      n_catch = "n",
      count_method = "collection_type",
      "catch_taxon",
      "catch_estimate",
      "n_buckets",
      "weight_bucket",
      individuals = "counts",
      length = "length_class"
    ) |> # replace CLP with ANX and SKH to Carcharhiniformes as more pertinent
    dplyr::mutate(
      catch_taxon = dplyr::case_when(
        .data$catch_taxon == "TUN" ~ "TUS",
        .data$catch_taxon == "SKH" ~ "CVX",
        .data$catch_taxon == "CLP" ~ "ANX",
        TRUE ~ .data$catch_taxon
      )
    )

  # Try to get length-weight coefficients from Rfishbase
  lwcoeffs <- getLWCoeffs(
    taxa_list = unique(catch_info$catch_taxon),
    asfis_list = asfis
  )

  # add flyng fish estimates
  fly_lwcoeffs <- dplyr::tibble(
    catch_taxon = "FLY",
    n = 0,
    a_6 = 0.00631,
    b_6 = 3.05
  )
  lwcoeffs$lw <- dplyr::bind_rows(lwcoeffs$lw, fly_lwcoeffs)

  catch_df <-
    calculate_catch(catch_data = catch_info, lwcoeffs = lwcoeffs$lw) |>
    dplyr::left_join(lwcoeffs$ml, by = "catch_taxon") |>
    dplyr::select(-"max_weightkg_75")

  market_info <-
    raw_dat %>%
    dplyr::rename_with(
      ~ stringr::str_remove(., "group_conservation_trading/")
    ) %>%
    dplyr::select(
      "submission_id",
      "catch_use",
      "catch_price",
      "total_catch_value"
    ) %>%
    dplyr::mutate(
      catch_price = dplyr::coalesce(.data$catch_price, .data$total_catch_value)
    ) %>%
    dplyr::select(-"total_catch_value") %>%
    dplyr::mutate(catch_price = as.numeric(.data$catch_price))

  info_list <- list(general_info, trip_info, catch_df, market_info)

  preprocessed_landings <-
    purrr::reduce(
      info_list,
      dplyr::full_join,
      by = "submission_id"
    ) |>
    dplyr::filter(.data$survey_activity == "1") |>
    dplyr::select(
      -c("survey_activity", "survey_activity_whynot", "tracker_imei")
    )

  logger::log_info("Uploading preprocessed data to cloud storage")
  # upload preprocessed landings
  upload_parquet_to_cloud(
    data = preprocessed_landings,
    prefix = conf$ingestion$`kobo-v1`$preprocessed_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
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
  empty_submissions <- group_species_data[
    apply(group_species_data[, group_species_cols], 1, function(x) {
      all(is.na(x))
    }),
  ]

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
  non_empty_data <- group_species_data[
    !group_species_data$submission_id %in% empty_submissions$submission_id,
  ]

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
    dplyr::mutate(
      selected_species = dplyr::coalesce(
        .data$group_species_001,
        .data$selected_species
      )
    ) %>%
    dplyr::filter(!is.na(.data$selected_species)) %>%
    dplyr::select(-"group_species_001", -"dummy")

  # ----------------------------
  # Step 3: Process length class data for non-empty submissions
  # ----------------------------
  no_individuals_cols <- grep(
    "^no_individuals",
    names(processed_data),
    value = TRUE
  )
  over_length_cols <- grep(
    "^fish_length_over",
    names(processed_data),
    value = TRUE
  )

  length_data <- processed_data %>%
    dplyr::select(
      "submission_id",
      "n",
      "selected_species",
      tidyselect::all_of(c(no_individuals_cols, over_length_cols))
    ) %>%
    tidyr::pivot_longer(
      cols = tidyselect::all_of(no_individuals_cols),
      names_to = "length_class",
      values_to = "value"
    ) %>%
    dplyr::filter(!is.na(.data$value), !.data$value == "0") %>%
    dplyr::mutate(
      length_class = stringr::str_replace(
        .data$length_class,
        "^no_individuals_",
        ""
      ),
      length_class = tolower(stringr::str_trim(.data$length_class)),
      length_class = dplyr::case_when(
        !is.na(.data$fish_length_over60) & .data$length_class == "over60" ~
          .data$fish_length_over60,
        !is.na(.data$fish_length_over35) & .data$length_class == "over35" ~
          .data$fish_length_over35,
        TRUE ~ .data$length_class
      )
    ) %>%
    dplyr::select(-tidyselect::all_of(over_length_cols)) |>
    dplyr::filter(!.data$length_class == "0")

  # ----------------------------
  # Step 4: Get bucket-related data and other variables for non-empty submissions
  # ----------------------------
  buckets_data <- processed_data %>%
    dplyr::select(
      "submission_id",
      "n",
      "collection_type",
      "selected_species",
      "n_buckets",
      "weight_bucket",
      "catch_estimate"
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

#' Calculate Catch Weight from Length-Weight Relationships or Bucket Measurements
#'
#' @description
#' Calculates total catch weight using either length-weight relationships or bucket measurements.
#' The function prioritizes length-based calculations when available, falling back to bucket-based
#' measurements when length data is missing. For Octopus (OCZ), the function converts total length (TL)
#' to mantle length (ML) by dividing TL by 5.5 before applying the length-weight formula.
#' This accounts for species-specific differences in body morphology.
#'
#' @param catch_data A data frame containing catch information with columns:
#'   \itemize{
#'     \item submission_id - Unique identifier for the catch
#'     \item n_catch - Number of catch events
#'     \item catch_taxon - FAO 3-alpha code
#'     \item individuals - Number of individuals (for length-based calculations)
#'     \item length - Length measurement in cm
#'     \item n_buckets - Number of buckets
#'     \item weight_bucket - Weight per bucket in kg
#'   }
#' @param lwcoeffs A data frame containing length-weight coefficients with columns:
#'   \itemize{
#'     \item catch_taxon - FAO 3-alpha code
#'     \item a_6 - 60th percentile of parameter 'a'
#'     \item b_6 - 60th percentile of parameter 'b'
#'   }
#'
#' @return A tibble with the following columns:
#'   \itemize{
#'     \item submission_id - Unique identifier for the catch
#'     \item n_catch - Number of catch events
#'     \item catch_taxon - FAO 3-alpha code
#'     \item individuals - Number of individuals
#'     \item length - Length measurement in cm
#'     \item n_buckets - Number of buckets
#'     \item weight_bucket - Weight per bucket in kg
#'     \item catch_kg - Total catch weight in kg
#'   }
#'
#' @details
#' The function calculates catch weight using two methods:
#' 1. Length-based calculation: W = a * L^b * N / 1000
#'    Where:
#'    - W is total weight in kg
#'    - a and b are length-weight relationship coefficients (75th percentile)
#'    - L is length in cm
#'    - N is number of individuals
#'
#' 2. Bucket-based calculation: W = n_buckets * weight_bucket
#'    Where:
#'    - W is total weight in kg
#'    - n_buckets is number of buckets
#'    - weight_bucket is weight per bucket in kg
#'
#' The final catch_kg uses length-based calculation when available,
#' falling back to bucket-based calculation when length data is missing.
#'
#' @examples
#' \dontrun{
#' # Calculate catch weights
#' catch_weights <- calculate_catch(
#'   catch_data = catch_data,
#'   lwcoeffs = length_weight_coeffs
#' )
#' }
#'
#' @note
#' - Length-based calculations use 75th percentile of length-weight coefficients
#' - All weights are returned in kilograms
#' - NA values are returned when neither calculation method is possible
#'
#' @keywords mining preprocessing
#' @export
calculate_catch <- function(catch_data = NULL, lwcoeffs = NULL) {
  catch_data |>
    dplyr::left_join(lwcoeffs, by = "catch_taxon") |>
    dplyr::mutate(
      # Calculate weight in grams for records with length measurements
      catch_length_gr = dplyr::case_when(
        # Specific case for Octopus cyanea (OCZ) - using length conversion
        !is.na(.data$length) &
          !is.na(.data$a_6) &
          !is.na(.data$b_6) &
          .data$catch_taxon == "OCZ" ~
          .data$a_6 * ((.data$length / 5.5)^.data$b_6),
        # General case for other species - direct calculation
        !is.na(.data$length) & !is.na(.data$a_6) & !is.na(.data$b_6) ~
          .data$a_6 * (.data$length^.data$b_6),
        # Otherwise NA
        TRUE ~ NA_real_
      ),
      # Convert to kilograms
      catch_length_kg = (.data$catch_length_gr * .data$individuals) / 1000,
      # Calculate weight from bucket information if available
      catch_bucket_kg = dplyr::case_when(
        !is.na(.data$n_buckets) & !is.na(.data$weight_bucket) ~
          .data$n_buckets * .data$weight_bucket,
        # Otherwise NA
        TRUE ~ NA_real_
      )
    ) |>
    dplyr::mutate(
      catch_kg = dplyr::coalesce(.data$catch_length_kg, .data$catch_bucket_kg)
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "catch_taxon",
      "individuals",
      "length",
      "n_buckets",
      "weight_bucket",
      "catch_kg",
      "catch_estimate"
    )
}


#' Calculate Fishery Metrics
#'
#' Transforms catch-level data into normalized fishery performance indicators.
#' Calculates site-level, gear-specific, and species-specific metrics.
#'
#' @param data A data frame with catch records containing required columns:
#'   submission_id, landing_date, district, gear, catch_outcome, no_men_fishers,
#'   no_women_fishers, no_child_fishers, catch_taxon, catch_price, catch_kg
#'
#' @return A data frame in normalized long format with columns: landing_site,
#'   year_month, metric_type, metric_value, gear_type, species, rank
#'
#' @keywords preprocessing
#' @export
calculate_fishery_metrics <- function(data = NULL) {
  catch_data <- data |>
    dplyr::filter(.data$catch_outcome == "1") |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "gear",
      "trip_duration",
      n_fishers = "tot_fishers",
      "catch_taxon",
      "catch_price",
      "catch_kg"
    ) |>
    dplyr::mutate(
      year_month = lubridate::floor_date(.data$landing_date, "month")
    ) |>
    dplyr::rename(
      landing_site = "district",
      species = "catch_taxon"
    )

  trip_level_data <- catch_data |>
    dplyr::group_by(
      .data$submission_id,
      .data$landing_date,
      .data$landing_site,
      .data$gear,
      .data$n_fishers,
      .data$year_month
    ) |>
    dplyr::summarise(
      trip_total_catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      trip_total_revenue = sum(.data$catch_price, na.rm = TRUE),
      .groups = "drop"
    )

  site_level_metrics <- trip_level_data |>
    dplyr::group_by(.data$landing_site, .data$year_month) |>
    dplyr::summarise(
      avg_fishers_per_trip = mean(.data$n_fishers, na.rm = TRUE),
      avg_catch_per_trip = mean(.data$trip_total_catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_longer(
      cols = c("avg_fishers_per_trip", "avg_catch_per_trip"),
      names_to = "metric_type",
      values_to = "metric_value"
    ) |>
    dplyr::mutate(
      gear_type = NA_character_,
      species = NA_character_,
      rank = NA_integer_
    )

  gear_metrics <- trip_level_data |>
    dplyr::group_by(.data$landing_site, .data$year_month) |>
    dplyr::mutate(total_trips = dplyr::n()) |>
    dplyr::add_count(.data$gear, name = "gear_count") |>
    dplyr::slice_max(.data$gear_count, n = 1, with_ties = FALSE) |>
    dplyr::distinct(.data$landing_site, .data$year_month, .keep_all = TRUE) |>
    dplyr::mutate(
      pct_main_gear = (.data$gear_count / .data$total_trips) * 100
    ) |>
    dplyr::select(
      "landing_site",
      "year_month",
      "gear",
      "pct_main_gear"
    ) |>
    dplyr::ungroup()

  predominant_gear_metrics <- gear_metrics |>
    dplyr::transmute(
      landing_site = .data$landing_site,
      year_month = .data$year_month,
      metric_type = "predominant_gear",
      metric_value = NA_real_,
      gear_type = .data$gear,
      species = NA_character_,
      rank = NA_integer_
    )

  pct_main_gear_metrics <- gear_metrics |>
    dplyr::transmute(
      landing_site = .data$landing_site,
      year_month = .data$year_month,
      metric_type = "pct_main_gear",
      metric_value = .data$pct_main_gear,
      gear_type = NA_character_,
      species = NA_character_,
      rank = NA_integer_
    )

  cpue_metrics <- trip_level_data |>
    dplyr::mutate(cpue = .data$trip_total_catch_kg / .data$n_fishers) |>
    dplyr::group_by(.data$landing_site, .data$year_month, .data$gear) |>
    dplyr::summarise(
      avg_cpue = mean(.data$cpue, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::transmute(
      landing_site = .data$landing_site,
      year_month = .data$year_month,
      metric_type = "cpue",
      metric_value = .data$avg_cpue,
      gear_type = .data$gear,
      species = NA_character_,
      rank = NA_integer_
    )

  rpue_metrics <- trip_level_data |>
    dplyr::mutate(rpue = .data$trip_total_revenue / .data$n_fishers) |>
    dplyr::group_by(.data$landing_site, .data$year_month, .data$gear) |>
    dplyr::summarise(
      avg_rpue = mean(.data$rpue, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::transmute(
      landing_site = .data$landing_site,
      year_month = .data$year_month,
      metric_type = "rpue",
      metric_value = .data$avg_rpue,
      gear_type = .data$gear,
      species = NA_character_,
      rank = NA_integer_
    )

  species_metrics <- catch_data |>
    dplyr::group_by(.data$landing_site, .data$year_month, .data$species) |>
    dplyr::summarise(
      total_species_catch = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$landing_site, .data$year_month) |>
    dplyr::mutate(
      total_site_catch = sum(.data$total_species_catch),
      species_pct = (.data$total_species_catch / .data$total_site_catch) * 100
    ) |>
    dplyr::arrange(
      .data$landing_site,
      .data$year_month,
      dplyr::desc(.data$species_pct)
    ) |>
    dplyr::mutate(rank = dplyr::row_number()) |>
    dplyr::filter(.data$rank <= 2) |>
    dplyr::transmute(
      landing_site = .data$landing_site,
      year_month = .data$year_month,
      metric_type = "species_pct",
      metric_value = .data$species_pct,
      gear_type = NA_character_,
      species = .data$species,
      rank = .data$rank
    ) |>
    dplyr::ungroup()

  fishery_metrics <- dplyr::bind_rows(
    site_level_metrics,
    predominant_gear_metrics,
    pct_main_gear_metrics,
    cpue_metrics,
    rpue_metrics,
    species_metrics
  ) |>
    dplyr::arrange(.data$landing_site, .data$year_month, .data$metric_type)

  return(fishery_metrics)
}


#' Preprocess Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' Downloads raw GPS tracks and creates a gridded summary of fishing activity.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#' @param grid_size Numeric. Size of grid cells in meters (100, 250, 500, or 1000).
#'
#' @return None (invisible). Creates and uploads preprocessed files.
#'
#' @keywords workflow preprocessing
#' @export
preprocess_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  grid_size = 500
) {
  logger::log_threshold(log_threshold)
  pars <- read_config()

  # Get already preprocessed tracks
  logger::log_info("Checking existing preprocessed tracks...")
  preprocessed_filename <- cloud_object_name(
    prefix = paste0(pars$pds$pds_tracks$file_prefix, "-preprocessed"),
    provider = pars$storage$google$key,
    extension = "parquet",
    version = pars$pds$pds_tracks$version,
    options = pars$storage$google$options
  )

  # Get preprocessed trip IDs if file exists
  preprocessed_trips <- tryCatch(
    {
      download_cloud_file(
        name = preprocessed_filename,
        provider = pars$storage$google$key,
        options = pars$storage$google$options
      )
      preprocessed_data <- arrow::read_parquet(preprocessed_filename)
      unique(preprocessed_data$Trip)
    },
    error = function(e) {
      logger::log_info("No existing preprocessed tracks file found")
      character(0)
    }
  )

  # List raw tracks
  logger::log_info("Listing raw tracks...")
  raw_tracks <- googleCloudStorageR::gcs_list_objects(
    bucket = pars$pds_storage$google$options$bucket,
    prefix = pars$pds$pds_tracks$file_prefix
  )$name

  raw_trip_ids <- extract_trip_ids_from_filenames(raw_tracks)
  new_trip_ids <- setdiff(raw_trip_ids, preprocessed_trips)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to preprocess")
    return(invisible())
  }

  # Get raw tracks that need preprocessing
  new_tracks <- raw_tracks[raw_trip_ids %in% new_trip_ids]

  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  logger::log_info("Processing {length(new_tracks)} tracks in parallel...")
  new_processed_data <- furrr::future_map_dfr(
    new_tracks,
    function(track_file) {
      download_cloud_file(
        name = track_file,
        provider = pars$pds_storage$google$key,
        options = pars$pds_storage$google$options
      )

      track_data <- arrow::read_parquet(track_file) %>%
        preprocess_track_data(grid_size = grid_size)

      unlink(track_file)
      track_data
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  future::plan(future::sequential)

  # Combine with existing preprocessed data if it exists
  final_data <- if (length(preprocessed_trips) > 0) {
    dplyr::bind_rows(preprocessed_data, new_processed_data)
  } else {
    new_processed_data
  }

  output_filename <-
    paste0(pars$pds$pds_tracks$file_prefix, "-preprocessed") |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    final_data,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading preprocessed tracks...")
  upload_cloud_file(
    file = output_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )

  unlink(output_filename)
  if (exists("preprocessed_filename")) {
    unlink(preprocessed_filename)
  }

  logger::log_success("Track preprocessing complete")

  grid_summaries <- generate_track_summaries(final_data)

  output_filename <-
    paste0(pars$pds$pds_tracks$file_prefix, "-grid_summaries") |>
    add_version(extension = "parquet")

  arrow::write_parquet(
    grid_summaries,
    sink = output_filename,
    compression = "lz4",
    compression_level = 12
  )

  logger::log_info("Uploading preprocessed tracks...")
  upload_cloud_file(
    file = output_filename,
    provider = pars$storage$google$key,
    options = pars$storage$google$options
  )
}


#' Preprocess Track Data into Spatial Grid Summary
#'
#' @description
#' This function processes GPS track data into a spatial grid summary, calculating time spent
#' and other metrics for each grid cell. The grid size can be specified to analyze spatial
#' patterns at different scales.
#'
#' @param data A data frame containing GPS track data with columns:
#'   - Trip: Unique trip identifier
#'   - Time: Timestamp of the GPS point
#'   - Lat: Latitude
#'   - Lng: Longitude
#'   - Speed (M/S): Speed in meters per second
#'   - Range (Meters): Range in meters
#'   - Heading: Heading in degrees
#'
#' @param grid_size Numeric. Size of grid cells in meters. Must be one of:
#'   - 100: ~100m grid cells
#'   - 250: ~250m grid cells
#'   - 500: ~500m grid cells (default)
#'   - 1000: ~1km grid cells
#'
#' @return A tibble with the following columns:
#'   - Trip: Trip identifier
#'   - lat_grid: Latitude of grid cell center
#'   - lng_grid: Longitude of grid cell center
#'   - time_spent_mins: Total time spent in grid cell in minutes
#'   - mean_speed: Average speed in grid cell (M/S)
#'   - mean_range: Average range in grid cell (Meters)
#'   - first_seen: First timestamp in grid cell
#'   - last_seen: Last timestamp in grid cell
#'   - n_points: Number of GPS points in grid cell
#'
#' @details
#' The function creates a grid by rounding coordinates based on the specified grid size.
#' Grid sizes are approximate due to the conversion from meters to degrees, with calculations
#' based on 1 degree ≈ 111km at the equator. Time spent is calculated using the time
#' differences between consecutive points.
#'
#' @keywords preprocessing
#'
#' @examples
#' \dontrun{
#' # Process tracks with 500m grid (default)
#' result_500m <- preprocess_track_data(tracks_data)
#'
#' # Use 100m grid for finer resolution
#' result_100m <- preprocess_track_data(tracks_data, grid_size = 100)
#'
#' # Use 1km grid for broader patterns
#' result_1km <- preprocess_track_data(tracks_data, grid_size = 1000)
#' }
#'
#' @keywords preprocessing
#' @export
preprocess_track_data <- function(data, grid_size = 500) {
  # Define grid size in meters to degrees (approximately)
  # 1 degree = 111km at equator
  grid_degrees <- switch(
    as.character(grid_size),
    "100" = 0.001, # ~100m
    "250" = 0.0025, # ~250m
    "500" = 0.005, # ~500m
    "1000" = 0.01, # ~1km
    stop("grid_size must be one of: 100, 250, 500, 1000")
  )

  data %>%
    dplyr::select(
      "Trip",
      "Time",
      "Lat",
      "Lng",
      "Speed (M/S)",
      "Range (Meters)",
      "Heading"
    ) %>%
    dplyr::group_by(.data$Trip) %>%
    dplyr::arrange(.data$Time) %>%
    dplyr::mutate(
      # Create grid cells based on selected size
      lat_grid = round(.data$Lat / grid_degrees, 0) * grid_degrees,
      lng_grid = round(.data$Lng / grid_degrees, 0) * grid_degrees,

      # Calculate time spent (difference with next point)
      time_diff = as.numeric(difftime(
        dplyr::lead(.data$Time),
        .data$Time,
        units = "mins"
      )),
      # For last point in series, use difference with previous point
      time_diff = dplyr::if_else(
        is.na(.data$time_diff),
        as.numeric(difftime(
          .data$Time,
          dplyr::lag(.data$Time),
          units = "mins"
        )),
        .data$time_diff
      )
    ) %>%
    # Group by trip and grid cell
    dplyr::group_by(.data$Trip, .data$lat_grid, .data$lng_grid) %>%
    dplyr::summarise(
      time_spent_mins = sum(.data$time_diff, na.rm = TRUE),
      mean_speed = mean(.data$`Speed (M/S)`, na.rm = TRUE),
      mean_range = mean(.data$`Range (Meters)`, na.rm = TRUE),
      first_seen = min(.data$Time),
      last_seen = max(.data$Time),
      n_points = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::filter(.data$time_spent_mins > 0) %>%
    dplyr::group_by(.data$Trip) %>%
    dplyr::arrange(.data$first_seen) %>%
    dplyr::filter(
      !(dplyr::row_number() %in% c(1, 2, dplyr::n() - 1, dplyr::n()))
    ) %>%
    dplyr::ungroup()
}

#' Generate Grid Summaries for Track Data
#'
#' @description
#' Processes GPS track data into 1km grid summaries for visualization and analysis.
#'
#' @param data Preprocessed track data
#' @param min_hours Minimum hours threshold for filtering (default: 0.15)
#' @param max_hours Maximum hours threshold for filtering (default: 10)
#'
#' @return A dataframe with grid summary statistics
#'
#' @keywords preprocessing
#' @export
generate_track_summaries <- function(data, min_hours = 0.15, max_hours = 15) {
  data %>%
    # First summarize by current grid (500m)
    dplyr::group_by(.data$lat_grid, .data$lng_grid) %>%
    dplyr::summarise(
      avg_time_mins = mean(.data$time_spent_mins),
      avg_speed = mean(.data$mean_speed),
      avg_range = mean(.data$mean_range),
      visits = dplyr::n_distinct(.data$Trip),
      total_points = sum(.data$n_points),
      .groups = "drop"
    ) %>%
    # Then regrid to 1km
    dplyr::mutate(
      lat_grid_1km = round(.data$lat_grid / 0.01) * 0.01,
      lng_grid_1km = round(.data$lng_grid / 0.01) * 0.01
    ) %>%
    dplyr::group_by(.data$lat_grid_1km, .data$lng_grid_1km) %>%
    dplyr::summarise(
      avg_time_mins = mean(.data$avg_time_mins),
      avg_speed = mean(.data$avg_speed),
      avg_range = mean(.data$avg_range),
      total_visits = sum(.data$visits),
      original_cells = dplyr::n(),
      total_points = sum(.data$total_points),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      avg_time_hours = .data$avg_time_mins / 60
    ) %>%
    dplyr::filter(
      .data$avg_time_hours >= min_hours,
      .data$avg_time_hours <= max_hours
    ) |>
    dplyr::select(-"avg_time_mins")
}
