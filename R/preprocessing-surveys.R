#' Preprocess Lurio Landings Data
#'
#' This function preprocesses raw Lurio survey data from Google Cloud Storage.
#' It performs data cleaning, transformation, catch weight calculations using
#' length-weight relationships, and uploads processed data back to GCS.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return Invisible NULL. Function processes data and uploads to Google Cloud Storage.
#'
#' @details
#' The function performs the following main operations:
#' 1. Downloads ASFIS species data and Airtable form assets from GCS
#' 2. Downloads raw survey data from GCS as Parquet file
#' 3. Extracts and processes general trip information (dates, location, GPS)
#' 4. Extracts and processes trip details (vessel, gear, fishers, duration)
#' 5. Processes catch data using `process_species_group()` to reshape from wide to long format
#' 6. Calculates catch weights via `calculate_catch_lurio()` using length-weight coefficients
#' 7. Processes market information (catch use and price)
#' 8. Joins all datasets and filters for active surveys
#' 9. Maps survey codes to standardized names using Airtable assets
#' 10. Uploads preprocessed data as versioned Parquet file to GCS
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' preprocess_landings_lurio()
#' }
#' @export
preprocess_landings_lurio <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  asfis <- download_parquet_from_cloud(
    prefix = "asfis",
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  assets <- fetch_assets(
    form_id = get_airtable_form_id(
      kobo_asset_id = conf$ingestion$`kobo-lurio`$asset_id,
      conf = conf
    ),
    conf = conf
  )

  # get raw landings from cloud storage
  raw_dat <- download_parquet_from_cloud(
    prefix = conf$ingestion$`kobo-lurio`$raw_surveys$file_prefix,
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
      district = stringr::str_to_title(.data$district),
      landing_site = dplyr::coalesce(
        .data$landing_site_palma,
        .data$landing_site_mocimboa
      )
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
      #propulsion_gear = dplyr::case_when(
      #  .data$propulsion_gear == "1" ~ "Engine",
      #  .data$propulsion_gear == "2" ~ "Sail",
      #  .data$propulsion_gear == "3" ~ "Oar",
      #  TRUE ~ NA_character_
      #),
      hook_size = dplyr::coalesce(.data$hook_size, .data$hook_size_other)
    ) %>%
    dplyr::select(
      -c(
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
      assets$taxa,
      by = c("catch_number" = "survey_label")
    ) %>%
    dplyr::select(
      "submission_id",
      "n",
      catch_taxon = "alpha3_code",
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
    calculate_catch_lurio(catch_data = catch_info, lwcoeffs = lwcoeffs$lw) |>
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

  preprocessed_data <-
    map_surveys(
      data = preprocessed_landings,
      taxa_mapping = assets$taxa,
      gear_mapping = assets$gear,
      vessels_mapping = assets$vessels,
      sites_mapping = assets$sites
    ) |>
    dplyr::mutate(
      habitat = dplyr::case_when(
        .data$habitat == "1" ~ "Reef",
        .data$habitat == "2" ~ "FAD",
        .data$habitat == "3" ~ "Open Sea",
        .data$habitat == "4" ~ "Shore",
        .data$habitat == "6" ~ "Mangrove",
        .data$habitat == "7" ~ "Seagrass",
        TRUE ~ .data$habitat
      )
    )

  logger::log_info("Uploading preprocessed data to cloud storage")
  # upload preprocessed landings
  upload_parquet_to_cloud(
    data = preprocessed_data,
    prefix = conf$ingestion$`kobo-lurio`$preprocessed_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  invisible(NULL)
}

#' Preprocess ADNAP Landings Data
#'
#' This function preprocesses raw ADNAP survey data from Google Cloud Storage.
#' It performs data cleaning, transformation, catch weight calculations using
#' length-weight relationships, and uploads processed data back to GCS.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return Invisible NULL. Function processes data and uploads to Google Cloud Storage.
#'
#' @details
#' The function performs the following main operations:
#' 1. Downloads ASFIS species data and Airtable form assets from GCS
#' 2. Downloads raw survey data from GCS as Parquet file
#' 3. Processes general trip information using `preprocess_general_adnap()`
#' 4. Processes catch data using `preprocess_catch()` which handles survey version detection
#' 5. Calculates catch weights via `calculate_catch_adnap()` using length-weight coefficients
#' 6. Joins trip and catch data
#' 7. Maps survey codes to standardized names using Airtable assets
#' 8. Uploads preprocessed data as versioned Parquet file to GCS
#'
#' @keywords workflow preprocessing
#' @examples
#' \dontrun{
#' preprocess_landings_adnap()
#' }
#' @export
preprocess_landings_adnap <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  asfis <- download_parquet_from_cloud(
    prefix = "asfis",
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  assets <- fetch_assets(
    form_id = get_airtable_form_id(
      kobo_asset_id = conf$ingestion$`kobo-adnap`$asset_id,
      conf = conf
    ),
    conf = conf
  )

  # get raw landings from cloud storage
  raw_dat <- download_parquet_from_cloud(
    prefix = conf$ingestion$`kobo-adnap`$raw_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) %>%
    dplyr::mutate_all(as.character)

  trip_info <- preprocess_general_adnap(data = raw_dat)
  catch_info <- preprocess_catch(data = raw_dat)

  catch_df <- process_version_data(catch_info = catch_info, asfis = asfis)

  preprocessed_landings <-
    dplyr::left_join(trip_info, catch_df, by = "submission_id") |>
    dplyr::arrange(.data$submission_id, .data$n_catch) |>
    dplyr::distinct()

  preprocessed_data <-
    map_surveys(
      data = preprocessed_landings,
      taxa_mapping = assets$taxa,
      gear_mapping = assets$gear,
      vessels_mapping = assets$vessels,
      sites_mapping = assets$sites
    ) |>
    dplyr::mutate(
      habitat = dplyr::case_when(
        .data$habitat == "creef" ~ "Reef",
        .data$habitat == "fad" ~ "FAD",
        .data$habitat == "opsea" ~ "Open sea",
        .data$habitat == "shore" ~ "Shore",
        .data$habitat == "mang" ~ "Mangrove",
        .data$habitat == "seagr" ~ "Seagrass",
        .data$habitat == "east" ~ "Estuary",
        .data$habitat == "intzone" ~ "Intertidal zone",
        .data$habitat == "rock" ~ "Rocky area / Reef base",
        .data$habitat == "mud" ~ "Mud / Algae / Sand",
        TRUE ~ .data$habitat
      )
    )

  logger::log_info("Uploading preprocessed data to cloud storage")
  # upload preprocessed landings
  upload_parquet_to_cloud(
    data = preprocessed_data,
    prefix = conf$ingestion$`kobo-adnap`$preprocessed_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  invisible(NULL)
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
#' catch_weights <- calculate_catch_lurio(
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
calculate_catch_lurio <- function(catch_data = NULL, lwcoeffs = NULL) {
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

#' Get Airtable Form ID from KoBoToolbox Asset ID
#'
#' @description
#' Retrieves the Airtable record ID for a form based on its KoBoToolbox asset ID.
#'
#' @param kobo_asset_id Character. The KoBoToolbox asset ID to match.
#' @param conf Configuration object from read_config().
#'
#' @return Character. The Airtable record ID for the matching form.
#' @keywords preprocessing helper
#' @export
get_airtable_form_id <- function(kobo_asset_id = NULL, conf = NULL) {
  airtable_to_df(
    base_id = conf$airtable$frame$base_id,
    table_name = "forms",
    token = conf$airtable$token
  ) |>
    janitor::clean_names() |>
    dplyr::filter(.data$form_id == kobo_asset_id) |>
    dplyr::pull(.data$airtable_id)
}

#' Map Survey Labels to Standardized Taxa, Gear, and Vessel Names
#'
#' @description
#' Converts local species, gear, and vessel labels from surveys to standardized names using
#' Airtable reference tables. Replaces catch_taxon with scientific_name and alpha3_code,
#' and replaces local gear and vessel names with standardized types.
#'
#' @param data A data frame with preprocessed survey data containing catch_taxon, gear,
#'   vessel_type, and landing_site columns.
#' @param taxa_mapping A data frame from Airtable taxa table with survey_label, alpha3_code,
#'   and scientific_name columns.
#' @param gear_mapping A data frame from Airtable gears table with survey_label and
#'   standard_name columns.
#' @param vessels_mapping A data frame from Airtable vessels table with survey_label and
#'   standard_name columns.
#' @param sites_mapping A data frame from Airtable landing_sites table with site_code and
#'   site columns.
#'
#' @return A tibble with catch_taxon replaced by scientific_name and alpha3_code, gear and
#'   vessel_type replaced by standardized names, and landing_site replaced by the full site name.
#'   Records without matches will have NA values.
#'
#' @details
#' This function is called within `preprocess_landings()` after processing raw survey data.
#' The mapping tables are retrieved from Airtable frame base and filtered by
#' form ID before being passed to this function.
#'
#' @keywords preprocessing helper
#' @export
map_surveys <- function(
  data = NULL,
  taxa_mapping = NULL,
  gear_mapping = NULL,
  vessels_mapping = NULL,
  sites_mapping = NULL
) {
  data |>
    dplyr::left_join(taxa_mapping, by = c("catch_taxon" = "survey_label")) |>
    dplyr::select(-c("catch_taxon")) |>
    dplyr::relocate("scientific_name", .after = "n_catch") |>
    dplyr::relocate("alpha3_code", .after = "scientific_name") |>
    dplyr::left_join(gear_mapping, by = c("gear" = "survey_label")) |>
    dplyr::select(-c("gear")) |>
    dplyr::relocate("standard_name", .after = "vessel_type") |>
    dplyr::rename(gear = "standard_name") |>
    dplyr::left_join(
      vessels_mapping,
      by = c("vessel_type" = "survey_label")
    ) |>
    dplyr::select(-c("vessel_type")) |>
    dplyr::relocate("standard_name", .after = "habitat") |>
    dplyr::rename(vessel_type = "standard_name") |>
    dplyr::left_join(
      sites_mapping,
      by = c("landing_site" = "site_code")
    ) |>
    dplyr::select(-c("landing_site")) |>
    dplyr::relocate("site", .after = "district") |>
    dplyr::rename(landing_site = "site")
}


#' Fetch and Filter Asset Data from Airtable
#'
#' @description
#' Retrieves data from a specified Airtable table and filters it based on form ID.
#' Handles cases where form_id column contains multiple comma-separated IDs.
#'
#' @param table_name Character. Name of the Airtable table to fetch.
#' @param select_cols Character vector. Column names to select from the table.
#' @param form Character. Form ID to filter by.
#' @param conf Configuration object from read_config().
#'
#' @return A filtered and selected data frame from Airtable containing only rows where
#'   the form_id field contains the specified form ID.
#'
#' @details
#' This function uses string detection to handle multi-valued form_id fields that may
#' contain comma-separated lists of form IDs.
#'
#' @keywords preprocessing helper
#' @export
fetch_asset <- function(
  table_name = NULL,
  select_cols = NULL,
  form = NULL,
  conf = NULL
) {
  airtable_to_df(
    base_id = conf$airtable$frame$base_id,
    table_name = table_name,
    token = conf$airtable$token
  ) |>
    janitor::clean_names() |>
    # Filter rows where form_id contains the target ID
    dplyr::filter(stringr::str_detect(
      .data$form_id,
      stringr::fixed(form)
    )) |>
    dplyr::select(dplyr::all_of(select_cols))
}

#' Fetch Multiple Asset Tables from Airtable
#'
#' @description
#' Fetches taxa, gear, vessels, and landing sites data from Airtable filtered
#' by the specified form ID. Returns distinct records for each table.
#'
#' @param form_id Character. Form ID to filter assets by. This is passed to each
#'   individual fetch_asset call.
#' @param conf Configuration object from read_config().
#'
#' @return A named list containing four data frames:
#'   \itemize{
#'     \item \code{taxa}: Contains survey_label, alpha3_code, and scientific_name columns
#'     \item \code{gear}: Contains survey_label and standard_name columns
#'     \item \code{vessels}: Contains survey_label and standard_name columns
#'     \item \code{sites}: Contains site and site_code columns
#'   }
#'
#' @details
#' Each table is fetched separately using `fetch_asset()` and filtered to return
#' only distinct rows to avoid duplicates in the mapping tables.
#'
#' @keywords preprocessing helper
#' @export
fetch_assets <- function(form_id = NULL, conf = NULL) {
  assets_list <-
    list(
      taxa = fetch_asset(
        table_name = "taxa",
        select_cols = c("survey_label", "alpha3_code", "scientific_name"),
        form = form_id,
        conf = conf
      ),
      gear = fetch_asset(
        table_name = "gears",
        select_cols = c("survey_label", "standard_name"),
        form = form_id,
        conf = conf
      ),
      vessels = fetch_asset(
        table_name = "vessels",
        select_cols = c("survey_label", "standard_name"),
        form = form_id,
        conf = conf
      ),
      sites = fetch_asset(
        table_name = "landing_sites",
        select_cols = c("site", "site_code"),
        form = form_id,
        conf = conf
      )
    )

  purrr::map(assets_list, ~ dplyr::distinct(.x))
}

#' Preprocess General Survey Information for ADNAP
#'
#' Processes general survey information from ADNAP KoBoToolbox forms including
#' trip details, fisher counts, vessel information, and survey metadata.
#'
#' @param data A data frame containing raw ADNAP survey data
#'
#' @return A data frame with processed general survey information including:
#'   submission_id, dates, location, gear, trip details, fisher counts, etc.
#'
#' @keywords preprocessing
#' @export
preprocess_general_adnap <- function(data = NULL) {
  general_info <-
    data %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "no_fishers/")) %>%
    dplyr::rename_with(
      ~ stringr::str_remove(., "group_conservation_trading/")
    ) %>%
    dplyr::select(
      "submission_id",
      submitted_by = "_submitted_by",
      submission_date = "today",
      "landing_date",
      district = "District",
      dplyr::contains("landing_site"),
      "collect_data_today",
      "survey_activity",
      dplyr::contains("fishing_days_week"),
      "has_boat",
      "vessel_type",
      "propulsion_gear",
      "fuel_L",
      "has_PDS",
      "habitat",
      "fishing_ground",
      "gear",
      "mesh_size",
      "fishing_start",
      "fishing_end",
      dplyr::ends_with("_fishers"),
      "catch_outcome",
      "conservation",
      "catch_use",
      "trader",
      "catch_price",
      "happiness_rating"
    ) %>%
    dplyr::mutate(
      landing_site = dplyr::coalesce(
        !!!dplyr::select(., dplyr::contains("landing_site"))
      )
    ) |>
    dplyr::select(-dplyr::contains("landing_site"), "landing_site") |>
    dplyr::relocate("landing_site", .after = "district") |>
    # dplyr::mutate(landing_code = dplyr::coalesce(.data$landing_site_palma, .data$landing_site_mocimboa)) %>%
    # tidyr::separate(.data$gps,
    #  into = c("lat", "lon", "drop1", "drop2"),
    #  sep = " "
    # ) %>%
    # dplyr::relocate("landing_code", .after = "district") %>%
    dplyr::mutate(
      landing_date = lubridate::as_date(.data$landing_date),
      submission_date = lubridate::as_date(.data$submission_date),
      fishing_start = lubridate::as_datetime(.data$fishing_start),
      fishing_end = lubridate::as_datetime(.data$fishing_end),
      trip_duration = as.numeric(difftime(
        .data$fishing_end,
        .data$fishing_start,
        units = "hours"
      )),
      dplyr::across(
        c(
          dplyr::contains("fishing_days_week"),
          "catch_price",
          dplyr::ends_with("_fishers")
        ),
        ~ as.double(.x)
      )
    )
  general_info
}

#' Process Version Data Helper Function
#'
#' Internal helper function to process catch and general info for a specific survey version
#'
#' @param catch_info Processed catch information
#' @param asfis ASFIS species data
#'
#' @return Combined and processed survey data
#' @keywords internal
process_version_data <- function(catch_info = NULL, asfis = NULL) {
  # Try to get length-weight coefficients from Rfishbase
  lwcoeffs <- tryCatch(
    {
      getLWCoeffs(
        taxa_list = unique(catch_info$catch_taxon),
        asfis_list = asfis
      )
    },
    error = function(e) {
      message("Error in getLWCoeffs, using local fallback: ", e$message)
      # Fallback to local data
      readr::read_rds(system.file(
        "length_weight_params.rds",
        package = "peskas.mozambique.data.pipeline"
      ))
    }
  )

  # add flying fish estimates
  fly_lwcoeffs <- dplyr::tibble(
    catch_taxon = "FLY",
    n = 0,
    a_6 = 0.00631,
    b_6 = 3.05
  )
  lwcoeffs$lw <- dplyr::bind_rows(lwcoeffs$lw, fly_lwcoeffs)

  catch_df <-
    calculate_catch_adnap(catch_data = catch_info, lwcoeffs = lwcoeffs$lw) |>
    dplyr::left_join(lwcoeffs$ml, by = "catch_taxon") |>
    dplyr::select(-"max_weightkg_75")

  return(catch_df)
}
