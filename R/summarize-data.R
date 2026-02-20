#' Summarize WorldFish Survey Data
#'
#' @description
#' Processes validated survey data from WorldFish sources, filtering out flagged submissions
#' and generating summary datasets for various dimensions:
#' - Monthly summaries with aggregated catch metrics
#' - Taxa summaries with species-specific information
#' - District summaries with submission and effort metrics
#' - Gear summaries with gear-specific performance metrics
#' - Grid summaries from vessel tracking data
#'
#' @details
#' The function performs the following operations:
#' - Retrieves validated WF survey data
#' - Filters for approved validation status
#' - Creates multiple summary datasets:
#'   - Monthly summaries: Average catch, price, CPUE, and RPUE by district and month
#'   - Taxa summaries: Catch metrics by species, district, and month
#'   - District summaries: Submission counts and effort metrics by district
#'   - Gear summaries: Performance metrics by gear type
#'   - Grid summaries: Downloaded from cloud storage
#' - Uploads all summaries to cloud storage as versioned parquet files
#'
#' The metrics calculated include:
#' - Total and mean catch weight
#' - Price per kg of catch
#' - CPUE (Catch Per Unit Effort) - both hourly and daily
#' - RPUE (Revenue Per Unit Effort) - both hourly and daily
#' - Number of submissions and fishers
#' - Trip duration
#'
#' @param log_threshold The logging level threshold for the logger package (e.g., DEBUG, INFO)
#'   See `logger::log_levels` for available options.
#'
#' @return NULL (invisible). The function uploads summary files to cloud storage as a side effect.
#'
#' @examples
#' \dontrun{
#' # Summarize WF data with default debug logging
#' summarize_data()
#'
#' # Summarize with info-level logging only
#' summarize_data(logger::INFO)
#' }
#'
#' @seealso
#' * [get_validation_status()] for retrieving validation information
#' * [upload_cloud_file()] for uploading results to cloud storage
#' * [download_parquet_from_cloud()] for retrieving validated surveys and grid summaries
#'
#' @keywords workflow pipeline mining
#' @export
summarize_data <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()
  metadata_tables <- get_metadata()

  validated_surveys <-
    download_parquet_from_cloud(
      prefix = conf$surveys$`adnap`$validated$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  # Choose a parallelization strategy - adjust the number of workers as needed
  future::plan(
    strategy = future::multisession,
    workers = future::availableCores() - 2
  ) # Use available cores minus 2

  # get validation table and store approved ids from both assets
  submission_ids <- unique(validated_surveys$submission_id)

  # Query validation status from both survey versions using same credentials
  logger::log_info(
    "Querying validation status from both wf_surveys_v1 and wf_surveys_v2 assets"
  )

  validation_results <- list()

  # Query wf_surveys v1 (original asset)
  validation_results$v1 <- submission_ids %>%
    furrr::future_map_dfr(
      get_validation_status,
      asset_id = conf$ingestion$`adnap`$asset_id,
      token = conf$ingestion$`lurio`$token,
      .options = furrr::furrr_options(seed = TRUE)
    )

  # Query wf_surveys v2 (new asset)
  validation_results$v2 <- submission_ids %>%
    furrr::future_map_dfr(
      get_validation_status,
      asset_id = conf$surveys$wf_surveys_v2$asset_id,
      token = conf$surveys$wf_surveys_v2$token,
      .options = furrr::furrr_options(seed = TRUE)
    )

  # Combine validation results and extract approved IDs
  valid_ids <- validation_results %>%
    dplyr::bind_rows() %>%
    dplyr::filter(.data$validation_status == "validation_status_approved") %>%
    dplyr::pull(.data$submission_id) %>%
    unique()

  logger::log_info(
    "Found {length(valid_ids)} approved submissions across both assets"
  )

  clean_data <-
    validated_surveys |>
    dplyr::filter(.data$submission_id %in% valid_ids)

  f_metrics <- calculate_fishery_metrics(data = clean_data)

  indicators_df <-
    clean_data |>
    dplyr::filter(.data$collect_data_today == "1") |>
    dplyr::mutate(
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "landing_site",
      "habitat",
      "gear",
      "vessel_type",
      "propulsion_gear",
      "fuel_L",
      "trip_duration",
      "vessel_type",
      "no_men_fishers",
      "no_men_fishers",
      "no_child_fishers",
      "n_fishers",
      "catch_taxon",
      "catch_price",
      "catch_kg"
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "landing_date",
          "district",
          "landing_site",
          "habitat",
          "gear",
          "vessel_type",
          "propulsion_gear",
          "fuel_L",
          "trip_duration",
          "vessel_type",
          "n_fishers",
          "no_men_fishers",
          "no_men_fishers",
          "no_child_fishers",
          "catch_price"
        ),
        ~ dplyr::first(.x)
      ),
      tot_catch_kg = sum(.data$catch_kg),
      catch_taxon = paste(unique(.data$catch_taxon), collapse = "-")
    ) |>
    dplyr::mutate(
      catch_price = .data$catch_price,
      price_kg = .data$catch_price / .data$tot_catch_kg,
      cpue = .data$tot_catch_kg / .data$n_fishers / .data$trip_duration,
      rpue = .data$catch_price / .data$n_fishers / .data$trip_duration,
      cpue_day = .data$tot_catch_kg / .data$n_fishers, # CPUE per day for map data (assuming 1 trip per day)
      rpue_day = .data$catch_price / .data$n_fishers # RPUE per day for map data (assuming 1 trip per day)
    )

  taxa_df <-
    clean_data |>
    dplyr::filter(.data$collect_data_today == "1") |>
    dplyr::mutate(
      n_fishers = .data$no_men_fishers +
        .data$no_women_fishers +
        .data$no_child_fishers
    ) |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "landing_site",
      "habitat",
      "gear",
      "vessel_type",
      "propulsion_gear",
      "fuel_L",
      "trip_duration",
      "vessel_type",
      "n_fishers",
      "catch_taxon",
      "length",
      "catch_price",
      "catch_kg"
    ) |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      n_catch = dplyr::n(),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$submission_id, .data$catch_taxon) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "n_catch",
          "landing_date",
          "district",
          "landing_site",
          "habitat",
          "gear",
          "vessel_type",
          "propulsion_gear",
          "fuel_L",
          "trip_duration",
          "vessel_type",
          "n_fishers",
          "catch_price"
        ),
        ~ dplyr::first(.x)
      ),
      lenght = mean(.data$length),
      tot_catch_kg = sum(.data$catch_kg),
      .groups = "drop"
    ) |>
    dplyr::relocate("n_catch", .after = "submission_id")

  monthly_summaries <-
    indicators_df |>
    dplyr::mutate(
      date = lubridate::floor_date(.data$landing_date, "month"),
      date = lubridate::as_datetime(.data$date)
    ) |>
    dplyr::group_by(.data$district, .data$date) |>
    dplyr::summarise(
      dplyr::across(
        .cols = c(
          "tot_catch_kg",
          "catch_price",
          "cpue",
          "cpue_day",
          "rpue",
          "rpue_day",
          "price_kg"
        ),
        ~ mean(.x, na.rm = TRUE)
      ),
      .groups = "drop"
    ) |>
    dplyr::rename(
      mean_catch_kg = "tot_catch_kg",
      mean_catch_price = "catch_price",
      mean_cpue = "cpue",
      mean_cpue_day = "cpue_day",
      mean_rpue = "rpue",
      mean_rpue_day = "rpue_day",
      mean_price_kg = "price_kg"
    ) |>
    tidyr::complete(
      .data$district,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      fill = list(
        mean_catch_kg = NA,
        mean_catch_price = NA,
        mean_cpue = NA,
        mean_cpue_day = NA,
        mean_rpue = NA,
        mean_rpue_day = NA,
        mean_price_kg = NA
      )
    ) |>
    dplyr::mutate(
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  taxa_summaries <-
    taxa_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$district, .data$date, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$tot_catch_kg, na.rm = T),
      catch_price = sum(.data$catch_price, na.rm = T),
      mean_length = mean(.data$lenght, na.rm = T),
      .groups = "drop"
    ) |>
    dplyr::rename(alpha3_code = "catch_taxon") |>
    dplyr::left_join(metadata_tables$catch_type, by = "alpha3_code") |>
    dplyr::select(
      "district",
      "date",
      "common_name",
      "catch_kg",
      "catch_price",
      "mean_length"
    ) |>
    tidyr::complete(
      .data$district,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      .data$common_name,
      fill = list(
        catch_kg = NA,
        catch_price = NA,
        length = NA
      )
    ) |>
    dplyr::mutate(price_kg = .data$catch_price / .data$catch_kg) |>
    dplyr::select(-c("catch_price")) |>
    tidyr::pivot_longer(
      -c("district", "date", "common_name"),
      names_to = "metric",
      values_to = "value"
    ) |>
    dplyr::mutate(
      date = lubridate::as_datetime(.data$date),
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  districts_summaries <-
    indicators_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$district, .data$date) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      n_fishers = mean(.data$n_fishers),
      trip_duration = mean(.data$trip_duration),
      mean_cpue = mean(.data$cpue, na.rm = TRUE),
      mean_rpue = mean(.data$rpue, na.rm = TRUE),
      mean_price_kg = mean(.data$price_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_longer(
      -c("district", "date"),
      names_to = "indicator",
      values_to = "value"
    ) |>
    tidyr::complete(
      .data$district,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      fill = list(
        n_submissions = NA,
        n_fishers = NA,
        trip_duration = NA,
        mean_cpue = NA,
        mean_rpue = NA,
        mean_price_kg = NA
      )
    ) |>
    dplyr::mutate(
      date = lubridate::as_datetime(.data$date),
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  gear_summaries <-
    indicators_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$district, .data$date, .data$gear) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      cpue = mean(.data$cpue, na.rm = T),
      rpue = mean(.data$rpue),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      gear = dplyr::case_when(
        stringr::str_count(.data$gear) > 3 ~ "Mixed gears",
        .data$gear == "HL" ~ "Hand line",
        .data$gear == "BS" ~ "Beach Seine",
        .data$gear == "CS" ~ "Cast Net",
        .data$gear == "GN" ~ "Gill Net",
        .data$gear == "LL" ~ "Long line",
        .data$gear == "SP" ~ "Spear gun",
        .data$gear == "SR" ~ "Stick Rod",
        .data$gear == "PS" ~ "Purse Seine",
        .data$gear == "RN" ~ "Ring Net",
        .data$gear == "SN" ~ "Shark Net",
        .data$gear == "TR" ~ "Trap",
        TRUE ~ .data$gear # Keep original value if no match
      )
    ) |>
    tidyr::pivot_longer(
      -c("district", "date", "gear"),
      names_to = "indicator",
      values_to = "value"
    ) |>
    tidyr::complete(
      .data$district,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      fill = list(
        gear = NA,
        indicator = NA,
        value = NA
      )
    ) |>
    dplyr::mutate(
      date = lubridate::as_datetime(.data$date),
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  grid_summaries <-
    download_parquet_from_cloud(
      prefix = paste0(conf$pds$pds_tracks$file_prefix, "-grid_summaries"),
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )
  # Dataframes to upload
  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    taxa_summaries = taxa_summaries,
    districts_summaries = districts_summaries,
    gear_summaries = gear_summaries,
    grid_summaries = grid_summaries
  )

  # Write each data frame to its own parquet file with versioning and upload
  for (name in names(dataframes_to_upload)) {
    filename <- conf$surveys$wf_surveys_v1$summaries$file_prefix %>%
      paste0("_", name) %>% # Add the table name to distinguish files
      add_version(extension = "parquet")

    arrow::write_parquet(
      x = dataframes_to_upload[[name]],
      sink = filename,
      compression = "lz4",
      compression_level = 12
    )

    logger::log_info("Uploading {filename} to cloud storage")
    upload_cloud_file(
      file = filename,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )
  }

  upload_parquet_to_cloud(
    data = f_metrics,
    prefix = "zanzibar_fishery_metrics",
    provider = conf$storage$google$key,
    options = conf$storage$google$options_coasts
  )
}
