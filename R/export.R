#' Export Processed Landings Data
#'
#' @description
#' Exports validated landings data to a MongoDB collection. This function filters and
#' transforms preprocessed data, calculates total catch weights, and exports selected
#' fields to a validated collection.
#'
#' @details
#' The function performs the following steps:
#' 1. Pulls preprocessed data from MongoDB
#' 2. Calculates total catch weights per submission
#' 3. Filters for valid survey activities (survey_activity == 1)
#' 4. Selects relevant fields for export
#' 5. Uploads the filtered data to the validated collection
#'
#' @return None (invisible). Data is exported directly to MongoDB.
#'
#' @note
#' Only submissions with survey_activity == 1 are included in the export.
#' Catch weights are summed per submission, with NA values removed.
#'
#' @section Exported Fields:
#' The following fields are included in the export:
#' \itemize{
#'   \item submission_id: Unique identifier for the submission
#'   \item landing_date: Date and time of landing
#'   \item district: Administrative district
#'   \item landing_site: Name of landing site
#'   \item catch_outcome: Outcome of catch
#'   \item lat: Latitude
#'   \item lon: Longitude
#'   \item habitat: Fishing habitat
#'   \item vessel_type: Type of fishing vessel
#'   \item propulsion_gear: Type of propulsion
#'   \item trip_duration: Duration of fishing trip
#'   \item gear: Fishing gear used
#'   \item catch_df: Nested catch data
#' }
#'
#' @seealso
#' \code{\link{preprocess_landings_adnap}} for data preprocessing
#' \code{\link{calculate_catch_adnap}} for catch weight calculations
#'
#' @importFrom dplyr as_tibble filter select group_by summarise
#' @importFrom tidyr unnest
#' @importFrom logger log_info
#'
#' @examples
#' \dontrun{
#' export_landings()
#' }
#'
#' @keywords workflow export
#' @export
export_landings <- function() {
  conf <- read_config()

  #metadata_tables <- get_metadata()

  validated_data <-
    download_parquet_from_cloud(
      prefix = conf$ingestion$`kobo-lurio`$validated_surveys$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    dplyr::filter(.data$landing_date >= "2023-01-01") |>
    #TO DO: fix this in validation
    dplyr::mutate(landing_date = .data$submission_date)

  #f_metrics <- calculate_fishery_metrics(validated_data)

  indicators_df <-
    validated_data |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "landing_site",
      "habitat",
      "gear",
      "propulsion_gear",
      # "fuel_L",
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
          "landing_date",
          "district",
          "landing_site",
          "habitat",
          "gear",
          "vessel_type",
          "propulsion_gear",
          # "fuel_L",
          "trip_duration",
          "n_fishers",
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
    validated_data |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "landing_site",
      "habitat",
      "gear",
      "vessel_type",
      "propulsion_gear",
      # "fuel_L",
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
          # "fuel_L",
          "trip_duration",
          "vessel_type",
          "n_fishers",
          "catch_price"
        ),
        ~ dplyr::first(.x)
      ),
      length = mean(.data$length),
      tot_catch_kg = sum(.data$catch_kg),
      .groups = "drop"
    ) |>
    dplyr::relocate("n_catch", .after = "submission_id")

  taxa_summaries <-
    taxa_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$district, .data$date, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$tot_catch_kg, na.rm = T),
      catch_price = sum(.data$catch_price, na.rm = T),
      mean_length = mean(.data$length, na.rm = T),
      .groups = "drop"
    ) |>
    # dplyr::rename(interagency_code = "catch_taxon") |>
    # dplyr::left_join(metadata_tables$catch_groups, by = "alpha3_code") |>
    dplyr::select(
      "district",
      "date",
      "catch_taxon",
      "catch_kg",
      "catch_price",
      "mean_length"
    ) |>
    tidyr::complete(
      .data$district,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      .data$catch_taxon,
      fill = list(
        catch_kg = NA,
        catch_price = NA,
        length = NA
      )
    ) |>
    dplyr::mutate(price_kg = .data$catch_price / .data$catch_kg) |>
    dplyr::select(-c("catch_price")) |>
    tidyr::pivot_longer(
      -c("district", "date", "catch_taxon"),
      names_to = "metric",
      values_to = "value"
    ) |>
    dplyr::mutate(
      date = lubridate::as_datetime(.data$date),
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    ) |>
    # TODO: add common names
    dplyr::rename(common_name = "catch_taxon")

  districts_summaries <-
    indicators_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$district, .data$date) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      n_fishers = mean(.data$n_fishers),
      trip_duration = mean(.data$trip_duration),
      mean_cpue = stats::median(.data$cpue, na.rm = TRUE),
      mean_rpue = stats::median(.data$rpue, na.rm = TRUE),
      mean_price_kg = stats::median(.data$price_kg, na.rm = TRUE),
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
        ~ stats::median(.x, na.rm = TRUE)
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

  region_monthly_summaries <-
    monthly_summaries |>
    dplyr::distinct() |>
    dplyr::rename(region = "district") |>
    dplyr::mutate(
      date = format(.data$date, "%Y-%m-%dT%H:%M:%SZ"),
      country = "mozambique",
      region = tolower(.data$region)
    ) |>
    dplyr::relocate("country", .before = "region")

  upload_parquet_to_cloud(
    data = region_monthly_summaries,
    prefix = "mozambique_monthly_summaries_map",
    provider = conf$storage$google$key,
    options = conf$storage$google$options_coasts
  )

  # filename <- add_version("mozambique_fishery_metrics", extension = "parquet")

  # upload_parquet_to_cloud(
  #   data = f_metrics,
  #   prefix = filename,
  #   provider = conf$storage$google$key,
  #   options = conf$storage$google$options_coasts
  # )

  grid_summaries <-
    download_parquet_from_cloud(
      prefix = "pds-tracks-grid_summaries",
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  # Transform monthly summaries to long format for portal
  monthly_summaries <-
    monthly_summaries |>
    dplyr::select(-c("mean_cpue_day", "mean_rpue_day")) |> # Drop map-specific metrics
    tidyr::pivot_longer(
      -c("date", "district"),
      names_to = "metric",
      values_to = "value"
    )

  districts_summaries <-
    districts_summaries |>
    tidyr::pivot_wider(names_from = "indicator", values_from = "value") |>
    dplyr::select(dplyr::where(~ !all(is.na(.)))) |>
    tidyr::pivot_longer(
      -c("date", "district"),
      names_to = "indicator",
      values_to = "value"
    )

  dataframes_to_upload <- list(
    monthly_summaries = monthly_summaries,
    taxa_summaries = taxa_summaries,
    districts_summaries = districts_summaries,
    gear_summaries = gear_summaries,
    grid_summaries = grid_summaries
  )

  # Collection names
  collection_names <- list(
    monthly_summaries = "monthly_summaries",
    taxa_summaries = "taxa_summaries",
    districts_summaries = "districts_summaries",
    gear_summaries = "gear_summaries",
    grid_summaries = "grid_summaries"
  )

  # Iterate over the dataframes and upload them
  purrr::walk2(
    .x = dataframes_to_upload,
    .y = collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = conf$storage$mongodb$connection_string$main,
        collection_name = .y,
        db_name = "portal-dev"
      )
    }
  )

  ######

  export_data <-
    validated_data %>%
    # dplyr::filter(.data$survey_activity == 1) %>%
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "landing_site",
      "catch_outcome",
      "lat",
      "lon",
      "tot_fishers",
      "habitat",
      "vessel_type",
      "propulsion_gear",
      "trip_duration",
      "gear",
      "catch_use",
      "catch_price",
      "catch_df"
    )

  logger::log_info("Uploading landings data to mongodb")
  # upload preprocessed landings
  mdb_collection_push(
    data = export_data,
    connection_string = conf$storage$mongodb$connection_string$main,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$validated,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}


#' Export Lurio landings data
#'
#' Processes validated landings data and exports multiple summary datasets to MongoDB.
#' Calculates fishery performance metrics (CPUE, RPUE), aggregates data by site and taxa,
#' and creates visualization-ready formats for the monitoring portal.
#'
#' @details
#' Exports the following collections to MongoDB:
#' \itemize{
#'   \item monthly_metrics: Time series of catch and revenue metrics by district
#'   \item sites_stats: Landing site statistics and performance indicators
#'   \item taxa_length: Length frequency distributions by species
#'   \item taxa_sites: Top 5 taxa composition per landing site
#'   \item habitat_gears_list: CPUE/RPUE metrics by habitat and gear type
#' }
#'
#' @examples
#' \dontrun{
#' export_lurio_landings()
#' }
#'
#' @keywords workflow export
#' @export
#'
export_lurio_landings <- function() {
  conf <- read_config()

  target_form_id <- get_airtable_form_id(
    kobo_asset_id = conf$ingestion$`kobo-lurio`$asset_id,
    conf = conf
  )

  assets <-
    cloud_object_name(
      prefix = conf$airtable$assets,
      provider = conf$storage$google$key,
      version = "latest",
      extension = "rds",
      options = conf$storage$google$options_coasts
    ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options_coasts
    ) |>
    readr::read_rds() |>
    purrr::keep_at("taxa") |>
    purrr::map(
      ~ dplyr::filter(
        .x,
        stringr::str_detect(
          .data$form_id,
          paste0("(^|,\\s*)", !!target_form_id, "(\\s*,|$)")
        )
      )
    )

  validated_data <-
    download_parquet_from_cloud(
      prefix = conf$ingestion$`kobo-lurio`$validated_surveys$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    )

  indicators_df <-
    validated_data |>
    dplyr::select(
      "submission_id",
      "landing_date",
      "district",
      "landing_site",
      "habitat",
      "gear",
      "propulsion_gear",
      # "fuel_L",
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
          "landing_date",
          "district",
          "landing_site",
          "habitat",
          "gear",
          "vessel_type",
          "propulsion_gear",
          # "fuel_L",
          "trip_duration",
          "n_fishers",
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
    ) |>
    # TO DO: fix this in validation, we should not have infinite values
    dplyr::mutate(
      dplyr::across(
        .cols = c("cpue", "rpue", "cpue_day", "rpue_day"),
        ~ dplyr::if_else(is.infinite(.x), NA_real_, .x)
      )
    )

  monthly_metrics <-
    indicators_df |>
    dplyr::mutate(date = lubridate::floor_date(.data$landing_date, "month")) |>
    dplyr::group_by(.data$district, .data$date) |>
    dplyr::summarise(
      n = dplyr::n(),
      n_fishers = mean(.data$n_fishers),
      trip_duration = mean(.data$trip_duration),
      median_cpue = stats::median(.data$cpue, na.rm = TRUE),
      median_rpue = stats::median(.data$rpue, na.rm = TRUE),
      median_price_kg = stats::median(.data$price_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    tidyr::pivot_longer(
      -c("district", "date", "n"),
      names_to = "metric",
      values_to = "value"
    ) |>
    tidyr::complete(
      .data$district,
      date = seq(min(.data$date), max(.data$date), by = "month"),
      fill = list(
        n = NA,
        n_fishers = NA,
        trip_duration = NA,
        median_cpue = NA,
        median_rpue = NA,
        median_price_kg = NA
      )
    ) |>
    dplyr::mutate(
      date = lubridate::as_datetime(.data$date),
      district = stringr::str_to_title(.data$district),
      district = stringr::str_replace(.data$district, "_", " ")
    )

  sites_stats <-
    indicators_df |>
    dplyr::group_by(.data$district, .data$landing_site) |>
    dplyr::summarise(
      n_submissions = dplyr::n(),
      n_fishers = mean(.data$n_fishers, na.rm = TRUE),
      trip_duration_hrs = mean(.data$trip_duration),
      mean_catch_kg = mean(.data$tot_catch_kg, na.rm = TRUE),
      cpue_kg_fisher_hr = mean(.data$cpue, na.rm = TRUE),
      mean_catch_price_mzn = mean(.data$price_kg, na.rm = TRUE),
      price_per_kg_mzn = mean(.data$rpue, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::filter(.data$n_submissions > 3, !is.na(.data$landing_site))

  taxa_length <-
    validated_data |>
    dplyr::filter(!is.na(.data$catch_taxon)) |>
    dplyr::select(
      "submission_id",
      catch_taxon = "scientific_name",
      "individuals",
      length_class = "length"
    ) |>
    dplyr::filter(!is.na(.data$length_class) & !is.na(.data$catch_taxon)) |>
    dplyr::select("catch_taxon", "length_class") |>
    dplyr::left_join(
      assets$taxa |> dplyr::distinct(.data$scientific_name, .keep_all = TRUE),
      by = c("catch_taxon" = "scientific_name")
    ) |>
    dplyr::select(catch_taxon = "english_name", "length_class")

  taxa_sites <-
    validated_data |>
    dplyr::filter(!is.na(.data$landing_site) & !is.na(.data$catch_taxon)) |>
    dplyr::select(
      "landing_site",
      catch_taxon = "scientific_name",
      "catch_kg"
    ) |>
    dplyr::group_by(.data$landing_site, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$landing_site) |>
    dplyr::mutate(
      rank = dplyr::row_number(dplyr::desc(.data$catch_kg)),
      catch_taxon = dplyr::if_else(
        .data$rank <= 5,
        .data$catch_taxon,
        "Other"
      )
    ) |>
    dplyr::group_by(.data$landing_site, .data$catch_taxon) |>
    dplyr::summarise(
      catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$landing_site) |>
    dplyr::mutate(
      catch_percent = (.data$catch_kg / sum(.data$catch_kg, na.rm = TRUE)) *
        100
    ) |>
    dplyr::ungroup() |>
    dplyr::arrange(.data$landing_site, dplyr::desc(.data$catch_percent)) |>
    dplyr::left_join(
      assets$taxa |> dplyr::distinct(.data$scientific_name, .keep_all = TRUE),
      by = c("catch_taxon" = "scientific_name")
    ) |>
    dplyr::mutate(
      english_name = dplyr::if_else(
        .data$catch_taxon == "Other",
        "Other",
        .data$english_name
      )
    ) |>
    dplyr::select(
      "landing_site",
      catch_taxon = "english_name",
      "catch_kg",
      "catch_percent"
    ) |>
    dplyr::mutate(
      catch_taxon = dplyr::if_else(
        .data$catch_taxon == "Marine fishes nei",
        "Miscellaneous fish",
        .data$catch_taxon
      )
    )

  # Create the complete document structure
  habitat_gears_list <- list(
    cpue = create_metric_structure(indicators_df, "cpue"),
    rpue = create_metric_structure(indicators_df, "rpue")
  )

  # Convert to JSON
  indicators_json <- jsonlite::toJSON(
    habitat_gears_list,
    auto_unbox = TRUE,
    pretty = TRUE
  )

  # TO DO: replace below with a proper geojson approach using ADM3 features in the future
  # get gps from preprocessed surveys

  moz_geo <-
    cloud_object_name(
      prefix = "MOZ_regions",
      provider = conf$storage$google$key,
      options = conf$storage$google$options_coasts,
      extension = "geojson",
      version = "latest"
    ) |>
    download_cloud_file(
      provider = conf$storage$google$key,
      options = conf$storage$google$options_coasts,
    ) |>
    sf::st_read()

  sites_stats_map <-
    sites_stats |>
    dplyr::group_by(.data$district) |>
    dplyr::summarise(
      n_submissions = sum(.data$n_submissions, na.rm = TRUE),
      n_fishers = stats::median(.data$n_fishers, na.rm = TRUE),
      trip_duration = stats::median(.data$trip_duration_hrs, na.rm = TRUE),
      mean_catch_kg = stats::median(.data$mean_catch_kg, na.rm = TRUE),
      mean_cpue = stats::median(.data$cpue_kg_fisher_hr, na.rm = TRUE),
      mean_rpue = stats::median(.data$mean_catch_price_mzn, na.rm = TRUE),
      mean_price_kg = stats::median(.data$price_per_kg_mzn, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(district = tolower(.data$district)) |>
    dplyr::rename(region = "district")

  moz_geo_indicators <-
    moz_geo |>
    dplyr::left_join(
      sites_stats_map,
      by = "region"
    )

  dataframes_to_upload <- list(
    monthly_metrics = monthly_metrics,
    sites_stats = sites_stats,
    taxa_length = taxa_length,
    taxa_sites = taxa_sites,
    indicators_json = indicators_json,
    moz_geo_indicators = moz_geo_indicators
  )

  # Collection names
  collection_names <- list(
    monthly_metrics = "monthly-metrics",
    sites_stats = "sites-stats",
    taxa_length = "taxa-length",
    taxa_sites = "taxa-sites",
    indicators_json = "gear_habitat_metrics",
    moz_geo_indicators = "geo-indicators"
  )

  # Iterate over the dataframes and upload them
  purrr::walk2(
    .x = dataframes_to_upload,
    .y = collection_names,
    .f = ~ {
      logger::log_info(paste("Uploading", .y, "data to MongoDB"))
      mdb_collection_push(
        data = .x,
        connection_string = conf$storage$mongodb$connection_string$main,
        collection_name = .y,
        db_name = conf$storage$mongodb$pipeline$database_name
      )
    }
  )
}

#' Create metric structure for MongoDB/ApexCharts
#'
#' Transforms gear and habitat data into the nested JSON structure required by
#' ApexCharts for visualization. Aggregates metrics by habitat, ranks gears by
#' performance, and formats as series data.
#'
#' @param data Data frame containing gear, habitat, cpue, and rpue columns
#' @param metric_col Name of the metric column to structure ("cpue" or "rpue")
#'
#' @return List with nested structure: each habitat contains an array of gear/value pairs
#' @keywords export helper
#' @export
create_metric_structure <- function(data, metric_col) {
  data |>
    dplyr::select("gear", "habitat", "cpue", "rpue") |>
    tidyr::drop_na() |>
    dplyr::filter(
      !is.na(.data$gear) & !is.na(.data$habitat) & !is.na(.data[[metric_col]])
    ) |>
    dplyr::group_by(.data$habitat, .data$gear) |>
    dplyr::summarise(
      metric_value = stats::median(.data[[metric_col]], na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::group_by(.data$habitat) |>
    dplyr::arrange(.data$habitat, dplyr::desc(.data$metric_value)) |>
    dplyr::summarise(
      name = list(unique(.data$habitat)[1]),
      data = list(
        purrr::map2(
          .data$gear,
          .data$metric_value,
          ~ list(x = list(.x), y = list(.y))
        )
      ),
      .groups = "drop"
    ) |>
    purrr::transpose()
}
