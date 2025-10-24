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

  metadata_tables <- get_metadata()

  validated_data <-
    download_parquet_from_cloud(
      prefix = conf$ingestion$`kobo-lurio`$validated_surveys$file_prefix,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    dplyr::filter(.data$landing_date >= "2023-01-01")

  f_metrics <- calculate_fishery_metrics(validated_data)

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
      #"fuel_L",
      "trip_duration",
      "vessel_type",
      n_fishers = "tot_fishers",
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
          #"fuel_L",
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
      #"fuel_L",
      "trip_duration",
      "vessel_type",
      n_fishers = "tot_fishers",
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
          #"fuel_L",
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
      mean_length = mean(.data$lenght, na.rm = T),
      .groups = "drop"
    ) |>
    #dplyr::rename(interagency_code = "catch_taxon") |>
    #dplyr::left_join(metadata_tables$catch_groups, by = "alpha3_code") |>
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
    #TODO: add common names
    dplyr::rename(common_name = "catch_taxon")

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

  filename <- add_version("mozambique_fishery_metrics", extension = "parquet")

  upload_parquet_to_cloud(
    data = f_metrics,
    prefix = filename,
    provider = conf$storage$google$key,
    options = conf$storage$google$options_coasts
  )

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
        connection_string = conf$storage$mongodb$connection_string,
        collection_name = .y,
        db_name = "portal-dev"
      )
    }
  )

  ######

  export_data <-
    validated_data %>%
    #dplyr::filter(.data$survey_activity == 1) %>%
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
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_name$validated,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}
