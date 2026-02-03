#' Merge GPS Tracker Trips with Validated Survey Landings
#'
#' Merges PDS trip data with validated survey landings. Joins only when there is
#' exactly one trip and one survey per device per day.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#'
#' @return Invisible NULL. Uploads merged data to Google Cloud Storage.
#'
#' @note Landing date is assumed to be the trip end date from PDS. Records with
#' multiple trips or surveys per device-day are included but not joined.
#'
#' @keywords workflow
#' @export
#'
#' @examples
#' \dontrun{
#' merge_trips()
#' }
merge_trips <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  logger::log_info("Getting trips file from cloud storage...")
  pds_trips_parquet <- cloud_object_name(
    prefix = conf$pds$pds_trips$file_prefix,
    provider = conf$storage$google$key,
    extension = "parquet",
    version = conf$pds$pds_trips$version,
    options = conf$storage$google$options
  )
  logger::log_info("Downloading {pds_trips_parquet}")

  pds <-
    download_cloud_file(
      name = pds_trips_parquet,
      provider = conf$storage$google$key,
      options = conf$storage$google$options
    ) |>
    arrow::read_parquet() |>
    janitor::clean_names() |>
    # We assume the landing date to be the same as the date when the trip ended
    dplyr::mutate(
      landing_date = lubridate::as_date(.data$ended),
      imei = as.character(.data$imei)
    ) %>%
    dplyr::group_by(.data$landing_date, .data$imei) %>%
    dplyr::mutate(unique_trip_per_day = dplyr::n() == 1) %>%
    dplyr::ungroup() %>%
    split(.$unique_trip_per_day)

  logger::log_info("Loading validated surveys...")
  landings <- download_parquet_from_cloud(
    prefix = conf$ingestion$`kobo-adnap`$validated_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  ) |>
    dplyr::rename(imei = "boat_pds") |>
    dplyr::group_by(.data$landing_date, .data$imei) %>%
    dplyr::mutate(unique_trip_per_day = dplyr::n() == 1) %>%
    dplyr::ungroup() %>%
    split(.$unique_trip_per_day)

  logger::log_info("Merging datasets datasets...")
  # Only join when we have one landing and one tracking per day, otherwise we
  # cannot do guarantee that the landing corresponds to a trip
  merged_trips <- dplyr::full_join(
    landings$`TRUE`,
    pds$`TRUE`,
    by = c("landing_date", "imei", "unique_trip_per_day")
  ) |>
    dplyr::bind_rows(landings$`FALSE`) %>%
    dplyr::bind_rows(pds$`FALSE`) %>%
    dplyr::select(-"unique_trip_per_day")

  logger::log_info("Uploading trips to cloud storage...")
  upload_parquet_to_cloud(
    data = merged_trips,
    prefix = conf$ingestion$`kobo-adnap`$merged_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}
