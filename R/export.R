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
#' \code{\link{preprocess_landings}} for data preprocessing
#' \code{\link{calculate_catch}} for catch weight calculations
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

  preprocessed_data <-
    mdb_collection_pull(
      collection_name = conf$storage$mongodb$database$pipeline$collection_name$preprocessed,
      db_name = conf$storage$mongodb$database$pipeline$name,
      connection_string = conf$storage$mongodb$connection_string
    ) |>
    dplyr::as_tibble()
 
  catch_df <-
    preprocessed_data %>%
    dplyr::select("submission_id", "catch_df") %>%
    tidyr::unnest(.data$catch_df, keep_empty = TRUE) %>%
    dplyr::group_by(.data$submission_id) %>%
    dplyr::summarise(catch_gr = sum(.data$catch_gr, na.rm = T))


  export_data <-
    preprocessed_data %>%
    dplyr::filter(.data$survey_activity == 1) %>%
    dplyr::select(
      "submission_id", "landing_date", "district",
      "landing_site", "catch_outcome", "lat", "lon", "tot_fishers",
      "habitat", "vessel_type", "propulsion_gear",
      "trip_duration", "gear", "catch_use", "catch_price", "catch_df"
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
