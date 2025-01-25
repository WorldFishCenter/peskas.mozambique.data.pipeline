#' Validate Fisheries Data
#'
#' This function imports and validates preprocessed fisheries data from a MongoDB
#' collection. It conducts a series of validation checks to ensure data integrity,
#' including checks on \strong{catch weights} at both the taxon level and aggregated
#' level, and checks on catch \strong{prices}.
#'
#' @param log_threshold Logging threshold level (default: logger::DEBUG)
#' @return This function does not return a value. Instead, it processes the data and
#'   uploads the validated results to a MongoDB collection in the pipeline database.
#'
#' @details
#' The function performs the following main operations:
#' \enumerate{
#'   \item Pulls preprocessed landings data from the MongoDB collection specified in the config.
#'   \item Summarizes data by \code{submission_id} and \code{catch_taxon}, computing total
#'         \code{catch_kg}, \code{catch_estimate}, and keeping the first value of certain
#'         metadata fields (e.g., \code{landing_date}, \code{landing_site}, \code{gear}, etc.).
#'   \item Filters out records where \code{catch_outcome == 1} but \code{catch_gr == 0}.
#'   \item Validates:
#'         \itemize{
#'           \item \emph{Taxon-level catch} with \code{\link{validate_catch_taxa}}
#'           \item \emph{Total catch} with \code{\link{validate_total_catch}}
#'           \item \emph{Price} with \code{\link{validate_price}}
#'         }
#'   \item Calculates additional price-based checks, such as \emph{price per kg}.
#'   \item Merges all validation flags into a single dataset and constructs a combined
#'         alert column.
#'   \item Finally, it filters out any records with alerts, leaving only valid data,
#'         and uploads the filtered data back to another MongoDB collection (the
#'         "validated" collection).
#' }
#'
#' @note This function requires a configuration file to be present and readable by
#'   the \code{read_config} function, which should provide MongoDB connection details
#'   and parameters for validation.
#'
#' @examples
#' \dontrun{
#' validate_landings()
#' }
#'
#' @keywords workflow validation
#' @export
validate_landings <- function(log_threshold = logger::DEBUG) {
  conf <- read_config()

  preprocessed_landings <-
    mdb_collection_pull(
      collection_name = conf$storage$mongodb$database$pipeline$collection_name$preprocessed,
      db_name = conf$storage$mongodb$database$pipeline$name,
      connection_string = conf$storage$mongodb$connection_string
    ) |>
    dplyr::as_tibble()

  summarized_data <-
    preprocessed_landings %>%
    dplyr::filter(.data$survey_activity == 1) %>%
    tidyr::unnest("catch_df") %>%
    dplyr::select(
      "submission_id", "landing_date", "landing_site", "catch_outcome",
      "gear", "trip_duration", "tot_fishers", "catch_taxon",
      "length_class", "catch_gr", "catch_estimate", "catch_price"
    ) %>%
    dplyr::filter(!(.data$catch_gr == 0 & .data$catch_outcome == 1)) %>%
    dplyr::group_by(.data$submission_id, .data$catch_taxon) %>%
    dplyr::summarise(
      catch_kg = sum(.data$catch_gr, na.rm = TRUE) / 1000, # from grams to kg
      catch_estimate = sum(.data$catch_estimate, na.rm = TRUE),
      dplyr::across(
        c(
          "landing_date", "landing_site", "catch_outcome", "gear",
          "trip_duration", "tot_fishers", "length_class", "catch_price"
        ),
        dplyr::first
      ),
      .groups = "drop"
    ) |>
    dplyr::filter(.data$catch_outcome == 1)


  catch_alert <- validate_catch_taxa(summarized_data, k = 3, flag_value = 4)
  total_catch_alert <- validate_total_catch(summarized_data, k = 3, flag_value = 5)
  price_alert <- validate_price(summarized_data, k = 3, flag_value = 6)

  validated_vars <-
    catch_alert |>
    dplyr::left_join(total_catch_alert, by = "submission_id", suffix = c("", "_total")) |>
    dplyr::left_join(price_alert, by = "submission_id", suffix = c("", "_price")) |>
    dplyr::mutate(
      catch_price_usd = .data$catch_price * 0.016,
      price_per_kg = .data$catch_price_usd / .data$total_catch_kg,
      alert_price_kg = dplyr::case_when(
        .data$price_per_kg < 0.5 | .data$price_per_kg > 15 ~ 7,
        TRUE ~ NA_real_
      )
    ) |>
    dplyr::select(-"catch_price_usd", -"price_per_kg", -"catch_taxon_price")


  # Merge them back with summarized_data
  validated_data <- summarized_data %>%
    # drop the validated columns from summarized_data to avoid duplication:
    dplyr::select(-"catch_kg", -"catch_price") %>%
    dplyr::left_join(validated_vars, by = c("submission_id", "catch_taxon"))

  # unify alerts into one column or keep them separate
  validated_data <-
    validated_data %>%
    dplyr::mutate(
      combined_alerts = paste(
        dplyr::coalesce(as.character(.data$alert_catch), ""),
        dplyr::coalesce(as.character(.data$alert_total), ""),
        dplyr::coalesce(as.character(.data$alert_price), ""),
        # dplyr::coalesce(as.character(alert_price_kg), ""),
        sep = "-"
      ) %>%
        # Remove leading/trailing dashes:
        gsub("^-+|-+$", "", .) %>%
        # Compress multiple consecutive dashes into one
        gsub("-+", "-", .)
    ) |>
    dplyr::select(-c("alert_catch", "alert_total", "alert_price", "alert_price_kg"))

  # Keep only records with no alerts:
  filtered_data <-
    validated_data |>
    dplyr::filter(.data$combined_alerts == "") |>
    dplyr::select(-"combined_alerts")

  # add remaining preprocessed variables
  preprocessed_vars <-
    preprocessed_landings |>
    dplyr::select("submission_id", "district", "lat", "lon", "habitat")

  filtered_data <-
    filtered_data |>
    dplyr::left_join(preprocessed_vars, by = "submission_id")

  mdb_collection_push(
    data = filtered_data,
    connection_string = conf$storage$mongodb$connection_string,
    collection_name = conf$storage$mongodb$database$pipeline$collection_nam$validated,
    db_name = conf$storage$mongodb$database$pipeline$name
  )
}
