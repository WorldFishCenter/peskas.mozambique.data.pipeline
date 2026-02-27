#' Download and Process Lurio Surveys from Kobotoolbox
#'
#' This function retrieves Lurio survey data from Kobotoolbox, processes it,
#' and uploads the raw data to Google Cloud Storage as Parquet files. It uses the
#' `get_kobo_data` function to retrieve survey submissions via the Kobotoolbox API.
#'
#' @return Invisible NULL. Function downloads data, processes it, and uploads to Google Cloud Storage.
#'
#' @details
#' The function performs the following steps:
#' 1. Reads configuration settings from config.yml
#' 2. Downloads survey data from Kobotoolbox using `get_kobo_data`
#' 3. Checks for uniqueness of submissions
#' 4. Flattens nested JSON data to tabular format
#' 5. Uploads raw data as versioned Parquet file to Google Cloud Storage
#'
#' @note The function uses configuration values from config.yml:
#' - Hardcoded URL: "eu.kobotoolbox.org"
#' - Hardcoded encoding: "UTF-8"
#' - Configuration values for: asset_id, username, password
#' - GCS bucket and credentials from configuration
#'
#' @keywords workflow ingestion
#' @export
#'
#' @examples
#' \dontrun{
#' ingest_landings_lurio()
#' }
ingest_landings_lurio <- function() {
  conf <- read_config()

  logger::log_info("Downloading Lurio Fish Catch Survey Kobo data...")

  data_raw <-
    coasts::get_kobo_data(
      url = "eu.kobotoolbox.org",
      assetid = conf$ingestion$`lurio`$asset_id,
      uname = conf$ingestion$`lurio`$username,
      pwd = conf$ingestion$`lurio`$password,
      encoding = "UTF-8",
      format = "json"
    )

  # Check that submissions are unique in case there is overlap in the pagination
  if (
    dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`)) != length(data_raw)
  ) {
    stop("Number of submission ids not the same as number of records")
  }

  logger::log_info(
    "Converting Lurio Fish Catch Survey Kobo data to tabular format..."
  )
  raw_survey <-
    purrr::map(data_raw, flatten_row) %>%
    dplyr::bind_rows() %>%
    dplyr::rename("submission_id" = .data$`_id`)

  logger::log_info("Uploading raw data to cloud storage")
  coasts::upload_parquet_to_cloud(
    data = raw_survey,
    prefix = conf$surveys$`lurio`$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  invisible(NULL)
}

#' Download and Process ADNAP Surveys from Kobotoolbox
#'
#' This function retrieves ADNAP survey data from Kobotoolbox, processes it,
#' and uploads the raw data to Google Cloud Storage as Parquet files. It uses the
#' `get_kobo_data` function to retrieve survey submissions via the Kobotoolbox API.
#'
#' @return Invisible NULL. Function downloads data, processes it, and uploads to Google Cloud Storage.
#'
#' @details
#' The function performs the following steps:
#' 1. Reads configuration settings from config.yml
#' 2. Downloads survey data from Kobotoolbox using `get_kobo_data`
#' 3. Checks for uniqueness of submissions
#' 4. Flattens nested JSON data to tabular format
#' 5. Uploads raw data as versioned Parquet file to Google Cloud Storage
#'
#' @note The function uses configuration values from config.yml:
#' - Hardcoded URL: "eu.kobotoolbox.org"
#' - Hardcoded encoding: "UTF-8"
#' - Configuration values for: asset_id, username, password (shared with Lurio)
#' - GCS bucket and credentials from configuration
#'
#' @keywords workflow ingestion
#' @export
#'
#' @examples
#' \dontrun{
#' ingest_landings_adnap()
#' }
ingest_landings_adnap <- function() {
  conf <- read_config()

  logger::log_info("Downloading WCS Fish Catch Survey Kobo data...")
  data_raw <-
    coasts::get_kobo_data(
      url = "eu.kobotoolbox.org",
      assetid = conf$ingestion$`adnap`$asset_id,
      uname = conf$ingestion$`lurio`$username,
      pwd = conf$ingestion$`lurio`$password,
      encoding = "UTF-8",
      format = "json"
    )

  # Check that submissions are unique in case there is overlap in the pagination
  if (
    dplyr::n_distinct(purrr::map_dbl(data_raw, ~ .$`_id`)) != length(data_raw)
  ) {
    stop("Number of submission ids not the same as number of records")
  }

  logger::log_info(
    "Converting WCS Fish Catch Survey Kobo data to tabular format..."
  )
  raw_survey <-
    purrr::map(data_raw, flatten_row) %>%
    dplyr::bind_rows() %>%
    dplyr::rename("submission_id" = .data$`_id`)

  logger::log_info("Uploading raw data to cloud storage")
  coasts::upload_parquet_to_cloud(
    data = raw_survey,
    prefix = conf$surveys$`adnap`$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  invisible(NULL)
}


#' Flatten Survey Data Rows
#'
#' Transforms each row of nested survey data into a flat tabular format using a mapping and flattening process.
#'
#' @param x A list representing a row of data, potentially containing nested lists or vectors.
#' @return A tibble with each row representing flattened survey data.
#' @keywords internal
#' @export
flatten_row <- function(x) {
  x %>%
    # Each row is composed of several fields
    purrr::imap(flatten_field) %>%
    rlang::squash() %>%
    # Remove NULL values before creating tibble
    purrr::compact() %>%
    tibble::as_tibble(.name_repair = "unique")
}

#' Flatten Survey Data Fields
#'
#' Processes each field within a row of survey data, handling both simple vectors and nested lists. For lists with named elements, renames and unlists them for flat structure preparation.
#'
#' @param x A vector or list representing a field in the data.
#' @param p The prefix or name associated with the field, used for naming during the flattening process.
#' @return Modified field, either unchanged, unnested, or appropriately renamed.
#' @keywords internal
#' @export
flatten_field <- function(x, p) {
  # If the field is a simple vector do nothing but if the field is a list we
  # need more logic
  if (inherits(x, "list")) {
    if (length(x) > 0) {
      if (purrr::vec_depth(x) == 2) {
        # If the field-list has named elements is we just need to rename the list
        x <- list(x) %>%
          rlang::set_names(p) %>%
          unlist() %>%
          as.list()
      } else {
        # If the field-list is an "array" we need to iterate over its children
        x <- purrr::imap(x, rename_child, p = p)
      }
    } else {
      # Handle empty lists by returning NULL (will be removed by compact)
      return(NULL)
    }
  } else {
    if (is.null(x)) x <- NA
  }
  x
}

#' Rename Nested Survey Data Elements
#'
#' Appends a parent name or index to child elements within a nested list, assisting in creating a coherent and traceable data structure during the flattening process.
#'
#' @param x A list element, possibly nested, to be renamed.
#' @param i The index or key of the element within the parent list.
#' @param p The parent name to prepend to the element's existing name for context.
#' @return A renamed list element, structured to maintain contextual relevance in a flattened dataset.
#' @keywords internal
#' @export
rename_child <- function(x, i, p) {
  if (length(x) == 0) {
    if (is.null(x)) {
      x <- NA
    }
    x <- list(x)
    x <- rlang::set_names(x, paste(p, i - 1, sep = "."))
  } else {
    if (inherits(i, "character")) {
      x <- rlang::set_names(x, paste(p, i, sep = "."))
    } else if (inherits(i, "integer")) {
      x <- rlang::set_names(x, paste(p, i - 1, names(x), sep = "."))
    }
  }
  x
}
