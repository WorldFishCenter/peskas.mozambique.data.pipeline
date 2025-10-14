#' Download Parquet File from Cloud Storage
#'
#' This function downloads a Parquet file from cloud storage and loads it as a data frame.
#'
#' @param prefix A character string specifying the file prefix path in cloud storage.
#' @param provider A character string specifying the cloud storage provider key.
#' @param options A named list of cloud storage provider options.
#'
#' @return A tibble containing the data from the Parquet file.
#'
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Download survey data
#' data <- download_parquet_from_cloud(
#'   prefix = "raw-data/survey-data",
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options
#' )
#' }
#'
#' @export
download_parquet_from_cloud <- function(prefix, provider, options) {
  # Generate cloud object name
  parquet_file <- cloud_object_name(
    prefix = prefix,
    provider = provider,
    extension = "parquet",
    options = options
  )

  # Log and download file
  logger::log_info("Retrieving {parquet_file}")
  download_cloud_file(
    name = parquet_file,
    provider = provider,
    options = options
  )

  # Read parquet file
  arrow::read_parquet(file = parquet_file)
}

#' Upload Data as Parquet File to Cloud Storage
#'
#' This function uploads a data frame as a Parquet file to cloud storage
#' with versioning support.
#'
#' @param data A data frame containing the data to be uploaded.
#' @param prefix A character string specifying the file prefix path.
#' @param provider A character string specifying the cloud storage provider key.
#' @param options A named list of cloud storage provider options.
#' @param compression A character string specifying compression type. Default is "lz4".
#' @param compression_level An integer specifying compression level. Default is 12.
#'
#' @return NULL (called for side effects)
#'
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Upload survey data
#' upload_parquet_to_cloud(
#'   data = survey_data,
#'   prefix = "raw-data/survey-data",
#'   provider = conf$storage$google$key,
#'   options = conf$storage$google$options
#' )
#' }
#'
#' @export
upload_parquet_to_cloud <- function(
  data,
  prefix,
  provider,
  options,
  compression = "lz4",
  compression_level = 12
) {
  # Generate filename with version
  preprocessed_filename <- prefix %>%
    add_version(extension = "parquet")

  # Write parquet file
  arrow::write_parquet(
    x = data,
    sink = preprocessed_filename,
    compression = compression,
    compression_level = compression_level
  )

  # Log and upload file
  logger::log_info("Uploading {preprocessed_filename} to cloud storage")
  upload_cloud_file(
    file = preprocessed_filename,
    provider = provider,
    options = options
  )

  invisible(NULL)
}

#' Upload File to Cloud Storage
#'
#' This function uploads one or more files to cloud storage.
#'
#' @param file A character vector of file paths to upload.
#' @param provider A character string specifying the cloud storage provider.
#' @param options A named list of cloud storage provider options.
#' @param name A character vector of names to assign files in cloud storage.
#'   Defaults to local filenames.
#'
#' @return A list of upload metadata.
#'
#' @keywords storage
#' @export
#'
#' @examples
#' \dontrun{
#' upload_cloud_file(
#'   file = "data.parquet",
#'   provider = "gcs",
#'   options = list(bucket = "my-bucket"),
#'   name = "data/processed.parquet"
#' )
#' }
upload_cloud_file <- function(file, provider, options, name = file) {
  cloud_storage_authenticate(provider, options)

  out <- list()
  if ("gcs" %in% provider) {
    # Iterate over multiple files (and names)
    google_output <- purrr::map2(
      file,
      name,
      ~ googleCloudStorageR::gcs_upload(
        file = .x,
        bucket = options$bucket,
        name = .y,
        predefinedAcl = "bucketLevel"
      )
    )

    out <- c(out, google_output)
  }

  out
}

#' Get metadata tables
#'
#' Get Metadata tables from Google sheets. This function downloads
#' the tables include information about the fishery.
#'
#' The parameters needed in `conf.yml` are:
#'
#' ```
#' storage:
#'   storage_name:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' @param log_threshold The logging threshold level. Default is logger::DEBUG.
#'
#' @export
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Ensure you have the necessary configuration in conf.yml
#' metadata_tables <- get_metadata()
#' }
get_metadata <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Authenticating for google drive")
  googlesheets4::gs4_auth(
    path = conf$storage$google$options$service_account_key,
    use_oob = TRUE
  )
  logger::log_info("Downloading metadata tables")

  tables <-
    conf$metadata$google_sheets$tables %>%
    rlang::set_names() %>%
    purrr::map(
      ~ googlesheets4::range_read(
        ss = conf$metadata$google_sheets$sheet_id,
        sheet = .x,
        col_types = "c"
      )
    )

  tables
}

#' Authenticate to a Cloud Storage Provider
#'
#' This function authenticates to a cloud storage provider using the specified
#' authentication method. Currently supports Google Cloud Storage (GCS) and
#' Amazon Web Services (AWS).
#'
#' @param provider A character string specifying the cloud provider. Currently
#'   supports "gcs" for Google Cloud Storage or "aws" for Amazon Web Services.
#' @param options A named list of options specific to the cloud provider. For
#'   GCS, this should include `service_account_key` with the contents of the
#'   authentication JSON file from your Google Project.
#'
#' @return NULL (called for side effects)
#'
#' @keywords storage
#' @export
#'
#' @examples
#' \dontrun{
#' # Authenticate with Google Cloud Storage
#' authentication_details <- readLines("path/to/json_file.json")
#' cloud_storage_authenticate("gcs", list(service_account_key = authentication_details))
#' }
cloud_storage_authenticate <- function(provider, options) {
  if (provider == "gcs") {
    logger::log_info("Authenticating with Google Cloud Storage...")

    # Create temporary file for service account key
    temp_key_file <- tempfile(fileext = ".json")
    writeLines(options$service_account_key, temp_key_file)

    # Authenticate
    googleCloudStorageR::gcs_auth(json_file = temp_key_file)

    # Clean up temporary file
    unlink(temp_key_file)

    logger::log_info("Successfully authenticated with Google Cloud Storage")
  } else if (provider == "aws") {
    stop("AWS authentication not yet implemented")
  } else {
    stop("Unsupported cloud provider: ", provider)
  }

  invisible(NULL)
}

#' Download File from Cloud Storage
#'
#' This function downloads one or more files from cloud storage.
#'
#' @param name A character vector of object names in cloud storage.
#' @param provider A character string specifying the cloud storage provider.
#' @param options A named list of cloud storage provider options.
#' @param file A character vector of local file paths. Defaults to object names.
#'
#' @return A character vector of local file paths.
#'
#' @keywords storage
#' @export
#'
#' @examples
#' \dontrun{
#' download_cloud_file(
#'   name = "data/processed.parquet",
#'   provider = "gcs",
#'   options = list(bucket = "my-bucket"),
#'   file = "local_data.parquet"
#' )
#' }
download_cloud_file <- function(name, provider, options, file = name) {
  cloud_storage_authenticate(provider, options)

  if ("gcs" %in% provider) {
    purrr::map2(
      name,
      file,
      ~ googleCloudStorageR::gcs_get_object(
        object_name = .x,
        bucket = options$bucket,
        saveToDisk = .y,
        overwrite = ifelse(is.null(options$overwrite), TRUE, options$overwrite)
      )
    )
  }

  file
}

#' Generate Cloud Object Name
#'
#' This function generates the full object name for a file in cloud storage,
#' finding the latest version if not specified.
#'
#' @param prefix A character string specifying the file prefix.
#' @param version A character string specifying the version. Default is "latest".
#' @param extension A character string specifying the file extension.
#' @param provider A character string specifying the cloud storage provider.
#' @param exact_match A logical value indicating whether to match prefix exactly.
#' @param options A named list of cloud storage provider options.
#'
#' @return A character string with the full object name.
#'
#' @keywords storage internal
cloud_object_name <- function(
  prefix,
  version = "latest",
  extension = "",
  provider,
  exact_match = FALSE,
  options
) {
  cloud_storage_authenticate(provider, options)

  if ("gcs" %in% provider) {
    gcs_files <- googleCloudStorageR::gcs_list_objects(
      bucket = options$bucket,
      prefix = prefix
    )

    if (nrow(gcs_files) == 0) {
      return(character(0))
    }

    gcs_files_formatted <- gcs_files %>%
      tidyr::separate(
        col = .data$name,
        into = c("base_name", "version", "ext"),
        # Version is separated with the "__" string
        sep = "__",
        remove = FALSE
      ) %>%
      dplyr::filter(stringr::str_detect(.data$ext, paste0(extension, "$"))) %>%
      dplyr::group_by(.data$base_name, .data$ext)

    if (isTRUE(exact_match)) {
      selected_rows <- gcs_files_formatted %>%
        dplyr::filter(.data$base_name == prefix)
    } else {
      selected_rows <- gcs_files_formatted
    }

    if (version == "latest") {
      selected_rows <- selected_rows %>%
        dplyr::filter(max(.data$updated) == .data$updated)
    } else {
      this_version <- version
      selected_rows <- selected_rows %>%
        dplyr::filter(.data$version == this_version)
    }

    return(selected_rows$name[1])
  }
}

#' Retrieve Data from MongoDB
#'
#' This function connects to a MongoDB database and retrieves all documents from a specified collection,
#' maintaining the original column order if available.
#'
#' @param connection_string A character string specifying the MongoDB connection URL. Default is NULL.
#' @param collection_name A character string specifying the name of the collection to query. Default is NULL.
#' @param db_name A character string specifying the name of the database. Default is NULL.
#'
#' @return A data frame containing all documents from the specified collection, with columns ordered
#'         as they were when the data was originally pushed to MongoDB.
#'
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Retrieve data from a MongoDB collection
#' result <- mdb_collection_pull(
#'   connection_string = "mongodb://localhost:27017",
#'   collection_name = "my_collection",
#'   db_name = "my_database"
#' )
#' }
#'
#' @export
mdb_collection_pull <- function(
  connection_string = NULL,
  collection_name = NULL,
  db_name = NULL
) {
  # Connect to the MongoDB collection
  collection <- mongolite::mongo(
    collection = collection_name,
    db = db_name,
    url = connection_string
  )

  # Retrieve the metadata document
  metadata <- collection$find(query = '{"type": "metadata"}')

  # Retrieve all data documents
  data <- collection$find(query = '{"type": {"$ne": "metadata"}}')

  if (nrow(metadata) > 0 && "columns" %in% names(metadata)) {
    stored_columns <- metadata$columns[[1]]

    # Ensure all stored columns exist in the data
    for (col in stored_columns) {
      if (!(col %in% names(data))) {
        data[[col]] <- NA
      }
    }

    # Reorder columns to match stored order, and include any extra columns at the end
    data <- data[, c(stored_columns, setdiff(names(data), stored_columns))]
  }

  return(data)
}

#' Upload Data to MongoDB and Overwrite Existing Content
#'
#' This function connects to a MongoDB database, removes all existing documents
#' from a specified collection, and then inserts new data. It also stores the
#' original column order to maintain data structure consistency.
#'
#' @param data A data frame containing the data to be uploaded.
#' @param connection_string A character string specifying the MongoDB connection URL.
#' @param collection_name A character string specifying the name of the collection.
#' @param db_name A character string specifying the name of the database.
#'
#' @return The number of data documents inserted into the collection (excluding the order document).
#'
#' @keywords storage
#'
#' @examples
#' \dontrun{
#' # Upload and overwrite data in a MongoDB collection
#' result <- mdb_collection_push(
#'   data = processed_legacy_landings,
#'   connection_string = "mongodb://localhost:27017",
#'   collection_name = "my_collection",
#'   db_name = "my_database"
#' )
#' }
#'
#' @export
mdb_collection_push <- function(
  data = NULL,
  connection_string = NULL,
  collection_name = NULL,
  db_name = NULL
) {
  # Connect to the MongoDB collection
  collection <- mongolite::mongo(
    collection = collection_name,
    db = db_name,
    url = connection_string
  )

  # Remove all existing documents in the collection
  collection$remove("{}")

  # Create a metadata document with column information
  metadata <- list(
    type = "metadata",
    columns = names(data),
    timestamp = Sys.time()
  )

  # Insert the metadata document first
  collection$insert(metadata)

  # Insert the new data
  collection$insert(data)

  # Return the number of documents in the collection (excluding the metadata document)
  return(collection$count() - 1)
}

#' Retrieve Trip Details from Pelagic Data API
#'
#' This function retrieves trip details from the Pelagic Data API for a specified time range,
#' with options to filter by IMEIs and include additional information.
#'
#' @param token Character string. The API token for authentication.
#' @param secret Character string. The API secret for authentication.
#' @param dateFrom Character string. Start date in 'YYYY-MM-dd' format.
#' @param dateTo Character string. End date in 'YYYY-MM-dd' format.
#' @param imeis Character vector. Optional. Filter by IMEI numbers.
#' @param deviceInfo Logical. If TRUE, include device IMEI and ID fields in the response. Default is FALSE.
#' @param withLastSeen Logical. If TRUE, include device last seen date in the response. Default is FALSE.
#' @param tags Character vector. Optional. Filter by trip tags.
#'
#' @return A data frame containing trip details.
#' @keywords ingestion
#' @examples
#' \dontrun{
#' trips <- get_trips(
#'   token = "your_token",
#'   secret = "your_secret",
#'   dateFrom = "2020-05-01",
#'   dateTo = "2020-05-03",
#'   imeis = c("123456789", "987654321"),
#'   deviceInfo = TRUE,
#'   withLastSeen = TRUE,
#'   tags = c("tag1", "tag2")
#' )
#' }
#'
#' @export
#'
get_trips <- function(
  token = NULL,
  secret = NULL,
  dateFrom = NULL,
  dateTo = NULL,
  imeis = NULL,
  deviceInfo = FALSE,
  withLastSeen = FALSE,
  tags = NULL
) {
  # Base URL
  base_url <- paste0(
    "https://analytics.pelagicdata.com/api/",
    token,
    "/v1/trips/",
    dateFrom,
    "/",
    dateTo
  )

  # Build query parameters
  query_params <- list()
  if (!is.null(imeis)) {
    query_params$imeis <- paste(imeis, collapse = ",")
  }
  if (deviceInfo) {
    query_params$deviceInfo <- "true"
  }
  if (withLastSeen) {
    query_params$withLastSeen <- "true"
  }
  if (!is.null(tags)) {
    query_params$tags <- paste(tags, collapse = ",")
  }

  # Build the request
  req <- httr2::request(base_url) %>%
    httr2::req_headers(
      "X-API-SECRET" = secret,
      "Content-Type" = "application/json"
    ) %>%
    httr2::req_url_query(!!!query_params)

  # Perform the request
  resp <- req %>% httr2::req_perform()

  # Check for HTTP errors
  if (httr2::resp_status(resp) != 200) {
    stop(
      "Request failed with status: ",
      httr2::resp_status(resp),
      "\n",
      httr2::resp_body_string(resp)
    )
  }

  # Read CSV content
  content_text <- httr2::resp_body_string(resp)
  trips_data <- readr::read_csv(content_text, show_col_types = FALSE)

  return(trips_data)
}


#' Get Trip Points from Pelagic Data Systems API
#'
#' Retrieves trip points data from the Pelagic Data Systems API. The function can either
#' fetch data for a specific trip ID or for a date range. The response can be returned
#' as a data frame or written directly to a file.
#'
#' @param token Character string. Access token for the PDS API.
#' @param secret Character string. Secret key for the PDS API.
#' @param id Numeric or character. Optional trip ID. If provided, retrieves points for
#'   specific trip. If NULL, dateFrom and dateTo must be provided.
#' @param dateFrom Character string. Start date for data retrieval in format "YYYY-MM-DD".
#'   Required if id is NULL.
#' @param dateTo Character string. End date for data retrieval in format "YYYY-MM-DD".
#'   Required if id is NULL.
#' @param path Character string. Optional path where the CSV file should be saved.
#'   If provided, the function returns the path instead of the data frame.
#' @param imeis Vector of character or numeric. Optional IMEI numbers to filter the data.
#' @param deviceInfo Logical. If TRUE, includes device information in the response.
#'   Default is FALSE.
#' @param errant Logical. If TRUE, includes errant points in the response.
#'   Default is FALSE.
#' @param withLastSeen Logical. If TRUE, includes last seen information.
#'   Default is FALSE.
#' @param tags Vector of character. Optional tags to filter the data.
#' @param overwrite Logical. If TRUE, will overwrite existing file when path is provided.
#'   Default is TRUE.
#'
#' @return If path is NULL, returns a tibble containing the trip points data.
#'   If path is provided, returns the file path as a character string.
#'
#' @examples
#' \dontrun{
#' # Get data for a specific trip
#' trip_data <- get_trip_points(
#'   token = "your_token",
#'   secret = "your_secret",
#'   id = "12345",
#'   deviceInfo = TRUE
#' )
#'
#' # Get data for a date range
#' date_data <- get_trip_points(
#'   token = "your_token",
#'   secret = "your_secret",
#'   dateFrom = "2024-01-01",
#'   dateTo = "2024-01-31"
#' )
#'
#' # Save data directly to file
#' file_path <- get_trip_points(
#'   token = "your_token",
#'   secret = "your_secret",
#'   id = "12345",
#'   path = "trip_data.csv"
#' )
#' }
#'
#' @keywords ingestion
#'
#' @export
get_trip_points <- function(
  token = NULL,
  secret = NULL,
  id = NULL,
  dateFrom = NULL,
  dateTo = NULL,
  path = NULL,
  imeis = NULL,
  deviceInfo = FALSE,
  errant = FALSE,
  withLastSeen = FALSE,
  tags = NULL,
  overwrite = TRUE
) {
  # Build base URL based on whether ID is provided
  if (!is.null(id)) {
    base_url <- paste0(
      "https://analytics.pelagicdata.com/api/",
      token,
      "/v1/trips/",
      id,
      "/points"
    )
  } else {
    if (is.null(dateFrom) || is.null(dateTo)) {
      stop("dateFrom and dateTo are required when id is not provided")
    }
    base_url <- paste0(
      "https://analytics.pelagicdata.com/api/",
      token,
      "/v1/points/",
      dateFrom,
      "/",
      dateTo
    )
  }

  # Build query parameters
  query_params <- list()
  if (!is.null(imeis)) {
    query_params$imeis <- paste(imeis, collapse = ",")
  }
  if (deviceInfo) {
    query_params$deviceInfo <- "true"
  }
  if (errant) {
    query_params$errant <- "true"
  }
  if (withLastSeen) {
    query_params$withLastSeen <- "true"
  }
  if (!is.null(tags)) {
    query_params$tags <- paste(tags, collapse = ",")
  }
  # Add format=csv if saving to file
  if (!is.null(path)) {
    query_params$format <- "csv"
  }

  # Build the request
  req <- httr2::request(base_url) %>%
    httr2::req_headers(
      "X-API-SECRET" = secret,
      "Content-Type" = "application/json"
    ) %>%
    httr2::req_url_query(!!!query_params)

  # Perform the request
  resp <- req %>% httr2::req_perform()

  # Check for HTTP errors first
  if (httr2::resp_status(resp) != 200) {
    stop(
      "Request failed with status: ",
      httr2::resp_status(resp),
      "\n",
      httr2::resp_body_string(resp)
    )
  }

  # Handle the response based on whether path is provided
  if (!is.null(path)) {
    # Write the response content to file
    writeBin(httr2::resp_body_raw(resp), path)
    result <- path
  } else {
    # Read CSV content
    content_text <- httr2::resp_body_string(resp)
    result <- readr::read_csv(content_text, show_col_types = FALSE)
  }

  return(result)
}
