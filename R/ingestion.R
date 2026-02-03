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
    get_kobo_data(
      url = "eu.kobotoolbox.org",
      assetid = conf$ingestion$`kobo-lurio`$asset_id,
      uname = conf$ingestion$`kobo-lurio`$username,
      pwd = conf$ingestion$`kobo-lurio`$password,
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
  upload_parquet_to_cloud(
    data = raw_survey,
    prefix = conf$ingestion$`kobo-lurio`$raw_surveys$file_prefix,
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
    get_kobo_data(
      url = "eu.kobotoolbox.org",
      assetid = conf$ingestion$`kobo-adnap`$asset_id,
      uname = conf$ingestion$`kobo-lurio`$username,
      pwd = conf$ingestion$`kobo-lurio`$password,
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
  upload_parquet_to_cloud(
    data = raw_survey,
    prefix = conf$ingestion$`kobo-adnap`$raw_surveys$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  invisible(NULL)
}

#' Retrieve Data from Kobotoolbox API
#'
#' This function retrieves survey data from Kobotoolbox API for a specific asset.
#' It supports pagination and handles both JSON and XML formats.
#'
#' @param assetid The asset ID of the Kobotoolbox form.
#' @param url The URL of Kobotoolbox (default is "eu.kobotoolbox.org").
#' @param uname Username for Kobotoolbox account.
#' @param pwd Password for Kobotoolbox account.
#' @param encoding Encoding to be used for data retrieval (default is "UTF-8").
#' @param format Format of the data to retrieve, either "json" or "xml" (default is "json").
#'
#' @return A list containing all retrieved survey results.
#' @keywords ingestion
#' @details
#' The function uses pagination to retrieve large datasets, with a limit of 30,000 records per request.
#' It continues to fetch data until all records are retrieved or an error occurs.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' kobo_data <- get_kobo_data(
#'   assetid = "your_asset_id",
#'   uname = "your_username",
#'   pwd = "your_password"
#' )
#' }
get_kobo_data <- function(
  assetid,
  url = "eu.kobotoolbox.org",
  uname = NULL,
  pwd = NULL,
  encoding = "UTF-8",
  format = "json"
) {
  if (!is.character(url)) {
    stop("URL entered is not a string")
  }
  if (!is.character(uname)) {
    stop("uname (username) entered is not a string")
  }
  if (!is.character(pwd)) {
    stop("pwd (password) entered is not a string")
  }
  if (!is.character(assetid)) {
    stop("assetid entered is not a string")
  }
  if (is.null(url) | url == "") {
    stop("URL empty")
  }
  if (is.null(uname) | uname == "") {
    stop("uname (username) empty")
  }
  if (is.null(pwd) | pwd == "") {
    stop("pwd (password) empty")
  }
  if (is.null(assetid) | assetid == "") {
    stop("assetid empty")
  }
  if (!format %in% c("json", "xml")) {
    stop("format must be either 'json' or 'xml'")
  }

  base_url <- paste0(
    "https://",
    url,
    "/api/v2/assets/",
    assetid,
    "/data.",
    format
  )

  message("Starting data retrieval from ", base_url)

  get_page <- function(url, limit = 30000, start = 0) {
    full_url <- paste0(url, "?limit=", limit, "&start=", start)

    message("Retrieving page starting at record ", start)

    respon.kpi <- tryCatch(
      expr = {
        httr2::request(full_url) |>
          httr2::req_auth_basic(uname, pwd) |>
          httr2::req_perform()
      },
      error = function(x) {
        message(
          "Error on page starting at record ",
          start,
          ". Please try again or check the input parameters."
        )
        return(NULL)
      }
    )

    if (!is.null(respon.kpi)) {
      content_type <- httr2::resp_content_type(respon.kpi)

      if (grepl("json", content_type)) {
        message("Successfully retrieved JSON data starting at record ", start)
        return(httr2::resp_body_json(respon.kpi, encoding = encoding))
      } else if (grepl("xml", content_type)) {
        message("Successfully retrieved XML data starting at record ", start)
        return(httr2::resp_body_string(respon.kpi, encoding = encoding))
      } else if (grepl("html", content_type)) {
        warning(
          "Unexpected HTML response for start ",
          start,
          ". Unable to parse."
        )
        return(NULL)
      } else {
        warning(
          "Unexpected content type: ",
          content_type,
          " for start ",
          start,
          ". Unable to parse."
        )
        return(NULL)
      }
    } else {
      return(NULL)
    }
  }

  all_results <- list()
  start <- 0
  limit <- 30000
  get_next <- TRUE

  while (get_next) {
    page_results <- get_page(base_url, limit, start)

    if (is.null(page_results)) {
      message("Error occurred. Stopping data retrieval.")
      break
    }

    new_results <- page_results$results
    all_results <- c(all_results, new_results)

    message("Total records retrieved so far: ", length(all_results))

    if (length(new_results) < limit) {
      message("Retrieved all available records.")
      get_next <- FALSE
    } else {
      start <- start + limit
    }
  }

  message(
    "Data retrieval complete. Total records retrieved: ",
    length(all_results)
  )

  # Check for unique submission IDs
  submission_ids <- sapply(all_results, function(x) x$`_id`)
  if (length(unique(submission_ids)) != length(all_results)) {
    warning(
      "Number of unique submission IDs does not match the number of records. There may be duplicates."
    )
  }

  return(all_results)
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

#' Ingest Pelagic Data Systems (PDS) Trip Data
#'
#' @description
#' This function handles the automated ingestion of GPS boat trip data from Pelagic Data Systems (PDS).
#' It performs the following operations:
#' 1. Retrieves device metadata from the configured source
#' 2. Downloads trip data from PDS API using device IMEIs
#' 3. Converts the data to parquet format
#' 4. Uploads the processed file to configured cloud storage
#'
#' @details
#' The function requires specific configuration in the `conf.yml` file with the following structure:
#'
#' ```yaml
#' pds:
#'   token: "your_pds_token"               # PDS API token
#'   secret: "your_pds_secret"             # PDS API secret
#'   pds_trips:
#'     file_prefix: "pds_trips"            # Prefix for output files
#' storage:
#'   google:                               # Storage provider name
#'     key: "google"                       # Storage provider identifier
#'     options:
#'       project: "project-id"             # Cloud project ID
#'       bucket: "bucket-name"             # Storage bucket name
#'       service_account_key: "path/to/key.json"
#' ```
#'
#' The function processes trips sequentially:
#' - Retrieves device metadata using `get_metadata()`
#' - Downloads trip data using the `get_trips()` function
#' - Converts the data to parquet format
#' - Uploads the resulting file to configured storage provider
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#'   See `logger::log_levels` for available options.
#'
#' @return None (invisible). The function performs its operations for side effects:
#'   - Creates a parquet file locally with trip data
#'   - Uploads file to configured cloud storage
#'   - Generates logs of the process
#'
#' @examples
#' \dontrun{
#' # Run with default debug logging
#' ingest_pds_trips()
#'
#' # Run with info-level logging only
#' ingest_pds_trips(logger::INFO)
#' }
#'
#' @seealso
#' * [get_trips()] for details on the PDS trip data retrieval process
#' * [get_metadata()] for details on the device metadata retrieval
#' * [upload_cloud_file()] for details on the cloud upload process
#'
#' @keywords workflow ingestion
#' @export
ingest_pds_trips <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  logger::log_info("Loading device registry...")
  devices <- cloud_object_name(
    prefix = conf$metadata$airtable$assets,
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
    purrr::pluck("devices") |>
    dplyr::filter(
      .data$customer_name %in%
        c("WorldFish - Mozambique", "Syberintel - distributor")
    )

  boats_trips <- get_trips(
    token = conf$pds$token,
    secret = conf$pds$secret,
    dateFrom = "2025-01-01",
    dateTo = Sys.Date(),
    deviceInfo = TRUE,
    imeis = unique(devices$imei)
  )

  filename <- conf$pds$pds_trips$file_prefix %>%
    add_version(extension = "parquet")

  arrow::write_parquet(
    x = boats_trips,
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
#' Ingest Pelagic Data Systems (PDS) Track Data
#'
#' @description
#' This function handles the automated ingestion of GPS boat track data from Pelagic Data Systems (PDS).
#' It downloads and stores only new tracks that haven't been previously uploaded to Google Cloud Storage.
#' Uses parallel processing for improved performance.
#'
#' @param log_threshold The logging threshold to use. Default is logger::DEBUG.
#' @param batch_size Optional number of tracks to process. If NULL, processes all new tracks.
#'
#' @return None (invisible). The function performs its operations for side effects.
#'
#' @keywords workflow ingestion
#' @export
ingest_pds_tracks <- function(
  log_threshold = logger::DEBUG,
  batch_size = NULL
) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  # Get trips file from cloud storage
  logger::log_info("Getting trips file from cloud storage...")
  pds_trips_parquet <- cloud_object_name(
    prefix = conf$pds$pds_trips$file_prefix,
    provider = conf$storage$google$key,
    extension = "parquet",
    version = conf$pds$pds_trips$version,
    options = conf$storage$google$options
  )

  logger::log_info("Downloading {pds_trips_parquet}")
  download_cloud_file(
    name = pds_trips_parquet,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )

  # Read trip IDs
  logger::log_info("Reading trip IDs...")
  trips_data <- arrow::read_parquet(file = pds_trips_parquet) %>%
    dplyr::pull("Trip") %>%
    unique()

  # Clean up downloaded file
  unlink(pds_trips_parquet)

  # List existing files in GCS bucket
  logger::log_info("Checking existing tracks in cloud storage...")
  existing_tracks <-
    googleCloudStorageR::gcs_list_objects(
      bucket = conf$pds_storage$google$options$bucket,
      prefix = conf$pds$pds_tracks$file_prefix
    )$name

  # Get new trip IDs
  existing_trip_ids <- extract_trip_ids_from_filenames(existing_tracks)
  new_trip_ids <- setdiff(trips_data, existing_trip_ids)

  if (length(new_trip_ids) == 0) {
    logger::log_info("No new tracks to download")
    return(invisible())
  }

  # Setup parallel processing
  workers <- parallel::detectCores() - 1
  logger::log_info("Setting up parallel processing with {workers} workers...")
  future::plan(future::multisession, workers = workers)

  # Select tracks to process
  process_ids <- if (!is.null(batch_size)) {
    new_trip_ids[1:batch_size]
  } else {
    new_trip_ids
  }
  logger::log_info("Processing {length(process_ids)} new tracks in parallel...")

  # Process tracks in parallel with progress bar
  results <- furrr::future_map(
    process_ids,
    function(trip_id) {
      tryCatch(
        {
          # Create filename for this track
          track_filename <- sprintf(
            "%s_%s.parquet",
            conf$pds$pds_tracks$file_prefix,
            trip_id
          )

          # Get track data
          track_data <- get_trip_points(
            token = conf$pds$token,
            secret = conf$pds$secret,
            id = as.character(trip_id),
            deviceInfo = TRUE
          )

          # Save to parquet
          arrow::write_parquet(
            x = track_data,
            sink = track_filename,
            compression = "lz4",
            compression_level = 12
          )

          # Upload to cloud
          logger::log_info("Uploading track for trip {trip_id}")
          upload_cloud_file(
            file = track_filename,
            provider = conf$pds_storage$google$key,
            options = conf$pds_storage$google$options
          )

          # Clean up local file
          unlink(track_filename)

          list(
            status = "success",
            trip_id = trip_id,
            message = "Successfully processed"
          )
        },
        error = function(e) {
          list(
            status = "error",
            trip_id = trip_id,
            message = e$message
          )
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE),
    .progress = TRUE
  )

  # Clean up parallel processing
  future::plan(future::sequential)

  # Summarize results
  successes <- sum(purrr::map_chr(results, "status") == "success")
  failures <- sum(purrr::map_chr(results, "status") == "error")

  logger::log_info(
    "Processing complete. Successfully processed {successes} tracks."
  )
  if (failures > 0) {
    logger::log_warn("Failed to process {failures} tracks.")
    failed_results <- results[purrr::map_chr(results, "status") == "error"]
    failed_trips <- purrr::map_chr(failed_results, "trip_id")
    failed_messages <- purrr::map_chr(failed_results, "message")

    logger::log_warn("Failed trip IDs and reasons:")
    purrr::walk2(
      failed_trips,
      failed_messages,
      ~ logger::log_warn("Trip {.x}: {.y}")
    )
  }
}

#' Extract Trip IDs from Track Filenames
#'
#' @param filenames Character vector of track filenames
#' @return Character vector of trip IDs
#' @keywords internal
extract_trip_ids_from_filenames <- function(filenames) {
  if (length(filenames) == 0) {
    return(character(0))
  }
  # Assuming filenames are in format: pds-tracks_TRIPID.parquet
  gsub(".*_([0-9]+)\\.parquet$", "\\1", filenames)
}

#' Process Single PDS Track
#'
#' @param trip_id Character. The ID of the trip to process.
#' @param conf List. Configuration parameters.
#' @return List with processing status and details.
#' @keywords internal
process_single_track <- function(trip_id, conf) {
  tryCatch(
    {
      # Create filename for this track
      track_filename <- sprintf(
        "%s_%s.parquet",
        conf$pds$pds_tracks$file_prefix,
        trip_id
      )

      # Get track data
      track_data <- get_trip_points(
        token = conf$pds$token,
        secret = conf$pds$secret,
        id = trip_id,
        deviceInfo = TRUE
      )

      # Save to parquet
      arrow::write_parquet(
        x = track_data,
        sink = track_filename,
        compression = "lz4",
        compression_level = 12
      )

      # Upload to cloud
      logger::log_info("Uploading track for trip {trip_id}")
      upload_cloud_file(
        file = track_filename,
        provider = conf$pds_storage$google$key,
        options = conf$pds_storage$google$options
      )

      # Clean up local file
      unlink(track_filename)

      list(
        status = "success",
        trip_id = trip_id,
        message = "Successfully processed"
      )
    },
    error = function(e) {
      list(
        status = "error",
        trip_id = trip_id,
        message = e$message
      )
    }
  )
}
