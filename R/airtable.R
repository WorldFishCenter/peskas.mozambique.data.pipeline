#' Get All Records from Airtable with Pagination
#'
#' Retrieves ALL records from an Airtable table, handling pagination automatically.
#'
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. The name of the table to retrieve.
#' @param token Character string. Airtable API token for authentication.
#' @param list_handler Character string. "collapse" (default) or "count" for list fields.
#'
#' @return A tibble with all records and an 'airtable_id' column.
#'
#' @export
airtable_to_df <- function(
    base_id,
    table_name,
    token,
    list_handler = "collapse") {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}"
  )

  all_records <- list()
  offset <- NULL
  page_count <- 0
  total_retrieved <- 0

  repeat {
    page_count <- page_count + 1
    cat("Fetching page", page_count, "...")

    # Make the request
    req <- httr2::request(base_url) %>%
      httr2::req_headers(Authorization = paste("Bearer", token))

    if (!is.null(offset)) {
      req <- req %>% httr2::req_url_query(offset = offset)
    }

    res <- req %>% httr2::req_perform()
    content <- res %>% httr2::resp_body_json()

    # Add records from this page
    page_records <- content$records
    all_records <- c(all_records, page_records)
    total_retrieved <- total_retrieved + length(page_records)

    cat(
      " retrieved",
      length(page_records),
      "records (total:",
      total_retrieved,
      ")\n"
    )

    # Check if there are more pages
    if (is.null(content$offset)) {
      cat("Retrieved all available records\n")
      break
    }

    offset <- content$offset
  }

  cat("Converting", length(all_records), "records to data frame...\n")

  # Convert all records to tibble
  df <- all_records %>%
    purrr::map_dfr(
      ~ {
        fields <- .x$fields
        fields$airtable_id <- .x$id

        # Handle lists
        if (list_handler == "collapse") {
          fields <- fields %>%
            purrr::map_if(is.list, ~ paste(.x, collapse = ", "))
        } else if (list_handler == "count") {
          fields <- fields %>%
            purrr::map_if(is.list, length)
        }

        dplyr::as_tibble(fields)
      }
    )

  cat("Successfully converted", nrow(df), "records to tibble\n")
  return(df)
}

#' Get Writable Fields from Airtable Table
#'
#' Returns which fields can be updated (excludes computed fields).
#'
#' @param base_id Character string. The Airtable base ID.
#' @param token Character string. Airtable API token.
#' @param table_name Character string. Name of the table.
#'
#' @return Character vector of writable field names.
#'
#' @export
get_writable_fields <- function(base_id, token, table_name) {
  schema_url <- glue::glue(
    "https://api.airtable.com/v0/meta/bases/{base_id}/tables"
  )

  response <- httr2::request(schema_url) %>%
    httr2::req_headers(Authorization = paste("Bearer", token)) %>%
    httr2::req_perform()

  content <- response %>% httr2::resp_body_json()

  # Find the table
  target_table <- content$tables %>%
    purrr::keep(~ .x$name == table_name) %>%
    purrr::pluck(1)

  if (is.null(target_table)) {
    available_tables <- content$tables %>% purrr::map_chr("name")
    stop(
      "Table '",
      table_name,
      "' not found. Available: ",
      paste(available_tables, collapse = ", ")
    )
  }

  # Show field types
  cat("Field types in", table_name, "table:\n")
  for (field in target_table$fields) {
    status <- if (
      !is.null(field$options$isComputed) && field$options$isComputed
    ) {
      " [COMPUTED]"
    } else {
      " [WRITABLE]"
    }
    cat("- '", field$name, "' (", field$type, ")", status, "\n", sep = "")
  }

  # Return writable fields
  writable_fields <- target_table$fields %>%
    purrr::keep(~ is.null(.x$options$isComputed) || !.x$options$isComputed) %>%
    purrr::map_chr("name")

  return(writable_fields)
}

#' Update Single Airtable Record
#'
#' Updates specific fields in one record.
#'
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table.
#' @param token Character string. Airtable API token.
#' @param record_id Character string. ID of the record to update.
#' @param updates Named list. Fields and values to update.
#'
#' @return httr2 response object.
#'
#' @export
update_airtable_record <- function(
    base_id,
    table_name,
    token,
    record_id,
    updates) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}/{record_id}"
  )
  payload <- list(fields = updates)

  tryCatch(
    {
      response <- httr2::request(base_url) %>%
        httr2::req_headers(
          Authorization = paste("Bearer", token),
          `Content-Type` = "application/json"
        ) %>%
        httr2::req_body_json(payload) %>%
        httr2::req_method("PATCH") %>%
        httr2::req_perform()
      return(response)
    },
    error = function(e) {
      if (!is.null(e$resp)) {
        error_body <- e$resp %>% httr2::resp_body_string()
        cat("Error response:\n", error_body, "\n")
      }
      stop(e)
    }
  )
}

#' Bulk Update Multiple Airtable Records
#'
#' Updates multiple records in batches of 10.
#'
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table.
#' @param token Character string. Airtable API token.
#' @param updates_df Data frame with 'airtable_id' column and fields to update.
#'
#' @return List of response objects.
#'
#' @export
bulk_update_airtable <- function(base_id, table_name, token, updates_df) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}"
  )

  # Prepare records
  records <- updates_df %>%
    purrr::pmap(function(airtable_id, ...) {
      updates <- list(...)
      updates$airtable_id <- NULL
      updates <- updates[!is.na(updates)] # Remove NAs

      list(id = airtable_id, fields = updates)
    })

  # Split into batches of 10
  batches <- split(records, ceiling(seq_along(records) / 10))

  # Send each batch
  responses <- purrr::map(batches, function(batch) {
    payload <- list(records = batch)
    cat("Updating batch of", length(batch), "records...\n")

    tryCatch(
      {
        httr2::request(base_url) %>%
          httr2::req_headers(
            Authorization = paste("Bearer", token),
            `Content-Type` = "application/json"
          ) %>%
          httr2::req_body_json(payload) %>%
          httr2::req_method("PATCH") %>%
          httr2::req_perform()
      },
      error = function(e) {
        if (!is.null(e$resp)) {
          error_body <- e$resp %>% httr2::resp_body_string()
          cat("Error response:\n", error_body, "\n")
        }
        stop(e)
      }
    )
  })

  cat("Successfully updated", nrow(updates_df), "records!\n")
  return(responses)
}

#' Create New Airtable Records
#'
#' Creates new records in batches of 10.
#'
#' @param df Data frame containing data to create.
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table.
#' @param token Character string. Airtable API token.
#'
#' @return List of response objects.
#'
#' @export
df_to_airtable <- function(df, base_id, table_name, token) {
  base_url <- glue::glue(
    "https://api.airtable.com/v0/{base_id}/{URLencode(table_name)}"
  )

  # Convert to records format
  records <- df %>%
    purrr::pmap(function(...) {
      row_data <- list(...)
      row_data <- row_data[!is.na(row_data)] # Remove NAs
      list(fields = row_data)
    })

  # Split into batches of 10
  batches <- split(records, ceiling(seq_along(records) / 10))

  responses <- purrr::map(batches, function(batch) {
    payload <- list(records = batch)
    cat("Creating batch of", length(batch), "records...\n")

    tryCatch(
      {
        httr2::request(base_url) %>%
          httr2::req_headers(
            Authorization = paste("Bearer", token),
            `Content-Type` = "application/json"
          ) %>%
          httr2::req_body_json(payload) %>%
          httr2::req_method("POST") %>%
          httr2::req_perform()
      },
      error = function(e) {
        if (!is.null(e$resp)) {
          error_body <- e$resp %>% httr2::resp_body_string()
          cat("Error response:\n", error_body, "\n")
        }
        stop(e)
      }
    )
  })

  cat("Successfully created", nrow(df), "records!\n")
  return(responses)
}

#' Sync Data with Airtable (Update + Create)
#'
#' Main function for daily syncing. Updates existing records and creates new ones.
#'
#' @param boats_df Data frame with device data. Must include key_field column.
#' @param base_id Character string. The Airtable base ID.
#' @param table_name Character string. Name of the table (default: "pds_devices").
#' @param token Character string. Airtable API token.
#' @param key_field Character string. Field to match on (default: "imei").
#'
#' @return List with update and create results.
#'
#' @export
device_sync <- function(
    boats_df,
    base_id,
    table_name = "pds_devices",
    token,
    key_field = "imei") {
  # Validation
  if (!key_field %in% names(boats_df)) {
    stop("Key field '", key_field, "' not found in data")
  }

  if (nrow(boats_df) == 0) {
    cat("No records to sync\n")
    return(NULL)
  }

  # Remove duplicates
  original_count <- nrow(boats_df)
  boats_df <- boats_df %>%
    dplyr::group_by(!!rlang::sym(key_field)) %>%
    dplyr::slice_tail(n = 1) %>%
    dplyr::ungroup()

  if (original_count > nrow(boats_df)) {
    cat("Removed", original_count - nrow(boats_df), "duplicate records\n")
  }

  cat("Syncing", nrow(boats_df), "devices...")

  # Get existing records
  existing_df <- airtable_to_df(base_id, table_name, token)

  # Filter to writable fields only
  tryCatch(
    {
      writable_fields <- get_writable_fields(base_id, token, table_name)
      cat("\nFiltering to writable fields only...\n")
      boats_df <- boats_df %>%
        dplyr::select(dplyr::any_of(c(writable_fields, key_field)))
    },
    error = function(e) {
      cat("Warning: Could not get writable fields\n")
    }
  )

  # Convert key field to character for matching
  boats_df[[key_field]] <- as.character(boats_df[[key_field]])
  existing_df[[key_field]] <- as.character(existing_df[[key_field]])

  # Separate updates vs creates
  updates <- boats_df %>%
    dplyr::filter(!!rlang::sym(key_field) %in% existing_df[[key_field]]) %>%
    dplyr::left_join(
      existing_df %>%
        dplyr::select(dplyr::all_of(key_field), "airtable_id"),
      by = key_field
    ) %>%
    # Remove duplicate airtable_ids
    dplyr::group_by(.data$airtable_id) %>%
    dplyr::slice_tail(n = 1) %>%
    dplyr::ungroup()

  creates <- boats_df %>%
    dplyr::filter(!(!!rlang::sym(key_field) %in% existing_df[[key_field]]))

  # Fix data types for Airtable
  numeric_fields <- c("last_seen", "external_id", "external_boat_id")
  for (field in numeric_fields) {
    if (field %in% names(updates)) {
      updates[[field]] <- as.numeric(updates[[field]])
    }
    if (field %in% names(creates)) {
      creates[[field]] <- as.numeric(creates[[field]])
    }
  }

  if ("active" %in% names(updates)) {
    updates$active <- as.logical(updates$active)
  }
  if ("active" %in% names(creates)) {
    creates$active <- as.logical(creates$active)
  }

  results <- list(updates = NULL, creates = NULL)

  # Perform operations
  if (nrow(updates) > 0) {
    results$updates <- bulk_update_airtable(base_id, table_name, token, updates)
  }

  if (nrow(creates) > 0) {
    results$creates <- df_to_airtable(creates, base_id, table_name, token)
  }

  cat("Updated:", nrow(updates), "| Created:", nrow(creates), "\n")
  return(results)
}
