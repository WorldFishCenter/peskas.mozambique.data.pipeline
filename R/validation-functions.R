#' Outlier Alert for Numeric Vectors
#'
#' This helper function identifies numeric outliers based on the bounds computed by
#' \code{\link[univOutl]{LocScaleB}}. It optionally applies a log transform within
#' \code{LocScaleB} (through the \code{...} argument) and flags values outside the computed
#' lower or upper bounds. Values below the lower bound receive the alert code
#' \code{alert_if_smaller}, values above the upper bound receive \code{alert_if_larger},
#' and in-range values receive \code{no_alert_value}.
#'
#' @param x A numeric vector in which to detect outliers.
#' @param no_alert_value A numeric code (default \code{NA_real_}) to assign to non-outlier values.
#' @param alert_if_larger A numeric code to assign if \code{x > upper bound}.
#' @param alert_if_smaller A numeric code to assign if \code{x < lower bound}.
#' @param ... Additional arguments passed to \code{\link[univOutl]{LocScaleB}}, such as
#'   \code{logt} or \code{k}.
#'
#' @details
#' \itemize{
#'   \item The function checks if all \code{x} values are \code{NA} or zero, or if the MAD
#'         (median absolute deviation) is zero. In these cases, it returns \code{NA} for
#'         all elements, since meaningful bounds cannot be computed.
#'   \item Otherwise, it calls \code{univOutl::LocScaleB(x, ...)} to compute \code{bounds}.
#'   \item If a log transform was used (\code{logt = TRUE}), \code{LocScaleB} returns the
#'         log-scale bounds; typically, you would back-transform them to compare with
#'         the raw values.
#' }
#'
#' @return A numeric vector of the same length as \code{x}, containing the alert codes
#'   for each element (\code{alert_if_smaller}, \code{alert_if_larger}, or
#'   \code{no_alert_value}).
#'
#' @seealso \code{\link[univOutl]{LocScaleB}} for the underlying outlier detection algorithm.
#'
#' @examples
#' \dontrun{
#' x <- c(1, 2, 3, 100)
#' alert_outlier(x, no_alert_value = NA, alert_if_larger = 9, logt = TRUE, k = 3)
#' }
#'
#' @keywords validation
#' @export

alert_outlier <- function(
    x,
    no_alert_value = NA_real_,
    alert_if_larger = no_alert_value,
    alert_if_smaller = no_alert_value,
    ...) {
  # If all values are NA or zero, we can't compute meaningful bounds
  all_na_or_zero <- function(x) {
    isTRUE(all(is.na(x) | x == 0))
  }
  if (all_na_or_zero(x)) {
    return(rep(NA_real_, length(x)))
  }
  if (stats::mad(x, na.rm = TRUE) <= 0) {
    return(rep(NA_real_, length(x)))
  }

  bounds <- univOutl::LocScaleB(x, ...)$bounds
  # If log transform was used inside LocScaleB, we have to back-transform
  # but we handle that outside if we set logt=TRUE
  dplyr::case_when(
    x < bounds[1] ~ alert_if_smaller,
    x > bounds[2] ~ alert_if_larger,
    TRUE ~ no_alert_value
  )
}

#' Get Catch Bounds by Gear + Taxon
#'
#' Computes upper/lower outlier bounds for catch data (\code{catch_kg}) grouped by
#' the interaction of \code{gear} and \code{catch_taxon}. This function uses
#' \code{\link[univOutl]{LocScaleB}} with a (default) log transform to find outlier
#' thresholds for each \code{gear-taxont} group, then exponentiates the upper bound.
#'
#' @param data A data frame that includes the columns \code{gear}, \code{catch_taxon},
#'   and \code{catch_kg}.
#' @param k A numeric parameter passed to \code{\link[univOutl]{LocScaleB}}, controlling
#'   how "wide" the outlier threshold will be (often \code{k = 3} or \code{k = 2}).
#'
#' @details
#' \enumerate{
#'   \item Filters to columns \code{gear}, \code{catch_taxon}, and \code{catch_kg}.
#'   \item Splits the data by the interaction of \code{gear} and \code{catch_taxon}.
#'   \item Runs \code{\link[univOutl]{LocScaleB}} on each subset (with \code{logt = TRUE}),
#'         retrieving the \code{bounds}.
#'   \item Binds the subset results, exponentiates the \code{upper.up} bound, then separates
#'         \code{gear_taxon} back into \code{gear} and \code{catch_taxon}.
#' }
#'
#' @return A data frame with columns \code{gear}, \code{catch_taxon}, \code{upper.up},
#'   and the bounds from \code{LocScaleB}. The \code{lower.low} column is dropped.
#'
#' @seealso \code{\link[univOutl]{LocScaleB}} for outlier detection,
#'   \code{\link{validate_catch_taxa}} for usage of these bounds in a validation step.
#'
#' @examples
#' \dontrun{
#' data_bounds <- get_catch_bounds_taxon(data, k = 3)
#' head(data_bounds)
#' }
#'
#' @keywords validation
#' @export

get_catch_bounds_taxon <- function(data, k) {
  # (1) Filter relevant columns
  # (2) Split by gear+taxon
  # (3) Use LocScaleB() on the numeric data (log scale if you prefer)
  # (4) Re-combine
  data %>%
    dplyr::select("gear", "catch_taxon", "catch_kg") %>%
    dplyr::filter(.data$catch_taxon != "other") %>%
    split(interaction(.$gear, .$catch_taxon)) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(
      ~ univOutl::LocScaleB(.x[["catch_kg"]], logt = TRUE, k = k)$bounds
    ) %>%
    dplyr::bind_rows(.id = "gear_taxon") %>%
    dplyr::mutate(
      # Bound is on log scale, so exponentiate:
      upper.up = exp(.data$upper.up)
    ) %>%
    tidyr::separate(
      col = "gear_taxon",
      into = c("gear", "catch_taxon"),
      sep = "\\."
    ) %>%
    dplyr::select(-"lower.low")
}

#' Validate Catch at Taxon Level
#'
#' Flags outliers in \code{catch_kg} by comparing values to upper bounds computed per
#' \code{gear + catch_taxon} group (via \code{\link{get_catch_bounds_taxon}}).
#' Rows with \code{catch_kg} above the bound receive a numeric alert code and optionally
#' have their \code{catch_kg} set to \code{NA}.
#'
#' @param data A data frame containing (at least) \code{submission_id}, \code{gear},
#'   \code{catch_taxon}, and \code{catch_kg}.
#' @param k A numeric parameter passed to \code{\link[univOutl]{LocScaleB}}; used for
#'   computing outlier bounds in \code{\link{get_catch_bounds_taxon}}.
#' @param flag_value A numeric value to assign when \code{catch_kg} exceeds the upper bound.
#'   Default is \code{4}.
#'
#' @details
#' \enumerate{
#'   \item Calls \code{\link{get_catch_bounds_taxon}} to retrieve outlier bounds for each
#'         \code{gear + catch_taxon} group.
#'   \item Joins these bounds to \code{data} on \code{gear} and \code{catch_taxon}.
#'   \item Rowwise, checks if \code{catch_kg} is greater than the bound \code{upper.up}.
#'         If so, sets \code{alert_catch = flag_value} and optionally replaces
#'         \code{catch_kg} with \code{NA_real_}.
#' }
#'
#' @return A data frame with the columns:
#'   \describe{
#'     \item{\code{submission_id}}{Carried from the input data.}
#'     \item{\code{catch_taxon}}{Carried from the input data.}
#'     \item{\code{catch_kg}}{Potentially modified if outlier.}
#'     \item{\code{alert_catch}}{The alert code \code{flag_value} for outliers, or
#'       \code{NA_real_} if no outlier.}
#'   }
#'
#' @seealso \code{\link{get_catch_bounds_taxon}}, \code{\link[univOutl]{LocScaleB}}
#'
#' @examples
#' \dontrun{
#' catch_alert <- validate_catch_taxa(data, k = 3, flag_value = 4)
#' head(catch_alert)
#' }
#'
#' @keywords validation
#' @export
validate_catch_taxa <- function(data, k = 3, flag_value = 4) {
  bounds <- get_catch_bounds_taxon(data, k)

  # Join the original data with the bounds, rowwise check
  data %>%
    dplyr::left_join(bounds, by = c("gear", "catch_taxon")) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      # If catch_kg is beyond the upper bound, we flag it
      alert_catch = dplyr::case_when(
        is.na(.data$upper.up) ~ NA_real_,
        .data$catch_kg > .data$upper.up ~ flag_value,
        TRUE ~ NA_real_
      ),
      # Optionally set outliers to NA
      catch_kg = ifelse(is.na(.data$alert_catch), .data$catch_kg, NA_real_)
    ) %>%
    dplyr::ungroup() %>%
    # Keep only columns of interest
    dplyr::select(
      "submission_id",
      "catch_taxon",
      "catch_kg",
      "alert_catch"
    )
}

#' Get Total Catch Bounds by Landing Site and Gear
#'
#' Computes outlier bounds for total catch (\code{total_catch_kg}) grouped by the
#' combination of \code{landing_site} and \code{gear}. Internally, it first aggregates
#' \code{catch_kg} to get a single \code{total_catch_kg} per submission, then applies
#' \code{\link[univOutl]{LocScaleB}} (with a log transform) within each site-gear group.
#'
#' @param data A data frame containing \code{submission_id}, \code{landing_site},
#'   \code{gear}, and \code{catch_kg}.
#' @param k A numeric parameter passed to \code{\link[univOutl]{LocScaleB}}.
#'
#' @details
#' \enumerate{
#'   \item Groups by \code{submission_id, landing_site, gear} and sums \code{catch_kg}
#'         into \code{total_catch_kg}.
#'   \item Splits the results by site-gear combination.
#'   \item Calls \code{\link[univOutl]{LocScaleB}} on each group, exponentiates the
#'         \code{upper.up} bound, then returns a data frame with \code{landing_site},
#'         \code{gear}, and \code{upper.up}.
#' }
#'
#' @return A data frame with columns:
#' \itemize{
#'   \item \code{landing_site}
#'   \item \code{gear}
#'   \item \code{upper.up} (the exponentiated upper bound)
#' }
#'
#' @seealso \code{\link{validate_total_catch}} for usage of these bounds in validation.
#'
#' @examples
#' \dontrun{
#' total_bounds <- get_total_catch_bounds(data, k = 3)
#' head(total_bounds)
#' }
#'
#' @keywords validation
#' @export
get_total_catch_bounds <- function(data, k) {
  # Summarize from (submission_id, catch_taxon) to total by submission_id
  # (assuming catch_taxon is a sub-component). Then group by gear+site.

  # 1) Re-aggregate to get a "submission-level total catch"
  data_total <- data %>%
    dplyr::group_by(.data$submission_id, .data$landing_site, .data$gear) %>%
    dplyr::summarise(
      total_catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    )

  # 2) Split by landing_site, gear
  data_bounds <- data_total %>%
    dplyr::mutate(
      group_id = paste(.data$landing_site, .data$gear, sep = ".")
    ) %>%
    split(.$group_id) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(
      ~ univOutl::LocScaleB(.x$total_catch_kg, logt = TRUE, k = k)$bounds
    ) %>%
    dplyr::bind_rows(.id = "group_id") %>%
    tidyr::separate(
      .data$group_id,
      into = c("landing_site", "gear"),
      sep = "\\."
    ) %>%
    dplyr::mutate(upper.up = exp(.data$upper.up)) %>%
    dplyr::select(-"lower.low")

  return(data_bounds)
}

#' Validate Total Catch
#'
#' Aggregates \code{catch_kg} to a total per \code{submission_id, landing_site, gear},
#' computes outlier bounds by landing site + gear, and flags outlier total catches.
#' Outliers are assigned a numeric \code{flag_value} and optionally set to \code{NA}.
#'
#' @param data A data frame containing \code{submission_id}, \code{landing_site},
#'   \code{gear}, and \code{catch_kg}.
#' @param k A numeric parameter passed to \code{\link[univOutl]{LocScaleB}}, used for
#'   bounding outliers in \code{\link{get_total_catch_bounds}}.
#' @param flag_value A numeric value to assign to records whose total catch is above
#'   the upper bound. Default is \code{5}.
#'
#' @details
#' \enumerate{
#'   \item Groups \code{data} by \code{submission_id, landing_site, gear} and sums
#'         \code{catch_kg} into \code{total_catch_kg}.
#'   \item Retrieves outlier bounds from \code{\link{get_total_catch_bounds}}.
#'   \item Flags any \code{total_catch_kg} above \code{upper.up} with \code{alert_total}.
#'         Sets those outlier values to \code{NA_real_}.
#' }
#'
#' @return A data frame with columns:
#' \itemize{
#'   \item \code{submission_id}
#'   \item \code{total_catch_kg} (potentially set to \code{NA} if outlier)
#'   \item \code{alert_total} (the alert code or \code{NA_real_})
#' }
#'
#' @seealso \code{\link{get_total_catch_bounds}}, \code{\link[univOutl]{LocScaleB}}
#'
#' @examples
#' \dontrun{
#' total_catch_alert <- validate_total_catch(data, k = 3, flag_value = 5)
#' head(total_catch_alert)
#' }
#'
#' @keywords validation
#' @export
validate_total_catch <- function(data, k = 3, flag_value = 5) {
  # 1) Re-aggregate data to get "total_catch_kg" per submission
  data_total <- data %>%
    dplyr::group_by(.data$submission_id, .data$landing_site, .data$gear) %>%
    dplyr::summarise(
      total_catch_kg = sum(.data$catch_kg, na.rm = TRUE),
      .groups = "drop"
    )

  # 2) Get bounds by (landing_site, gear)
  bounds <- get_total_catch_bounds(data, k)

  # 3) Rowwise compare
  data_total %>%
    dplyr::left_join(bounds, by = c("landing_site", "gear")) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      alert_total = dplyr::case_when(
        is.na(.data$upper.up) ~ NA_real_,
        .data$total_catch_kg > .data$upper.up ~ flag_value,
        TRUE ~ NA_real_
      ),
      total_catch_kg = ifelse(
        is.na(.data$alert_total),
        .data$total_catch_kg,
        NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select("submission_id", "total_catch_kg", "alert_total")
}

#' Get Price Bounds by Gear + Taxon
#'
#' Computes outlier bounds for \code{catch_price} grouped by the interaction of
#' \code{gear} and \code{catch_taxon}. By default, it applies \code{logt = TRUE} in
#' \code{\link[univOutl]{LocScaleB}} and exponentiates the resulting upper bound.
#'
#' @param data A data frame containing \code{gear}, \code{catch_taxon}, and
#'   \code{catch_price}.
#' @param k A numeric parameter passed to \code{\link[univOutl]{LocScaleB}}.
#'
#' @details
#' Similar to \code{\link{get_catch_bounds_taxon}}, but operating on \code{catch_price}
#' rather than \code{catch_kg}. Splits the data by gear + taxon, uses
#' \code{LocScaleB} to find outlier bounds, and exponentiates the upper bound.
#'
#' @return A data frame with columns \code{gear}, \code{catch_taxon}, and \code{upper.up}
#'   representing the exponentiated upper bound. The \code{lower.low} column is dropped.
#'
#' @seealso \code{\link{validate_price}} for usage in a validation step.
#'
#' @examples
#' \dontrun{
#' price_bounds <- get_price_bounds(data, k = 3)
#' head(price_bounds)
#' }
#'
#' @keywords validation
#' @export
get_price_bounds <- function(data, k) {
  # Group by gear+taxon, detect outliers in catch_price
  data %>%
    dplyr::select("gear", "catch_taxon", "catch_price") %>%
    # Split by gear and catch_taxon
    split(interaction(.$gear, .$catch_taxon)) %>%
    # Remove empty groups
    purrr::discard(~ nrow(.) == 0) %>%
    # Filter out groups with no variation (all values the same)
    purrr::keep(~ length(unique(.x[["catch_price"]])) > 1) %>%
    # Now run outlier detection
    purrr::map(
      ~ univOutl::LocScaleB(.x[["catch_price"]], logt = TRUE, k = k)$bounds
    ) %>%
    # Combine results
    dplyr::bind_rows(.id = "gear_taxon") %>%
    # Transform upper bound
    dplyr::mutate(upper.up = exp(.data$upper.up)) %>%
    # Split gear_taxon column
    tidyr::separate(
      col = "gear_taxon",
      into = c("gear", "catch_taxon"),
      sep = "\\."
    ) %>%
    # Remove lower.low column
    dplyr::select(-"lower.low")
}

#' Validate Catch Price
#'
#' Detects outliers in \code{catch_price} by \code{gear + catch_taxon}, assigning a
#' numeric alert code to records exceeding the computed upper bound. Outliers can be
#' replaced with \code{NA}.
#'
#' @param data A data frame with \code{submission_id}, \code{gear}, \code{catch_taxon},
#'   and \code{catch_price}.
#' @param k A numeric parameter passed to \code{\link[univOutl]{LocScaleB}}, used in
#'   \code{\link{get_price_bounds}}.
#' @param flag_value A numeric code (default \code{6}) assigned to outlier prices.
#'
#' @details
#' \enumerate{
#'   \item Calls \code{\link{get_price_bounds}} to compute outlier thresholds.
#'   \item Joins thresholds back to \code{data} via \code{gear} and \code{catch_taxon}.
#'   \item Rowwise, flags prices above the \code{upper.up} bound with \code{alert_price},
#'         and replaces them with \code{NA_real_}.
#' }
#'
#' @return A data frame with columns:
#'   \describe{
#'     \item{\code{submission_id}}{Carried from the input.}
#'     \item{\code{catch_taxon}}{Carried from the input.}
#'     \item{\code{catch_price}}{Potentially set to \code{NA} if outlier.}
#'     \item{\code{alert_price}}{The numeric alert code, or \code{NA}.}
#'   }
#'
#' @seealso \code{\link{get_price_bounds}}, \code{\link[univOutl]{LocScaleB}}
#'
#' @examples
#' \dontrun{
#' price_alert <- validate_price(data, k = 3, flag_value = 6)
#' head(price_alert)
#' }
#'
#' @keywords validation
#' @export

validate_price <- function(data, k = 3, flag_value = 6) {
  bounds <- get_price_bounds(data, k)

  data %>%
    dplyr::left_join(bounds, by = c("gear", "catch_taxon")) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      alert_price = dplyr::case_when(
        is.na(.data$upper.up) ~ NA_real_,
        .data$catch_price > .data$upper.up ~ flag_value,
        TRUE ~ NA_real_
      ),
      catch_price = ifelse(
        is.na(.data$alert_price),
        .data$catch_price,
        NA_real_
      )
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select("submission_id", "catch_taxon", "catch_price", "alert_price")
}

#' Get Validation Status from KoboToolbox
#'
#' Retrieves the validation status for a specific submission in KoboToolbox.
#' The function handles NULL responses and returns a consistent tibble structure
#' regardless of the API response.
#'
#' @param submission_id Character string. The ID of the submission to check.
#' @param asset_id Character string. The asset ID from KoboToolbox.
#' @param token Character string. The authorization token for KoboToolbox API.
#' @param debug Logical. If TRUE, prints the request object. Default is FALSE.
#'
#' @return A tibble with one row containing:
#'   \item{submission_id}{The ID of the checked submission}
#'   \item{validation_status}{The validation status (e.g., "validation_status_approved" or "not_validated")}
#'   \item{validated_at}{Timestamp of validation as POSIXct}
#'   \item{validated_by}{Username of the validator}
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' # Single submission
#' get_validation_status(
#'   submission_id = "1234567",
#'   asset_id = "your asset id",
#'   token = "Token YOUR_TOKEN_HERE"
#' )
#'
#' # Multiple submissions using purrr
#' submission_ids <- c("1234567", "154267")
#' submission_ids %>%
#'   purrr::map_dfr(get_validation_status,
#'     asset_id = "your asset id",
#'     token = "Token YOUR_TOKEN_HERE"
#'   )
#' }
#'
#' @keywords workflow validation
#' @export
get_validation_status <- function(
    submission_id = NULL,
    asset_id = NULL,
    token = NULL,
    debug = FALSE) {
  base_url <- paste0(
    "https://eu.kobotoolbox.org/api/v2/assets/",
    asset_id,
    "/data/"
  )
  url <- paste0(base_url, submission_id, "/validation_status/")

  # Add "Token " prefix to token if it doesn't already have it
  if (!grepl("^Token ", token)) {
    token <- paste("Token", token)
  }

  req <- httr2::request(url) %>%
    httr2::req_headers(
      Authorization = token
    ) %>%
    httr2::req_method("GET")

  if (debug) {
    print(req)
  }

  tryCatch(
    {
      response <- httr2::req_perform(req)
      if (httr2::resp_status(response) == 200) {
        validation_data <- httr2::resp_body_json(response)

        # Handle NULL validation data
        timestamp <- if (
          !is.null(validation_data) && !is.null(validation_data$timestamp)
        ) {
          lubridate::as_datetime(validation_data$timestamp)
        } else {
          lubridate::as_datetime(NA)
        }

        status <- if (
          !is.null(validation_data) && !is.null(validation_data$uid)
        ) {
          validation_data$uid
        } else {
          "not_validated"
        }

        validator <- if (
          !is.null(validation_data) && !is.null(validation_data$by_whom)
        ) {
          validation_data$by_whom
        } else {
          NA_character_
        }

        dplyr::tibble(
          submission_id = submission_id,
          validation_status = status,
          validated_at = timestamp,
          validated_by = validator
        )
      } else {
        dplyr::tibble(
          submission_id = submission_id,
          validation_status = "not_validated",
          validated_at = lubridate::as_datetime(NA),
          validated_by = NA_character_
        )
      }
    },
    error = function(e) {
      if (debug) {
        cat("Error:", as.character(e), "\n")
      }

      dplyr::tibble(
        submission_id = submission_id,
        validation_status = "not_validated",
        validated_at = lubridate::as_datetime(NA),
        validated_by = NA_character_
      )
    }
  )
}
#' Update Validation Status in KoboToolbox
#'
#' Updates the validation status for a specific submission in KoboToolbox.
#' The function allows setting the status to approved, not approved, or on hold.
#'
#' @param submission_id Character string. The ID of the submission to update.
#' @param asset_id Character string. The asset ID from KoboToolbox.
#' @param token Character string. The authorization token for KoboToolbox API.
#' @param status Character string. The validation status to set. Must be one of:
#'        "validation_status_approved", "validation_status_not_approved", or
#'        "validation_status_on_hold".
#' @param debug Logical. If TRUE, prints the request object and response. Default is FALSE.
#'
#' @return A tibble with one row containing:
#'   \item{submission_id}{The ID of the updated submission}
#'   \item{validation_status}{The new validation status}
#'   \item{validated_at}{Timestamp of validation as POSIXct}
#'   \item{validated_by}{Username of the validator}
#'   \item{update_success}{Logical indicating if the update was successful}
#'
#' @keywords validation
#' @examples
#' \dontrun{
#' # Update a single submission
#' update_validation_status(
#'   submission_id = "1234567",
#'   asset_id = "your asset id",
#'   token = "Token YOUR_TOKEN_HERE",
#'   status = "validation_status_approved"
#' )
#'
#' # Update multiple submissions using purrr
#' submission_ids <- c("1234567", "154267")
#' submission_ids %>%
#'   purrr::map_dfr(update_validation_status,
#'     asset_id = "your asset id",
#'     token = "Token YOUR_TOKEN_HERE",
#'     status = "validation_status_approved"
#'   )
#' }
#'
#' @keywords workflow validation
#' @export
update_validation_status <- function(
    submission_id = NULL,
    asset_id = NULL,
    token = NULL,
    status = "validation_status_approved",
    debug = FALSE) {
  # Validate status
  valid_statuses <- c(
    "validation_status_approved",
    "validation_status_not_approved",
    "validation_status_on_hold"
  )

  if (!status %in% valid_statuses) {
    stop("Status must be one of: ", paste(valid_statuses, collapse = ", "))
  }

  # Construct the URL
  base_url <- paste0(
    "https://eu.kobotoolbox.org/api/v2/assets/",
    asset_id,
    "/data/"
  )
  url <- paste0(base_url, submission_id, "/validation_status/")

  # Set up request body
  body <- list(
    "validation_status.uid" = status
  )

  # Add "Token " prefix to token if it doesn't already have it
  if (!grepl("^Token ", token)) {
    token <- paste("Token", token)
  }

  # Set up request
  req <- httr2::request(url) %>%
    httr2::req_headers(
      Authorization = token,
      "Content-Type" = "application/json"
    ) %>%
    httr2::req_method("PATCH") %>%
    httr2::req_body_json(body)

  if (debug) {
    print(req)
    print(body)
  }

  tryCatch(
    {
      response <- httr2::req_perform(req)

      if (debug) {
        cat("Response status:", httr2::resp_status(response), "\n")
        cat("Response body:", httr2::resp_body_string(response), "\n")
      }

      if (httr2::resp_status(response) %in% c(200, 201, 204)) {
        # If update was successful, get the current validation status
        updated_data <- get_validation_status(
          submission_id = submission_id,
          asset_id = asset_id,
          token = token,
          debug = debug
        )

        # Add success indicator
        updated_data %>%
          dplyr::mutate(update_success = TRUE)
      } else {
        dplyr::tibble(
          submission_id = submission_id,
          validation_status = NA_character_,
          validated_at = lubridate::as_datetime(NA),
          validated_by = NA_character_,
          update_success = FALSE
        )
      }
    },
    error = function(e) {
      if (debug) {
        cat("Error:", as.character(e), "\n")
      }

      dplyr::tibble(
        submission_id = submission_id,
        validation_status = NA_character_,
        validated_at = lubridate::as_datetime(NA),
        validated_by = NA_character_,
        update_success = FALSE
      )
    }
  )
}

#' Process Submissions in Parallel with Rate Limiting
#'
#' @description
#' Helper function to process a list of submission IDs in parallel with built-in rate limiting
#' to avoid overwhelming the KoboToolbox API. This function is used internally by validation
#' and sync functions to apply operations to submissions while respecting API constraints.
#'
#' @param submission_ids Character vector of submission IDs to process
#' @param process_fn Function to apply to each submission ID. Should accept a single
#'        submission_id parameter and return a tibble with results.
#' @param description Character string describing the type of submissions being processed
#'        (used in log messages). Default is "submissions".
#' @param rate_limit Numeric value specifying the delay in seconds between API calls.
#'        Default is 0.2 seconds. Adjust based on API rate limits.
#'
#' @details
#' The function uses parallel processing via furrr with intentional delays between requests
#' to prevent overwhelming the API server. It includes progress tracking and optional
#' failure logging if the result includes an `update_success` column.
#'
#' Key features:
#' - Parallel execution using existing future plan
#' - Rate limiting via Sys.sleep() between requests
#' - Progress tracking via progressr
#' - Automatic failure detection and logging
#' - Returns combined results as a data frame
#'
#' @return A tibble containing the combined results from all processed submissions.
#'         If the process_fn returns an `update_success` column, failures are logged.
#'
#' @examples
#' \dontrun{
#' # Setup parallel processing first
#' future::plan(future::multisession, workers = 4)
#'
#' # Fetch validation status for multiple submissions
#' results <- process_submissions_parallel(
#'   submission_ids = c("123", "456", "789"),
#'   process_fn = function(id) {
#'     get_validation_status(
#'       submission_id = id,
#'       asset_id = "your_asset_id",
#'       token = "your_token"
#'     )
#'   },
#'   description = "validation statuses",
#'   rate_limit = 0.1
#' )
#'
#' # Update validation status for multiple submissions
#' update_results <- process_submissions_parallel(
#'   submission_ids = c("123", "456"),
#'   process_fn = function(id) {
#'     update_validation_status(
#'       submission_id = id,
#'       status = "validation_status_approved",
#'       asset_id = "your_asset_id",
#'       token = "your_token"
#'     )
#'   },
#'   description = "approval updates",
#'   rate_limit = 0.2
#' )
#' }
#'
#' @keywords validation
#' @export
process_submissions_parallel <- function(
    submission_ids,
    process_fn,
    description = "submissions",
    rate_limit = 0.2) {
  logger::log_info("Processing {length(submission_ids)} {description}")

  progressr::with_progress({
    p <- progressr::progressor(along = submission_ids)

    results <- furrr::future_map_dfr(
      submission_ids,
      function(id) {
        # Add delay to respect rate limits
        Sys.sleep(rate_limit)

        result <- process_fn(id)
        p()
        result
      },
      .options = furrr::furrr_options(seed = TRUE)
    )

    # Log failures if update_success column exists
    if ("update_success" %in% names(results)) {
      failures <- results %>% dplyr::filter(!.data$update_success)
      if (nrow(failures) > 0) {
        logger::log_warn(
          "Failed to update {nrow(failures)} {description}: {paste(failures$submission_id, collapse = ', ')}"
        )
      } else {
        logger::log_info(
          "Successfully processed all {length(submission_ids)} {description}"
        )
      }
    }

    results
  })
}
