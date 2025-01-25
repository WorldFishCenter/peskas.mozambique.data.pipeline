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
    purrr::map(~ univOutl::LocScaleB(.x[["catch_kg"]], logt = TRUE, k = k)$bounds) %>%
    dplyr::bind_rows(.id = "gear_taxon") %>%
    dplyr::mutate(
      # Bound is on log scale, so exponentiate:
      upper.up = exp(.data$upper.up)
    ) %>%
    tidyr::separate(col = "gear_taxon", into = c("gear", "catch_taxon"), sep = "\\.") %>%
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
      "submission_id", "catch_taxon", "catch_kg", "alert_catch"
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
    dplyr::mutate(group_id = paste(.data$landing_site, .data$gear, sep = ".")) %>%
    split(.$group_id) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(~ univOutl::LocScaleB(.x$total_catch_kg, logt = TRUE, k = k)$bounds) %>%
    dplyr::bind_rows(.id = "group_id") %>%
    tidyr::separate(.data$group_id, into = c("landing_site", "gear"), sep = "\\.") %>%
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
      total_catch_kg = ifelse(is.na(.data$alert_total), .data$total_catch_kg, NA_real_)
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
    # Exclude NAs or zeros as needed
    split(interaction(.$gear, .$catch_taxon)) %>%
    purrr::discard(~ nrow(.) == 0) %>%
    purrr::map(~ univOutl::LocScaleB(.x[["catch_price"]], logt = TRUE, k = k)$bounds) %>%
    dplyr::bind_rows(.id = "gear_taxon") %>%
    dplyr::mutate(upper.up = exp(.data$upper.up)) %>%
    tidyr::separate(col = "gear_taxon", into = c("gear", "catch_taxon"), sep = "\\.") %>%
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
      catch_price = ifelse(is.na(.data$alert_price), .data$catch_price, NA_real_)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select("submission_id", "catch_taxon", "catch_price", "alert_price")
}
