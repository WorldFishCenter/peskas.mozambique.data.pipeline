#' Reshape Species Groups from Wide to Long Format
#'
#' This function converts a data frame containing repeated species group columns
#' (species_group.0, species_group.1, etc.) into a long format where each species group
#' is represented as a separate row, with a column indicating which group it belongs to.
#'
#' @param df A data frame containing species group data in wide format
#'
#' @return A data frame in long format with each row representing a single species group record
#' @export
#'
#' @details
#' The function identifies columns that follow the pattern "species_group.X" and restructures
#' the data so that each species group from a submission is represented as a separate row.
#' It removes the position prefix from column names and adds an n_catch column to track
#' the group number (1-based indexing). Rows that contain only NA values are filtered out.
#'
#' @keywords preprocessing
#'
#' @examples
#' \dontrun{
#' long_species_data <- reshape_species_groups(catch_info)
#' }
#'
reshape_species_groups <- function(df = NULL) {
  # Get all column names that have the pattern species_group.X
  species_columns <- names(df)[grep("species_group\\.[0-9]+", names(df))]

  # Find unique position indicators (0, 1, 2, etc.)
  positions <- unique(stringr::str_extract(
    species_columns,
    "species_group\\.[0-9]+"
  ))

  # Create a list to store each position's data
  position_dfs <- list()

  # For each position, extract its columns and create a data frame
  for (pos in positions) {
    # Get columns for this position
    pos_cols <- c(
      "submission_id",
      names(df)[grep(paste0(pos, "\\."), names(df))]
    )

    # Skip if there are no columns for this position
    if (length(pos_cols) <= 1) {
      next
    }

    # Create data frame for this position
    pos_df <- df[, pos_cols, drop = FALSE]

    # Rename columns to remove the position prefix
    new_names <- names(pos_df)
    new_names[-1] <- stringr::str_replace(new_names[-1], paste0(pos, "\\."), "")
    names(pos_df) <- new_names

    # Add position identifier column
    pos_num <- as.numeric(stringr::str_extract(pos, "[0-9]+"))
    pos_df$n_catch <- pos_num + 1 # Adding 1 to make it 1-based instead of 0-based

    # Add to list
    position_dfs[[length(position_dfs) + 1]] <- pos_df
  }

  # Bind all position data frames
  result <- dplyr::bind_rows(position_dfs)

  # Remove rows where all species group columns are NA
  # (These are empty species group entries)
  species_detail_cols <- names(result)[
    !names(result) %in% c("submission_id", "n_catch")
  ]
  result <- result %>%
    dplyr::filter(
      rowSums(is.na(.[species_detail_cols])) != length(species_detail_cols)
    )

  # Sort by submission_id and n_catch
  result <- result %>%
    dplyr::arrange(.data$submission_id, .data$n_catch) |>
    dplyr::rename_with(~ stringr::str_remove(., "species_group/"))

  return(result)
}


#' Preprocess Catch Data from Survey Forms
#'
#' Processes catch data from KoBoToolbox survey forms, handling different field structures
#' that result from various survey form configurations. Automatically normalizes species
#' field variations and processes length group data.
#'
#' @param data A data frame containing raw survey data with species groups
#'
#' @return A data frame with processed catch data including:
#'   submission_id, n_catch, count_method, catch_taxon, n_buckets, weight_bucket,
#'   individuals, length, catch_weight
#'
#' @details
#' The function uses `reshape_catch_data()` internally which automatically handles:
#' - Multiple species field formats (species_TL, species_RF, species_SH, etc.)
#' - Separate length group structures for fish over 100cm
#' - Length range conversion to midpoint values
#' - Species code standardization (e.g., TUN→TUS, SKH→CVX)
#'
#' @keywords internal
#' @export
preprocess_catch <- function(data = NULL) {
  catch_info <-
    data |>
    dplyr::select("submission_id", dplyr::starts_with("species_group")) |>
    reshape_catch_data()

  catch_info |>
    dplyr::mutate(
      length_range = dplyr::case_when(
        .data$length_range == "over100" ~ NA_character_,
        TRUE ~ .data$length_range
      )
    ) |>
    tidyr::separate_wider_delim(
      "length_range",
      delim = "_",
      names = c("min", "max")
    ) |>
    dplyr::mutate(
      length = as.numeric(.data$min) +
        ((as.numeric(.data$max) - as.numeric(.data$min)) / 2),
      length = dplyr::coalesce(.data$length, as.numeric(.data$length_over))
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      count_method = "counting_method",
      catch_taxon = "species",
      "n_buckets",
      "weight_bucket",
      individuals = "count",
      "length",
      "catch_weight"
    ) |>
    # fix fields
    dplyr::mutate(
      dplyr::across(c("n_buckets":"catch_weight"), ~ as.double(.x))
    ) |>
    # replace TUN with TUS and SKH to Carcharhiniformes as more pertinent
    dplyr::mutate(
      catch_taxon = dplyr::case_when(
        .data$catch_taxon == "TUN" ~ "TUS",
        .data$catch_taxon == "SKH" ~ "CVX",
        TRUE ~ .data$catch_taxon
      )
    )
}

#' Reshape Catch Data with Length Groupings
#'
#' This function takes KoBoToolbox survey data with species catch information and reshapes
#' it into long format while properly handling nested length group information. It supports
#' survey forms where fish over 100cm are stored in separate repeated group structures.
#'
#' @param df A data frame containing catch data with species groups and length information.
#'   Expected to have columns following the pattern:
#'   - species_group.X columns (where X is a position number)
#'   - Multiple species fields within each group (species_TL, species_RF, species_SH, etc.)
#'   - Optional length group columns (species_group/no_fish_by_length_group_100/)
#'
#' @return A data frame in long format with each row representing a species catch record.
#'         The output includes columns:
#'         - submission_id: Unique survey submission identifier
#'         - n_catch: Catch record number within submission (1-based)
#'         - counting_method: Method used to count/measure catch
#'         - species: FAO 3-alpha species code (normalized from multiple species_* fields)
#'         - n_buckets, weight_bucket, catch_weight: Bucket-based measurements
#'         - length_range: Length category (e.g., "5_10", "over100")
#'         - length_over: Specific length for fish over 100cm
#'         - count: Number of individuals in that length range
#'
#' @export
#'
#' @details
#' The function performs the following steps:
#' 1. Reshapes species groups from wide to long using `reshape_species_groups()`
#' 2. Normalizes multiple species fields (species_TL for teleosts, species_RF for rays/fish,
#'    species_SH for sharks, species_FSH for finfish, species_CRB for crabs, species_CE for
#'    cephalopods, species_LO for lobster, species_CR for crustacea, species_MA for marine animals,
#'    species_OY for oysters, species_FI for fish, species_FFI for flatfish, species_RA for rays,
#'    species_SHK for sharks, species_MZZ for miscellaneous) into a single 'species' column
#' 3. Processes separate length group data for fish over 100cm when available
#' 4. Combines length data with species metadata
#' 5. Handles submissions with and without length data appropriately
#'
#' For fish over 100cm, the function extracts both the specific length measurement
#' and the count, properly associating them with the corresponding species group.
#'
#' @keywords preprocessing
#'
#' @examples
#' \dontrun{
#' # Reshape catch data from raw survey
#' catch_long <- reshape_catch_data(raw_survey_data)
#'
#' # Analyze counts by length range
#' catch_long |>
#'   dplyr::filter(!is.na(count)) |>
#'   dplyr::group_by(species, length_range) |>
#'   dplyr::summarize(total_count = sum(as.numeric(count), na.rm = TRUE))
#'
#' # View fish over 100cm with their specific lengths
#' catch_long |>
#'   dplyr::filter(length_range == "over100") |>
#'   dplyr::select(submission_id, species, length_over, count)
#' }
reshape_catch_data <- function(df = NULL) {
  # First, reshape species groups
  species_long <- reshape_species_groups(df)

  # Normalize multiple species fields into single 'species' field
  # Get all species column names
  species_cols <- names(species_long)[startsWith(
    names(species_long),
    "species_"
  )]

  # Create the species column by coalescing
  species_long <- species_long |>
    dplyr::mutate(
      species = dplyr::coalesce(!!!rlang::syms(species_cols))
    ) |>
    dplyr::select(-dplyr::all_of(species_cols)) |>
    dplyr::relocate("species", .after = "counting_method")

  # Get separate length group columns
  separate_length_cols <- names(df)[grep(
    "species_group/no_fish_by_length_group_100/",
    names(df)
  )]

  # If no separate length group data, return base data
  if (length(separate_length_cols) == 0) {
    return(species_long)
  }

  # Process the separate length group data
  separate_length_data <- df |>
    dplyr::select("submission_id", dplyr::all_of(separate_length_cols)) |>
    tidyr::pivot_longer(
      cols = -"submission_id",
      names_to = "length_category",
      values_to = "count_value",
      values_drop_na = TRUE
    ) |>
    dplyr::mutate(
      length_category_clean = stringr::str_remove(
        .data$length_category,
        "species_group/no_fish_by_length_group_100/"
      ),
      # Remove any trailing suffixes like ...32, ...38
      length_category_clean = stringr::str_remove(
        .data$length_category_clean,
        "\\.\\.\\..*$"
      ),
      # Extract actual length ranges or special categories
      length_range = dplyr::case_when(
        stringr::str_detect(
          .data$length_category_clean,
          "no_individuals_over100"
        ) ~
          "over100",
        stringr::str_detect(
          .data$length_category_clean,
          "fish_length_over100"
        ) ~
          "over100",
        TRUE ~ .data$length_category_clean
      ),
      # For fish_length_over100 entries, move the value to length_over and set count to 1
      length_over = dplyr::if_else(
        stringr::str_detect(
          .data$length_category_clean,
          "fish_length_over100"
        ),
        .data$count_value,
        NA_character_
      ),
      count = dplyr::if_else(
        stringr::str_detect(
          .data$length_category_clean,
          "fish_length_over100"
        ),
        "1",
        .data$count_value
      )
    ) |>
    dplyr::filter(
      !(.data$length_range == "over100" &
        is.na(.data$length_over) &
        is.na(.data$count))
    ) |>
    dplyr::select("submission_id", "length_range", "length_over", "count")

  # If no length data after processing, return base species data
  if (nrow(separate_length_data) == 0) {
    return(species_long)
  }

  # Create base species data for separate length entries
  base_species_cols <- intersect(
    c(
      "submission_id",
      "n_catch",
      "counting_method",
      "species",
      "fish_group",
      "catch_label",
      "photo",
      "other_species_name",
      "n_buckets",
      "weight_bucket",
      "catch_weight"
    ),
    names(species_long)
  )

  base_species_data <- species_long |>
    dplyr::select(dplyr::all_of(base_species_cols)) |>
    dplyr::distinct() |>
    dplyr::filter(
      .data$submission_id %in% separate_length_data$submission_id
    )

  # Get max n_catch per submission_id to avoid conflicts
  max_n_catch <- species_long |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      max_n_catch = max(.data$n_catch, na.rm = TRUE),
      .groups = "drop"
    )

  # Join separate length data with base species data
  length_data <- separate_length_data |>
    dplyr::left_join(base_species_data, by = "submission_id") |>
    dplyr::left_join(max_n_catch, by = "submission_id") |>
    # Create a new n_catch value for the separate length entries
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      n_catch = .data$max_n_catch + dplyr::row_number()
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-"max_n_catch")

  # Process rows without length data
  rows_with_length <- species_long$submission_id %in%
    separate_length_data$submission_id

  if (any(!rows_with_length)) {
    no_length_data <- species_long |>
      dplyr::filter(
        !.data$submission_id %in% separate_length_data$submission_id
      ) |>
      # Add empty length columns to match length_data structure
      dplyr::mutate(
        count = NA_character_,
        length_range = NA_character_,
        length_over = NA_character_
      )

    # Combine both datasets
    result <- dplyr::bind_rows(length_data, no_length_data) |>
      dplyr::arrange(.data$submission_id, .data$n_catch)
  } else {
    result <- length_data
  }

  return(result)
}
