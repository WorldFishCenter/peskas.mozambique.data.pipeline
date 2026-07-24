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
#' - Regular length group data for fish under 100cm (e.g., 5_10, 10_15, etc.)
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
      names = c("min", "max"),
      too_few = "align_start" # Handle NA values and ranges without enough parts
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
      "fish_group",
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
    dplyr::mutate(
      catch_taxon = dplyr::case_when(
        is.na(.data$catch_taxon) & .data$fish_group == "MZZ" ~ "MZZ",
        TRUE ~ .data$catch_taxon
      )
    ) |>
    dplyr::select(-"fish_group")
}

#' Expand Length Frequency Data for a Single Species Row
#'
#' Takes a single species row and expands it into multiple rows if length frequency
#' data is present. Preserves all metadata (counting_method, species, etc.) throughout.
#'
#' @param species_row A single-row data frame containing one species record
#'
#' @return A data frame with one or more rows:
#'   - If length frequency data exists: multiple rows (one per length bin)
#'   - If no length frequency data: single row with NA for length fields
#'
#' @keywords internal
#'
#' @details
#' This function processes length frequency data in a row-by-row manner, which is
#' simpler and more robust than extracting all length data first and then joining.
#'
#' The function:
#' 1. Identifies length group columns in the row
#' 2. If found: pivots them to create multiple rows (one per length bin)
#' 3. If not found: returns the row as-is with NA length fields
#' 4. Always preserves all metadata columns
expand_length_frequency <- function(species_row) {
  # Get all column names
  all_cols <- names(species_row)

  # Find length group columns (under 100cm)
  length_cols <- all_cols[grep(
    "no_fish_by_length_group/no_individuals_[0-9]+_[0-9]+",
    all_cols
  )]

  # If no length columns found, return row with NA length fields
  if (length(length_cols) == 0) {
    return(
      species_row |>
        dplyr::mutate(
          length_range = NA_character_,
          length_over = NA_character_,
          count = NA_character_
        )
    )
  }

  # Get metadata columns (everything except length columns)
  metadata_cols <- setdiff(all_cols, length_cols)

  # Pivot length columns to long format
  expanded <- species_row |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(length_cols),
      names_to = "length_category",
      values_to = "count",
      values_drop_na = TRUE # Drop empty length bins
    ) |>
    dplyr::mutate(
      # Extract length range (e.g., "5_10") from column name
      length_range = stringr::str_extract(
        .data$length_category,
        "[0-9]+_[0-9]+"
      ),
      length_over = NA_character_
    ) |>
    dplyr::select(-"length_category")

  # If pivot resulted in zero rows (all NA), return original row with NA length
  if (nrow(expanded) == 0) {
    return(
      species_row |>
        dplyr::select(dplyr::all_of(metadata_cols)) |>
        dplyr::mutate(
          length_range = NA_character_,
          length_over = NA_character_,
          count = NA_character_
        )
    )
  }

  return(expanded)
}


#' Process Regular Length Groups (Under 100cm) - DEPRECATED
#'
#' This function is kept for reference but is no longer used.
#' Use expand_length_frequency() instead for simpler row-by-row processing.
#'
#' @param species_long A data frame with species groups already reshaped to long format
#'
#' @return A data frame with columns: submission_id, n_catch, length_range, count
#'   Returns NULL if no regular length group data is found
#'
#' @keywords internal
process_regular_length_groups_old <- function(species_long = NULL) {
  # Find columns that contain regular length group data (under 100cm)
  # These follow pattern: no_fish_by_length_group/no_individuals_5_10, etc.
  length_cols <- names(species_long)[grep(
    "no_fish_by_length_group/no_individuals_[0-9]+_[0-9]+",
    names(species_long)
  )]

  # If no regular length group columns exist, return NULL
  if (length(length_cols) == 0) {
    return(NULL)
  }

  # Pivot length group columns to long format
  length_data <- species_long |>
    dplyr::select(
      "submission_id",
      "n_catch",
      dplyr::all_of(length_cols)
    ) |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(length_cols),
      names_to = "length_category",
      values_to = "count",
      values_drop_na = TRUE
    ) |>
    dplyr::mutate(
      # Extract just the length range (e.g., "5_10") from full column name
      # Pattern: no_fish_by_length_group/no_individuals_5_10 -> 5_10
      length_range = stringr::str_extract(
        .data$length_category,
        "[0-9]+_[0-9]+"
      ),
      # Set length_over to NA for regular length groups
      length_over = NA_character_
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "length_range",
      "length_over",
      "count"
    )

  return(length_data)
}


#' Process Length Groups for Fish Over 100cm
#'
#' Extracts and processes length data for large fish (over 100cm) from KoBoToolbox
#' survey data. These are stored in separate repeated group structures outside
#' the main species_group.
#'
#' @param df The original raw data frame containing separate length group columns
#' @param species_long A data frame with species groups already reshaped to long format
#'
#' @return A data frame with columns: submission_id, n_catch, length_range, length_over, count
#'   Returns NULL if no over-100cm length group data is found
#'
#' @keywords internal
#'
#' @details
#' Length groups for fish over 100cm are stored separately from the main species_group
#' structure, in columns following the pattern:
#' species_group/no_fish_by_length_group_100/fish_length_over100
#'
#' The function:
#' 1. Identifies columns matching the over-100cm pattern
#' 2. Pivots them to long format
#' 3. For fish_length_over100 entries, extracts the actual length value
#' 4. Links this data back to the corresponding species group via submission_id
#' 5. Assigns new n_catch values to avoid conflicts with existing species groups
process_over100_length_groups <- function(df = NULL, species_long = NULL) {
  # Find columns for fish over 100cm
  # These follow pattern: species_group/no_fish_by_length_group_100/...
  over100_cols <- names(df)[grep(
    "species_group/no_fish_by_length_group_100/",
    names(df)
  )]

  # If no over-100cm columns exist, return NULL
  if (length(over100_cols) == 0) {
    return(NULL)
  }

  # Process the over-100cm length group data
  length_data_over100 <- df |>
    dplyr::select("submission_id", dplyr::all_of(over100_cols)) |>
    tidyr::pivot_longer(
      cols = -"submission_id",
      names_to = "length_category",
      values_to = "count_value",
      values_drop_na = TRUE
    ) |>
    dplyr::mutate(
      # Clean up the column name by removing the prefix
      length_category_clean = stringr::str_remove(
        .data$length_category,
        "species_group/no_fish_by_length_group_100/"
      ),
      # Remove any trailing suffixes like ...32, ...38
      length_category_clean = stringr::str_remove(
        .data$length_category_clean,
        "\\.\\.\\..*$"
      ),
      # Determine the length range category
      length_range = dplyr::case_when(
        stringr::str_detect(
          .data$length_category_clean,
          "no_individuals_over100"
        ) ~ "over100",
        stringr::str_detect(
          .data$length_category_clean,
          "fish_length_over100"
        ) ~ "over100",
        TRUE ~ .data$length_category_clean
      ),
      # For fish_length_over100, the count_value is the actual length
      # Move it to length_over column and set count to 1
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
    # Remove entries that don't have valid length or count data
    dplyr::filter(
      !(.data$length_range == "over100" &
        is.na(.data$length_over) &
        is.na(.data$count))
    ) |>
    dplyr::select("submission_id", "length_range", "length_over", "count")

  # If no valid length data after processing, return NULL
  if (nrow(length_data_over100) == 0) {
    return(NULL)
  }

  # Get base species data (metadata) for these submissions
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
      .data$submission_id %in% length_data_over100$submission_id
    )

  # Get max n_catch per submission to assign new n_catch values
  # This prevents conflicts with existing species groups
  max_n_catch <- species_long |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      max_n_catch = max(.data$n_catch, na.rm = TRUE),
      .groups = "drop"
    )

  # Combine length data with species metadata and assign new n_catch values
  result <- length_data_over100 |>
    dplyr::left_join(base_species_data, by = "submission_id") |>
    dplyr::left_join(max_n_catch, by = "submission_id") |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::mutate(
      n_catch = .data$max_n_catch + dplyr::row_number()
    ) |>
    dplyr::ungroup() |>
    dplyr::select(-"max_n_catch")

  return(result)
}


#' Reshape Catch Data with Length Groupings
#'
#' This function takes KoBoToolbox survey data with species catch information and reshapes
#' it into long format while properly handling nested length group information. It supports
#' both regular length groups (under 100cm) and separate structures for fish over 100cm.
#'
#' @param df A data frame containing catch data with species groups and length information.
#'   Expected to have columns following the pattern:
#'   - species_group.X columns (where X is a position number)
#'   - Multiple species fields within each group (species_TL, species_RF, species_SH, etc.)
#'   - Optional regular length group columns (species_group.X.species_group/no_fish_by_length_group/no_individuals_Y_Z)
#'   - Optional over-100cm length group columns (species_group/no_fish_by_length_group_100/)
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
#' 3. Processes each species row individually using `expand_length_frequency()` to:
#'    - Check if length frequency data exists for that row
#'    - If yes: expand to multiple rows (one per length bin)
#'    - If no: keep single row with NA for length fields
#'    - Always preserve all metadata (counting_method, species, etc.)
#' 4. Removes length group columns after pivoting
#' 5. Sorts by submission_id and n_catch
#'
#' This approach is simpler and more robust than the old extract-and-join method,
#' as it processes each species row individually and preserves all metadata throughout.
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
  # Step 1: Reshape species groups from wide to long format
  # Converts species_group.0, species_group.1, etc. into separate rows
  species_long <- reshape_species_groups(df)

  # Step 2: Normalize multiple species fields into a single 'species' column
  # Different survey forms may use different field names (species_TL, species_RF, etc.)
  # We coalesce them all into one 'species' field
  species_cols <- names(species_long)[startsWith(
    names(species_long),
    "species_"
  )]

  species_long <- species_long |>
    dplyr::mutate(
      species = dplyr::coalesce(!!!rlang::syms(species_cols))
    ) |>
    dplyr::select(-dplyr::all_of(species_cols)) |>
    dplyr::relocate("species", .after = "counting_method")

  # Step 3: Expand length frequency data row-by-row
  # This approach is simpler and more robust than the old extract-and-join method
  # For each species row, check if it has length data and expand if needed
  result <- species_long |>
    # Process each row individually
    dplyr::rowwise() |>
    dplyr::group_split() |>
    purrr::map_dfr(expand_length_frequency) |>
    # Remove the length group columns now that they've been pivoted
    dplyr::select(-dplyr::matches("no_fish_by_length_group/")) |>
    dplyr::arrange(.data$submission_id, .data$n_catch)

  return(result)
}
