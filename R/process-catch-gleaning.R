#' Pre-process WorldFish Zanzibar Gleaning Surveys
#'
#' Downloads raw structured gleaning survey data from cloud storage and
#' preprocesses it into a single analysis-ready data frame. The function
#' assembles three pieces and joins them on the submission:
#' \enumerate{
#'   \item \strong{General info} -- strips the Kobo group prefixes
#'         (`group_general/`, `group_trip/`, `no_fishers/`, `demographics/`,
#'         `group_gleaning_activity/`, `group_supply_chain/`), selects and
#'         renames the trip, demographic, activity and supply-chain fields,
#'         coalesces the conditional `landing_site` columns into one, and
#'         coerces dates and numeric fields.
#'   \item \strong{Catch info} -- reshapes the wide `group_catch` block into a
#'         tidy long table (one row per submission x shell group x size class)
#'         via \code{\link{reshape_gleaning_catch}}, unifying the parallel
#'         bucket/plastic container fields and applying conservative
#'         sanitisation.
#'   \item \strong{Catch totals} -- per submission, sums individuals across
#'         size classes (`total_individuals`) and reconstructs catch weight as
#'         `unit_weight_kg * n_containers` (`total_catch_kg`); the container
#'         fields are constant within a submission, hence `first()`.
#' }
#'
#' Configurations are read from `config.yml` with the following necessary
#' parameters:
#'
#' ```
#' surveys:
#'   wf_gleaning:
#'     raw:
#'       file_prefix:
#'       version:
#' storage:
#'   google:
#'     key:
#'     options:
#'       project:
#'       bucket:
#'       service_account_key:
#' ```
#'
#' The function uses logging to track progress.
#'
#' @param log_threshold Logging threshold level (default: `logger::DEBUG`).
#' @return A data frame of preprocessed gleaning surveys: one row per
#'   submission x shell group x size class, with general/demographic/activity/
#'   supply-chain fields plus `total_individuals` and `total_catch_kg`.
#' @export
#' @keywords workflow preprocessing
#' @seealso \code{\link{reshape_gleaning_catch}}, \code{\link{sanitize_gleaning_inputs}}
preprocess_wf_gleaning <- function(log_threshold = logger::DEBUG) {
  logger::log_threshold(log_threshold)
  conf <- read_config()

  raw_dat <- coasts::download_parquet_from_cloud(
    prefix = conf$surveys$wf_gleaning$raw$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options,
    version = conf$surveys$wf_gleaning$raw$version
  )

  general_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "no_fishers/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "demographics/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_gleaning_activity/")) %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_supply_chain/")) %>%
    dplyr::select(
      # general
      "submission_id",
      submitted_by = "_submitted_by",
      submission_date = "_submission_time",
      "landing_date",
      "district",
      dplyr::contains("landing_site"),
      "collect_data_today",
      survey_activity = "gleaners_collected",
      # demographics
      fisher_name = "gleaner_name",
      "gender",
      "age",
      "education",
      # activity
      "days_collection_week",
      trip_duration = "hours_collection",
      fishing_ground = "collection_area",
      habitat = "Where_did_you_go_to_collect_shells",
      "transport",
      vessel_type = "vessel",
      "vessel_status",
      "vessel_cost",
      fuel_L = "fuel",
      propulsion_gear = "propulsion",
      dplyr::ends_with("_n"),
      "gear",
      catch_outcome = "collect_data_today",
      # supply chain
      "conservation",
      "catch_use",
      "market",
      "who_selling",
      "selling_time",
      "home_consumption",
      catch_price = "revenue",
      happiness_rating = "happiness"
    ) %>%
    dplyr::mutate(
      district = dplyr::coalesce(
        !!!dplyr::select(., dplyr::contains("district"))
      )
    ) |>
    # dplyr::select(-dplyr::contains("landing_site"), "landing_site") |>
    # dplyr::relocate("landing_site", .after = "district") |>
    dplyr::mutate(
      landing_date = lubridate::as_date(.data$landing_date),
      submission_date = lubridate::as_date(.data$submission_date),
      dplyr::across(
        c(
          dplyr::contains("days_collection_week"),
          "trip_duration",
          "catch_price",
          dplyr::ends_with("_n")
        ),
        ~ as.double(.x)
      )
    )

  catch_info <-
    raw_dat %>%
    dplyr::rename_with(~ stringr::str_remove(., "group_general/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "group_trip/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "no_fishers/")) |>
    dplyr::rename_with(~ stringr::str_remove(., "demographics/")) |>
    dplyr::select(
      "submission_id",
      "collect_data_today",
      survey_activity = "gleaners_collected",
      catch_outcome = "collect_data_today",
      dplyr::contains("group_catch")
    ) |>
    reshape_gleaning_catch()

  catch_totals <-
    catch_info |>
    dplyr::group_by(.data$submission_id) |>
    dplyr::summarise(
      total_individuals = sum(.data$n_individuals, na.rm = TRUE),
      total_catch_kg = dplyr::first(.data$unit_weight_kg) *
        dplyr::first(.data$n_containers),
      .groups = "drop"
    )

  gleaning <- general_info |>
    dplyr::left_join(
      catch_info,
      by = c("submission_id", "catch_outcome", "survey_activity")
    ) |>
    dplyr::left_join(catch_totals, by = "submission_id") |>
    #fix fields
    dplyr::mutate(
      size_class = as.character(.data$size_class),
      happiness_rating = as.integer(.data$happiness_rating)
    )

  coasts::upload_parquet_to_cloud(
    data = gleaning,
    prefix = conf$surveys$wf_gleaning$preprocessed$file_prefix,
    provider = conf$storage$google$key,
    options = conf$storage$google$options
  )
}

#' Sanitize Gleaning Catch Inputs
#'
#' Drops (sets to NA) values that fall outside physically plausible bounds.
#' Defensive against data-entry slips and unit/decimal-separator confusion
#' (e.g. a single 10 kg bucket recorded as `bucket_weight = 13000`, or a full
#' bucket count of `7000`). Bounds are deliberately conservative: they remove
#' only the unambiguously impossible. Stricter, taxon-aware checks belong
#' downstream — this only protects the reshaped output from extreme outliers.
#'
#' Bounds:
#' \itemize{
#'   \item `n_individuals` in 0-10000 — a bucket of small shells can hold
#'         thousands; counts above this are almost certainly errors. Recorded
#'         zeros are kept (a true "none in this size class").
#'   \item `unit_weight_kg` in 0-100 — nominal containers are 5–50 kg.
#'   \item `n_containers` in 0-200 — hand gleaning rarely exceeds a few
#'         full containers; very large counts are data slips.
#' }
#'
#' Columns absent from the input are left untouched.
#'
#' @param df A data frame containing any subset of the columns above.
#' @return The input with out-of-range values replaced by NA.
#' @keywords internal
#' @export
sanitize_gleaning_inputs <- function(df) {
  df |>
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of("n_individuals"),
        ~ dplyr::if_else(.x >= 0 & .x <= 10000, .x, NA_real_)
      ),
      dplyr::across(
        dplyr::any_of("unit_weight_kg"),
        ~ dplyr::if_else(.x > 0 & .x <= 100, .x, NA_real_)
      ),
      dplyr::across(
        dplyr::any_of("n_containers"),
        ~ dplyr::if_else(.x >= 0 & .x <= 200, .x, NA_real_)
      )
    )
}


#' Reshape Gleaning Catch Data from Wide to Long Format
#'
#' Reshapes the `group_catch` section of the Zanzibar/Pemba intertidal gleaning
#' KoboToolbox survey into a tidy long format. Unlike the boat-fishery surveys
#' (see `reshape_catch_data_v2()`), `group_catch` is a *single* (non-repeated)
#' group per submission, so there is no `n_catch` index. The wide-to-long work
#' instead unpacks three layered structures:
#'
#' \enumerate{
#'   \item \strong{Counting method.} `counting_method` (`backet` / `plastic_bag`)
#'         populates two mutually exclusive sets of columns (`bucket_*` vs
#'         `plastic_*`). These are coalesced into one unified container block:
#'         `container_type`, `container_size`, `container_size_kg`,
#'         `catch_fraction`, `unit_weight_kg`, `n_containers`.
#'   \item \strong{Shell group + species.} `shell_group`
#'         (`bivalves` / `gastropod` / `both` / `others`) drives which species
#'         multi-selects (`Group_Bivalves`, `Group_Gastropod`) and which length
#'         block apply. Species stay as a space-separated code list per group
#'         (no per-species quantities exist in the instrument), with a derived
#'         `n_species` token count.
#'   \item \strong{Length frequency.} Individual counts by size class
#'         (`<5`, `5-15`, `>15` cm) are recorded once per shell GROUP, not per
#'         species. Each present group emits one row per size class.
#' }
#'
#' Output grain: one row per `submission_id` x `group` x `size_class`. A
#' submission recorded under `bivalves` or `gastropod` yields 3 rows; `both`
#' yields 6; `others` or a missing `shell_group` (no shell detail) is preserved
#' as a single context row with `group`/`size_class`/`n_individuals` = NA so no
#' submission is silently dropped. All three size classes are retained per
#' present group (NA = not recorded, 0 = a recorded zero) so the size-frequency
#' distribution stays explicit and complete.
#'
#' @param data A data frame of the gleaning survey export. Catch columns are
#'   expected with the raw Kobo prefixes (`group_catch/...`, including the
#'   nested `group_catch/group_length_gastropods/...` /
#'   `group_catch/group_length_bivalves/...`).
#'
#' @return A long data frame with one row per submission x shell group x size
#'   class (plus a single context row for submissions without shell detail).
#' @export
#' @keywords preprocessing
#' @examples
#' \dontrun{
#' gleaning_long <- reshape_gleaning_catch(gleaning)
#'
#' # Size-frequency by shell group across all submissions
#' gleaning_long |>
#'   dplyr::filter(!is.na(n_individuals)) |>
#'   dplyr::group_by(group, size_class) |>
#'   dplyr::summarise(total = sum(n_individuals), .groups = "drop")
#' }
#'
reshape_gleaning_catch <- function(data = NULL) {
  # ---- 0. Strip the outer group prefix; keep the nested length-group prefix
  #         (group_length_*/...) so the two length blocks stay distinguishable.
  catch <- data |>
    dplyr::select(
      "submission_id",
      dplyr::any_of(c("catch_outcome", "survey_activity")),
      dplyr::starts_with("group_catch/")
    ) |>
    dplyr::rename_with(~ stringr::str_remove(., "^group_catch/"))

  # ---- 1. Unify the parallel bucket_* / plastic_* container fields ----------
  catch <- catch |>
    dplyr::mutate(
      container_type = dplyr::case_when(
        .data$counting_method == "backet" ~ "bucket",
        .data$counting_method == "plastic_bag" ~ "plastic_bag",
        TRUE ~ NA_character_
      ),
      container_size = dplyr::coalesce(.data$bucket_size, .data$plastic_size),
      # Nominal capacity parsed from the size code (kg).
      container_size_kg = dplyr::case_when(
        .data$container_size == "small_bucket_5kg" ~ 5,
        .data$container_size == "medium_bucket_10kg" ~ 10,
        .data$container_size == "large_bucket_20_kg" ~ 20,
        .data$container_size == "small_size_10_kg" ~ 10,
        .data$container_size == "medium_size_25kg" ~ 25,
        .data$container_size == "large_50kg" ~ 50,
        TRUE ~ NA_real_
      ),
      # half_of_the_bucket / half_of_plastic_bag -> "half"; full_* -> "full".
      catch_fraction = dplyr::coalesce(
        .data$bucket_catch_size,
        .data$plastic_catch_size
      ),
      catch_fraction = stringr::str_replace(
        .data$catch_fraction,
        "^(half|full)_.*$",
        "\\1"
      ),
      unit_weight_kg = dplyr::coalesce(
        as.double(.data$bucket_weight),
        as.double(.data$plastic_weight)
      ),
      n_containers = dplyr::coalesce(
        as.double(.data$full_bucket_number),
        as.double(.data$full_plastic_number)
      )
    )

  # Submission-level context carried onto every output row.
  context <- catch |>
    dplyr::select(
      "submission_id",
      dplyr::any_of(c("catch_outcome", "survey_activity")),
      "shell_group",
      "container_type",
      "container_size",
      "container_size_kg",
      "catch_fraction",
      "unit_weight_kg",
      "n_containers",
      others_catch = dplyr::any_of("others")
    )

  # ---- 2. Per-group length-frequency long table ----------------------------
  bivalve_cols <- c(
    species_codes = "Group_Bivalves",
    length_photo = "group_length_bivalves/photo_001",
    `<5` = "group_length_bivalves/small_5_001",
    `5-15` = "group_length_bivalves/_5_15_001",
    `>15` = "group_length_bivalves/large_15_001"
  )

  bivalve_long <- catch |>
    dplyr::filter(.data$shell_group %in% c("bivalves", "both")) |>
    # 2. Safely select and rename ONLY the columns that actually exist in 'catch'
    dplyr::select("submission_id", dplyr::any_of(bivalve_cols)) |>
    # 3. Add your static column and safely cast the numeric columns to double
    dplyr::mutate(
      group = "bivalves",
      # across() + any_of() safely applies as.double ONLY if the columns were found
      dplyr::across(dplyr::any_of(c("<5", "5-15", ">15")), as.double)
    )

  # 1. Map your gastropod columns
  gastropod_cols <- c(
    species_codes = "Group_Gastropod",
    length_photo = "group_length_gastropods/photo",
    `<5` = "group_length_gastropods/small_5",
    `5-15` = "group_length_gastropods/_5_15",
    `>15` = "group_length_gastropods/large_15"
  )

  # 2. Apply the safe select and mutate
  gastropod_long <- catch |>
    dplyr::filter(.data$shell_group %in% c("gastropod", "both")) |>
    dplyr::select("submission_id", dplyr::any_of(gastropod_cols)) |>
    dplyr::mutate(
      group = "gastropod",
      dplyr::across(dplyr::any_of(c("<5", "5-15", ">15")), as.double)
    )

  size_levels <- c("<5", "5-15", ">15")

  groups_long <- dplyr::bind_rows(bivalve_long, gastropod_long) |>
    tidyr::pivot_longer(
      cols = dplyr::all_of(size_levels),
      names_to = "size_class",
      values_to = "n_individuals"
      # NB: NA counts are intentionally kept so all three size classes stay
      # present per group (do not set values_drop_na = TRUE here).
    ) |>
    dplyr::mutate(
      size_class = factor(
        .data$size_class,
        levels = size_levels,
        ordered = TRUE
      ),
      # token count of the space-separated multi-select (NA stays NA).
      n_species = stringr::str_count(.data$species_codes, "\\S+")
    )

  # ---- 3. Submissions with no shell detail (shell_group 'others' or NA) -----
  #         Preserve them as a single context row.
  no_detail <- context |>
    dplyr::filter(!.data$submission_id %in% groups_long$submission_id) |>
    dplyr::transmute(
      submission_id = .data$submission_id,
      group = NA_character_,
      species_codes = NA_character_,
      length_photo = NA_character_,
      n_individuals = NA_real_,
      size_class = factor(NA_character_, levels = size_levels, ordered = TRUE),
      n_species = NA_integer_
    )

  # ---- 4. Assemble, attach context, sanitize, order ------------------------
  dplyr::bind_rows(groups_long, no_detail) |>
    dplyr::left_join(context, by = "submission_id") |>
    sanitize_gleaning_inputs() |>
    dplyr::select(
      "submission_id",
      dplyr::any_of(c("catch_outcome", "survey_activity")),
      "shell_group",
      "group",
      "species_codes",
      "n_species",
      "size_class",
      "n_individuals",
      "container_type",
      "container_size_kg",
      "catch_fraction",
      "unit_weight_kg",
      "n_containers",
      dplyr::any_of("others_catch"),
      "length_photo"
    ) |>
    dplyr::arrange(.data$submission_id, .data$group, .data$size_class)
}
