#' Calculate Catch Weights Using Length-Weight Relationships
#'
#' @description
#' This function calculates catch weights using length-weight relationships from FishBase.
#' It processes catch data by mapping common names to scientific names, retrieving
#' length-weight coefficients, and applying the length-weight equation to estimate catches.
#'
#' @param catch_df A tibble containing catch data with columns:
#'   \itemize{
#'     \item submission_id: Unique identifier for the submission
#'     \item catch_taxon: Species or group name
#'     \item length_class: Length class in centimeters
#'     \item counts: Number of individuals
#'   }
#'
#' @details
#' The function performs several steps:
#' 1. Groups related species categories (e.g., combining sardines and pilchards)
#' 2. Maps common names to scientific names using FishBase
#' 3. Retrieves length-weight relationships for species in the area (C_Code = 508)
#' 4. Calculates median length-weight coefficients for each species group
#' 5. Estimates catch weights using the length-weight equation: W = a * L^b
#'
#' @return A tibble containing the original catch data plus:
#' \itemize{
#'   \item catch_gr: Estimated catch weight in grams
#' }
#'
#' @note
#' - Length-weight relationships are filtered for area code 508
#' - Median values of length-weight coefficients are used when multiple relationships exist
#' - Only relationships with no quality issues (EsQ is NA) are used
#'
#' @seealso
#' \code{\link{preprocess_landings}} for data preprocessing steps
#'
#' @examples
#' \dontrun{
#' catch_estimates <- calculate_catch(preprocessed_catch_data)
#' }
#'
#' @keywords modeling analytics
#' @export
calculate_catch <- function(catch_df = NULL) {
  group_mapping <- list(
    "Sardines/pilchards" = c("sardine", "pilchard"),
    "Jacks/Trevally/Other Scad" = c("jack", "trevally", "scad"),
    "Snapper/seaperch" = c("snapper", "seaperch"),
    "Tuna/Bonito/Other Mackerel" = c("tuna", "bonito", "mackerel"),
    "Javelin/Grunt" = c("javelin", "grunt"),
    "Mojarra/Silverbelly" = c("mojarra", "silverbelly")
  )

  # Get all unique terms
  decomposed_list <- unlist(group_mapping)

  # Create initial sci_names list
  sci_names <-
    stats::na.omit(c(unique(catch_df$catch_taxon), decomposed_list)) %>%
    purrr::set_names() %>%
    purrr::map(rfishbase::common_to_sci) %>%
    purrr::map(~ rfishbase::country(unique(.$Species), fields = c("Species", "SpecCode", "C_Code")))

  # Combine groups and remove individual components
  for (group_name in names(group_mapping)) {
    components <- group_mapping[[group_name]]
    sci_names[[group_name]] <- dplyr::bind_rows(sci_names[components])
    sci_names[components] <- NULL # Remove the individual components
  }


  lw_coeffs <-
    sci_names %>%
    dplyr::bind_rows(.id = "catch_taxon") %>%
    dplyr::filter(.data$C_Code == 508) %>%
    dplyr::select("catch_taxon", "Species") %>%
    dplyr::distinct() %>%
    {
      species_list <- unique(.$Species)
      dplyr::left_join(
        .,
        rfishbase::length_weight(species_list, fields = c("Species", "a", "b", "EsQ")),
        by = "Species",
        relationship = "many-to-many"
      )
    } %>%
    dplyr::filter(is.na(.data$EsQ)) %>%
    dplyr::select(-"EsQ") %>%
    dplyr::group_by(.data$catch_taxon) %>%
    dplyr::summarise(
      a = stats::median(.data$a, na.rm = TRUE),
      b = stats::median(.data$b, na.rm = TRUE)
    )

  catch_df %>%
    dplyr::left_join(lw_coeffs, by = "catch_taxon") %>%
    dplyr::mutate(catch_gr = dplyr::case_when(
      is.na(.data$collection_type) | .data$collection_type == "1" ~ (.data$a * .data$length_class^.data$b) * .data$counts,
      .data$collection_type == "2" ~ (.data$n_buckets * .data$weight_bucket) / 1000,
      TRUE ~ NA_real_
    )) %>%
    dplyr::select(-c("a", "b"))
}
