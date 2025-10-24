#' Calculate Catch Weight from Length-Weight Relationships or Bucket Measurements
#'
#' @description
#' Calculates total catch weight using either length-weight relationships or bucket measurements.
#' The function prioritizes length-based calculations when available, falling back to bucket-based
#' measurements when length data is missing. For Octopus (OCZ), the function converts total length (TL)
#' to mantle length (ML) by dividing TL by 5.5 before applying the length-weight formula.
#' This accounts for species-specific differences in body morphology.
#'
#' @param catch_data A data frame containing catch information with columns:
#'   \itemize{
#'     \item submission_id - Unique identifier for the catch
#'     \item n_catch - Number of catch events
#'     \item catch_taxon - FAO 3-alpha code
#'     \item individuals - Number of individuals (for length-based calculations)
#'     \item length - Length measurement in cm
#'     \item n_buckets - Number of buckets
#'     \item weight_bucket - Weight per bucket in kg
#'   }
#' @param lwcoeffs A data frame containing length-weight coefficients with columns:
#'   \itemize{
#'     \item catch_taxon - FAO 3-alpha code
#'     \item a_6 - 60th percentile of parameter 'a'
#'     \item b_6 - 60th percentile of parameter 'b'
#'   }
#'
#' @return A tibble with the following columns:
#'   \itemize{
#'     \item submission_id - Unique identifier for the catch
#'     \item n_catch - Number of catch events
#'     \item catch_taxon - FAO 3-alpha code
#'     \item individuals - Number of individuals
#'     \item length - Length measurement in cm
#'     \item n_buckets - Number of buckets
#'     \item weight_bucket - Weight per bucket in kg
#'     \item catch_kg - Total catch weight in kg
#'   }
#'
#' @details
#' The function calculates catch weight using two methods:
#' 1. Length-based calculation: W = a * L^b * N / 1000
#'    Where:
#'    - W is total weight in kg
#'    - a and b are length-weight relationship coefficients (75th percentile)
#'    - L is length in cm
#'    - N is number of individuals
#'
#' 2. Bucket-based calculation: W = n_buckets * weight_bucket
#'    Where:
#'    - W is total weight in kg
#'    - n_buckets is number of buckets
#'    - weight_bucket is weight per bucket in kg
#'
#' The final catch_kg uses length-based calculation when available,
#' falling back to bucket-based calculation when length data is missing.
#'
#' @examples
#' \dontrun{
#' # Calculate catch weights
#' catch_weights <- calculate_catch_adnap(
#'   catch_data = catch_data,
#'   lwcoeffs = length_weight_coeffs
#' )
#' }
#'
#' @note
#' - Length-based calculations use 75th percentile of length-weight coefficients
#' - All weights are returned in kilograms
#' - NA values are returned when neither calculation method is possible
#'
#' @keywords mining preprocessing
#' @export
calculate_catch_adnap <- function(catch_data = NULL, lwcoeffs = NULL) {
  catch_data |>
    dplyr::left_join(lwcoeffs, by = "catch_taxon") |>
    dplyr::mutate(
      # Calculate weight in grams for records with length measurements
      catch_length_gr = dplyr::case_when(
        # Specific case for Octopus cyanea (OCZ) - using length conversion
        !is.na(.data$length) &
          !is.na(.data$a_6) &
          !is.na(.data$b_6) &
          .data$catch_taxon == "OCZ" ~
          .data$a_6 * ((.data$length / 5.5)^.data$b_6),
        # General case for other species - direct calculation
        !is.na(.data$length) & !is.na(.data$a_6) & !is.na(.data$b_6) ~
          .data$a_6 * (.data$length^.data$b_6),
        # Otherwise NA
        TRUE ~ NA_real_
      ),
      # Convert to kilograms
      catch_length_kg = (.data$catch_length_gr * .data$individuals) / 1000,
      # Calculate weight from bucket information if available
      catch_bucket_kg = dplyr::case_when(
        !is.na(.data$n_buckets) & !is.na(.data$weight_bucket) ~
          .data$n_buckets * .data$weight_bucket,
        # Otherwise NA
        TRUE ~ NA_real_
      )
    ) |>
    dplyr::mutate(
      catch_kg = dplyr::coalesce(.data$catch_length_kg, .data$catch_bucket_kg)
    ) |>
    dplyr::select(
      "submission_id",
      "n_catch",
      "catch_taxon",
      "individuals",
      "length",
      "n_buckets",
      "weight_bucket",
      "catch_kg"
    )
}

#' Get Length-Weight Coefficients and Morphological Data for Species
#'
#' @description
#' Retrieves and summarizes length-weight relationship coefficients and morphological data
#' by handling both FishBase and SeaLifeBase data in a single workflow.
#'
#' @param taxa_list Character vector of FAO 3-alpha codes
#' @param asfis_list ASFIS list data frame
#' @return A list with two elements:
#'   \itemize{
#'     \item lw - A data frame with length-weight coefficients:
#'       \itemize{
#'         \item catch_taxon - FAO 3-alpha code
#'         \item n - Number of measurements
#'         \item a_6 - 60th percentile of parameter 'a'
#'         \item b_6 - 60th percentile of parameter 'b'
#'       }
#'     \item ml - A data frame with morphological data:
#'       \itemize{
#'         \item catch_taxon - FAO 3-alpha code
#'         \item n - Number of measurements
#'         \item max_length_75 - 75th percentile of maximum length
#'         \item max_weightkg_75 - 75th percentile of maximum weight in kg
#'       }
#'   }
#' @examples
#' \dontrun{
#' # Get coefficients and morphological data
#' results <- getLWCoeffs(taxa_list, asfis_list)
#'
#' # Access length-weight coefficients
#' lw_coeffs <- results$lw
#'
#' # Access morphological data
#' morph_data <- results$ml
#' }
#' @keywords mining preprocessing
#' @export
#'
getLWCoeffs <- function(taxa_list = NULL, asfis_list = NULL) {
  # 1. Load both databases
  taxa_data <- list(
    fishbase = rfishbase::load_taxa(server = "fishbase"),
    sealifebase = rfishbase::load_taxa(server = "sealifebase")
  )

  # 2. Process species list
  species_list <- process_species_list(
    fao_codes = taxa_list,
    asfis_list = asfis_list
  )

  # 3. Match species in databases
  matched_species <- match_species_from_taxa(species_list, taxa_data)

  # 4. Get FAO areas and filter for area 51
  species_areas <- get_species_areas_batch(matched_species)
  species_areas_filtered <- species_areas %>%
    dplyr::filter(.data$area_code == 51)

  # 5. Get length-weight parameters
  lw_data <- get_length_weight_batch(
    species_areas_filtered,
    include_morphology = TRUE
  )

  # 6. Format output
  lw <-
    lw_data$length_weight %>%
    dplyr::filter(!(.data$a3_code == "PEZ" & .data$type != "TL")) %>%
    dplyr::filter(!(.data$a3_code == "OCZ" & !.data$type == "ML")) %>%
    dplyr::filter(!(.data$a3_code == "IAX" & !.data$type == "TL")) %>%
    dplyr::group_by(.data$a3_code) %>%
    dplyr::summarise(
      n = dplyr::n(),
      a_6 = stats::quantile(.data$a, 0.6, na.rm = TRUE),
      b_6 = stats::quantile(.data$b, 0.6, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::select(
      catch_taxon = "a3_code",
      "n",
      "a_6",
      "b_6"
    )

  ml <-
    lw_data$morphology %>%
    dplyr::group_by(.data$a3_code) %>%
    dplyr::summarise(
      n = dplyr::n(),
      min_length = min(.data$CommonLength, na.rm = TRUE),
      max_length_75 = stats::quantile(.data$Length, 0.75, na.rm = TRUE),
      max_weightkg_75 = stats::quantile(.data$Weight, 0.75, na.rm = TRUE) /
        1000,
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      max_length_75 = dplyr::case_when(
        .data$a3_code == "IAX" ~ 100,
        TRUE ~ .data$max_length_75
      ),
      min_length = .data$min_length - 0.25 * .data$min_length,
      min_length = dplyr::case_when(
        .data$a3_code %in% c("OCZ", "IAX") ~ 15,
        .data$a3_code == "PEZ" ~ 5,
        .data$a3_code == "COZ" ~ 2,
        TRUE ~ .data$min_length
      )
    ) |>
    dplyr::select(
      catch_taxon = "a3_code",
      "n",
      "min_length",
      "max_length_75",
      "max_weightkg_75"
    )

  return(list(lw = lw, ml = ml))
}

#' Extract and Format FAO Taxonomic Groups
#'
#' @description
#' Filters and formats taxonomic information from the FAO ASFIS list for specified FAO 3-alpha codes,
#' excluding miscellaneous ("MZZ") and unknown ("UNKN") categories.
#'
#' @param fao_codes A character vector of FAO 3-alpha codes to extract. If NULL, returns empty dataset.
#' @param asfis_list A data frame containing the FAO ASFIS list with required columns:
#'   \itemize{
#'     \item Alpha3_Code - FAO 3-alpha code
#'     \item Scientific_Name - Scientific name of taxon
#'     \item English_name - Common name in English
#'     \item Family - Family name
#'     \item Order - Order name
#'     \item ISSCAAP_Group - FAO ISSCAAP group number
#'   }
#'
#' @return A tibble with standardized column names containing taxonomic information:
#'   \itemize{
#'     \item a3_code - FAO 3-alpha code
#'     \item scientific_name - Scientific name
#'     \item english_name - Common name in English
#'     \item family - Family name
#'     \item order - Order name
#'     \item taxon_group - ISSCAAP group number
#'   }
#'
#' @details
#' The function:
#' 1. Filters ASFIS list for specified FAO codes
#' 2. Standardizes column names for consistency
#' 3. Removes miscellaneous ("MZZ") and unknown ("UNKN") categories
#' 4. Preserves the taxonomic hierarchy information
#'
#' @examples
#' # Example ASFIS data
#' asfis <- data.frame(
#'   Alpha3_Code = c("TUN", "MZZ", "RAG"),
#'   Scientific_Name = c("Thunnini", "Marine fishes nei", "Rastrelliger kanagurta"),
#'   English_name = c("Tunas", "Marine fishes", "Indian mackerel"),
#'   Family = c("SCOMBRIDAE", NA, "SCOMBRIDAE"),
#'   Order = c("PERCIFORMES", NA, "PERCIFORMES"),
#'   ISSCAAP_Group = c(36, 39, 37)
#' )
#'
#' # Get taxonomic information for specific codes
#' fao_taxa <- get_fao_groups(c("TUN", "RAG"), asfis)
#'
#' @note
#' - Requires dplyr package
#' - MZZ (Miscellaneous marine fishes) and UNKN (Unknown) are automatically excluded
#' - Column names are standardized for consistency with other functions
#'
#' @keywords mining preprocessing
#'
#' @export
get_fao_groups <- function(fao_codes = NULL, asfis_list = NULL) {
  asfis_list %>%
    dplyr::filter(.data$Alpha3_Code %in% fao_codes) %>%
    dplyr::select(
      a3_code = "Alpha3_Code",
      scientific_name = "Scientific_Name",
      english_name = "English_name",
      family = "Family",
      order = "Order",
      taxon_group = "ISSCAAP_Group"
    ) |>
    dplyr::filter(!.data$a3_code %in% c("MZZ", "UNKN"))
}


#' Load Taxa Data from FishBase and SeaLifeBase
#'
#' @description
#' Retrieves taxonomic data from both FishBase and SeaLifeBase databases in a single function call.
#' This is typically the first step in species identification and classification.
#'
#' @return A list with two elements:
#'   \itemize{
#'     \item fishbase: Data frame containing FishBase taxonomic data
#'     \item sealifebase: Data frame containing SeaLifeBase taxonomic data
#'   }
#' @examples
#' \dontrun{
#' taxa_data <- load_taxa_databases()
#' fishbase_taxa <- taxa_data$fishbase
#' sealifebase_taxa <- taxa_data$sealifebase
#' }
#' @keywords mining preprocessing
#' @export
load_taxa_databases <- function() {
  list(
    fishbase = rfishbase::load_taxa(server = "fishbase"),
    sealifebase = rfishbase::load_taxa(server = "sealifebase")
  )
}

#' Process Species List with Taxonomic Information
#'
#' @description
#' Processes a list of species by assigning database sources and taxonomic ranks.
#' Determines whether species should be looked up in FishBase or SeaLifeBase based
#' on their ISSCAAP group.
#'
#' @param fao_codes Vector of FAO 3-alpha codes
#' @param asfis_list ASFIS list data frame containing taxonomic information
#' @return A data frame with columns:
#'   \itemize{
#'     \item a3_code: FAO 3-alpha code
#'     \item scientific_name: Scientific name (cleaned)
#'     \item database: "fishbase" or "sealifebase"
#'     \item rank: Taxonomic rank ("Genus", "Family", "Order", "Species")
#'     \item ... (other taxonomic fields)
#'   }
#' @note
#' ISSCAAP groups 57, 45, 43, 42, 56 are assigned to SeaLifeBase;
#' all others to FishBase
#' @examples
#' \dontrun{
#' species_list <- process_species_list(c("TUN", "PEZ"), asfis_data)
#' }
#' @keywords mining preprocessing
#' @export
process_species_list <- function(fao_codes, asfis_list) {
  get_fao_groups(fao_codes = fao_codes, asfis_list = asfis_list) %>%
    dplyr::mutate(
      database = dplyr::case_when(
        .data$taxon_group %in% c(57, 45, 43, 42, 56) ~ "sealifebase",
        TRUE ~ "fishbase"
      ),
      rank = dplyr::case_when(
        grepl(" spp$", .data$scientific_name) ~ "Genus",
        grepl("idae$", .data$scientific_name) ~ "Family",
        grepl("formes$", .data$scientific_name) ~ "Order",
        grepl(" ", .data$scientific_name) &
          !grepl(" spp$|nei$", .data$scientific_name) ~
          "Species",
        TRUE ~ NA_character_
      ),
      scientific_name = gsub(" spp$", "", .data$scientific_name)
    )
}

#' Match Species from Taxa Databases
#'
#' @description
#' Matches species between FAO codes and database records, handling different taxonomic
#' levels (species, genus, family, order) appropriately.
#'
#' @param species_list Processed species list from process_species_list()
#' @param taxa_data Taxa data from load_taxa_databases()
#' @return A data frame with columns:
#'   \itemize{
#'     \item a3_code: FAO 3-alpha code
#'     \item species: Scientific name
#'     \item database: Source database
#'   }
#' @examples
#' \dontrun{
#' taxa_data <- load_taxa_databases()
#' species_list <- process_species_list(fao_codes, asfis)
#' matches <- match_species_from_taxa(species_list, taxa_data)
#' }
#' @keywords mining preprocessing
#' @export
match_species_from_taxa <- function(species_list, taxa_data) {
  matches <- list()

  for (i in 1:nrow(species_list)) {
    row <- species_list[i, ]
    taxa <- taxa_data[[row$database]]

    matched_species <- switch(
      row$rank,
      "Genus" = taxa %>% dplyr::filter(.data$Genus == row$scientific_name),
      "Family" = taxa %>% dplyr::filter(.data$Family == row$scientific_name),
      "Order" = taxa %>% dplyr::filter(.data$Order == row$scientific_name),
      "Species" = taxa %>% dplyr::filter(.data$Species == row$scientific_name),
      NULL
    )

    if (!is.null(matched_species) && nrow(matched_species) > 0) {
      matches[[i]] <- matched_species %>%
        dplyr::mutate(
          a3_code = row$a3_code,
          original_rank = row$rank,
          original_name = row$scientific_name,
          database = row$database
        )
    }
  }

  dplyr::bind_rows(matches) %>%
    dplyr::select(
      "a3_code",
      species = "Species",
      "database"
      # original_rank,
      # original_name
    ) %>%
    dplyr::distinct()
}


#' Get FAO Areas for Species (Batch Version)
#'
#' @description
#' Efficiently retrieves FAO areas for multiple species by processing them in batches
#' by database source, reducing API calls and processing time.
#'
#' @param matched_species Data frame from match_species_from_taxa()
#' @return A data frame with columns:
#'   \itemize{
#'     \item a3_code: FAO 3-alpha code
#'     \item species: Scientific name
#'     \item area_code: FAO area code
#'     \item database: Source database
#'   }
#' @examples
#' \dontrun{
#' species_areas <- get_species_areas_batch(matched_species)
#' # Filter for specific FAO area
#' area_51_species <- species_areas %>%
#'   dplyr::filter(area_code == 51)
#' }
#' @keywords mining preprocessing
#' @export
get_species_areas_batch <- function(matched_species) {
  fishbase_species <- matched_species %>%
    dplyr::filter(.data$database == "fishbase") %>%
    dplyr::pull(.data$species)

  sealifebase_species <- matched_species %>%
    dplyr::filter(.data$database == "sealifebase") %>%
    dplyr::pull(.data$species)

  areas_fishbase <- if (length(fishbase_species) > 0) {
    rfishbase::faoareas(
      fishbase_species,
      fields = "AreaCode",
      server = "fishbase"
    ) %>%
      dplyr::mutate(database = "fishbase")
  } else {
    data.frame(
      Species = character(0),
      AreaCode = integer(0),
      database = character(0)
    )
  }

  areas_sealifebase <- if (length(sealifebase_species) > 0) {
    rfishbase::faoareas(
      sealifebase_species,
      fields = "AreaCode",
      server = "sealifebase"
    ) %>%
      dplyr::mutate(database = "sealifebase")
  } else {
    data.frame(
      Species = character(0),
      AreaCode = integer(0),
      database = character(0)
    )
  }

  dplyr::bind_rows(areas_fishbase, areas_sealifebase) %>%
    dplyr::left_join(
      matched_species,
      by = c("Species" = "species", "database")
    ) %>%
    dplyr::select(
      .data$a3_code,
      species = "Species",
      area_code = "AreaCode",
      .data$database
    ) %>%
    dplyr::distinct()
}


#' Get Length-Weight and Morphological Parameters for Species (Batch Version)
#'
#' @description
#' Retrieves length-weight relationship parameters and optional morphological data
#' for multiple species efficiently by processing them in batches. Handles both
#' fish and non-fish species appropriately.
#'
#' @param species_areas_filtered Data frame with filtered species
#' @param include_morphology Logical, whether to include morphological data (Length,
#'   CommonLength, Weight). Default is FALSE.
#' @return If include_morphology is FALSE (default), a data frame with columns:
#'   \itemize{
#'     \item a3_code: FAO 3-alpha code
#'     \item species: Scientific name
#'     \item area_code: FAO area code
#'     \item database: Source database
#'     \item type: Measurement type (e.g., "TL" for total length)
#'     \item a: Length-weight parameter a
#'     \item b: Length-weight parameter b
#'   }
#'
#'   If include_morphology is TRUE, a list with two elements:
#'   \itemize{
#'     \item length_weight: Data frame as described above
#'     \item morphology: Data frame with columns:
#'       \itemize{
#'         \item a3_code: FAO 3-alpha code
#'         \item species: Scientific name
#'         \item area_code: FAO area code
#'         \item database: Source database
#'         \item Length: Maximum recorded length
#'         \item CommonLength: Common length
#'         \item Weight: Maximum weight
#'       }
#'   }
#'
#' @note
#' - For FishBase species, only total length (TL) measurements are used
#' - Questionable estimates (EsQ = "yes") are excluded
#' @examples
#' \dontrun{
#' # Get just length-weight parameters
#' lw_data <- get_length_weight_batch(species_areas_filtered)
#'
#' # Get both length-weight and morphological data
#' results <- get_length_weight_batch(species_areas_filtered, include_morphology = TRUE)
#' lw_data <- results$length_weight
#' morph_data <- results$morphology
#' }
#' @keywords mining preprocessing
#' @export
#'
get_length_weight_batch <- function(
  species_areas_filtered,
  include_morphology = FALSE
) {
  fishbase_species <- species_areas_filtered %>%
    dplyr::filter(.data$database == "fishbase") %>%
    dplyr::pull(.data$species)

  sealifebase_species <- species_areas_filtered %>%
    dplyr::filter(.data$database == "sealifebase") %>%
    dplyr::pull(.data$species)

  # Get length-weight parameters
  lw_fishbase <- if (length(fishbase_species) > 0) {
    rfishbase::length_weight(
      fishbase_species,
      fields = c("Species", "SpecCode", "Type", "EsQ", "a", "b"),
      server = "fishbase"
    ) %>%
      dplyr::mutate(database = "fishbase")
  } else {
    data.frame(
      Species = character(0),
      SpecCode = integer(0),
      Type = character(0),
      EsQ = character(0),
      a = numeric(0),
      b = numeric(0),
      database = character(0)
    )
  }

  lw_sealifebase <- if (length(sealifebase_species) > 0) {
    rfishbase::length_weight(
      sealifebase_species,
      fields = c("Species", "SpecCode", "Type", "EsQ", "a", "b"),
      server = "sealifebase"
    ) %>%
      dplyr::mutate(database = "sealifebase")
  } else {
    data.frame(
      Species = character(0),
      SpecCode = integer(0),
      Type = character(0),
      EsQ = character(0),
      a = numeric(0),
      b = numeric(0),
      database = character(0)
    )
  }

  lw_data <- dplyr::bind_rows(lw_fishbase, lw_sealifebase) %>%
    dplyr::left_join(
      species_areas_filtered,
      by = c("Species" = "species", "database")
    ) %>%
    dplyr::filter(
      (.data$database == "fishbase" & .data$Type == "TL") |
        .data$database == "sealifebase",
      is.na(.data$EsQ) | tolower(.data$EsQ) != "yes"
    ) %>%
    dplyr::select(
      .data$a3_code,
      species = "Species",
      .data$area_code,
      .data$database,
      type = "Type",
      .data$a,
      .data$b
    ) %>%
    dplyr::distinct()

  # Get morphological information if requested
  if (include_morphology) {
    morph_fishbase <- if (length(fishbase_species) > 0) {
      rfishbase::species(
        fishbase_species,
        fields = c("Species", "SpecCode", "Length", "CommonLength", "Weight"),
        server = "fishbase"
      ) %>%
        dplyr::mutate(database = "fishbase")
    } else {
      data.frame(
        Species = character(0),
        SpecCode = integer(0),
        Length = numeric(0),
        CommonLength = numeric(0),
        Weight = numeric(0),
        database = character(0)
      )
    }

    morph_sealifebase <- if (length(sealifebase_species) > 0) {
      rfishbase::species(
        sealifebase_species,
        fields = c("Species", "SpecCode", "Length", "CommonLength", "Weight"),
        server = "sealifebase"
      ) %>%
        dplyr::mutate(database = "sealifebase")
    } else {
      data.frame(
        Species = character(0),
        SpecCode = integer(0),
        Length = numeric(0),
        CommonLength = numeric(0),
        Weight = numeric(0),
        database = character(0)
      )
    }

    morph_data <- dplyr::bind_rows(morph_fishbase, morph_sealifebase) %>%
      dplyr::left_join(
        species_areas_filtered,
        by = c("Species" = "species", "database")
      ) %>%
      dplyr::select(
        .data$a3_code,
        species = "Species",
        .data$area_code,
        .data$database,
        .data$Length,
        .data$CommonLength,
        .data$Weight
      ) %>%
      dplyr::distinct()

    # Return a list with both datasets
    return(list(
      length_weight = lw_data,
      morphology = morph_data
    ))
  }

  # Return only length-weight data if morphology not requested
  return(lw_data)
}
