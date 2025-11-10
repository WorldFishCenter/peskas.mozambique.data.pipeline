# Changelog

## peskas.mozambique.data.pipeline 2.1.0

### Major Changes

- **Enhanced Validation Sync System**: Restructured validation
  synchronization following Kenya pipeline best practices.
  - Added
    [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/sync_validation_submissions.md)
    for bidirectional validation status updates with rate limiting
  - Implemented
    [`process_submissions_parallel()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_submissions_parallel.md)
    helper function for consistent API interactions
  - **Rate limiting** (0.1-0.2s delays) prevents overwhelming
    KoboToolbox API
  - **Manual approval respect**: Human review decisions are never
    overwritten by system updates
  - **Optimized API usage**: Skips already-approved submissions to
    minimize unnecessary calls
  - Fetches current validation status BEFORE making updates for smarter
    decision-making
  - Automated approval/rejection of submissions in KoboToolbox based on
    validation results
  - Stores validation metadata in MongoDB for enumerator performance
    tracking
  - Enhanced error tracking with success/failure logging
- **Improved Asset Management**: Enhanced preprocessing with
  form-specific asset filtering.
  - Preprocessing functions now automatically filter Airtable assets by
    form_id
  - Better separation of metadata between Lurio and ADNAP survey forms
  - Improved handling of shared assets across multiple survey versions
  - More reliable species, gear, vessel, and site mappings
- **Optimized GitHub Actions Workflow**: Streamlined pipeline execution
  by combining ingestion and preprocessing stages.
  - Combined ingest and preprocess jobs for each data source (Lurio,
    ADNAP, PDS) reducing container startups by ~30%
  - Simplified dependency graph from 10 jobs to 7 jobs for faster
    pipeline execution
  - Maintained separation of validation stages for better error
    isolation and independent re-runs
  - Aligned workflow structure with Kenya pipeline best practices

### Improvements

- **Validation System Enhancements**:
  - Manual approvals by human reviewers now properly bypass automatic
    validation flags
  - System-generated approvals are re-validated to ensure data quality
  - Better logging of validation status queries with submission counts
  - Enhanced validation flag preservation for monitoring and reporting
  - Improved handling of catch_taxon field mapping in Lurio surveys
- **Configuration Management**:
  - Restructured MongoDB connection strings to support separate
    validation database
  - Added KOBO_TOKEN authentication for ADNAP asset
  - Improved configuration structure for multiple database contexts
  - Enhanced PDS storage configuration organization
  - Added explicit assets configuration for Airtable integration
- **Workflow Performance**:
  - Reduced overall pipeline execution time through job consolidation
  - Parallel execution of independent data streams (Lurio, ADNAP, PDS)
  - Cleaner job naming for better CI/CD monitoring
  - Maintained robust error handling with granular validation stages
- **Code Quality**:
  - Added new exported functions:
    [`summarize_data()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/summarize_data.md),
    [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/sync_validation_submissions.md),
    [`process_submissions_parallel()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_submissions_parallel.md)
  - Enhanced function documentation with proper importFrom declarations
  - Improved variable scoping and data pipeline clarity
  - Better separation of concerns between preprocessing and validation
  - Centralized API interaction logic in reusable helper functions

### Bug Fixes

- Fixed asset fetching logic to properly filter by target form_id
- Corrected catch_taxon column mapping in Lurio validation (changed to
  alpha3_code)
- Fixed validation status query to exclude system approvals from manual
  approval overrides
- Fixed MongoDB configuration path typo (collection → collections) in
  enumerators_stats
- Removed redundant asset fetching code in preprocessing functions
- Added missing KOBO_USERNAME configuration for ADNAP asset
- Fixed sync function to never overwrite manual approvals from human
  reviewers

### Infrastructure & Dependencies

- Added MONGODB_CONNECTION_STRING_VALIDATION environment variable for
  separate validation database
- Enhanced GitHub Actions workflow with combined ingest-preprocess jobs
- Improved parallel processing configuration in validation sync
- Updated NAMESPACE with new imports for future, furrr, and progressr
  packages
- Maintained compatibility with existing storage and authentication
  systems

## peskas.mozambique.data.pipeline 2.0.0

### Major Changes

- **Dual Survey System Integration**: Full support for both Lurio and
  ADNAP fisheries surveys with parallel processing workflows.
  - Added
    [`ingest_landings_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_landings_lurio.md)
    and
    [`ingest_landings_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_landings_adnap.md)
    for separate survey data streams
  - Implemented
    [`preprocess_landings_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_landings_lurio.md)
    and
    [`preprocess_landings_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_landings_adnap.md)
    with survey-specific transformations
  - Created
    [`validate_surveys_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_surveys_lurio.md)
    and
    [`validate_surveys_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_surveys_adnap.md)
    with tailored validation rules
  - Added
    [`calculate_catch_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/calculate_catch_lurio.md)
    and
    [`calculate_catch_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/calculate_catch_adnap.md)
    for survey-specific catch weight estimation
  - Introduced survey version detection and adaptive processing via
    [`reshape_catch_data()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/reshape_catch_data.md)
- **Enhanced Validation System for ADNAP**: Advanced validation with
  KoBoToolbox integration.
  - Integrated KoBoToolbox validation status API for manual approval
    workflow
  - Added
    [`get_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_validation_status.md)
    to query submission approval status
  - Implemented parallel processing for validation status queries across
    multiple submissions
  - Manual approvals in KoBoToolbox now bypass automatic validation
    flags
  - Maintained two-stage validation (7 basic checks + 3 composite
    economic indicators)
- **Flexible Survey Data Reshaping**: New module for handling multiple
  survey form structures.
  - Introduced
    [`reshape_species_groups()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/reshape_species_groups.md)
    for converting wide-format species data to long format
  - Created
    [`reshape_catch_data()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/reshape_catch_data.md)
    supporting both version 1 and version 2 survey structures
  - Added
    [`preprocess_catch()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_catch.md)
    with automatic survey version detection
  - Implemented
    [`preprocess_general_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_general_adnap.md)
    for ADNAP-specific trip information processing
  - Enhanced handling of nested length groups and fish over 100cm
- **Improved GitHub Actions Workflow**: Enhanced automation with clearer
  job naming and parallel execution.
  - Renamed workflow jobs for better visibility (e.g., “Ingest Lurio
    landings”, “Validate ADNAP landings”)
  - Parallel execution of Lurio and ADNAP pipelines for faster
    processing
  - Separate PDS data ingestion and preprocessing jobs
  - Clear dependency chains between ingestion, preprocessing, and
    validation stages

### Improvements

- **Documentation Overhaul**:
  - Corrected all storage backend references from MongoDB to Google
    Cloud Storage
  - Updated function documentation to accurately reflect Parquet file
    usage
  - Added specific titles to distinguish Lurio and ADNAP functions
  - Removed misleading unused parameters from ingestion functions
  - Added explicit `invisible(NULL)` returns to all workflow functions
    for consistency
  - Enhanced documentation for KoBoToolbox validation integration
  - Improved parameter documentation honesty (noting hardcoded values)
- **Code Quality Enhancements**:
  - Cleaned up function signatures by removing unused parameters
  - Made return values explicit across all workflow functions
  - Improved consistency between function documentation and
    implementation
  - Enhanced examples to reflect actual usage patterns
- **Survey Processing Pipeline**:
  - Added support for multiple species field normalization
  - Improved handling of separate length group structures for large fish
  - Enhanced catch data validation with species-specific thresholds
  - Better integration with Airtable form assets for data mapping

### Bug Fixes

- Fixed incorrect package reference in documentation (removed
  non-existent KoboconnectR package)
- Corrected validation threshold documentation (200 individuals,
  not 100) for ADNAP surveys
- Fixed duplicate GitHub Actions job names that made debugging difficult
- Corrected storage backend documentation throughout codebase (MongoDB →
  GCS)
- Updated validation flag numbering documentation for consistency across
  surveys

### Infrastructure & Dependencies

- Maintained compatibility with parallel processing packages (`future`,
  `furrr`)
- Enhanced configuration system to support multiple survey sources
- Improved separation of concerns between Lurio and ADNAP processing
  pipelines
- Updated documentation generation with roxygen2
- Package maintains clean R CMD check status

## peskas.mozambique.data.pipeline 1.0.0

### Major Changes

- **GPS Tracking Integration with Pelagic Data Systems (PDS)**: Full
  support for vessel tracking data ingestion and preprocessing.
  - Added
    [`ingest_pds_trips()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_pds_trips.md)
    to retrieve and store GPS trip metadata from PDS API
  - Added
    [`ingest_pds_tracks()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_pds_tracks.md)
    to download individual trip point data with parallel processing
    support
  - Implemented
    [`get_trips()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_trips.md)
    and
    [`get_trip_points()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_trip_points.md)
    functions for PDS API interaction
  - Added
    [`preprocess_pds_tracks()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_pds_tracks.md)
    for track data cleaning and feature extraction
  - Introduced
    [`preprocess_track_data()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_track_data.md)
    with configurable track processing pipelines
  - Created
    [`generate_track_summaries()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/generate_track_summaries.md)
    for trip-level metrics calculation
- **Airtable Integration Module**: Complete suite of functions for
  two-way synchronization with Airtable.
  - Implemented
    [`airtable_to_df()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/airtable_to_df.md)
    with automatic pagination for full table retrieval
  - Added
    [`device_sync()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/device_sync.md)
    for intelligent device data synchronization (updates existing,
    creates new)
  - Created
    [`sync_device_users()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/sync_device_users.md)
    to manage vessel user credentials across Airtable and MongoDB
  - Developed
    [`bulk_update_airtable()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/bulk_update_airtable.md)
    and
    [`df_to_airtable()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/df_to_airtable.md)
    for batch operations
  - Added
    [`get_writable_fields()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_writable_fields.md)
    to identify editable fields and prevent computed field errors
- **Comprehensive Data Validation Framework**: Implemented multi-stage
  validation adapted from Peskas Zanzibar pipeline.
  - Redesigned `validate_landings()` with 10 validation flags across two
    stages
  - Stage 1: Basic data quality checks (form completeness, catch info,
    length validation, bucket/individual counts)
  - Stage 2: Composite economic indicators (price per kg, CPUE, RPUE)
    following Zanzibar thresholds
  - Created modular validation functions:
    [`validate_catch_taxa()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_catch_taxa.md),
    [`validate_price()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_price.md),
    [`validate_total_catch()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_total_catch.md)
  - Validation results exclude flagged submissions from final dataset
    while preserving flags for monitoring
- **Taxa Modeling and Species Intelligence**: New module for automated
  species identification and biological data enrichment.
  - Introduced
    [`match_species_from_taxa()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/match_species_from_taxa.md)
    using fuzzy matching against FishBase and SeaLifeBase
  - Implemented
    [`get_fao_groups()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_fao_groups.md)
    for commercial species categorization
  - Added
    [`get_species_areas_batch()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_species_areas_batch.md)
    for biogeographic validation
  - Created
    [`get_length_weight_batch()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_length_weight_batch.md)
    for length-weight relationship parameters
  - Developed
    [`getLWCoeffs()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/getLWCoeffs.md)
    to retrieve stored coefficients from local database
  - Added
    [`process_species_list()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_species_list.md)
    for batch processing with automatic fallback logic

### Improvements

- **Storage System Enhancements**:
  - Migrated primary storage to Google Cloud Storage with Parquet format
  - Added
    [`upload_parquet_to_cloud()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/upload_parquet_to_cloud.md)
    and
    [`download_parquet_from_cloud()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/download_parquet_from_cloud.md)
    with versioning support
  - Implemented
    [`cloud_storage_authenticate()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/cloud_storage_authenticate.md)
    with temporary credential file handling
  - Created
    [`cloud_object_name()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/cloud_object_name.md)
    for version-aware object retrieval
  - MongoDB maintained as secondary storage for legacy compatibility
  - Moved length-weight coefficients from `data/` to `inst/` directory
    for better package structure
- **Configuration Management**:
  - Switched to `dotenv` package for environment variable management
  - Added
    [`load_dotenv()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/load_dotenv.md)
    function with configurable .env file paths
  - Updated
    [`read_config()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/read_config.md)
    to automatically load environment variables
  - Expanded configuration schema to support PDS, Airtable, and
    multi-cloud storage
  - Added support for separate storage buckets for different data types
    (surveys vs. tracks)
- **Data Preprocessing Pipeline**:
  - Enhanced `preprocess_landings()` with metadata table joins (landing
    sites, boats, enumerators)
  - Implemented
    [`process_species_group()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_species_group.md)
    for handling species group disaggregation
  - Added species validation and enrichment with FishBase/SeaLifeBase
    data
  - Integrated length-weight conversion using local coefficient database
  - Added habitat information from species area data
  - Improved catch weight calculation with multiple estimation methods
- **Export Functionality**:
  - Expanded
    [`export_landings()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_landings.md)
    to generate multiple analytical outputs
  - Added
    [`calculate_fishery_metrics()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/calculate_fishery_metrics.md)
    for aggregated statistics
  - Created MongoDB portal collections for dashboard integration
  - Implemented trip-level summarization for GPS track data
  - Enhanced data transformation for consumption by visualization tools
- **Workflow Automation**:
  - Added GitHub Actions workflow for automated releases
    (`release.yaml`)
  - Updated data pipeline workflow with improved error handling and
    notifications
  - Integrated cloud authentication in CI/CD pipeline
  - Added support for scheduled and manual workflow triggers

### Bug Fixes

- Fixed price validation logic that was incorrectly flagging valid
  entries (#PR/issue reference if applicable)
- Corrected global variable bindings in validation functions to prevent
  R CMD check warnings
- Removed invalid `geo` parameter from
  [`mdb_collection_push()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/mdb_collection_push.md)
  function call
- Fixed `customer_name` and `submission_id` variable scoping issues
  using `.data$` notation

### Infrastructure & Dependencies

- Added new package dependencies: `furrr`, `future`, `glue`, `readr` for
  enhanced functionality
- Updated `.Rbuildignore` to exclude development files (`.env`,
  `.claude`, `CLAUDE.md`)
- Package now passes R CMD check with no warnings or notes
- Improved documentation coverage with 34 new exported functions
- Enhanced type safety and code consistency throughout codebase

## peskas.mozambique.data.pipeline 0.2.0

#### New features

- Updates data ingestion and preprocessing workflows
- Renames `ingest_surveys` to `ingest_landings`
- Adds new metadata joins and data transformations in
  `preprocess_landings`
- Introduces `calculate_catch` function for catch weight estimation
- Updates configuration to include Google Cloud Storage and additional
  metadata tables

## peskas.mozambique.data.pipeline 0.1.0

- Initial CRAN submission.
