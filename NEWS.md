# peskas.mozambique.data.pipeline 1.0.0

## Major Changes

* **GPS Tracking Integration with Pelagic Data Systems (PDS)**: Full support for vessel tracking data ingestion and preprocessing.
  * Added `ingest_pds_trips()` to retrieve and store GPS trip metadata from PDS API
  * Added `ingest_pds_tracks()` to download individual trip point data with parallel processing support
  * Implemented `get_trips()` and `get_trip_points()` functions for PDS API interaction
  * Added `preprocess_pds_tracks()` for track data cleaning and feature extraction
  * Introduced `preprocess_track_data()` with configurable track processing pipelines
  * Created `generate_track_summaries()` for trip-level metrics calculation

* **Airtable Integration Module**: Complete suite of functions for two-way synchronization with Airtable.
  * Implemented `airtable_to_df()` with automatic pagination for full table retrieval
  * Added `device_sync()` for intelligent device data synchronization (updates existing, creates new)
  * Created `sync_device_users()` to manage vessel user credentials across Airtable and MongoDB
  * Developed `bulk_update_airtable()` and `df_to_airtable()` for batch operations
  * Added `get_writable_fields()` to identify editable fields and prevent computed field errors

* **Comprehensive Data Validation Framework**: Implemented multi-stage validation adapted from Peskas Zanzibar pipeline.
  * Redesigned `validate_landings()` with 10 validation flags across two stages
  * Stage 1: Basic data quality checks (form completeness, catch info, length validation, bucket/individual counts)
  * Stage 2: Composite economic indicators (price per kg, CPUE, RPUE) following Zanzibar thresholds
  * Created modular validation functions: `validate_catch_taxa()`, `validate_price()`, `validate_total_catch()`
  * Validation results exclude flagged submissions from final dataset while preserving flags for monitoring

* **Taxa Modeling and Species Intelligence**: New module for automated species identification and biological data enrichment.
  * Introduced `match_species_from_taxa()` using fuzzy matching against FishBase and SeaLifeBase
  * Implemented `get_fao_groups()` for commercial species categorization
  * Added `get_species_areas_batch()` for biogeographic validation
  * Created `get_length_weight_batch()` for length-weight relationship parameters
  * Developed `getLWCoeffs()` to retrieve stored coefficients from local database
  * Added `process_species_list()` for batch processing with automatic fallback logic

## Improvements

* **Storage System Enhancements**:
  * Migrated primary storage to Google Cloud Storage with Parquet format
  * Added `upload_parquet_to_cloud()` and `download_parquet_from_cloud()` with versioning support
  * Implemented `cloud_storage_authenticate()` with temporary credential file handling
  * Created `cloud_object_name()` for version-aware object retrieval
  * MongoDB maintained as secondary storage for legacy compatibility
  * Moved length-weight coefficients from `data/` to `inst/` directory for better package structure

* **Configuration Management**:
  * Switched to `dotenv` package for environment variable management
  * Added `load_dotenv()` function with configurable .env file paths
  * Updated `read_config()` to automatically load environment variables
  * Expanded configuration schema to support PDS, Airtable, and multi-cloud storage
  * Added support for separate storage buckets for different data types (surveys vs. tracks)

* **Data Preprocessing Pipeline**:
  * Enhanced `preprocess_landings()` with metadata table joins (landing sites, boats, enumerators)
  * Implemented `process_species_group()` for handling species group disaggregation
  * Added species validation and enrichment with FishBase/SeaLifeBase data
  * Integrated length-weight conversion using local coefficient database
  * Added habitat information from species area data
  * Improved catch weight calculation with multiple estimation methods

* **Export Functionality**:
  * Expanded `export_landings()` to generate multiple analytical outputs
  * Added `calculate_fishery_metrics()` for aggregated statistics
  * Created MongoDB portal collections for dashboard integration
  * Implemented trip-level summarization for GPS track data
  * Enhanced data transformation for consumption by visualization tools

* **Workflow Automation**:
  * Added GitHub Actions workflow for automated releases (`release.yaml`)
  * Updated data pipeline workflow with improved error handling and notifications
  * Integrated cloud authentication in CI/CD pipeline
  * Added support for scheduled and manual workflow triggers

## Bug Fixes

* Fixed price validation logic that was incorrectly flagging valid entries (#PR/issue reference if applicable)
* Corrected global variable bindings in validation functions to prevent R CMD check warnings
* Removed invalid `geo` parameter from `mdb_collection_push()` function call
* Fixed `customer_name` and `submission_id` variable scoping issues using `.data$` notation

## Infrastructure & Dependencies

* Added new package dependencies: `furrr`, `future`, `glue`, `readr` for enhanced functionality
* Updated `.Rbuildignore` to exclude development files (`.env`, `.claude`, `CLAUDE.md`)
* Package now passes R CMD check with no warnings or notes
* Improved documentation coverage with 34 new exported functions
* Enhanced type safety and code consistency throughout codebase

# peskas.mozambique.data.pipeline 0.2.0

### New features

- Updates data ingestion and preprocessing workflows
- Renames `ingest_surveys` to `ingest_landings`
- Adds new metadata joins and data transformations in `preprocess_landings`
- Introduces `calculate_catch` function for catch weight estimation
- Updates configuration to include Google Cloud Storage and additional metadata tables

# peskas.mozambique.data.pipeline 0.1.0

* Initial CRAN submission.
