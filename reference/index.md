# Package index

## Workflow

These are arguably the most important functions in the package. Each of
these functions executes a step in the data pipeline.

- [`export_api_raw()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_api_raw.md)
  : Export Raw API-Ready Trip Data
- [`export_api_validated()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_api_validated.md)
  : Export Validated API-Ready Trip Data
- [`export_landings()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_landings.md)
  : Export Processed Landings Data
- [`export_lurio_landings()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_lurio_landings.md)
  : Export Lurio landings data
- [`export_validation_flags()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_validation_flags.md)
  : Export Validation Flags to MongoDB
- [`get_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_validation_status.md)
  : Get Validation Status from KoboToolbox
- [`ingest_landings_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_landings_adnap.md)
  : Download and Process ADNAP Surveys from Kobotoolbox
- [`ingest_landings_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_landings_lurio.md)
  : Download and Process Lurio Surveys from Kobotoolbox
- [`ingest_pds_tracks()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_pds_tracks.md)
  : Ingest Pelagic Data Systems (PDS) Track Data
- [`ingest_pds_trips()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_pds_trips.md)
  : Ingest Pelagic Data Systems (PDS) Trip Data
- [`preprocess_landings_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_landings_adnap.md)
  : Preprocess ADNAP Landings Data
- [`preprocess_landings_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_landings_lurio.md)
  : Preprocess Lurio Landings Data
- [`preprocess_pds_tracks()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_pds_tracks.md)
  : Preprocess Pelagic Data Systems (PDS) Track Data
- [`summarize_data()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/summarize_data.md)
  : Summarize WorldFish Survey Data
- [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/sync_validation_submissions.md)
  : Synchronize Validation Statuses with KoboToolbox
- [`update_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/update_validation_status.md)
  : Update Validation Status in KoboToolbox
- [`validate_surveys_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_surveys_adnap.md)
  : Validate ADNAP Survey Data
- [`validate_surveys_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_surveys_lurio.md)
  : Validate Lurio Survey Data

## Cloud Storage

Functions that interact with cloud storage providers.

- [`cloud_object_name()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/cloud_object_name.md)
  : Generate Cloud Object Name
- [`cloud_storage_authenticate()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/cloud_storage_authenticate.md)
  : Authenticate to a Cloud Storage Provider
- [`download_cloud_file()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/download_cloud_file.md)
  : Download File from Cloud Storage
- [`download_parquet_from_cloud()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/download_parquet_from_cloud.md)
  : Download Parquet File from Cloud Storage
- [`get_metadata()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_metadata.md)
  : Get metadata tables
- [`mdb_collection_pull()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/mdb_collection_pull.md)
  : Retrieve Data from MongoDB
- [`mdb_collection_push()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/mdb_collection_push.md)
  : Upload Data to MongoDB and Overwrite Existing Content
- [`upload_cloud_file()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/upload_cloud_file.md)
  : Upload File to Cloud Storage
- [`upload_parquet_to_cloud()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/upload_parquet_to_cloud.md)
  : Upload Data as Parquet File to Cloud Storage

## Ingestion

Functions dedicated to the ingestion module

- [`get_kobo_data()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_kobo_data.md)
  : Retrieve Data from Kobotoolbox API
- [`get_trip_points()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_trip_points.md)
  : Get Trip Points from Pelagic Data Systems API
- [`get_trips()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_trips.md)
  : Retrieve Trip Details from Pelagic Data API
- [`ingest_landings_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_landings_adnap.md)
  : Download and Process ADNAP Surveys from Kobotoolbox
- [`ingest_landings_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_landings_lurio.md)
  : Download and Process Lurio Surveys from Kobotoolbox
- [`ingest_pds_tracks()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_pds_tracks.md)
  : Ingest Pelagic Data Systems (PDS) Track Data
- [`ingest_pds_trips()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/ingest_pds_trips.md)
  : Ingest Pelagic Data Systems (PDS) Trip Data

## Preprocessing

Functions dedicated to the preprocessing module

- [`calculate_catch_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/calculate_catch_adnap.md)
  : Calculate Catch Weight from Length-Weight Relationships or Bucket
  Measurements
- [`calculate_catch_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/calculate_catch_lurio.md)
  : Calculate Catch Weight from Length-Weight Relationships or Bucket
  Measurements
- [`calculate_fishery_metrics()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/calculate_fishery_metrics.md)
  : Calculate Fishery Metrics
- [`fetch_asset()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/fetch_asset.md)
  : Fetch and Filter Asset Data from Airtable
- [`fetch_assets()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/fetch_assets.md)
  : Fetch Multiple Asset Tables from Airtable
- [`generate_track_summaries()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/generate_track_summaries.md)
  : Generate Grid Summaries for Track Data
- [`getLWCoeffs()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/getLWCoeffs.md)
  : Get Length-Weight Coefficients and Morphological Data for Species
- [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_airtable_form_id.md)
  : Get Airtable Form ID from KoBoToolbox Asset ID
- [`get_fao_groups()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_fao_groups.md)
  : Extract and Format FAO Taxonomic Groups
- [`get_length_weight_batch()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_length_weight_batch.md)
  : Get Length-Weight and Morphological Parameters for Species (Batch
  Version)
- [`get_species_areas_batch()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_species_areas_batch.md)
  : Get FAO Areas for Species (Batch Version)
- [`load_taxa_databases()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/load_taxa_databases.md)
  : Load Taxa Data from FishBase and SeaLifeBase
- [`map_surveys()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/map_surveys.md)
  : Map Survey Labels to Standardized Taxa, Gear, and Vessel Names
- [`match_species_from_taxa()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/match_species_from_taxa.md)
  : Match Species from Taxa Databases
- [`preprocess_general_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_general_adnap.md)
  : Preprocess General Survey Information for ADNAP
- [`preprocess_landings_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_landings_adnap.md)
  : Preprocess ADNAP Landings Data
- [`preprocess_landings_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_landings_lurio.md)
  : Preprocess Lurio Landings Data
- [`preprocess_pds_tracks()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_pds_tracks.md)
  : Preprocess Pelagic Data Systems (PDS) Track Data
- [`preprocess_track_data()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/preprocess_track_data.md)
  : Preprocess Track Data into Spatial Grid Summary
- [`process_species_group()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_species_group.md)
  : Process Species Length and Catch Data
- [`process_species_list()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_species_list.md)
  : Process Species List with Taxonomic Information
- [`reshape_catch_data()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/reshape_catch_data.md)
  : Reshape Catch Data with Length Groupings
- [`reshape_species_groups()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/reshape_species_groups.md)
  : Reshape Species Groups from Wide to Long Format
- [`standardize_enumerator_names()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/standardize_enumerator_names.md)
  : Standardize Enumerator Names

## Validation

Functions dedicated to the validation module

- [`alert_outlier()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/alert_outlier.md)
  : Outlier Alert for Numeric Vectors
- [`export_validation_flags()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_validation_flags.md)
  : Export Validation Flags to MongoDB
- [`get_catch_bounds_taxon()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_catch_bounds_taxon.md)
  : Get Catch Bounds by Gear + Taxon
- [`get_price_bounds()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_price_bounds.md)
  : Get Price Bounds by Gear + Taxon
- [`get_total_catch_bounds()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_total_catch_bounds.md)
  : Get Total Catch Bounds by Landing Site and Gear
- [`get_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_validation_status.md)
  : Get Validation Status from KoboToolbox
- [`process_submissions_parallel()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/process_submissions_parallel.md)
  : Process Submissions in Parallel with Rate Limiting
- [`sync_validation_submissions()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/sync_validation_submissions.md)
  : Synchronize Validation Statuses with KoboToolbox
- [`update_validation_status()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/update_validation_status.md)
  : Update Validation Status in KoboToolbox
- [`validate_catch_taxa()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_catch_taxa.md)
  : Validate Catch at Taxon Level
- [`validate_price()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_price.md)
  : Validate Catch Price
- [`validate_surveys_adnap()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_surveys_adnap.md)
  : Validate ADNAP Survey Data
- [`validate_surveys_lurio()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_surveys_lurio.md)
  : Validate Lurio Survey Data
- [`validate_total_catch()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/validate_total_catch.md)
  : Validate Total Catch

## Export

Functions dedicated dissemination of processed and analysed fisheries
data

- [`create_metric_structure()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/create_metric_structure.md)
  : Create metric structure for MongoDB/ApexCharts
- [`export_api_raw()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_api_raw.md)
  : Export Raw API-Ready Trip Data
- [`export_api_validated()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_api_validated.md)
  : Export Validated API-Ready Trip Data
- [`export_landings()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_landings.md)
  : Export Processed Landings Data
- [`export_lurio_landings()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/export_lurio_landings.md)
  : Export Lurio landings data

## Helper functions

Functions dedicated to data processing.

- [`add_version()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/add_version.md)
  : Add timestamp and sha string to a file name
- [`create_metric_structure()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/create_metric_structure.md)
  : Create metric structure for MongoDB/ApexCharts
- [`fetch_asset()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/fetch_asset.md)
  : Fetch and Filter Asset Data from Airtable
- [`fetch_assets()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/fetch_assets.md)
  : Fetch Multiple Asset Tables from Airtable
- [`get_airtable_form_id()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_airtable_form_id.md)
  : Get Airtable Form ID from KoBoToolbox Asset ID
- [`load_dotenv()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/load_dotenv.md)
  : Load environment variables from .env file
- [`map_surveys()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/map_surveys.md)
  : Map Survey Labels to Standardized Taxa, Gear, and Vessel Names
- [`read_config()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/read_config.md)
  : Read configuration file
- [`standardize_enumerator_names()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/standardize_enumerator_names.md)
  : Standardize Enumerator Names

## Airtable Integration

Functions for interacting with Airtable API.

- [`airtable_to_df()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/airtable_to_df.md)
  : Get All Records from Airtable with Pagination
- [`get_writable_fields()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/get_writable_fields.md)
  : Get Writable Fields from Airtable Table
- [`update_airtable_record()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/update_airtable_record.md)
  : Update Single Airtable Record
- [`bulk_update_airtable()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/bulk_update_airtable.md)
  : Bulk Update Multiple Airtable Records
- [`df_to_airtable()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/df_to_airtable.md)
  : Create New Airtable Records
- [`device_sync()`](https://worldfishcenter.github.io/peskas.malawi.data.pipeline/reference/device_sync.md)
  : Sync Data with Airtable (Update + Create)
