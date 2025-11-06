# Upload Data to MongoDB and Overwrite Existing Content

This function connects to a MongoDB database, removes all existing
documents from a specified collection, and then inserts new data. It
also stores the original column order to maintain data structure
consistency.

## Usage

``` r
mdb_collection_push(
  data = NULL,
  connection_string = NULL,
  collection_name = NULL,
  db_name = NULL
)
```

## Arguments

- data:

  A data frame containing the data to be uploaded.

- connection_string:

  A character string specifying the MongoDB connection URL.

- collection_name:

  A character string specifying the name of the collection.

- db_name:

  A character string specifying the name of the database.

## Value

The number of data documents inserted into the collection (excluding the
order document).

## Examples

``` r
if (FALSE) { # \dontrun{
# Upload and overwrite data in a MongoDB collection
result <- mdb_collection_push(
  data = processed_legacy_landings,
  connection_string = "mongodb://localhost:27017",
  collection_name = "my_collection",
  db_name = "my_database"
)
} # }
```
