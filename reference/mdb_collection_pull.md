# Retrieve Data from MongoDB

This function connects to a MongoDB database and retrieves all documents
from a specified collection, maintaining the original column order if
available.

## Usage

``` r
mdb_collection_pull(
  connection_string = NULL,
  collection_name = NULL,
  db_name = NULL
)
```

## Arguments

- connection_string:

  A character string specifying the MongoDB connection URL. Default is
  NULL.

- collection_name:

  A character string specifying the name of the collection to query.
  Default is NULL.

- db_name:

  A character string specifying the name of the database. Default is
  NULL.

## Value

A data frame containing all documents from the specified collection,
with columns ordered as they were when the data was originally pushed to
MongoDB.

## Examples

``` r
if (FALSE) { # \dontrun{
# Retrieve data from a MongoDB collection
result <- mdb_collection_pull(
  connection_string = "mongodb://localhost:27017",
  collection_name = "my_collection",
  db_name = "my_database"
)
} # }
```
