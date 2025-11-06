# Load environment variables from .env file

Loads environment variables from .env file if it exists. This function
should be called before reading configuration to ensure dotenv variables
are available.

## Usage

``` r
load_dotenv(file = ".env")
```

## Arguments

- file:

  Path to .env file, defaults to ".env" in current working directory

## Value

NULL (called for side effects)
