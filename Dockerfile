FROM rocker/r-ver

# Install remotes package
RUN install2.r --error --skipinstalled remotes

# Install other packages except duckdb
RUN install2.r --error --skipinstalled \
    config \
    dplyr \
    duckdb \
    duckplyr \
    git2r \
    googlesheets4 \
    httr2 \
    logger \
    lubridate \
    magrittr \
    mongolite \
    purrr \
    rfishbase \
    rlang \
    stringr \
    tibble \
    tidyr \
    tidyselect

# Install suggests
RUN install2.r --error --skipinstalled \
    git2r \
    googlesheets4
