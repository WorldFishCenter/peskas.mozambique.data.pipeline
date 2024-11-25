FROM rocker/r-ver

# Install imports
RUN install2.r --error --skipinstalled \
    config \
    dplyr \
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
    tidyselect \
    remotes

# Install suggests
RUN install2.r --error --skipinstalled \
    git2r \
    googlesheets4
