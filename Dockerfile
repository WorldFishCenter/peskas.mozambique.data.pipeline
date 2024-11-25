FROM rocker/r-ver

# Install remotes package
RUN install2.r --error --skipinstalled remotes

# Install other packages except duckdb
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
    tidyselect

# Install duckdb version 1.1.2
RUN Rscript -e "remotes::install_version('duckdb', version = '1.1.2', repos = 'https://cran.r-project.org')"

# Install suggests
RUN install2.r --error --skipinstalled \
    git2r \
    googlesheets4
