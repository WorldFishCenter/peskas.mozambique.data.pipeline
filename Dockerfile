FROM rocker/r-ver

# Install remotes package
RUN install2.r --error --skipinstalled remotes

# Install other packages
RUN install2.r --error --skipinstalled \
    config \
    dplyr \
    httr2 \
    logger \
    lubridate \
    magrittr \
    mongolite \
    purrr \
    #rfishbase \
    rlang \
    stringr \
    tibble \
    tidyr \
    tidyselect

# Install suggests
RUN install2.r --error --skipinstalled \
    git2r \
    googlesheets4

RUN Rscript -e 'remotes::install_github("WorldFishCenter/peskas.coasts", ref = "dev")'
