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
    purrr \
    #rfishbase \
    rlang \
    stringr \
    tibble \
    tidyr \
    tidyselect

# Install suggests
RUN install2.r --error --skipinstalled \
    git2r

# Install GitHub packages
ARG COASTS_REF
RUN test -n "$COASTS_REF" && \
    Rscript -e "remotes::install_github('WorldFishCenter/peskas.coasts', ref = '${COASTS_REF}')"

# Pinned because rfishbase 5.0.3+ reads FishBase 26.06, where Caesionidae and
# Scaridae are empty. Must stay last: install_local(dependencies = TRUE) undoes
# any earlier pin. The release itself is pinned in inst/config.yml.
RUN Rscript -e "remotes::install_version('rfishbase', version = '5.0.1', repos = 'https://cloud.r-project.org', upgrade = 'never')"

# Fail the build, not the pipeline, if the pin was undone.
RUN Rscript -e "v <- as.character(packageVersion('rfishbase')); if (v != '5.0.1') stop('rfishbase pin lost: got ', v)"
