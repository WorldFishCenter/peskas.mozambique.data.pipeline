
<!-- README.md is generated from README.Rmd. Please edit that file -->

# peskas.mozambique.data.pipeline

<!-- badges: start -->

[![R-CMD-check](https://github.com/WorldFishCenter/peskas.mozambique.data.pipeline/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/WorldFishCenter/peskas.mozambique.data.pipeline/actions/workflows/R-CMD-check.yaml)
[![Codecov test
coverage](https://codecov.io/gh/WorldFishCenter/peskas.mozambique.data.pipeline/graph/badge.svg)](https://app.codecov.io/gh/WorldFishCenter/peskas.mozambique.data.pipeline)
<!-- badges: end -->

The goal of peskas.mozambique.data.pipeline is to implement, deploy, and
execute the data and modelling pipelines that underpin Peskas in
Mozambique. \## The pipeline is an R package

peskas.mozambique.data.pipeline is structured as an R package because it
makes it easier to write production-grade software. Specifically,
structuring the code as an R package allows us to:

- better handle system and package dependencies,
- forces us to split the code into functions,
- makes it easier to document the code, and
- makes it easier to test the code

We make heavy use of [tidyverse style
conventions](https://engineering-shiny.org) and the
[usethis](https://usethis.r-lib.org) package to automate tasks during
project setup and deployment.

For more information about the rationale of structuring the pipeline as
a package check [Chapter
3](https://engineering-shiny.org/structuring-project.html#structuring-your-app_)
in [*Engineering Production-Grade Shiny
Apps*](https://engineering-shiny.org). The book is focused on Shiny
applications but the rationale also applies to data pipelines and
production-ready code in general.

## How the pipeline works

The pipeline is composed of different modules:

1.  Data Collection: On site fishing landing surveys and continuous,
    solar-powered GPS vessel trackers to collect and send data in near
    real-time, alongside fishery metadata for a thorough data-gathering
    process.

2.  Pre-processing: Data formatting, shaping, and standardisation to
    prepare the raw data for analysis.

3.  Validation: Outlier detection and error identification, and includes
    an alert system to maintain data quality.

4.  Analytics: Modelling fisheries indicators, nutritional
    characterization, and data mining to extract valuable insights.

5.  Data export: Automated dissemination of processed and analysed
    fisheries data to ensure accessibility and comprehension. This
    involves restructuring data for dashboard integration and open
    publication.

6.  isualisation: Tools for data reporting and sharing of insights
    through a comprehensive dedicated web app dashboard (not hosted in
    this repository).

See [Peskas: Automated analytics for small-scale, data-deficient
fisheries](https://www.researchsquare.com/article/rs-4386336/v1) for
further details.

## Getting Started

This package uses a configuration file [`config.yml`](inst/config.yml)
to manage environment-specific settings and connections. To get started,
familiarize yourself with the package structure, particularly the
[`R`](R) directory where the main functions are located.

Each function typically reads the configuration using `read_config()` to
access necessary parameters. To work on this package locally, you'll
need to copy `.env.example` to `.env` and fill in your authentication
credentials. The `read_config()` function automatically loads these
environment variables from the `.env` file.
Remember to run `devtools::load_all()` when testing changes locally. If
you’re new to R package development, consider reviewing the [*R
packages*](https://r-pkgs.org) book by Hadley Wickham and Jenny Brian.

## Quick Guide for Contributors

To keep our repository clean and efficient, please keep these guidelines
in mind:

- Always work on a new branch, not directly on main.
- Write clear, concise commit messages.
- Avoid storing intermediate and garbage files, especially in the root
  folder.
- Strive for soft-coded solutions.
- Maintain consistent code style throughout the project.
- Document your code well - future you (and others) will thank you.
- Test your changes thoroughly before submitting a pull request.
- Keep your fork synced with the main repository.

These practices help us maintain a clean, efficient codebase that’s
easier for everyone to work with. For more detailed guidelines, check
out our [CONTRIBUTING.md](.github/CONTRIBUTING.md) file.
