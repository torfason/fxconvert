
# Here we go
library(here)
library(conflicted)
library(fxconvert)
library(dplyr)
library(zmisc)
library(rlang)
library(stringr)

# Preferences
conflicts_prefer(dplyr::filter)

source(here("build", "utils_build_fxdata.R"))

# This is already all in direct quotes and with na fills
# Update the version and trim partial 2026 data, other data is unchanged
d <- read_parquet_multi("../fxdata/v2/fed") |> as_tibble()
d$.version. <- 3L
d <- d |> filter(fxdate <= "2025-12-31")

# Write the data and metadata in v3
fxdata_write_lumpy_parquet_autocomp(d, "../fxdata", bank = "fed",  version = 3L)
fxdata_write_metadata_json(d, "../fxdata", bank = "fed",
                           quotation_method = "indirect", new_name_order = TRUE)


