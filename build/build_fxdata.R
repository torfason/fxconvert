
# Here we go!
library(here)

# Use conflicted for conflict resolution
library(conflicted)
conflicts_prefer(dplyr::filter)
conflicts_prefer(zmisc::qassert)
conflict_prefer_all("zmisc", "checkmate", quiet = TRUE)

# Some of the following are a bit slow, so we show a basic progress
cat("Loading packages: .")

# Load tidyverse and r-lib packages
library(lubridate)
library(stringr)
library(readr)
library(purrr)
library(dplyr)
library(waldo)
library(rlang)
library(glue)
cat(".")

# Other packages
library(checkmate)
rlang::warn("We should not need to load checkmate, zmisc should suffice")
library(snakecase)
library(rappdirs)
library(readxl)
library(zmisc)
library(cli)
cat(".")

# We use curl for web retrieval, but suppress start-up message
library(curl) |>
  pkgcond::suppress_messages("^Using libcurl .*")
cat(".")

# Suppress specific start-up messages because gert is a bit chatty
library(gert) |>
  pkgcond::suppress_messages("^Linking to libgit2 .*, ssh support: YES") |>
  pkgcond::suppress_messages("^Global config: .*$") |>
  pkgcond::suppress_messages("^Default user: .*$")
cat(".")

# Use nanoparquet to read and write parquet files
library(nanoparquet)
cat(".")

# Non-CRAN, including gitpins and fxconvert itself
library(gitpins)   # pak::pak("torfason/gitpins")
library(fxconvert) # pak::pak("torfason/fxconvert")
cat(".")

# Local utility functions
import::from(utils_build_fxdata.R,
             lapply_deep, unbox_non_na, unbox_deep, double_date, mac_alert, glue_vector,
             fxdata_fill, fxdata_write_metadata_json,
             fxdata_write_lumpy_parquet, fxdata_write_lumpy_parquet_new,
             .directory = here("build"))
cat(".")
cat("\n")


# Define error handler for sourcing from banks
err_to_warning <- function(e) {
  warn("Error retrieving data from one bank, continuing with next bank")
}

# = = = = = = = = = = = = = =
# Retrieve and write ECB data:
# = = = = = = = = = = = = = =
cat("\n")
source(here("build", "build_fxdata_ecb.R"), local = TRUE) |>
  system.time() |> try_fetch(error = err_to_warning)

# = = = = = = = = = = = = = =
# Retrieve and write CBI data:
# = = = = = = = = = = = = = =
cat("\n")
source(here("build", "build_fxdata_cbi.R"), local = TRUE) |>
  system.time() |> try_fetch(error = err_to_warning)

# = = = = = = = = = = = = = =
# Retrieve and write FED data:
# = = = = = = = = = = = = = =
cat("\n")
source(here("build", "build_fxdata_fed.R"), local = TRUE) |>
  system.time() |> try_fetch(error = err_to_warning)

cat("\n")


# = = = = = = = = = = = = = =
# Handle git commit of any changed files
# = = = = = = = = = = = = = =

message("Committing and pushing any updates to repo")

# Set folder for the generated parquet files
fxdata_folder <- here("..", "fxdata")

# Check status of repo modifications
d.git <- git_status(repo = fxdata_folder)
if (nrow(d.git) > 0) {
  cat("Listing all new/modified items in git status:\n")
  print(d.git)
}

# Check status, filtering out any lines that should not be in the result
# Only files related to the listed banks are legitimate for autocommits
v.banks <- c("ecb", "cbi", "fed", "xfed")
re.banks <- paste0("(", paste(v.banks, collapse = "|"), ")")
re.dates <- "[0-9X]{4}(-[0-9]{2})?(-[0-9X]{2})?"
d.git_status_errors <- d.git |>
  dplyr::filter(!( str_detect(file, glue("^{re.banks}_meta\\.json$") )                    & status == "modified" )) |>
  dplyr::filter(!( str_detect(file, glue("^meta_{re.banks}\\.json$") )                    & status == "modified" )) |>
  dplyr::filter(!( str_detect(file, glue("^{re.banks}/{re.banks}_{re.dates}\\.parquet$")) & status == "new" ))

# Check git status and panic/push/report based on status
if (nrow(d.git_status_errors) > 0) {

  # Panic if nrow(d.git_status_errors) > 0
  cat("Errors in git status (error tibble has ", nrow(d.git_status_errors), " rows):\n")
  print(d.git_status_errors)
  mac_alert("R-FX Error", paste("Error in git status,", nrow(d.git_status_errors), "files in error"))
  stop(paste("R-FX Error: Error in git status,", nrow(d.git_status_errors), "files in error"))

} else if (nrow(d.git) > 0) {

  # If we have rows in the status, but none of them are in error, we commit and push
  mod_count <- nrow(filter(d.git, status == "modified"))
  new_count <- nrow(filter(d.git, status == "new"))
  git_add(d.git$file, repo = fxdata_folder)
  git_commit(glue("New data snapshot {Sys.Date()} (autocommit)"), repo = fxdata_folder) |>
    glue_vector("Committed with ID: {.}") |>
    print()
  glue("Completed build_fxdata.R: {new_count} new files, {mod_count} changed") |>
    print()

  cat("Preparing to push ...\n")
  tryCatch({
    git_push(repo = fxdata_folder)
    cat("Push successful!\n")
  }, error = function(e) {
    cat("Push failed: {e$message}\n")
  })

} else {

  # No changes, report and we are done
  glue("Completed build_fxdata.R: No changes to repo\n") |> print()

}

