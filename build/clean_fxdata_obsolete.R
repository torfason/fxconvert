
# This script:
#   - Loops through each of the banks
#   - Uses fxdata_list_obsolete_files() to determine old files for specified folders
#   - Deletes each of the obsolete paths
#   - Logs its progress

# Here we go!
library(here)
library(conflicted)
conflicts_prefer(dplyr::filter)
library(glue)
library(cli)

# Other packages
library(jsonlite)

# Own packages
library(zmisc)
library(fxconvert)

# Non-packages utility files
import::from(utils_build_fxdata.R,
             glue_vector, fxdata_list_obsolete_files,
             .directory = here("build"))

# Set fxdata folders and banks to use
fxdata_folders <- c(here("..", "fxdata"), here("..", "fxdata_dev"))
banks <- c("ecb", "cbi", "fed", "xfed")

# Double loop, going through each fxdata folder and each bank to clean them up
for (fxdata_folder in fxdata_folders) {

  cat("\n")
  cli_inform("Working on {fxdata_folder}")

  for (bank in banks) {
    files_for_removal <- fxdata_list_obsolete_files(fxdata_folder, bank)
    paths_for_removal <- fs::path(here("..", "fxdata", bank), files_for_removal)

    cat("\n")
    if (length(files_for_removal) > 0) {
      cli_inform("Preparing to remove the following files for {bank}:")
      cli_inform("  {paths_for_removal}")
    }
    if (!all(fs::file_exists(paths_for_removal))) {
      cli_abort("Some files listed for removal do not exist!")
    }
    result <- fs::file_delete(paths_for_removal)
    if (any(fs::file_exists(paths_for_removal))) {
      cli_abort("Some files listed for removal still exist after delete operation!")
    }
    if (length(result) > 0) {
      cli_inform(c(v = "Successfully cleaned out {length(result)} obsolete files for {bank}."))
    } else {
      cli_inform(c(i = "No obsolete files found for {bank}."))
    }
  }
}

# A newline at the end prevents a gap in the zay() frame
cat("\n")
