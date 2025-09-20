
# This script:
#   - Loops through each of the banks
#   - Uses fxdata_list_obsolete_files() to determine old files for specified folders
#   - Deletes each of the obsolete paths
#   - Logs its progress

# Here we go!
library(here)
library(conflicted)
conflicts_prefer(dplyr::filter)
library(dplyr)
library(glue)
library(cli)
library(gert)
library(stringr)

# Other packages
library(jsonlite)

# Own packages
library(zmisc)
library(fxconvert)

# Non-packages utility files
import::from(utils_build_fxdata.R,
             glue_vector, fxdata_list_obsolete_files, mac_alert,
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


# = = = = = = = = = = = = = =
# Handle git commit of any changed files
# = = = = = = = = = = = = = =


message("Committing and pushing any updates to repo")

# Set folder for the generated parquet files
fxdata_folder <- here("..", "fxdata")

# Check status of repo modifications
d.git <- git_status(repo = fxdata_folder)
if (nrow(d.git) > 0) {
  cat("Listing all changed items in git status:\n")
  print(d.git)
}

# Check status, filtering out any lines that should not be in the result
# Only files related to the listed banks are legitimate for autocommits,
# and only deletion operations may be performed.
v.banks <- c("ecb", "cbi", "fed", "xfed")
re.banks <- paste0("(", paste(v.banks, collapse = "|"), ")")
re.dates <- "[0-9X]{4}(-[0-9]{2})?(-[0-9X]{2})?"
d.git_status_errors <- d.git |>
  dplyr::filter(!( str_detect(file, glue("^{re.banks}/{re.banks}_{re.dates}\\.parquet$")) & status == "deleted" ))

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
  del_count <- nrow(filter(d.git, status == "deleted"))
  git_add(d.git$file, repo = fxdata_folder)
  git_commit(glue("Remove obsolete parquet files on {Sys.Date()} (autocommit)"), repo = fxdata_folder) |>
    glue_vector("Committed with ID: {.}") |>
    print()
  glue("Completed clean_fxdata_obsolete.R: {new_count} new files, {mod_count} changed, {del_count} deleted") |>
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
  glue("Completed clean_fxdata_obsolete.R: No changes to repo\n") |> print()

}

# A newline at the end prevents a gap in the zay() frame
cat("\n")
