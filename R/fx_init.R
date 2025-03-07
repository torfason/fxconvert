
#' Initialize Foreign Exchange (FX) Data
#'
#' This function initializes and refreshes the local foreign exchange (FX) data
#' directory by downloading the necessary data files from the specified remote
#' server. The function allows different approaches for data management,
#' including incremental updates, full reinitialization, and local refreshes.
#'
#' @param ... Additional arguments (not currently used).
#' @param fxsource Character string specifying the source of FX data. Currently,
#'   only `"ecb"` is supported.
#' @param verbose Logical indicating whether to print progress messages.
#'   Defaults to `TRUE`.
#' @param approach Character string specifying how the function should handle
#'   existing data. Must be one of `"incremental"` (default), `"fresh"`,
#'   `"local_refresh"`, `"remove"`. See details.
#'
#' @details
#' This function ensures that the local FX data store is up to date by:
#' - Checking available data ranges on the remote server.
#' - Identifying missing data files for download.
#' - Removing outdated local files.
#' - Loading data into a DuckDB database for efficient querying.
#'
#' Currently, the function only supports the European Central Bank (`"ecb"`) as
#' the FX data source.
#'
#' The approach to updating data is determined by the `approach` parameter. This
#' can be set to four different options:
#' - `"incremental"`: Downloads missing data while keeping existing files.
#' - `"fresh"`: Deletes all local data and downloads everything anew.
#' - `"local_refresh"`: Rebuilds the local database without downloading new data.
#' - `"remove"`: Deletes all local data and exits without downloading new data.
#'
#' @export
fx_init <- function(..., fxsource = "ecb", verbose = TRUE,
                    approach = c("incremental", "fresh", "local_refresh", "remove")) {

  # Small helper function to only cat if verbose is true
  # (temporary solution until options and logging are implemented)
  vcat <- function(...) {
    if (verbose) cat(...)
  }

  # Parameters and key variables
  fxsource <- arg_match(fxsource)
  approach <- arg_match(approach)
  fxdata_dir <- fx_get_fxdata_dir()

  if (approach == "fresh") {
    # Force a full initialization
    vcat("Removing fxdir to trigger full and fresh reinitialization ...\n")
    unlink(fxdata_dir, recursive = TRUE)
  } else if (approach == "remove") {
    # Just remove the whole fxdata dir and return
    vcat("Removing fxdir completely and returning. \n",
        "Package will not work without reinitializing it.\n")
    unlink(fxdata_dir, recursive = TRUE)
    return(invisible(NULL))
  }

  # Download all parquet files from the server.
  # The url should point to the fxdata directory on the server
  # (temporary approach until options are implemented)
  fxdata_server_url <- "https://github.com/torfason/fxdata/raw/refs/heads/main/"

  # Prepare the fx_dir for usage as local data store,
  # as well as the subdirectory for the specific source being refreshed
  if (!dir.exists(fxdata_dir)) {
    vcat("Creating fxdir ...\n")
    dir.create(fxdata_dir, recursive = TRUE)
  }
  if (!dir.exists(fs::path(fxdata_dir, fxsource))) {
    dir.create(fs::path(fxdata_dir, fxsource), recursive = TRUE)
  }

  # Get available dates (first and last) from the remote server
  l.dates_available <- httr2::request(fxdata_server_url) |>
    httr2::req_url_path_append("ecb_meta.json") |>
    httr2::req_perform() |>
    httr2::resp_body_string() |>
    jsonlite::fromJSON()
  l.dates_available

  # Calculate the key date ranges to act on, adding ".parquet" to the end
  v.date_range_server <- fx_date_seq_lumpy(
      l.dates_available$first_date_available,
      l.dates_available$last_date_available)
  v.range_server <- paste0(fxsource, "_", v.date_range_server, ".parquet")
  v.range_local  <- list.files(fs::path(fxdata_dir, fxsource))
  v.range_for_download <- setdiff(v.range_server, v.range_local)
  v.range_for_deletion <- setdiff(v.range_local, v.range_server)

  # Download any missing fx data files
  if (length(v.range_for_download) > 0) {
    vcat("Preparing to download the following files from the server:\n")
    vcat(fs::path(fxdata_server_url, fxsource, v.range_for_download), sep = "\n")
    d.dlresult <- curl::multi_download(
      fs::path(fxdata_server_url, fxsource, v.range_for_download),
      fs::path(fxdata_dir, fxsource, v.range_for_download),
      progress = TRUE # For now, we always want download status on download
    )

    # Check success, so the code below that will only run if all downloads were successful
    if (!all(d.dlresult$success)) {
      stop("Some fx data downloads failed, aborting refresh")
    }

    # Verify that all downloaded files are uncorrupted parquet files
    tryCatch({
      results <- sapply(fs::path(fxdata_dir, fxsource, v.range_for_download), nanoparquet::read_parquet)
      vcat("All fx data downloads are valid parquet files\n")
    }, error = function(e) {
      stop("Some fx data downloads are not valid parquet files")
    })

  }

  # Construct a fresh duckdb. We do this either if there were any new parquet
  # files downloaded or if local_refresh was specified for approach
  if (length(v.range_for_download) > 0 || approach == "local_refresh") {
    vcat("Loading parquet files into duckdb database ...\n")
    duckdb_file <- fs::path(fxdata_dir, paste0(fxsource, ".duckdb"))
    if (file.exists(duckdb_file)) {
      file.remove(duckdb_file)
    }
    con <- duckdb::dbConnect(duckdb::duckdb(duckdb_file))
    on.exit(duckdb::dbDisconnect(con, shutdown = TRUE), add = TRUE)

    parquet_files_wildcard <- fs::path(fxdata_dir, fxsource, "*.parquet")
    res <- duckdb::dbSendQuery(con, paste0("CREATE TABLE fxtable AS SELECT * FROM '",
                               parquet_files_wildcard,
                               "' ORDER BY fxdate;"))
    duckdb::dbClearResult(res)

    # Write a filled version.
    if (exists("fxdate")) stop("Workaround for check error: fxdate already exists")
    fxdate <- "Workaround for check error"
    fxtable_filled <- dplyr::tbl(con, "fxtable") |>
      dplyr::arrange(fxdate) |>
      dplyr::collect() |>
      dplyr::mutate(dplyr::across(-fxdate, ~ fx_vec_fill_gaps(.x))) |>
      identity()
    duckdb::dbWriteTable(con, "fxtable_filled", fxtable_filled)

  } # End tasks conditional on downloads needed


  # We now know that downloads were successful, so we can unlink any parts of the
  # range that are no longer relevant (i.e. individual days after whole month is available
  # or individual months after the whole year is available).
  if (length(v.range_for_deletion > 0)) {
    vcat("Preparing to delete the following outdated local files:\n")
    print(fs::path(fxdata_dir, fxsource, v.range_for_deletion))
    unlink(
      fs::path(fxdata_dir, fxsource, v.range_for_deletion)
    )
  }

  # Print the sitrep
  if (verbose) {
    fx_sitrep()
  }

  # Return
  invisible(NULL)
}
