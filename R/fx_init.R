
#' Initialize Foreign Exchange (FX) Data
#'
#' This function initializes and refreshes the local foreign exchange (FX) data
#' directory by downloading the necessary data files from the specified remote
#' server. The function allows different approaches for data management,
#' including incremental updates, full reinitialization, and local refreshes.
#'
#' @param ... Reserved. All arguments must be named.
#' @param bank Character string specifying the source of FX data. Currently,
#'   `"ecb"` and `"cbi"` are supported.
#' @param verbose Logical indicating whether to print progress messages.
#'   Defaults to `TRUE`.
#' @param once Logical indicating whether to only perform actual initialization
#'   once per R session. Defaults to `FALSE`.
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
#' The approach to updating data is determined by the `approach` parameter. This
#' can be currently set to four different options:
#' - `"incremental"`: Downloads missing data while keeping existing files.
#' - `"fresh"`: Deletes all local data and downloads everything anew.
#' - `"local_refresh"`: Rebuilds the local database without downloading new data.
#' - `"remove"`: Deletes all local data and exits without downloading new data.
#'
#' The different approaches, or actions, are subject to change. In particular
#' they should support offline use with appropriate warnings. Potential actions:
#' - `auto`    (equivalent to `once`=TRUE, only updates if it has not been updated in this session (but must ensure sitrep is OK))
#' - `update`  (equivalent to older `incremental`, warns on offline, error if no data)
#' - `offline` (avoid internet access, no warn on offline unless data is missing)
#' - `remove`  (deletes all local data and exits without downloading new data.)
#'
#' TODO: This should take an options object and also
#'
#' @export
fx_init <- function(..., bank = c("ecb", "cbi", "fed", "xfed"),
                    verbose = TRUE,
                    once = FALSE,
                    approach = c("incremental", "fresh", "local_refresh", "remove")) {

  # Assert parameters
  assert_dots_empty()
  assert_flag(verbose)
  assert_flag(once)
  bank <- arg_match(bank)
  approach <- arg_match(approach)

  # Abort if once is TRUE and we have already initialized in this session
  if (once && !is.null(.globals[[bank]]))
    return(invisible(NULL))

  # We also abort if sitrep() looks good and we have no internet
  if (!curl::has_internet()) {
    if (fx_sitrep(bank = bank, verbose = verbose)) {
      # By now, we are assured that sitrep(bank) is true. Warn and exit
      .globals[[bank]] <- TRUE
      cli::cli_warn("No internet, not updating FX data")
      return(invisible(NULL))
    } else {
      cli::cli_abort("No internet and no FX data present. Aborting ...")
    }
  }


  # Small helper function to only cat if verbose is true
  # (temporary solution until options and logging are implemented)
  vcat <- function(...) {
    if (verbose) cat(...)
  }

  # Key variables (and options?)
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
  if (!dir.exists(fs::path(fxdata_dir, bank))) {
    dir.create(fs::path(fxdata_dir, bank), recursive = TRUE)
  }

  # Get available dates (first and last) from the remote server
  l.dates_available <- httr2::request(fxdata_server_url) |>
    httr2::req_url_path_append(glue::glue("{bank}_meta.json")) |>
    httr2::req_perform() |>
    httr2::resp_body_string() |>
    jsonlite::fromJSON()
  l.dates_available

  # Calculate the key date ranges to act on, adding ".parquet" to the end
  v.date_range_server <- fx_date_seq_lumpy(
      l.dates_available$first_date_available,
      l.dates_available$last_date_available)
  v.range_server <- paste0(bank, "_", v.date_range_server, ".parquet")
  v.range_local  <- list.files(fs::path(fxdata_dir, bank))
  v.range_for_download <- setdiff(v.range_server, v.range_local)
  v.range_for_deletion <- setdiff(v.range_local, v.range_server)

  # Download any missing fx data files
  if (length(v.range_for_download) > 0) {
    vcat("Preparing to download the following files from the server:\n")
    vcat(fs::path(fxdata_server_url, bank, v.range_for_download), sep = "\n")

    d.dlresult <- curl::multi_download(
      fs::path(fxdata_server_url, bank, v.range_for_download),
      fs::path(fxdata_dir, bank, v.range_for_download),
      progress = TRUE # For now, we always want download status on download
    )

    # Check success, so the code below that will only run if all downloads were successful
    if (!all(d.dlresult$success)) {
      stop("Some fx data downloads failed, aborting refresh")
    }

    # Verify that all downloaded files are uncorrupted parquet files
    tryCatch({
      results <- sapply(fs::path(fxdata_dir, bank, v.range_for_download), nanoparquet::read_parquet)
      vcat("All fx data downloads are valid parquet files\n")
    }, error = function(e) {
      stop("Some fx data downloads are not valid parquet files")
    })

  }

  # Construct a fresh duckdb. We do this either if there were any new parquet
  # files downloaded or if local_refresh was specified for approach
  if (length(v.range_for_download) > 0 || approach == "local_refresh") {
    vcat("Loading parquet files into duckdb database ...\n")
    duckdb_file <- fs::path(fxdata_dir, paste0(bank, ".duckdb"))
    if (file.exists(duckdb_file)) {
      file.remove(duckdb_file)
    }
    con <- duckdb::dbConnect(duckdb::duckdb(duckdb_file))
    on.exit(duckdb::dbDisconnect(con, shutdown = TRUE), add = TRUE)

    parquet_files_wildcard <- fs::path(fxdata_dir, bank, "*.parquet")
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
    print(fs::path(fxdata_dir, bank, v.range_for_deletion))
    unlink(
      fs::path(fxdata_dir, bank, v.range_for_deletion)
    )
  }

  # Test that state is OK
  if (fx_sitrep(bank = bank, verbose = verbose)) {
    # By now, we are assured that sitrep(bank) is true.
    .globals[[bank]] <- TRUE
    return(invisible(NULL))
  } else {
    cli::cli_abort("fx_sitrep() still FALSE at end of fx_init(). Aborting ...")
  }
}
