
#' Initialize Foreign Exchange (FX) Data
#'
#' This function initializes and refreshes the local foreign exchange (FX) data
#' directory by downloading the necessary data files from the specified remote
#' server. The function allows different approaches for data management,
#' including incremental updates, full re-initialization, and local refreshes.
#'
#' @param ... Reserved. All arguments must be named.
#' @param banks Character string specifying the source of FX data. Currently,
#'   `"ecb"`, `"cbi"`, and `"fed"` are supported.
#' @param verbose Logical indicating whether to print progress messages.
#'   Defaults to `TRUE`.
#' @param once Logical indicating whether to only perform actual initialization
#'   once per R session. Defaults to `FALSE`.
#' @param action String specifying which initialization action to take. One of
#'   `auto`, `update`, `offline`, `full`, `remove`. See details. Replaces
#'   `approach`.
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
#' - `full`    (remove and update rolled into one)
#' - `remove`  (deletes all local data and exits without downloading new data.)
#'
#' TODO: This should take an options object and also
#'
#' @export
fx_init <- function(..., banks = c("ecb", "cbi", "fed", "xfed"),
                   action = c("auto", "update", "offline", "full", "remove"),
                   verbose = TRUE,
                   once = FALSE) {

  # Assert parameters
  assert_dots_empty()
  assert_flag(verbose)
  assert_flag(once)
  zmisc::assert_character(banks)
  action <- arg_match(action)

  for (bank in banks) {
    fx_init_single(bank = bank, action = action, verbose = verbose, once = once)
  }
}

#' Initialize fxdata only for a single bank
#'
#' @param approach String specifying how the function should handle
#'   existing data. One of `"incremental"` (default), `"fresh"`,
#'   `"local_refresh"`, `"remove"`. See details. Obsolete, replaced by `action`
#'
#' @noRd
fx_init_single <- function(..., bank = c("ecb", "cbi", "fed", "xfed"),
                    action = c("auto", "update", "offline", "full", "remove"),
                    verbose = TRUE,
                    once = FALSE,

                    approach = c("incremental", "fresh", "local_refresh", "remove")) {

  # Assert parameters
  assert_dots_empty()
  assert_flag(verbose)
  assert_flag(once)
  bank <- arg_match(bank)
  action <- arg_match(action)
  approach <- arg_match(approach)

  # Abort if action is "auto" and we have already initialized in this session
  if (action == "auto" && !is.null(.globals[[bank]]))
    return(invisible(NULL))

  # We also abort if sitrep() looks good and we have no internet
  if (!curl::has_internet()) {
    if (fx_sitrep(bank = bank, verbose = verbose)) {
      # By now, we are assured that fx_sitrep(bank) is true. Warn and exit
      .globals[[bank]] <- TRUE
      if (action != "offline") {
        # Unless offline was explicitly called, we warn that no updates are possible
        cli::cli_warn("No internet, not updating FX data (but you can use previously downloaded rates)")
      }
      return(invisible(NULL))
    } else {
      cli::cli_abort("No internet and FX data not valid. Aborting ...")
    }
  }

  # Roll-your-own log levels for now
  if (verbose) {
    xcat <- base::cat
    xprint <- base::print
  } else {
    xcat <- function(x, ...) {invisible(x)}
    xprint <- function(x, ...) {invisible(x)}
  }

  # Key variables (and options?)
  fxdata_dir <- fx_get_fxdata_dir()

  if (action == "full") {
    # Force a full initialization
    xcat("Removing fxdir to trigger full and fresh reinitialization ...\n")
    unlink(fxdata_dir, recursive = TRUE)
  } else if (action == "remove") {
    # Just remove the whole fxdata dir and return
    xcat("Removing fxdir completely and returning. \n",
        "Package will not work without reinitializing it.\n")
    .globals[[bank]] <- NULL
    unlink(fxdata_dir, recursive = TRUE)
    return(invisible(NULL))
  }

  # Download all parquet files from the server. The url should point to the fxdata
  # directory on the server (temporary approach until options are implemented)
  # (Can be swapped to another version when developing new data versions)
  #fxdata_server_url <- "https://dev.vestur.net/fxdata/"
  fxdata_server_url <- "https://github.com/torfason/fxdata/raw/refs/heads/main/"

  # Prepare the fx_dir for usage as local data store,
  # as well as the subdirectory for the specific source being refreshed
  if (!dir.exists(fxdata_dir)) {
    xcat("Creating fxdir ...\n")
    dir.create(fxdata_dir, recursive = TRUE)
  }
  if (!dir.exists(fs::path(fxdata_dir, bank))) {
    dir.create(fs::path(fxdata_dir, bank), recursive = TRUE)
  }

  # # Get available dates (first and last) from the remote server
  # l.dates_available <- httr2::request(fxdata_server_url) |>
  #   httr2::req_url_path_append(glue::glue("{bank}_meta.json")) |>
  #   httr2::req_perform() |>
  #   httr2::resp_body_string() |>
  #   jsonlite::fromJSON()
  # l.dates_available

  # We would like to download the json file to disk
  l.dates_available <- curl::curl_download(
        url      = fs::path(fxdata_server_url, glue("meta_{bank}.json")),
        destfile = fs::path(fxdata_dir, glue("meta_{bank}.json"))) |>
    jsonlite::fromJSON()


  # Calculate the key date ranges to act on, adding ".parquet" to the end
  v.lumpy_date_range_server <- fx_lump_dates(
    fx_date_seq(l.dates_available$first_date_available,
                l.dates_available$last_date_available)) |> unique()
  # v.date_range_server <- fx_date_seq_lumpy(
  #     l.dates_available$first_date_available,
  #     l.dates_available$last_date_available)
  v.range_server <- glue("{bank}_{v.lumpy_date_range_server}.parquet")
  v.range_local  <- fs::path(fxdata_dir, bank) |> fs::dir_ls() |> fs::path_file()
  v.range_for_download <- setdiff(v.range_server, v.range_local)
  v.range_for_deletion <- setdiff(v.range_local, v.range_server)

  # Download any missing fx data files
  if (length(v.range_for_download) > 0) {
    xcat(glue("Preparing to download {length(v.range_for_download)} files from the server:\n\n"))
    xcat(fs::path(fxdata_server_url, bank, v.range_for_download), sep = "\n")

    d.dlresult <- curl::multi_download(
      fs::path(fxdata_server_url, bank, v.range_for_download),
      fs::path(fxdata_dir, bank, v.range_for_download),
      progress = verbose # show download status if verbose is set
    )

    # Check success, so the code below that will only run if all downloads were successful
    if (!all(d.dlresult$success)) {
      stop("Some fx data downloads failed, aborting refresh")
    }

    # Verify that all downloaded files are uncorrupted parquet files
    tryCatch({
      results <- sapply(fs::path(fxdata_dir, bank, v.range_for_download), nanoparquet::read_parquet)
      xcat("All fx data downloads are valid parquet files\n")
    }, error = function(e) {
      stop("Some fx data downloads are not valid parquet files")
    })

  }

  # Delete any obsolete files before reading into duckdb.
  # We now know that downloads were successful, so we can unlink any parts of
  # the range that are no longer relevant (i.e. individual days after whole
  # month is available or individual months after the whole year is available).
  if (length(v.range_for_deletion) > 0) {
    xcat(glue("Preparing to delete {length(v.range_for_deletion)} outdated local files:\n"))
    xcat(fs::path(fxdata_dir, bank, v.range_for_deletion), sep = "\n")
    unlink(
      fs::path(fxdata_dir, bank, v.range_for_deletion)
    )
  }

  # Construct a fresh duckdb. We do this if there were any new parquet files downloaded.
  if (length(v.range_for_download) > 0) {
    xcat("Loading parquet files into duckdb database ...\n")
    duckdb_file <- fs::path(fxdata_dir, paste0(bank, ".duckdb"))
    if (file.exists(duckdb_file)) {
      file.remove(duckdb_file)
    }
    # conn <- duckdb::dbConnect(duckdb::duckdb(duckdb_file))
    # withr::defer(duckdb::dbDisconnect(conn, shutdown = TRUE))
    conn <- fx_duck_local(bank, read_only = FALSE)


    # Construct three versions of data (for now)
    d.fxdata.org <- read_parquet_multi(fs::path(fxdata_dir, bank))
    if (length(unique(d.fxdata.org$.version.)) != 1 ) {
      cli::cli_warn("More than one version in data. This will be an error soon.")
    } else {
      xcat(glue("fxdata version: {unique(d.fxdata.org$.version.)}\n\n"))
    }
    d.fxdata.wide <- d.fxdata.org |>
      dplyr::select(-".version.") |>
      dplyr::arrange(.data$fxdate)
    d.fxdata.long <- d.fxdata.wide |>
      tidyr::pivot_longer(-"fxdate", names_to = "currency", values_to = "rate") |>
      dplyr::filter(!is.na(.data$rate)) |>
      dplyr::arrange(.data$fxdate, .data$currency)
    d.fxdata.filled <- d.fxdata.wide |>
      dplyr::arrange(.data$fxdate) |>
      dplyr::mutate(dplyr::across(-"fxdate", ~ fx_vec_fill_gaps(.x)))

    # Write all three versions to the database
    duckdb::dbWriteTable(conn, "fxtable", d.fxdata.wide)
    duckdb::dbWriteTable(conn, "fxtable_filled", d.fxdata.filled)
    duckdb::dbWriteTable(conn, "fxtable_long", d.fxdata.long)

  } # End tasks conditional on downloads needed



  # Test that state is OK
  if (fx_sitrep(bank = bank, verbose = verbose)) {
    # By now, we are assured that sitrep(bank) is true.
    .globals[[bank]] <- TRUE
    return(invisible(NULL))
  } else {
    cli::cli_abort("fx_sitrep() still FALSE at end of fx_init(). Aborting ...")
  }
}
