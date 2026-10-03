

# Download all parquet files from the repo for listed banks.
#
# This fetches parquet files for multiple banks in fewer roundtrips
# than prior approaches.
#
# repo should point to the fxdata directory on the server.
# parquet_dir indicates where to store the fetched files
# parquet_dir is either a fresh dir, or it is assumed to contain:
#   ./meta_{bank}.json
#   ./{bank}/{lumpydate}.parquet
# After running, it shall contain for each of {banks}
#   ./meta_{bank}.json
#   ./{bank}/{lumpydate}.parquet
#
# @param repo URL of remote server with available fxdata
# @param parquet_dir Directory where downloaded json and parquet files
#   should be placed (according to the fxdata format)
# @param banks A list of banks to process
# @param verbose Should verbose logging be shown as data is downloaded
# @return bool Was the data in parquet_dir modified (with server data) or not?
#
fx_init_fetch_parquet <- function(repo, parquet_dir,
                                  banks = c("ecb", "cbi", "fed", "xfed"),
                                  verbose = FALSE) {


  # Prepare known urls and directories, fs::dir_create() ensures all dirs exist
  bank_parquet_dirs <- fs::path(parquet_dir, banks) |> fs::dir_create()
  fxtmp_json_dir    <- fs::file_temp(pattern = "fxtmp_json_") |> fs::dir_create()
  fxtmp_json_files  <- fs::path(fxtmp_json_dir, glue("meta_{banks}.json"))
  fxold_json_files  <- fs::path(parquet_dir, glue("meta_{banks}.json"))
  json_urls         <- fs::path(repo, glue("meta_{banks}.json"))

  # Roll-your-own log levels for now
  if (verbose) {
    xcat <- base::cat
    xprint <- base::print
  } else {
    xcat <- function(x, ...) {invisible(x)}
    xprint <- function(x, ...) {invisible(x)}
  }

  # DOWNLOAD JSON FILES (FIRST ROUND TRIP)
  xcat(glue("Preparing to download {length(json_urls)} json metadata files from the server:\n\n"))
  d.dlresult_json   <- curl::multi_download(json_urls, fxtmp_json_files,
                progress = verbose) # show download status if verbose is set
  if (!all(d.dlresult_json$success) || !all(d.dlresult_json$status_code == 200)) {
    stop("Some fx json metadata downloads failed, aborting refresh")
  }

  # Read the fx metadata contained in the downloaded json files
  json_metas_tmp <- lapply(fxtmp_json_files, jsonlite::fromJSON)

  # Check if all old json files exist and are equal to the new tmp files
  # (if so, there have been no changes and we can return FALSE - no modification)
  if (all(fs::file_exists(fxold_json_files))) {
    json_metas_old <- lapply(unname(fxold_json_files), jsonlite::fromJSON)
    if (identical(json_metas_tmp, json_metas_old)) {
      return(FALSE)
    }
  }

  # If we are here, we expect there will be changes
  xcat("New fx data available ...")

  # Helper that calculates available parquet file range on server for single bank
  helper_get_range_server <- function(bank, meta) {
    v.lumpy_date_range_server <- fx_date_seq(
              meta$first_date_available, meta$last_date_available) |>
      fx_lump_dates() |>
      unique()
    fs::path(bank, glue("{bank}_{v.lumpy_date_range_server}.parquet"))
  }

  # Each v.range_<...> vector contains relevant file list for all banks
  # list is in all cases relative to the root (repo for server, parquet_dir for local)
  v.range_server <- purrr::pmap(list(bank = banks, meta = json_metas_tmp),
                                helper_get_range_server) |>
    unlist()
  v.range_local  <- withr::with_dir(parquet_dir, fs::dir_ls(banks))
  v.range_for_download <- setdiff(v.range_server, v.range_local)
  v.range_for_deletion <- setdiff(v.range_local, v.range_server)


  # Download any missing fx data files
  if (length(v.range_for_download) > 0) {
    xcat(glue("Preparing to download {length(v.range_for_download)} files from the server:\n\n"))
    xprint(v.range_for_download)

    # DOWNLOAD PARQUET FILES (SECOND ROUND TRIP)
    d.dlresult_parquet <- curl::multi_download(
      fs::path(repo, v.range_for_download),
      fs::path(parquet_dir, v.range_for_download),
      progress = verbose # show download status if verbose is set
    )

    # Check success, so the code below that will only run if all downloads were successful
    if (!all(d.dlresult_parquet$success) || !all(d.dlresult_parquet$status_code == 200)) {
      stop("Some fx data downloads failed, aborting refresh")
    }

    # Verify that all downloaded files are uncorrupted parquet files
    tryCatch({
      results <- sapply(fs::path(parquet_dir, v.range_for_download), nanoparquet::read_parquet)
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
    xcat(glue("Preparing to delete {length(v.range_for_deletion)} outdated local files:\n\n"))
    xprint(v.range_for_deletion)
    unlink(
      fs::path(parquet_dir, v.range_for_deletion)
    )
  }

  fs::file_move(fxtmp_json_files, parquet_dir)

  # If we got here, some things were modified
  TRUE
}



