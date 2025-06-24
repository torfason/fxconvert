
# Global definitions required for this function
utils::globalVariables(
  c(".data", "full_month", "full_year", "ym_char", "ymd_char", "start_date", "last_date", "end_date", "first_date")
)

#' Get Local FX Data Directory
#'
#' Returns the path to the local directory for storing FX data.
#'
#' @param ... Reserved. Options parameter must be named
#' @param options A valid `fx_options` object.
#' @return A character string representing the path to the local FX data
#'   directory.
#'
#' @keywords internal
#' @export
fx_get_fxdata_dir <- function(..., options = fx_options()) {

  # Check input
  assert_dots_empty()
  assert_fxoptions(options)

  # Construct
  app_dir <- rappdirs::user_data_dir("net.zulutime.r.fxconvert")
  ws_dir <- fs::path(app_dir, options$workspace) |> fs::path_norm()

  # Prevent user from referring outside of app dir
  stopifnot(fs::path_has_parent(ws_dir, app_dir))

  # Return result
  ws_dir
}

# Return a connection that will get handled after it has been used
fx_duck_local <- function(bank, ...,
                          read_only = TRUE,
                          wipe_db = FALSE,
                          options = fx_options(),
                          envir = parent.frame()) {

  # Check input
  assert_choice(bank, c("ecb", "cbi", "fed", "xfed"))
  assert_dots_empty()
  assert_flag(read_only)
  assert_flag(wipe_db)
  assert_fxoptions(options)
  assert_environment(envir)

  # Ensure dbstate is initialized with defaults if running for first time
  .globals$dbstate                    <- .globals$dbstate %||% new_environment()
  .globals$dbstate[[bank]]            <- .globals$dbstate[[bank]] %||% new_environment()
  .globals$dbstate[[bank]]$conn_count <- .globals$dbstate[[bank]]$conn_count %||% 0

  dbdir  <- fx_get_fxdata_dir()
  dbfile <- fs::path(dbdir, glue("{bank}.duckdb"))
  if (wipe_db && !read_only) {

    # Check outstanding connections, error if there are any
    if (!isTRUE(.globals$dbstate[[bank]]$conn_count == 0)) {
      cli::cli_abort(c("Cannot prepare read/write db connection if there are outstanding connections",
                       i = "There currently exist {(.globals$dbstate[[bank]]$conn_count)}"))
    }

    # We want to wipe db and make file system ready for a new one
    # The following should work regardless of a db exists or if we are fully fresh
    fs::dir_create(dbdir, recurse = TRUE)
    if (fs::file_exists(dbfile))
      fs::file_delete(dbfile)
  } else if (wipe_db) {
    cli::cli_abort("Attempting to wipe db but read_only was specified")
  } else if (!read_only)
    cli::cli_abort("Read/write connections are only allowed if wiping database is specified")

  # Verification of state seems to be OK, we
  #   - Create a conn (we follow conventions in https://r.duckdb.org/ docs and use conn rather than con )
  #   - Update number of connections outstanding
  #   - Add a defer to reverse both actions
  conn <- duckdb::dbConnect(duckdb::duckdb(dbfile, read_only = read_only))
  .globals$dbstate[[bank]]$conn_count <- .globals$dbstate[[bank]]$conn_count + 1
  #cli::cli_inform("fx_duck_local(): {(.globals$dbstate[[bank]]$conn_count)} conns exist")

  withr::defer({
    duckdb::dbDisconnect(conn, shutdown = TRUE)
    .globals$dbstate[[bank]]$conn_count <- .globals$dbstate[[bank]]$conn_count - 1
    #cli::cli_inform("fx_duck_local(): {(.globals$dbstate[[bank]]$conn_count)} conns exist (deferred run)")
    }, envir = envir)
  conn
}

# Local implementation of DBI::dbGetQuery, because DBI is not directly imported
db_get_query <- function(conn, statement, ..., n = -1L) {

  # Query results and immediately defer a clearing operation
  rs <- duckdb::dbSendQuery(conn, statement, ...)
  withr::defer(duckdb::dbClearResult(rs))

  # Return all relevant results
  duckdb::dbFetch(rs, n = n, ...)
}

# Local implementation of DBI::dbExecute, because DBI is not directly imported
db_execute <- function(conn, statement, ...) {

  # Send statement for DB for execution, and immediately defer clearing operation
  rs <- duckdb::dbSendQuery(conn, statement, ...)
  withr::defer(duckdb::dbClearResult(rs))

  # Return the number of rows affected
  duckdb::dbGetRowsAffected(rs)
}

#' Generate a random date between two dates
#'
#' Returns a single random date between `start_date` and `end_date`. If `seed` is
#' provided, the result is reproducible.
#'
#' @param n Length of the result
#' @param start_date Date or string on `ymd()` form. The start of the date range.
#' @param end_date Date or string on `ymd()` form. The end of the date range.
#' @param seed Optional integer. If provided, sets the random seed locally for
#'   reproducibility.
#' @return A `Date` object of length `n`.
#'
#' @examples
#' random_date(3, "2000-01-01", "2020-12-31")
#' random_date(5, as.Date("2000-01-01"), as.Date("2020-12-31"), seed = 42)
#'
#' @keywords internal
#' @export
random_date <- function(n,
                        start_date = "1970-01-01",
                        end_date = Sys.Date(),
                        replace = FALSE,
                        seed = NULL) {

  # Prep and verify input
  assert_number(n)
  start_date <- lubridate::ymd(start_date)
  end_date   <- lubridate::ymd(end_date)
  assert_number(seed, null.ok = TRUE)

  # Inner function to generate
  generate <- function() {
    as.Date(sample(as.integer(start_date):as.integer(end_date),
                   n, replace = replace),
            origin = "1970-01-01")
  }

  if (!is.null(seed)) {
    withr::with_seed(seed, {generate()})
  } else {
    generate()
  }
}


#' Convert an Object to a Double-Preserved Date
#'
#' This function converts an object to a numeric double representation
#' while preserving the "Date" class. It first unclasses the input,
#' converts it to `double`, and then reassigns the `"Date"` class.
#'
#' Used to ensure that dates read using `nanoparquet` are identical to
#' dates read from the same file using `arrow`
#'
#' @param x An object that can be coerced into a date-like numeric format.
#' @return A `Date` object stored as a `double`.
#'
#' @examples
#'   d <- 10957L
#'   class(d) <- "Date" # d is now "2000-01-01"
#'   double_date(d)  # Returns the same date but stored as a double
#'
#' @keywords internal
#' @export
double_date <- function(x) {

  # Apply recursively to lists
  if (is.list(x)) {
    x[] <- lapply(x[], double_date)
    return(x)
  }

  if (lubridate::is.Date(x)) {
    x <- x |> unclass() |> as.double()
    class(x) <- "Date"
  }

  x
}


#' Writes and verifies parquet file with maximum compression
#'
#' @description
#' For ma**X**imum compression, the function writes `length(compression_types)`
#' versions of parquet file with maximum compression options for each
#' compression type to a temporary location, before moving the maximally
#' compressed file to the target file.
#'
#' For **V**erification, the function checks if the target file exists, if it
#' does it is read to verify that its contents equal `x` (a mismatch is an
#' error). If the target file does not exist, it is written (see above) and
#' `TRUE` returned , then re-read to ensure a match (again, a mismatch is an
#' error).
#'
#' The function returns `TRUE` if the file is written, and `FALSE` if a matching
#' file already exists.
#'
#' @param x `tibble` with data to write to file.
#' @param file `string` with path name to write.
#' @param compression_types `character` with list of compression methods to try.
#' @param verbose `flag` determining verbosity level
#' @return `TRUE` if data was written to file, `FALSE` if a file existed that
#'   contained the exact same data as `x` (if a file with different data was
#'   found, an error is thrown).
#'
#' @keywords internal
#' @export
write_parquet_vx <- function(x, file, ...,
                             compression_types = c("gzip", "snappy", "uncompressed"),
                             verbose = FALSE) {

  # Verify inputs
  assert_dots_empty()
  assert_data_frame(x)
  assert_string(file)
  assert_character(compression_types)
  assert_flag(verbose)

  # Create a temporary directory to work with
  temp_dir <- fs::file_temp()
  fs::dir_create(temp_dir)
  withr::defer(fs::dir_delete(temp_dir))

  file_existed <- fs::file_exists(file)
  if (!file_existed) {

    # Write out file for each compression type
    cr <- compression_types |> sapply(\(cmpr){
      cmpr_level <- zmisc::recode_tilde(cmpr, "gzip" ~ 9, "zstd" ~ 22, .default = NA)
      filename <- fs::path(temp_dir, glue("{cmpr}.parquet"))
      x |> nanoparquet::write_parquet(file = filename,
                   compression = cmpr,
                   options = nanoparquet::parquet_options(write_minmax_values = FALSE,
                           write_arrow_metadata = FALSE,
                           compression_level = cmpr_level))
      fs::file_size(filename)
    }, USE.NAMES = FALSE)

    # Print all file sizes if verbose
    if (verbose)
      cr |> tibble::enframe() |> dplyr::mutate(name = fs::path_file(.data$name)) |> print()

    # Find the smallest and move it
    smallest <- cr[cr==min(cr)][1]
    fs::file_move(names(smallest), file)

  }

  # A file now exists at the target location, verify contents or error out
  xr <- nanoparquet::read_parquet(file)
  waldo_result <- waldo::compare(
    x |> tibble::as_tibble() |> double_date(),
    xr |> tibble::as_tibble() |> double_date()
  )

  if (length(waldo_result) > 0) {
    print(waldo_result)
    rlang::abort("Disk and memory data do not match")
    cli::cli_abort("Different data in old and new snapshots")
  }

  # We return TRUE if we wrote (if file did NOT exist)
  return(invisible(unname(!file_existed)))
}


#' Read and Combine Multiple Parquet Files Using nanoparquet
#'
#' Reads one or more Parquet files from specified directories, glob patterns,
#' or file paths using `nanoparquet::read_parquet()`, and combines them into
#' a single data frame using `dplyr::bind_rows()`.
#'
#' @param path A directory or glob pattern
#' @param ... Additional arguments passed to `nanoparquet::read_parquet()`.
#'
#' @return A single data frame containing the rows from all matched Parquet files.
#'         A `.file` column is added to indicate the source file of each row.
#'
#' @examples
#' \dontrun{
#'   df <- read_nanoparquet_multi(c("data/", "logs/*.parquet"))
#' }
#'
#' @keywords internal
#' @export
read_parquet_multi <- function(path, ..., add_file_column = FALSE) {

  # Verify input
  checkmate::assert_string(path)
  checkmate::assert_flag(add_file_column)

  # List parquet files
  if (fs::dir_exists(path)) {
    parquet_files <- fs::dir_ls(path, glob = "*.parquet")
  } else {
    parquet_files <- fs::dir_ls(fs::path_dir(path), glob = path, fail = FALSE)
  }

  if (length(parquet_files) == 0) {
    cli::cli_warn("No parquet files match criteria, the result will be an empty tibble")
  }
  l.frames <- purrr::map(parquet_files, function(x){
    result <- nanoparquet::read_parquet(x)
    if (add_file_column) {
      result <- dplyr::mutate(result, .file = x, .before = 1)
    }
    result
  })

  # Bind the rows
  l.frames |> dplyr::bind_rows()
}


#' Verify and clean FX data versioning
#'
#' Checks the `.version.` column of an FX dataset. Issues a warning if data is
#' unversioned or contains multiple versions. Removes the `.version.` column
#' before returning the data.
#'
#' @param d A data frame with a `.version.` column.
#'
#' @return The input data frame with the `.version.` column removed.
#'
#' @keywords internal
#' @export
fx_verify_data_version <- function(d) {
  if (is.null(d$.version.)) {
    # Legacy unversioned data
    cli::cli_warn(c("FX data version mismatch",
                    "!" = "Old and unversioned FX data detected",
                    "i" = 'It is recommended that you run
                           {.run [fx_init("full")](fxconvert::fx_init(action = "full"))}
                           for a full initialization, to refresh all FX data from the online repository.
                           If this persists after a full initialization, please file an issue.'))
  } else {
    if (!length(unique(d$.version.)) == 1) {
      # More that one version found
      cli::cli_warn(c("FX data version mismatch",
                      "!" = "More than one version of FX data detected",
                      "i" = 'It is recommended that you run
                           {.run [fx_init("full")](fxconvert::fx_init(action = "full"))}
                           for a full initialization, to refresh all FX data from the online repository.
                           If this persists after a full initialization, please file an issue.'))
    }
  }
  d$.version. <- NULL
  d
}




