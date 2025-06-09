
#' Print debug information about the exchange rate database
#'
#' Provides a status report for the exchange rate database, printing key
#' information about the database file and available data, including
#' a preview of available dates and consistency checks.
#'
#' @param bank Character string specifying the source of exchange rate data.
#'   Defaults to "ecb". Must match a valid source.
#' @param verbose Should it output results or not (only returning `TRUE`/`FALSE`)
#' @return `TRUE` if all looks well and correctly initialized. `FALSE` if the
#'   database does not seem to be initialized. Throws an error if there are
#'   errors in the data structures.
#'
#' @keywords internal
fx_sitrep <- function(bank = c("ecb", "cbi", "fed", "xfed"), verbose = TRUE) {

  # Verify and preprocess inputs
  bank <- arg_match(bank)
  assert_flag(verbose)

  # Roll-your-own log levels for now
  if (verbose) {
    xcat <- base::cat
    xprint <- base::print
  } else {
    xcat <- function(x, ...) {invisible(x)}
    xprint <- function(x, ...) {invisible(x)}
  }

  # Open conn, prepare dbplyr table, and register conn for closing
  fxdata_dir <- fx_get_fxdata_dir()
  duckdb_file <- fs::path(fxdata_dir, paste0(bank, ".duckdb"))

  # Output the param values (always present)
  xcat("Status report for fx package:\n")
  xcat("bank:     ", bank, "\n")
  xcat("fxdir:    ", fxdata_dir, "\n")
  xcat("dbfile:   ", duckdb_file, "\n")

  # # Abort if DB is not found
  # if (!fs::file_exists(duckdb_file)) {
  #   xcat("ALERT:     <dbfile> does not exist. You probably need to run fx_init()\n")
  #   return(invisible(FALSE))
  # }
  #
  # # Connect to db inside with_output_sink() because it outputs a newline
  # withr::with_output_sink(nullfile(), {
  #   conn <- duckdb::dbConnect(duckdb::duckdb(duckdb_file, read_only = TRUE))
  #   withr::defer(duckdb::dbDisconnect(conn, shutdown = TRUE))
  # })
  conn <- fx_duck_local(bank)

  # Create a table and select the dates.
  fxtable <- dplyr::tbl(conn, "fxtable")
  available_dates <- fxtable |>
    dplyr::select("fxdate") |>
    dplyr::collect() |>
    purrr::pluck("fxdate")

  xcat("tables:   ", duckdb::dbListTables(conn), "\n")
  xcat("start:     ")
  available_dates |> utils::head(3) |> format() |> c("...") |> xprint()
  xcat("end:       ")
  available_dates |> utils::tail(3) |> format() |> rev() |> c("...") |> xprint()

  # Sanity checks, if any of these fails, we stop with a loud error
  if (!all(diff(available_dates) == 1))
    cli::cli_abort("Some diffs in available dates do not equal one")

  # Return TRUE if sitrep() looks sane
  return(invisible(TRUE))
}
