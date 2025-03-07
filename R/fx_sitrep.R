
#' Print debug information about the exchange rate database
#'
#' Provides a status report for the exchange rate database, printing key
#' information about the database file and available data, including
#' a preview of available dates and consistency checks.
#'
#' @param fxsource Character string specifying the source of exchange rate data.
#'   Defaults to "ecb". Must match a valid source.
#'
#' @keywords internal
fx_sitrep <- function(fxsource = "ecb") {

  # Verify and preprocess inputs
  fxsource <- arg_match(fxsource)

  # Open con, prepare dbplyr table, and register con for closing
  fxdata_dir <- fx_get_fxdata_dir()
  duckdb_file <- fs::path(fxdata_dir, paste0(fxsource, ".duckdb"))

  # Output the param values (always present)
  cat("Status report for fx package:\n")
  cat("fxsource: ", fxsource, "\n")
  cat("fxdir:    ", fxdata_dir, "\n")
  cat("dbfile:   ", duckdb_file, "\n")

  # Abort if DB is not found
  if (!fs::file_exists(duckdb_file)) {
    cat("ALERT:     <dbfile> does not exist. You probably need to run fx_init()\n")
    return(invisible(NULL))
  }

  # Connect to db
  con <- duckdb::dbConnect(duckdb::duckdb(duckdb_file, read_only = TRUE))
  on.exit(duckdb::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  fxtable <- dplyr::tbl(con, "fxtable")

  if (exists("fxdate")) stop("Workaround for check error: fxdate already exists")
  fxdate <- "Workaround for check error"
  available_dates <- fxtable |>
    dplyr::select(fxdate) |>
    dplyr::collect() |>
    dplyr::mutate(fxdate = as.character(fxdate)) |>
    unlist() |>
    as.vector()

  cat("tables:   ", duckdb::dbListTables(con), "\n")
  cat("start:     ")
  available_dates |> utils::head(3) |> c("...") |> print()
  cat("end:       ")
  available_dates |> utils::tail(3) |> rev() |> c("...") |> print()

  # Sanity checks, if any of these fails, we stop with a loud error
  available_dates_maxdiff <- available_dates |> lubridate::ymd() |> diff() |> max()
  available_dates_mindiff <- available_dates |> lubridate::ymd() |> diff() |> min()
  if ( available_dates_maxdiff != 1 )
    stop("Gaps too wide in date ranges (available_dates_maxdiff != 1)")
  if (available_dates_maxdiff != 1 )
    stop("Gaps too narrow in date ranges (available_dates_maxdiff != 1)")

}
