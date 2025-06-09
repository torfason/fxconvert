
#' Retrieve Available Date Range for Foreign Exchange Data
#'
#' Determines the range of dates for which foreign exchange rate data is available
#' in a specified source. Currently, this function only supports retrieving data
#' ranges from local sources.
#'
#' @param bank Character string specifying the source of the exchange rate data.
#'        The default source is "ecb" (European Central Bank).
#' @param where Character vector indicating the data location. The default
#'        options are "local" for local data sources and "server" for remote servers.
#'        Currently, only "local" is implemented.
#'
#' @return A character vector with two elements: the first and last dates in
#'         "YYYY-MM-DD" format for which data is available in the specified source.
#'
#' @examples
#' #fx_available_range(bank = "ecb", where = "local")
#'
#' @keywords internal
fx_available_range <- function(bank = "ecb", where = c("local", "server")) {

  # Verify and preprocess inputs
  bank <- arg_match(bank)
  where <- arg_match(where)

  if (where == "local") {

    # # Open conn, prepare dbplyr table, and register conn for closing
    # fxdata_dir <- fx_get_fxdata_dir()
    # duckdb_file <- fs::path(fxdata_dir, paste0(bank, ".duckdb"))
    #
    # # Ensure db exists
    # if (!fs::file_exists(duckdb_file)) {
    #   cat("ALERT:     <dbfile> does not exist. You probably need to run fx_init()\n")
    #   return(invisible(NULL))
    # }
    #
    # # Connect to db
    # conn <- duckdb::dbConnect(duckdb::duckdb(duckdb_file))
    # on.exit(duckdb::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
    conn <- fx_duck_local(bank)

    # Check available range from db
    fxtable <- dplyr::tbl(conn, "fxtable")
    available_range <- fxtable |>
      dplyr::summarise(date_first = min(.data$fxdate, na.rm = TRUE),
                       date_last  = max(.data$fxdate, na.rm = TRUE)) |>
      dplyr::collect() |>
      sapply(format, "%Y-%m-%d")
    return(available_range)
  } else {
    stop("fx_available_range() is only implemented for local data")
  }
}

