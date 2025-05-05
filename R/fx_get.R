
#' Retrieve exchange rates or convert amounts between currencies
#'
#' These functions are used to convert amounts from one currency to another, or
#' to directly return the exchange rates between two specified currencies.
#'
#' @param from The ISO currency code (as a character string) for the base currency.
#' @param to The ISO currency code (as a character string) for the target currency.
#' @param fxdate The date for which to retrieve exchange rates, in "YYYY-MM-DD" format.
#' @param bank A character string specifying the source of the exchange rate data.
#'        Default is "ecb" (European Central Bank).
#' @param ... Reserved
#' @param .interpolate If a weekend date is requested, should the previous days rate
#'   be returned. If this is false, a weekend date will result in an error.
#'
#' @return For `fx_get()`, a numeric value representing the exchange rate from
#'   the `from` currency to the `to` currency on the specified `fxdate`. The
#'   exchange rate between`from` and `to`, given in a way that to convert, from
#'   `from` to `to` one would multiply by the exchange rate. This means that the
#'   function may not always give the expected value for well-known currency
#'   pairs that are always quoted in the same direction.
#' @export
fx_get <- function(from, to, fxdate, bank = "ecb", ..., .interpolate = FALSE) {

  # Initialize once per session before getting
  fx_init(bank = bank, once = TRUE, verbose = TRUE)

  # Ensure vectors are recyclable to the same length
  args <- vctrs::vec_recycle_common(
    from = from,
    to = to,
    fxdate = fxdate
  )

  # Apply fx_get_single over the rows of the tibble
  purrr::pmap_vec(
    args,
    ~ fx_get_single(..1, ..2, ..3, bank = bank, ..., .interpolate = .interpolate),
    .ptype = double()
  )
}



#' This function connects to a DuckDB database containing foreign exchange rate data,
#' retrieves exchange rates between two specified currencies for a given date,
#' and calculates the exchange rate from the first specified currency to the second.
#'
#' @keywords internal
fx_get_single <- function(from, to, fxdate, bank = "ecb", ..., .interpolate = FALSE) {

  stopifnot(length(from) == 1)
  stopifnot(length(to) == 1)
  stopifnot(length(fxdate) == 1)
  stopifnot(length(.interpolate) == 1)

  # Verify and preprocess parameters
  from     <- tolower(from)
  to       <- tolower(to)
  fxsource <- tolower(bank)

  # Choose whether to used the filled version of the table
  if (.interpolate) {
    fxtable_name = "fxtable_filled"
  } else {
    fxtable_name = "fxtable"
  }

  # Open con, prepare dbplyr table, and register con for closing
  fxdata_dir <- fx_get_fxdata_dir()
  duckdb_file <- file.path(fxdata_dir, paste0(fxsource, ".duckdb"))
  con <- duckdb::dbConnect(duckdb::duckdb(duckdb_file, read_only = TRUE))
  on.exit(duckdb::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  fxtable <- dplyr::tbl(con, fxtable_name)

  fxdate_param <- fxdate
  v.rates <- fxtable |>
    dplyr::filter(fxdate == fxdate_param) |>
    dplyr::select(dplyr::all_of(c(from, to))) |>
    dplyr::collect() |>
    unlist() |>
    as.vector()
  v.rates

  if ((length(v.rates) == 1) && (to == from)) {
    warning("Looks like you ae converting to same currency (", to,  "), which is OK, but a special case, hence this warning")
    return(1)
  }

  # R does not throw an error
  if (length(v.rates) != 2)
    stop("length(d.rates) != 2, probably date requested was not in range or one of the rates was not found\n",
          "(", from, ", ", to, ", ", fxdate, ")")

  # We error out on NA until further notice
  # Different handling depending on whether the source quotes direct or indirect,
  # TODO: Read this from json file for source
  if (bank %in% c("ecb", "fed")) {
    result = v.rates[2] / v.rates[1]
  } else if (bank %in% c("cbi", "xfed")) {
    result = v.rates[1] / v.rates[2]
  } else {
    cli::cli_abort(glue::glue("Unknown source: '{bank}'"))
  }
  if (is.na(result))
    stop("fx rate is NA, probably a weekend and .interpolate is false: ", "(", from, ", ", to, ", ", fxdate, ")")

  # Return
  result
}


