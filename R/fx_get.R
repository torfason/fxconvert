
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
fx_get <- function(from, to, fxdate = today(), bank = "ecb", ..., .interpolate = FALSE) {

  # Verify arguments
  assert_character(from)
  assert_character(to)
  assert_string(bank)
  assert_flag(.interpolate)

  # Initialize once per session before getting
  fx_init(banks = bank, once = TRUE, verbose = FALSE)

  method <- "duckdb_asof_sorted_onecall"
  if (method == "duckdb_asof_sorted") {
    fx_get_impl_join(from, to, fxdate, bank, .interpolate = .interpolate, join_engine = "duckdb", asof = TRUE, table_name = "fxtable_long_cur_date")
  } else if (method == "duckdb_asof_sorted_onecall") {
    fx_get_impl_join(from, to, fxdate, bank, .interpolate = .interpolate, join_engine = "duckdb_onecall", asof = TRUE, table_name = "fxtable_long_cur_date")
  }  else if (method == "join_dplyr") {
    fx_get_impl_join(from, to, fxdate, bank, .interpolate = .interpolate, join_engine = "dplyr")
  } else if (method == "join_duckdb") {
    fx_get_impl_join(from, to, fxdate, bank, .interpolate = .interpolate, join_engine = "duckdb" )
  } else if (method == "apply") {
    fx_get_impl_apply(from, to, fxdate, bank, .interpolate = .interpolate)
  } else {
    rlang::abort("Wrong method selected for fx_get_impl...()")
  }
}


# Private method implementing fx_get() by relying on baserate joins
fx_get_impl_join <- function(from, to, fxdate, bank, .interpolate, join_engine, asof, table_name = "fxtable_long") {

  # Get baserates for to and from
  if (join_engine == "duckdb_onecall") {

    # Recycle parameters and get length of the recycled vectors
    d.recycled <- tibble::tibble(from, to, fxdate)
    n_recycled <- nrow(d.recycled)

    # Call baserates function once with to and from concatenated together in
    # order to get both baserate results with a single db call, then split
    # them up here
    d.from_to <- fx_baserates_join_duckdb(c(d.recycled$from, d.recycled$to), rep(d.recycled$fxdate, 2), bank, asof = asof, table_name = table_name)
    d.from <- d.from_to[seq2(1, n_recycled),]
    d.to   <- d.from_to[seq2(n_recycled+1, n_recycled*2),]

  } else if (join_engine == "dplyr") {
    d.from <- fx_baserates_join_dplyr(from, fxdate, bank, table_name = table_name)
    d.to   <- fx_baserates_join_dplyr(to, fxdate, bank, table_name = table_name)
  } else if (join_engine == "duckdb") {
    d.from <- fx_baserates_join_duckdb(from, fxdate, bank, asof = asof, table_name = table_name)
    d.to   <- fx_baserates_join_duckdb(to, fxdate, bank, asof = asof, table_name = table_name)
  } else {
    rlang::abort("Invalid join_engine: {join_engine}")
  }

  # Check oldest rates and warn if older than max_age_warn
  # (if no rows were returned we treat the max as zero)
  oldest_rates <- ifelse(n_recycled>0, max(c(d.from$age, d.to$age)), 0)
  if (oldest_rates > 7) cli::cli_warn("Oldest rates used for conversion were {oldest_rates} days old.")

  # Calculate bilateral rates and return
  # TODO: This should be read from json and stored in .globals
  if (bank %in% c("ecb", "fed")) {
    result = d.to$rate / d.from$rate
  } else if (bank %in% c("cbi", "xfed")) {
    result = d.from$rate / d.to$rate
  } else {
    cli::cli_abort("Unknown source: '{bank}'")
  }
  result
}


# Private method implementing fx_get() by looking up single rows and mapping
fx_get_impl_apply <- function(from, to, fxdate, bank, .interpolate) {

  # Ensure vectors are recyclable to the same length
  args <- dplyr::tibble(
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

  # Verify arguments
  assert_string(from)
  assert_string(to)
  assert_string(bank)
  assert_flag(.interpolate)

  # Verify and preprocess parameters
  from   <- tolower(from)
  to     <- tolower(to)
  fxdate <- ymd(fxdate)
  bank   <- tolower(bank)

  # Choose whether to used the filled version of the table
  if (.interpolate) {
    fxtable_name = "fxtable_filled"
  } else {
    fxtable_name = "fxtable"
  }

  # Open conn, prepare dbplyr table, and register conn for closing
  # fxdata_dir <- fx_get_fxdata_dir()
  # duckdb_file <- file.path(fxdata_dir, paste0(bank, ".duckdb"))
  # conn <- duckdb::dbConnect(duckdb::duckdb(duckdb_file, read_only = TRUE))
  # on.exit(duckdb::dbDisconnect(conn, shutdown = TRUE), add = TRUE)
  conn <- fx_duck_local(bank)
  fxtable <- dplyr::tbl(conn, fxtable_name)

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

  # R does not throw an error if one of the rates is not found
  #
  if (length(v.rates) != 2) {

    # When .interpolate is true, we do special handling to get the latest date available
    # (often, this case means that a future date or date after last fx fetch was requested)
    if (.interpolate) {
      # Calculate the latest fxdate available
      latest_fxdate <- fxtable  |>
        dplyr::filter(fxdate == max(fxdate, na.rm = TRUE)) |>
        dplyr::collect() |> purrr::pluck("fxdate")
      if (ymd(fxdate) > ymd(latest_fxdate)) {
        cli::cli_warn("Requested fxdate later than available data, retry with latest available date")
        return(fx_get_single(from = from, to = to, fxdate = latest_fxdate, bank = bank, .interpolate = .interpolate))
      } else {
        cli::cli_abort(c("Failed with .interpolate == TRUE",
            i = "Arguments: [ from: {from} | to: {to} | fxdate: {fxdate} | length(d.rates): {length(v.rates)} ] "))
      }

    } else {
      cli::cli_abort(c("length(v.rates) != 2, probably date not in range or rate not in data",
            i = "Arguments: [ from: {from} | to: {to} | fxdate: {fxdate} | length(d.rates): {length(v.rates)} ]"))
    }
  }

  # We error out on NA until further notice
  # Different handling depending on whether the source quotes direct or indirect,
  # TODO: Read this from json file for source
  if (bank %in% c("ecb", "fed")) {
    result = v.rates[2] / v.rates[1]
  } else if (bank %in% c("cbi", "xfed")) {
    result = v.rates[1] / v.rates[2]
  } else {
    cli::cli_abort("Unknown source: '{bank}'")
  }
  if (is.na(result))
    stop("fx rate is NA, probably a weekend and .interpolate is false: ", "(", from, ", ", to, ", ", fxdate, ")")

  # Return
  result
}


#' Fetch FX rates from multiple banks
#'
#' Retrieves exchange rates for a given currency pair and date from one or more
#' data sources (ECB, CBI, or Fed), returning either a “wide” or “long” tibble.
#'
#' @param from          Base currency code (e.g., `"USD"`).
#' @param to            Quote currency code (e.g., `"EUR"`).
#' @param fxdate        Date for which the rate is requested.
#' @param bank          Source bank (one or more of `c("ecb", "cbi", "fed")`).
#' @param ...           Additional arguments passed to `fx_get()`.
#' @param .interpolate  Logical. If `TRUE`, interpolate missing dates.
#' @param result_shape  `"wide"` (col per bank) or `"long"` (row per bank).
#'
#' @return A tibble with the rates from each of the `bank`s.
#'
#' @keywords internal
#' @export
fx_get_multibank <- function(from, to, fxdate, bank = c("ecb", "cbi", "fed"), ...,
                             .interpolate = FALSE, result_shape = c("wide", "long")) {

  result_shape <- arg_match(result_shape)

  if (result_shape == "long") {
    f <- function(x) {
      result = fx_get(from, to, fxdate, bank = x, ..., .interpolate = .interpolate)
      tibble::tibble(fxdate, from, to, bank = x, rate = result)
    }
    l <- lapply(bank, f)
    result <- l |> dplyr::bind_rows()
  } else if (result_shape == "wide") {
    f <- function(x) {
      result = fx_get(from, to, fxdate, bank = x, ..., .interpolate = .interpolate)
    }
    l <- lapply(bank, f)
    names(l) = bank
    result <- dplyr::bind_cols(tibble::tibble(fxdate, from, to),
                               tibble::as_tibble(l))
  }
  result
}

# Implementation to construct baserates table with a dplyr join
fx_baserates_join_dplyr <- function(currency, fxdate, bank = "ecb", ..., table_name, options = fx_options()) {

  # Verify arguments
  assert_character(currency)
  assert_string(bank)

  # Verify and preprocess parameters
  currency   <- tolower(currency)
  fxdate <- ymd(fxdate)
  bank   <- tolower(bank)

  # Ensure vectors are recyclable to the same length
  d.fxrequest <- dplyr::tibble(
    currency = currency,
    fxdate = fxdate
  )

  conn <- fx_duck_local(bank)
  duckdb::duckdb_register(conn, "fxrequest", d.fxrequest)

  tbl.fxdata    <- dplyr::tbl(conn, table_name)
  tbl.fxrequest <- dplyr::tbl(conn, "fxrequest")

  d.fxdata <- tbl.fxdata |> dplyr::collect()

  # https://www.tidyverse.org/blog/2023/01/dplyr-1-1-0-joins/
  dplyr::left_join(d.fxrequest, d.fxdata, dplyr::join_by(currency, closest(x$fxdate >= y$fxdate))) |>
    dplyr::mutate(age = as.numeric(fxdate.x - fxdate.y)) |>
    dplyr::select(currency, rate, age)

}


# Implementation to construct baserates table with a duckdb join
fx_baserates_join_duckdb <- function(currency, fxdate, bank = "ecb", ..., asof, table_name, options = fx_options()) {

  # Verify arguments
  assert_character(currency)
  assert_string(bank)

  # Verify and preprocess parameters
  currency   <- tolower(currency)
  fxdate <- ymd(fxdate)
  bank   <- tolower(bank)

  # Ensure vectors are recyclable to the same length
  d.fxrequest <- dplyr::tibble(
    currency = currency,
    fxdate = fxdate
  ) |> dplyr::mutate (row_number = dplyr::row_number())

  conn <- fx_duck_local(bank)
  duckdb::duckdb_register(conn, "fxrequest", d.fxrequest)
  withr::defer(duckdb::duckdb_unregister(conn, "fxrequest"))

  # TODO: Wrap in db_get_query(conn, statement) { ... }
  if (asof) {
    res <- duckdb::dbSendQuery(conn, glue("
      SELECT
        rq.currency,
        rq.fxdate,
        fxd.rate,
        rq.fxdate - fxd.fxdate AS age
      FROM fxrequest AS rq
      ASOF LEFT JOIN {table_name} AS fxd
        ON rq.currency = fxd.currency
        AND rq.fxdate >= fxd.fxdate
      ORDER BY rq.row_number
    "))
  } else {
    res <- duckdb::dbSendQuery(conn, glue("
      SELECT
        rq.currency,
        fxd.rate,
        rq.fxdate - fxd.fxdate AS age
      FROM fxrequest AS rq
      LEFT JOIN {table_name} AS fxd
        ON rq.currency = fxd.currency
        AND fxd.fxdate <= rq.fxdate
      QUALIFY fxd.fxdate = MAX(fxd.fxdate) OVER (PARTITION BY rq.fxdate, rq.currency)
      ORDER BY rq.row_number
    "))
  }

  # Defer clearing results (not needed once we use db_get_query() for the query)
  withr::defer(duckdb::dbClearResult(res))

  # Get a tibble and return it
  data <- duckdb::dbFetch(res) |> dplyr::as_tibble()
  data

}



