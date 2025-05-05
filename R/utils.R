
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


