
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
  assert_nodots(...)
  assert_fxoptions(options)

  # Construct
  app_dir <- rappdirs::user_data_dir("net.zulutime.r.fxconvert")
  ws_dir <- fs::path(app_dir, options$workspace) |> fs::path_norm()

  # Prevent user from referring outside of app dir
  stopifnot(fs::path_has_parent(ws_dir, app_dir))

  # Return result
  ws_dir
}

