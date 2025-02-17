
# Global definitions required for this function
utils::globalVariables(
  c(".data", "full_month", "full_year", "ym_char", "ymd_char", "start_date", "last_date", "end_date", "first_date")
)

#' Get Local FX Data Directory
#'
#' Returns the path to the local directory for storing FX data.
#'
#' @return A character string representing the path to the local FX data
#'   directory.
#'
#' @keywords internal
#' @export
fx_get_fxdata_dir <- function() {
  app_dir <- rappdirs::user_data_dir("net.zulutime.r.fx")
  fs::path(app_dir, "fxdata")
}
