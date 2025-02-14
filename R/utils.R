
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


#' Fill Missing Values Within Observed Data Range
#'
#' Fills `NA` values in a vector within the index range of observed
#' (non-missing) values, without extending beyond the last non-missing value.
#'
#' @param x An atomic vector containing `NA` values to be filled.
#' @param direction A string indicating the fill direction. Must be one of:
#'   - `"down"` (default): Fill missing values downward.
#'   - `"up"`: Fill missing values upward.
#'   - `"downup"`: Fill first down, then up.
#'   - `"updown"`: Fill first up, then down.
#' @param max_fill A single positive integer specifying the maximum number of
#'   sequential missing values that will be filled. If NULL, there is no limit.
#' @return A vector of the same type as `x`, with missing values filled within
#'   the index range of observed (non-missing) values
#'
#' @details
#' The function identifies the first and last non-missing values in `x` and only
#' fills missing values within this index range. Any values beyond the last
#' non-missing value remain `NA`.
#'
#' This function is useful when you need to fill missing values but want to
#' avoid extending beyond known data points. Internally it
#' uses [vctrs::vec_fill_missing()] to do the filling.
#'
#' The `"downup"` and `"updown"` approaches are provided strictly for
#' compatibility with [vctrs::vec_fill_missing()]. In practice, the result will
#' always be exactly equal to just using `"up"` or `"down"`, respectively.
#'
#' @examples
#' x1 <- c(NA, NA, 1, NA, NA, 2, NA, NA)
#' fx_vec_fill_gaps(x1)
#' fx_vec_fill_gaps(x1, direction = "up")
#'
#' x2 <- c(NA, NA, 1, NA, NA)
#' fx_vec_fill_gaps(x2)  # Unchanged (single element means no gaps)
#'
#' x3 <- c(NA, NA, NA, NA, NA)
#' fx_vec_fill_gaps(x3)  # Unchanged (all NA)
#'
#' @keywords internal
#' @export
fx_vec_fill_gaps <- function(x, direction = c("down", "up", "downup", "updown"),
                  max_fill = NULL) {
  # Verify input
  stopifnot(is.atomic(x))
  direction <- match.arg(direction)
  result <- x

  # Find indexes of non-empty elements and fill only in that range
  ix <- which(!is.na(x))
  if (length(ix) > 0) {
    ix_first <- ix[1]
    ix_last  <- ix[length(ix)]
    result[ix_first:ix_last] <- vctrs::vec_fill_missing(x[ix_first:ix_last],
                                                        direction, max_fill)
  }
  result
}
