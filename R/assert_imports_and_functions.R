
#' @importFrom checkmate assert_flag assert_string
#'   assert_number assert_int assert_count
#' @importFrom checkmate assert_list assert_class
NULL

#' @importFrom rlang arg_match
NULL

#' Assert that no dots arguments are passed
#' @description This is an alias for `rlang::check_dots_empty()`, for
#'   consistency with other arguments. The function throws an error if any
#'   unnamed parameters were passed to the function where this is called.
#' @keywords internal
assert_dots_empty <- rlang::check_dots_empty


# This function is purely a workaround for check errors.
#
# Packages listed in imports but only used indirectly result in check errors.
# This function adds usage to these packages, silencing R check.
#
# This function should never be called.
workaround_for_import_checks <- function()
{
  dbplyr::lazy_frame(a = letters)
  rlang::int()
}
