

#' @importFrom zmisc chk_any chk_atomic chk_character chk_class chk_complex
#' @importFrom zmisc chk_count chk_data_frame chk_data_table chk_date chk_day
#' @importFrom zmisc chk_dnumber chk_dots_empty chk_double chk_environment
#' @importFrom zmisc chk_factor chk_flag chk_instant chk_integer chk_integerish
#' @importFrom zmisc chk_inumber chk_list chk_logical chk_match chk_naturalish
#' @importFrom zmisc chk_number chk_numeric chk_posixct chk_raw chk_scalar
#' @importFrom zmisc chk_string chk_that chk_tibble chk_true chk_znumber
NULL

#' @importFrom zmisc glue glue_data glue_vector
NULL

#' @importFrom rlang %||% arg_match seq2 new_environment
NULL

#' @importFrom lubridate ymd
#' @export
lubridate::ymd

#' @importFrom lubridate today
#' @export
lubridate::today

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

# Define globals needed to suppress errors related to dplyr::join_by()
utils::globalVariables(c(
  "x", "y", "closest"
))
