
# This function is purely a workaround for check.
#
# The dbplyr package must be listed in imports as it is used indirectly by
# dplyr code. R check, however, complains if it cannot determine that
# it is used in the package.
#
# This function should never be called.
workaround_for_dbplyr_check <- function()
{
  dbplyr::lazy_frame(a = letters)
}
