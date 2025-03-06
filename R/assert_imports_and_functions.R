
#' @importFrom checkmate assert_flag assert_string
#'   assert_number assert_int assert_count
#' @importFrom checkmate assert_list assert_class
NULL

#' Assert that no dots arguments are passed
#' @description Asserts that no named or unnamed parameters to the
#'   function end up assigned to the `ellipsis` or `dots` argument.
#'   The function throws an error if any parameters match the
#'   argument.
#' @param ... The `ellipsis` argument should be empty.
#' @keywords internal
assert_nodots <- function(...) {
  if (length(list(...)) != 0) {
    stop("No unnamed arguments are allowed / The ... argument must be empty")
  }
}
