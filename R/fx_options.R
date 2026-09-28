

#' @title Create an fx_options Object
#' @description Constructs an `fx_options` object with configurable parameters.
#'   The parameters can be set from the defaults, by
#' @param ... Reserved. All arguments must be named.
#' @param workspace A character string specifying the local workspace to use
#'   for operations.
#' @param bank A character string specifying which central bank rates to
#'   base the conversions on.
#' @param repo A character string specifying the remote repository source for
#'   exchange rate data.
#' @return A list representing the `fx_options` object.
#' @export
fx_options <- function(...,
    workspace = getOption("fxconvert.workspace", "main"),
    bank = getOption("fxconvert.bank", "ecb"),
    repo = getOption("fxconvert.repo", "torfason/fxdata")
) {

  # Generate the object
  obj <- structure (
    list(
      workspace = workspace,
      bank = bank,
      repo = repo
    ),
    class = "fxconvert_fx_options"
  )

  # Verify object and that ... was unused before returning the object
  chk_dots_empty()
  chk_fxoptions(obj)
}


#' @title Verify that x is a valid fx_options object
#' @description Verifies that `x` is an `fx_options` object (of class
#'   `fxconvert_fx_options`) and that all elements of the object are
#'   valid for such an object. Use `fx_options()` to create `fx_options`
#'   objects.
#' @param x An object to verify
#' @return Unchanged input if valid, otherwise an error is thrown.
#' @keywords internal
chk_fxoptions <- function(x) {

  # Verify input
  chk_list(x, attr.ok = c("names", "class"))
  chk_class(x, "fxconvert_fx_options")
  chk_string(x$workspace)
  chk_string(x$bank)
  chk_string(x$repo)

  # Return input unchanged if it is valid
  invisible(x)
}
