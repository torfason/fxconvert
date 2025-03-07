
#' @param amount Numeric; the amount of money to convert from the `from` currency
#' to the `to` currency. Only used in `fx_convert()`.
#' @return For `fx_convert()`, the converted amount in the `to` currency.
#'   `fx_convert()` relies directly on the exchange rates retrieved using
#'   `fx_get()`, and simply multiplies the amount with the rates that `fx_get()`
#'   return.
#'
#' @rdname fx_get
#' @export
fx_convert <- function(amount, from, to, fxdate, fxsource = "ecb", ..., .interpolate = FALSE) {

  # Validate and recycle
  args <- vctrs::vec_recycle_common(
    amount = amount,
    from = from,
    to = to,
    fxdate = fxdate)
  fxsource <- arg_match(fxsource)

  args$amount * fx_get(args$from, args$to, args$fxdate, fxsource, ..., .interpolate = .interpolate)
}
