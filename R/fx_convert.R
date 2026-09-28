
#' @param amount Numeric; the amount of money to convert from the `from` currency
#' to the `to` currency. Only used in `fx_convert()`.
#' @return For `fx_convert()`, the converted amount in the `to` currency.
#'   `fx_convert()` relies directly on the exchange rates retrieved using
#'   `fx_get()`, and simply multiplies the amount with the rates that `fx_get()`
#'   return.
#'
#' @rdname fx_get
#' @export
fx_convert <- function(amount, from, to, fxdate = today(), bank = "ecb",
                       ...,  .interpolate = FALSE) {

  # Verify arguments
  chk_numeric(amount)
  chk_character(from)
  chk_character(to)
  fxdate <- ymd(fxdate)
  chk_date(fxdate, na.ok = FALSE)
  chk_string(bank)
  chk_dots_empty()
  chk_flag(.interpolate)

  # Recycle arguments, to ensure amount is included in the recycling
  args <- tibble::tibble(
    amount = amount,
    from = from,
    to = to,
    fxdate = fxdate)

  # Multiply amount by exchange rate
  args$amount * fx_get(args$from, args$to, args$fxdate, bank, ..., .interpolate = .interpolate)
}
