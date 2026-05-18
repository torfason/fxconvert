# Retrieve exchange rates or convert amounts between currencies

These functions are used to convert amounts from one currency to
another, or to directly return the exchange rates between two specified
currencies.

## Usage

``` r
fx_convert(
  amount,
  from,
  to,
  fxdate = today(),
  bank = "ecb",
  ...,
  .interpolate = FALSE
)

fx_get(from, to, fxdate = today(), bank = "ecb", ..., .interpolate = FALSE)
```

## Arguments

- amount:

  Numeric; the amount of money to convert from the `from` currency to
  the `to` currency. Only used in `fx_convert()`.

- from:

  The ISO currency code (as a character string) for the base currency.

- to:

  The ISO currency code (as a character string) for the target currency.

- fxdate:

  The date for which to retrieve exchange rates, in "YYYY-MM-DD" format.

- bank:

  A character string specifying the source of the exchange rate data.
  Default is "ecb" (European Central Bank).

- ...:

  Reserved

- .interpolate:

  If a weekend date is requested, should the previous days rate be
  returned. If this is false, a weekend date will result in an error.

## Value

For `fx_convert()`, the converted amount in the `to` currency.
`fx_convert()` relies directly on the exchange rates retrieved using
`fx_get()`, and simply multiplies the amount with the rates that
`fx_get()` return.

For `fx_get()`, a numeric value representing the exchange rate from the
`from` currency to the `to` currency on the specified `fxdate`. The
exchange rate between`from` and `to`, given in a way that to convert,
from `from` to `to` one would multiply by the exchange rate. This means
that the function may not always give the expected value for well-known
currency pairs that are always quoted in the same direction.
