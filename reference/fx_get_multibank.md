# Fetch FX rates from multiple banks

Retrieves exchange rates for a given currency pair and date from one or
more data sources (ECB, CBI, or Fed), returning either a “wide” or
“long” tibble.

## Usage

``` r
fx_get_multibank(
  from,
  to,
  fxdate,
  bank = c("ecb", "cbi", "fed"),
  ...,
  .interpolate = FALSE,
  result_shape = c("wide", "long")
)
```

## Arguments

- from:

  Base currency code (e.g., `"USD"`).

- to:

  Quote currency code (e.g., `"EUR"`).

- fxdate:

  Date for which the rate is requested.

- bank:

  Source bank (one or more of `c("ecb", "cbi", "fed")`).

- ...:

  Additional arguments passed to
  [`fx_get()`](https://torfason.github.io/fxconvert/reference/fx_get.md).

- .interpolate:

  Logical. If `TRUE`, interpolate missing dates.

- result_shape:

  `"wide"` (col per bank) or `"long"` (row per bank).

## Value

A tibble with the rates from each of the `bank`s.
