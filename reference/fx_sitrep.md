# Print debug information about the exchange rate database

Provides a status report for the exchange rate database, printing key
information about the database file and available data, including a
preview of available dates and consistency checks.

## Usage

``` r
fx_sitrep(bank = c("ecb", "cbi", "fed", "xfed"), verbose = TRUE)
```

## Arguments

- bank:

  Character string specifying the source of exchange rate data. Defaults
  to "ecb". Must match a valid source.

- verbose:

  Should it output results or not (only returning `TRUE`/`FALSE`)

## Value

`TRUE` if all looks well and correctly initialized. `FALSE` if the
database does not seem to be initialized. Throws an error if there are
errors in the data structures.
