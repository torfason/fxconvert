
<!-- README.md is generated from README.Rmd. Please edit that file -->

# fxconvert

<!-- badges: start -->

<!-- badges: end -->

Convert currencies based on rates obtained directly from central banks
in an open and transparent way, without any API dependencies or
registration requirements. Conversion with `fx_convert()` is fully
vectorized in amounts, currencies, and dates, uses local data when
available, and downloads up-to-date rates from a GitHub repository in
`parquet` format when needed.

## Installation

You can install the development version of `fxconvert` like so:

``` r
pak::pak("torfason/fxconvert")
```

## Examples

Use `fx_get()` to quickly get exchange rates for different currencies
and dates, or use `fx_convert()` to convert amounts directly between
currencies based on the exchange rates on particular dates.

``` r
# Load the package
library(fxconvert)

# Initialize the local data store, downloading fx data from GitHub
fx_init(verbose = FALSE)

# Retrieve specific exchange rates on specific dates
fx_get("usd", "eur", "2020-01-03")
#> [1] 0.8971024

# The functions are fully vectorized
fx_get(from   = c("usd", "eur", "gbp"),
       to     = c("eur", "gbp", "usd"),
       fxdate = "2020-01-03") 
#> [1] 0.8971024 0.8511500 1.3096399

# Load dplyr for demonstrating use within a tibble manipulation pipe
library(dplyr, include.only = c("tibble", "mutate"))

# Use fx_convert() to convert amounts between currencies directly
# This is useful when manipulating data frames
d <- tibble(date = c("2020-01-03", "2024-05-06"), 
            price_usd = c(999, 13.7))
d |> 
  mutate(price_eur = fx_convert(price_usd, "usd", "eur", date))
#> # A tibble: 2 × 3
#>   date       price_usd price_eur
#>   <chr>          <dbl>     <dbl>
#> 1 2020-01-03     999       896. 
#> 2 2024-05-06      13.7      12.7
```

## Vectorization and recycling

Both functions are fully vectorized in terms of the following
parameters:

- `from` (The currency to convert from)
- `to` (The currency to convert to)
- `fxdate` (The date for the exchange rate to use)

The `fx_convert()` function is also vectorized in terms of the amount:

- `amount` (The amount to convert between currencies)

Recycling follows the tidy recycling rules.
