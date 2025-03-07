
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

## Example

The package includes a helper function to fill internal gaps in a vector
but not any gaps at the beginning or the end of the vector:

``` r
# Load the package
library(fxconvert)
library(dplyr)
#> 
#> Attaching package: 'dplyr'
#> The following objects are masked from 'package:stats':
#> 
#>     filter, lag
#> The following objects are masked from 'package:base':
#> 
#>     intersect, setdiff, setequal, union

# Initialize the local data store, downloading fx data from GitHub
fx_init(verbose = FALSE)

# Retrieve specific exchange rates (fully vectorized)
fx_get("usd", "eur", "2020-01-03")
#> [1] 0.8971024
fx_get(from   = c("usd", "eur", "gbp"),
       to     = c("eur", "gbp", "usd"),
       fxdate = "2020-01-03") 
#> [1] 0.8971024 0.8511500 1.3096399

# Convert amounts between currencies (fully vectorized),
# useful when manipulating data frames
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
