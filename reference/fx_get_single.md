# This function connects to a DuckDB database containing foreign exchange rate data, retrieves exchange rates between two specified currencies for a given date, and calculates the exchange rate from the first specified currency to the second.

This function connects to a DuckDB database containing foreign exchange
rate data, retrieves exchange rates between two specified currencies for
a given date, and calculates the exchange rate from the first specified
currency to the second.

## Usage

``` r
fx_get_single(from, to, fxdate, bank = "ecb", ..., .interpolate = FALSE)
```
