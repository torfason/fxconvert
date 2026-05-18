# Retrieve Available Date Range for Foreign Exchange Data

Determines the range of dates for which foreign exchange rate data is
available in a specified source. Currently, this function only supports
retrieving data ranges from local sources.

## Usage

``` r
fx_available_range(bank = "ecb", where = c("local", "server"))
```

## Arguments

- bank:

  Character string specifying the source of the exchange rate data. The
  default source is "ecb" (European Central Bank).

- where:

  Character vector indicating the data location. The default options are
  "local" for local data sources and "server" for remote servers.
  Currently, only "local" is implemented.

## Value

A character vector with two elements: the first and last dates in
"YYYY-MM-DD" format for which data is available in the specified source.

## Examples

``` r
#fx_available_range(bank = "ecb", where = "local")
```
