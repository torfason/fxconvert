# Generate a sequence of dates

Creates a daily date sequence between two dates, ensuring the end date
is not earlier than the start date. Useful to pass a range to the
`fxdate` argument of the
[`fx_get()`](https://torfason.github.io/fxconvert/reference/fx_get.md)
and
[`fx_convert()`](https://torfason.github.io/fxconvert/reference/fx_get.md)
functions

## Usage

``` r
fx_date_seq(from_date, to_date = lubridate::today())
```

## Arguments

- from_date:

  A date or character string in "YYYY-MM-DD" format.

- to_date:

  A date or character string in "YYYY-MM-DD" format. Defaults to today's
  date.

## Value

A sequence of dates from `from_date` to `to_date`.

## Examples

``` r
fx_date_seq("2024-01-01", "2024-01-10")
#>  [1] "2024-01-01" "2024-01-02" "2024-01-03" "2024-01-04" "2024-01-05"
#>  [6] "2024-01-06" "2024-01-07" "2024-01-08" "2024-01-09" "2024-01-10"
fx_date_seq(as.Date("2023-06-01"), as.Date("2023-06-05"))
#> [1] "2023-06-01" "2023-06-02" "2023-06-03" "2023-06-04" "2023-06-05"
```
