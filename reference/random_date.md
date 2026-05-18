# Generate a random date between two dates

Returns a single random date between `start_date` and `end_date`. If
`seed` is provided, the result is reproducible.

## Usage

``` r
random_date(
  n,
  start_date = "1970-01-01",
  end_date = Sys.Date(),
  replace = FALSE,
  seed = NULL
)
```

## Arguments

- n:

  Length of the result

- start_date:

  Date or string on
  [`ymd()`](https://lubridate.tidyverse.org/reference/ymd.html) form.
  The start of the date range.

- end_date:

  Date or string on
  [`ymd()`](https://lubridate.tidyverse.org/reference/ymd.html) form.
  The end of the date range.

- seed:

  Optional integer. If provided, sets the random seed locally for
  reproducibility.

## Value

A `Date` object of length `n`.

## Examples

``` r
random_date(3, "2000-01-01", "2020-12-31")
#> [1] "2014-06-28" "2015-02-17" "2018-01-29"
random_date(5, as.Date("2000-01-01"), as.Date("2020-12-31"), seed = 42)
#> [1] "2007-02-21" "2011-02-20" "2006-06-26" "2014-06-08" "2003-01-02"
```
