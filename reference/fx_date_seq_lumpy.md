# Generate a lumpy sequene of dates

Generates a "lumpy" sequence of dates in that the dates are grouped into
individual components of full years, full months, and remaining days.

## Usage

``` r
fx_date_seq_lumpy(from_date, to_date, lump_decades = FALSE)
```

## Arguments

- from_date:

  A date or character string in "YYYY-MM-DD" format.

- to_date:

  A date or character string in "YYYY-MM-DD" format.

- lump_decades:

  Should decades be lumped using `199X` format?

## Value

A character vector of separated date ranges, sorted chronologically. The
vector contains strings representing full years ("YYYY"), full months
("YYYY-MM"), and individual days ("YYYY-MM-DD") as needed.

## Examples

``` r
fx_date_seq_lumpy("2022-11-30", "2024-02-01")
#> [1] "2022-11-30" "2022-12"    "2023"       "2024-01"    "2024-02-01"
```
