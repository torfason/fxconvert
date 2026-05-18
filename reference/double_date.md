# Convert an Object to a Double-Preserved Date

This function converts an object to a numeric double representation
while preserving the "Date" class. It first unclasses the input,
converts it to `double`, and then reassigns the `"Date"` class.

## Usage

``` r
double_date(x)
```

## Arguments

- x:

  An object that can be coerced into a date-like numeric format.

## Value

A `Date` object stored as a `double`.

## Details

Used to ensure that dates read using `nanoparquet` are identical to
dates read from the same file using `arrow`

## Examples

``` r
  d <- 10957L
  class(d) <- "Date" # d is now "2000-01-01"
  double_date(d)  # Returns the same date but stored as a double
#> [1] "2000-01-01"
```
