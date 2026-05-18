# Fill Missing Values Within Observed Data Range

Fills `NA` values in a vector within the index range of observed
(non-missing) values, without extending beyond the last non-missing
value.

## Usage

``` r
fx_vec_fill_gaps(
  x,
  direction = c("down", "up", "downup", "updown"),
  max_fill = NULL
)
```

## Arguments

- x:

  An atomic vector containing `NA` values to be filled.

- direction:

  A string indicating the fill direction. Must be one of:

  - `"down"` (default): Fill missing values downward.

  - `"up"`: Fill missing values upward.

  - `"downup"`: Fill first down, then up.

  - `"updown"`: Fill first up, then down.

- max_fill:

  A single positive integer specifying the maximum number of sequential
  missing values that will be filled. If NULL, there is no limit.

## Value

A vector of the same type as `x`, with missing values filled within the
index range of observed (non-missing) values

## Details

The function identifies the first and last non-missing values in `x` and
only fills missing values within this index range. Any values beyond the
last non-missing value remain `NA`.

This function is useful when you need to fill missing values but want to
avoid extending beyond known data points. Internally it uses
[`vctrs::vec_fill_missing()`](https://vctrs.r-lib.org/reference/vec_fill_missing.html)
to do the filling.

The `"downup"` and `"updown"` approaches are provided strictly for
compatibility with
[`vctrs::vec_fill_missing()`](https://vctrs.r-lib.org/reference/vec_fill_missing.html).
In practice, the result will always be exactly equal to just using
`"up"` or `"down"`, respectively.

## Examples

``` r
x1 <- c(NA, NA, 1, NA, NA, 2, NA, NA)
fx_vec_fill_gaps(x1)
#> [1] NA NA  1  1  1  2 NA NA
fx_vec_fill_gaps(x1, direction = "up")
#> [1] NA NA  1  2  2  2 NA NA

x2 <- c(NA, NA, 1, NA, NA)
fx_vec_fill_gaps(x2)  # Unchanged (single element means no gaps)
#> [1] NA NA  1 NA NA

x3 <- c(NA, NA, NA, NA, NA)
fx_vec_fill_gaps(x3)  # Unchanged (all NA)
#> [1] NA NA NA NA NA
```
