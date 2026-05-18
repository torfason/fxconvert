# Read and Combine Multiple Parquet Files Using nanoparquet

Reads one or more Parquet files from specified directories, glob
patterns, or file paths using
[`nanoparquet::read_parquet()`](https://nanoparquet.r-lib.org/reference/read_parquet.html),
and combines them into a single data frame using
[`dplyr::bind_rows()`](https://dplyr.tidyverse.org/reference/bind_rows.html).

## Usage

``` r
read_parquet_multi(path, ..., add_file_column = FALSE)
```

## Arguments

- path:

  A directory or glob pattern

- ...:

  Additional arguments passed to
  [`nanoparquet::read_parquet()`](https://nanoparquet.r-lib.org/reference/read_parquet.html).

## Value

A single data frame containing the rows from all matched Parquet files.
A `.file` column is added to indicate the source file of each row.

## Examples

``` r
if (FALSE) { # \dontrun{
  df <- read_nanoparquet_multi(c("data/", "logs/*.parquet"))
} # }
```
