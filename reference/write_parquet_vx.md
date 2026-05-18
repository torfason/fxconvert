# Writes and verifies parquet file with maximum compression

For ma**X**imum compression, the function writes
`length(compression_types)` versions of parquet file with maximum
compression options for each compression type to a temporary location,
before moving the maximally compressed file to the target file.

For **V**erification, the function checks if the target file exists, if
it does it is read to verify that its contents equal `x` (a mismatch is
an error). If the target file does not exist, it is written (see above)
and `TRUE` returned , then re-read to ensure a match (again, a mismatch
is an error).

The function returns `TRUE` if the file is written, and `FALSE` if a
matching file already exists.

## Usage

``` r
write_parquet_vx(
  x,
  file,
  ...,
  compression_types = c("gzip", "snappy", "uncompressed"),
  verbose = FALSE
)
```

## Arguments

- x:

  `tibble` with data to write to file.

- file:

  `string` with path name to write.

- compression_types:

  `character` with list of compression methods to try.

- verbose:

  `flag` determining verbosity level

## Value

`TRUE` if data was written to file, `FALSE` if a file existed that
contained the exact same data as `x` (if a file with different data was
found, an error is thrown).
