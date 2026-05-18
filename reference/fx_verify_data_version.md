# Verify and clean FX data versioning

Checks the `.version.` column of an FX dataset. Issues a warning if data
is unversioned or contains multiple versions. Removes the `.version.`
column before returning the data.

## Usage

``` r
fx_verify_data_version(d)
```

## Arguments

- d:

  A data frame with a `.version.` column.

## Value

The input data frame with the `.version.` column removed.
