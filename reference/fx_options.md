# Create an fx_options Object

Constructs an `fx_options` object with configurable parameters. The
parameters can be set from the defaults, by

## Usage

``` r
fx_options(
  ...,
  workspace = getOption("fxconvert.workspace", "main"),
  bank = getOption("fxconvert.bank", "ecb"),
  repo = getOption("fxconvert.repo", "torfason/fxdata")
)
```

## Arguments

- ...:

  Reserved. All arguments must be named.

- workspace:

  A character string specifying the local workspace to use for
  operations.

- bank:

  A character string specifying which central bank rates to base the
  conversions on.

- repo:

  A character string specifying the remote repository source for
  exchange rate data.

## Value

A list representing the `fx_options` object.
