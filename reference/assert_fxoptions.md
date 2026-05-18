# Verify that x is a valid fx_options object

Verifies that `x` is an `fx_options` object (of class
`fxconvert_fx_options`) and that all elements of the object are valid
for such an object. Use
[`fx_options()`](https://torfason.github.io/fxconvert/reference/fx_options.md)
to create `fx_options` objects.

## Usage

``` r
assert_fxoptions(x)
```

## Arguments

- x:

  An object to verify

## Value

Unchanged input if valid, otherwise an error is thrown.
