# Assert that no dots arguments are passed

This is an alias for
[`rlang::check_dots_empty()`](https://rlang.r-lib.org/reference/check_dots_empty.html),
for consistency with other arguments. The function throws an error if
any unnamed parameters were passed to the function where this is called.

## Usage

``` r
assert_dots_empty(
  env = caller_env(),
  error = NULL,
  call = caller_env(),
  action = abort
)
```
