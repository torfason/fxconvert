# Generate a lumpy sequence of dates with a recursive approach

The function lumps dates into common eras, with eras of different
length, ranging from `millennium` to `decaday`. A `decaday` is the set
of days in a month that have a common number in the tens-place (1-9,
10-19, 20-29, 30-31).

Allowed lump eras are: `millennium`, `century`, `decade`, `year`,
`month`, and `decaday`.

It calls a non-exported function to do the actual recursion, after doing
some of the more expensive input checking only on the initial input.

## Usage

``` r
fx_lump_dates(dates, lump_from = "millennium", lump_to = "decaday")
```

## Arguments

- dates:

  Strictly increasing date sequence to lump

- lump_from:

  Largest era unit to lump

- lump_to:

  Smallest era unit to lump

## Value

A character vector of date ranges.
