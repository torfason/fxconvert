# Initialize Foreign Exchange (FX) Data

This function initializes and refreshes the local foreign exchange (FX)
data directory by downloading the necessary data files from the
specified remote server. The function allows different approaches for
data management, including incremental updates, full re-initialization,
and local refreshes.

## Usage

``` r
fx_init(
  ...,
  banks = c("ecb", "cbi", "fed", "xfed"),
  action = c("auto", "update", "offline", "full", "remove"),
  verbose = TRUE,
  once = FALSE,
  mirai = FALSE
)
```

## Arguments

- ...:

  Reserved. All arguments must be named.

- banks:

  Character string specifying the source of FX data. Currently, `"ecb"`,
  `"cbi"`, and `"fed"` are supported.

- action:

  String specifying which initialization action to take. One of `auto`,
  `update`, `offline`, `full`, `remove`. See details. Replaces
  `approach`.

- verbose:

  Logical indicating whether to print progress messages. Defaults to
  `TRUE`.

- once:

  Logical indicating whether to only perform actual initialization once
  per R session. Defaults to `FALSE`.

- mirai:

  Should data fetching use `mirai` for parallel processing.

## Details

This function ensures that the local FX data store is up to date by:

- Checking available data ranges on the remote server.

- Identifying missing data files for download.

- Removing outdated local files.

- Loading data into a DuckDB database for efficient querying.

The approach to updating data is determined by the `approach` parameter.
This can be currently set to four different options:

- `"incremental"`: Downloads missing data while keeping existing files.

- `"fresh"`: Deletes all local data and downloads everything anew.

- `"local_refresh"`: Rebuilds the local database without downloading new
  data.

- `"remove"`: Deletes all local data and exits without downloading new
  data.

The different approaches, or actions, are subject to change. In
particular they should support offline use with appropriate warnings.
Potential actions:

- `auto` (equivalent to `once`=TRUE, only updates if it has not been
  updated in this session (but must ensure sitrep is OK))

- `update` (equivalent to older `incremental`, warns on offline, error
  if no data)

- `offline` (avoid internet access, no warn on offline unless data is
  missing)

- `full` (remove and update rolled into one)

- `remove` (deletes all local data and exits without downloading new
  data.)

TODO: This should take an options object and also
