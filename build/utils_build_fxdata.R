
# This file defines utility functions that are useful for retrieving FX data
# from various central banks. Documentation follows roxygene style loosely for
# convenience, even though these functions are not part of a package


# Glue interpolation vectors in pipes
#
# Applies `glue::glue()` to each element of a character vector using a template
# string, enabling pipe-friendly, element-wise interpolation. Useful when the
# vector to process is not encapsulated in a `data.frame` or other
# environment-like object.
#
# @param . A character vector to be interpolated.
# @param template A glue template string. Use `{.}` to refer to the default
#   (unnamed) vector variable, or the names of any other variables accessible
#   in the relevant environment. Variables are recycled using tidyverse
#   recycling rules.
# @param ... Additional arguments passed to `glue::glue()`.
#
# @return A character vector with interpolated values. The length is determined
#   by tidyverse recycling rules for all referenced variables.
#
# @examples
#   letters |> glue_vector("Letters include {.} and {LETTERS}")
glue_vector <- function(., template = "{.}", ...,
      .sep = "", .envir = parent.frame(), .open = "{", .close = "}",
      .na = "NA", .null = character(), .comment = "#", .literal = FALSE,
      .transformer = glue::identity_transformer, .trim = TRUE) {
  zmisc::assert_dots_empty()
  d.point <- tibble::tibble(. = .)
  glue::glue_data(template, .x = d.point,
          .sep = .sep, .envir = .envir, .open = .open, .close = .close,
          .na = .na, .null = .null, .comment = .comment, .literal = .literal,
          .transformer = .transformer, .trim = .trim)
}


# Fill missing dates in fxdata tibble
#
# Takes a wide fxdata tibble that may be missing lines for some dates and
# returns a version where every date between first_date and last_date is
# represented.
fxdata_fill <- function(d, first_date, last_date) {
  d.date_filler <- tibble(fxdate = seq.Date(first_date_available, last_date_available, by = "day"))
  result <- left_join(d.date_filler, d, by = join_by(fxdate)) |>
    arrange(fxdate)
  result
}


# Write json metadata for an fxdata tibble
#
# Writes  metadata for d, a *date-filled* fxdata tibble of correct wide format
# to a json file.
#
# @examples
#   fxdata_write_metadata_json(d, here("..", "fxdata"), "ecb", "indirect, TRUE)
fxdata_write_metadata_json <- function(d, fxdata_folder, bank, quotation_method, new_name_order = FALSE) {

  # Determine the range of available data, output before writing to json
  l.ranges_for_json <- d |>
    summarise(first_date_available = min(fxdate), last_date_available=max(fxdate)) |>
    mutate_all(as.character) |>
    as.vector() # Strip class and attributes
  l.ranges_for_json |>
    with(paste0("Date Range: ", first_date_available, " -- ", last_date_available, "\n")) |>
    cat()

  # Add additional fields to json
  l.ranges_for_json$quotation_method = quotation_method

  # Output metadata (only first and last dates available for now)
  if (new_name_order) {
    metadata_json_file <- fs::path(fxdata_folder, str_glue("meta_{bank}.json"))
  } else {
    metadata_json_file <- fs::path(fxdata_folder, str_glue("{bank}_meta.json"))
  }
  l.ranges_for_json |>
    unbox_deep() |>
    jsonlite::write_json(metadata_json_file, pretty = TRUE, null = "null")

  metadata_json_file
}


# Write fxdata tibble to a set of lumpy parquet files
#
# Writes `d`, a *date-filled* `tibble` with FX data of correct format in a
# lumpy parquet format, with the name of the bank given in bank.
#
# NOTE: This original version relies on a hard-coded approach to lumping the
# underlying dates, only lumping months and years.
#
# @examples
#   fxdata_write_lumpy_parquet(d, here("..", "fxdata"), "ecb")
fxdata_write_lumpy_parquet <- function(d, fxdata_folder, bank) {

  fxdata_bank_folder <- file.path(fxdata_folder, bank)

  if (!fs::dir_exists(fxdata_bank_folder))
    stop("Bank directory missing:\n", fxdata_bank_folder)

  # Ensure all old files are identical before writing new ones
  bank_parquet_files_old <- list.files(fxdata_bank_folder) |>
    str_subset(str_glue("^{bank}_.*\\.parquet"))

  # Separate into immutable ranges and write out files to each range
  lumpy_date_range <- fx_date_seq_lumpy(min(d$fxdate), max(d$fxdate))

  # Some initialization before entering the loop, for parquet and speed
  d <- d |> mutate(fxdate_char = format(d$fxdate, "%Y-%m-%d"), .before = 0) # Optimization to not format date to string inside the loop (idempotent so OK to assign to same frame)
  pq_options = parquet_options(class = c("tbl_df", "tbl"))                  # Parquet files should be read as tibbles
  written_file_count <- 0                                                   # Loop variable initialization
  filenames <- character()                                                  # Collect written files into a vector

  # Loop through the date range, check if each lump exists, and write it if not
  for (single_range in lumpy_date_range) {

    # Construct the correct file name
    cat(single_range, "...")
    filename <- file.path(fxdata_bank_folder, paste0(bank, "_", single_range, ".parquet"))
    filenames <- c(filenames, filename)

    # Prepare the exact tibble to write for this range
    d.cur_range <- d |>
      filter(str_detect(fxdate_char, paste0("^", single_range))) |>
      select(-fxdate_char)

    if (fs::file_exists(filename)) {

      # File exists, parquet internals may differ, but data should match
      # Use double_date() to ensure dates use double rather than integer internally
      d.from_file <- nanoparquet::read_parquet(filename, options = pq_options) |>
        mutate(fxdate = fxdate |> double_date())
      waldo_result <- waldo::compare(d.from_file, d.cur_range)
      if (length(waldo_result) > 0) {
        print(waldo_result)
        stop("Different data in old and new snapshots")
      }
      cat(" found file ...\r")

    } else {

      # New file, write it out! (using \n so the output gets preserved)
      d.cur_range |> write_parquet(filename)
      written_file_count <- written_file_count + 1
      cat(" wrote new file ...\n")

    }
  }

  # Report number of written files, with a side effect of
  # overwriting any transient messages from the loop.
  cat("Wrote:", written_file_count, "parquet files            \n")

  # Return a vector of the file names that were written to
  filenames
}


# Write fxdata tibble to a set of lumpy parquet files
#
# Writes d, a *date-filled* fxdata tibble of correct format in a
# lumpy parquet format, with the name of the bank given in bank.
#
# NOTE: This updated version utilizes an improved approach to lumping the dates,
# which is faster and more configurable (can lump decadays, months, years,
# decades, centuries and millennia). It also takes a `version` parameter that it
# embeds into the parquet files to ensure that data version mismatch can be
# reliably detected, as well as a `compression` parameter to allow compression
# to be set from outside the function.
#
# @examples
#   fxdata_write_lumpy_parquet(d, here("..", "fxdata"), "ecb")
fxdata_write_lumpy_parquet_new <- function(d, fxdata_folder, bank, version, compression = "gzip") {

  # Verify inputs, ensure version is an integer
  assert_tibble(d)
  assert_string(fxdata_folder)
  assert_string(bank)
  qassert(version, "I1")   # typeof() == "integer" and length == 1

  # Determine fxdata bank folder and create if it does not exist
  fxdata_bank_folder <- file.path(fxdata_folder, bank)
  if (!fs::dir_exists(fxdata_bank_folder)) {
    cli::cli_warn("Bank directory missing, attempting to create it (recursively): {fxdata_bank_folder}")
    fs::dir_create(fxdata_bank_folder)
  }

  # Ensure all old files are identical before writing new ones
  bank_parquet_files_old <- list.files(fxdata_bank_folder) |>
    str_subset(str_glue("^{bank}_.*\\.parquet"))

  # Separate into immutable ranges and write out files to each range
  lumpy_date_range <- fx_date_seq_lumpy(min(d$fxdate), max(d$fxdate))
  lumpy_date_range <- fx_lump_dates(d$fxdate, lump_from = "year", lump_to = "month")
  lumpy_date_range <- fx_lump_dates(d$fxdate, lump_from = "millennium", lump_to = "decaday")

  # Prepare tibble to loop through, after adding a version column
  d.nest <- d |>
    mutate(.version. = version, .after = fxdate) |>
    mutate(fxdate_lump = lumpy_date_range, .before = 0) |>
    tidyr::nest(.by = fxdate_lump) |>
    mutate(filename = file.path(fxdata_bank_folder, paste0(bank, "_", fxdate_lump, ".parquet")))

  # Some initialization before entering the loop, for parquet and speed
  pq_options = parquet_options(
    class = c("tbl_df", "tbl"),  # Parquet files should be read as tibbles
    compression_level = zmisc::recode_tilde(compression, "gzip" ~ 9, "zstd" ~ 22, .default = NA))       # They should be maximally compressed
  written_file_count <- 0        # Loop variable initialization
  version_ignored_count <- 0 # Any files where mismatching version was ignored

  ## NEW APPROACH HERE
  # Loop through the date range, check if each lump exists, and write it if not
  for (i in seq2(1, nrow(d.nest))) {

    # Prepare the exact tibble to write for this range
    d.cur_range <- d.nest$data[[i]]
    cur_filename <- d.nest$filename[i]
    cur_lump <- d.nest$fxdate_lump[i]

    # Output current lump
    cat(cur_lump, "...")

    if (fs::file_exists(cur_filename)) {

      # File exists, parquet internals may differ, but data should match
      # Use double_date() to ensure dates use double rather than integer internally
      d.from_file <- nanoparquet::read_parquet(cur_filename, options = pq_options) |>
        mutate(fxdate = fxdate |> double_date())

      # Ignore version mismatches for now
      if (is.null(d.from_file$.version.)) {
        d.from_file <- d.from_file |> mutate(.version. = version, .after = fxdate)
        version_ignored_count <- version_ignored_count + 1
      }

      # Compare the processed tibbles
      waldo_result <- waldo::compare(d.from_file, d.cur_range)
      if (length(waldo_result) > 0) {
        print(waldo_result)
        cli::cli_abort("Different data in old and new snapshots")
      }
      cat(" found file ...\r")

    } else {

      # New file, write it out! (using \n so the output gets preserved)
      d.cur_range |> write_parquet(cur_filename, compression = compression, options = pq_options)
      written_file_count <- written_file_count + 1
      cat(" wrote new file ...\n")

    }
  }

  # Report number of written files, with a side effect of
  # overwriting any transient messages from the loop.
  cat("Wrote:", written_file_count, "parquet files            \n")
  cat("Ignored:", version_ignored_count, "version mismatches \n")

  # Return a vector of the file names that were written to (or skipped if existing)
  d.nest$filename
}


# Write fxdata tibble to a set of lumpy parquet files
#
# Writes d, a *date-filled* fxdata tibble of correct format in a
# lumpy parquet format, with the name of the bank given in bank.
#
# NOTE, this version:
#   - utilizes the improved (decaday) approach to lumping the dates,
#   - takes the `version` parameter
#   - takes a `compression_types` parameter that specifies which compression
#     types it should try
#
# @examples
#   fxdata_write_lumpy_parquet(d, here("..", "fxdata"), "ecb")
fxdata_write_lumpy_parquet_autocomp <- function(d, fxdata_folder, bank, version,
                  compression_types = c("gzip", "snappy", "uncompressed")) {

  # Verify inputs, ensure version is an integer
  assert_tibble(d)
  assert_string(fxdata_folder)
  assert_string(bank)
  assert_inumber(version)   # typeof() == "integer" and length == 1
  assert_character(compression_types)

  # Determine fxdata bank folder and create if it does not exist
  fxdata_bank_folder <- file.path(fxdata_folder, bank)
  if (!fs::dir_exists(fxdata_bank_folder)) {
    cli::cli_warn("Bank directory missing, attempting to create it (recursively): {fxdata_bank_folder}")
    fs::dir_create(fxdata_bank_folder)
  }

  # Separate into immutable ranges and write out files to each range
  lumpy_date_range <- fx_lump_dates(d$fxdate, lump_from = "millennium", lump_to = "decaday")

  # Prepare tibble to loop through, after adding a version column
  d.nest <- d |>
    mutate(.version. = version, .after = fxdate) |>
    mutate(fxdate_lump = lumpy_date_range, .before = 0) |>
    tidyr::nest(.by = fxdate_lump) |>
    mutate(filename = file.path(fxdata_bank_folder, paste0(bank, "_", fxdate_lump, ".parquet")))


  ## NEW APPROACH HERE
  # Loop through the date range, check if each lump exists, and write it if not
  written_file_count <- 0
  for (i in seq2(1, nrow(d.nest))) {

    # Prepare the exact tibble to write for this range
    d.cur_range <- d.nest$data[[i]]
    cur_filename <- d.nest$filename[i]
    cur_lump <- d.nest$fxdate_lump[i]

    # Output current lump
    cat(cur_lump, "...")

    # Write (or skip) file
    wrote_file <- write_parquet_vx(d.cur_range, cur_filename, verbose = TRUE)
    if (wrote_file) {
      # New file, note it! (using \n so the output gets preserved)
      written_file_count <- written_file_count + 1
      cat(" wrote new file ...\n")
    } else {
      cat(" found file ...\r")
    }
  }

  # Report number of written files, with a side effect of
  # overwriting any transient messages from the loop.
  cat("Wrote:", written_file_count, "parquet files            \n")
  #cat("Ignored:", version_ignored_count, "version mismatches \n")

  # Return a vector of the file names that were written to (or skipped if existing)
  d.nest$filename
}

# Helper to list obsolete parquet files, using the json metadata and
# fx_lump_dates() to determine which files should exist and compare it
# against the files that are actually found in the given fxdata_folder+bank
fxdata_list_obsolete_files <- function(fxdata_folder, bank) {

  # Read json to determine available range
  l.meta <- jsonlite::read_json(fs::path(fxdata_folder, glue("meta_{bank}.json")))

  # Determine which files exist
  files_found <-
    fs::dir_ls(here(fxdata_folder, bank)) |>
    fs::path_file()

  # Determine which files should exist
  files_expected <-
    fx_date_seq(l.meta$first_date_available, l.meta$last_date_available) |>
    fx_lump_dates() |>
    unique() |>
    glue_vector("{bank}_{.}.parquet") |>
    unclass()

  # Determine which files are missing
  files_missing  <- setdiff(files_expected, files_found)
  if (length(files_missing) > 0) {
    cli::cli_abort(c("Some parquet files are missing for {bank}:", "{files_missing}"))
  }

  # Determine which files are obsolete
  files_obsolete <- setdiff(files_found, files_expected)

  # And return ...
  files_obsolete
}


# Display macOS Notification and Alert
#
# Triggers a macOS notification and an alert dialog box sequentially to notify
# the user with a specified message and title. This function is designed to
# work on macOS systems and utilizes AppleScript via the `osascript` command.
#
# @param title The title for the notification and alert dialog. This should
#   be a concise string summarizing the nature of the alert.
# @param message The message content for the notification and alert dialog.
#   This provides additional details about the alert.
#
# @examples
#   mac_alert("This is an alert title", "And here comes the message")
mac_alert <- function(title = "Mac Alert from R", message = "") {

  # AppleScript command for notification
  notificationCmd <- sprintf('display notification "%s" with title "%s"', message, title)

  # AppleScript command for alert dialog
  alertCmd <- sprintf('display alert "%s" message "%s"', title, message)

  # Combine commands to execute sequentially
  fullCmd <- sprintf("osascript -e '%s' -e '%s'", notificationCmd, alertCmd)

  # Do some beeping
  beepr::beep()
  alarm()

  # Execute the command
  system(fullCmd)
}


# Apply a Function Recursively to Each Non-List Element of a List
#
# This function extends `lapply` by allowing a function to be applied recursively
# to each non-list element within a nested list structure. It is useful for
# performing operations on deeply nested lists where you want to apply a
# transformation or operation to individual elements rather than to sub-lists.
#
# @param X A list; the nested list structure to which the function `FUN` will be
#   applied. `X` can contain elements of any type or mode, but `FUN` will only
#   be applied to non-list elements.
# @param FUN Function; the function to apply to each non-list element in `X`.
#   The function should take a single argument and return a value.
#
# @return A list of the same structure as `X`, with `FUN` applied to each non-list
# element.
#
# @examples
#   # Define a deeply nested list
#   nested_list <- list(a = 1, b = list(b1 = 2, b2 = list(b21 = 3)), c = 4)
#
#   # Use lapply_deep to increment each number by 1
#   incremented_list <- lapply_deep(nested_list, function(x) x + 1)
#
#   # Compare the original and the modified list
#   print(nested_list)
#   print(incremented_list)
lapply_deep <- function(X, FUN) {
  # Use lapply to iterate and apply the logic
  lapply(X, function(element) {
    # If the element is a list, recurse
    if (is.list(element)) {
      return(lapply_deep(element, FUN))
    } else {
      # Apply FUN to non-list elements
      return(FUN(element))
    }
  })
}


# Unbox Single-Element Vectors in Nested Lists
#
# `unbox_non_na` unboxes any single-length vector in a list that is not `NA`. '
# Excluding `NA`, ensures that the resulting json output is `[null]`, rather
# than `null`, whic results in `fromJSON()` and `read_json()` correctly
# reparsing the resulting json as `NA` rather than `NULL`. ' For other single
# element vectors, the output contains the bare value, resulting in json that
# more closely matches the general expectation of how json formatting
#
# `unbox_deep` extends this functionality to nested lists, applying
# `unbox_non_na` recursively to each element within a nested list structure. It
# ensures that single-element vectors are unboxed at all levels of the list,
# except for `NA` values.
#
# Both functions are particularly useful in the context of JSON serialization,
# where the distinction between single-element vectors and scalar values can
# impact the structure and readability of the resulting JSON. By unboxing
# single-element vectors, lists can be converted to a more JSON-friendly format,
# with `unbox_deep` allowing for this conversion to apply throughout a complex,
# nested list structure.
#
# @param l A list or nested list structure. For `unbox_deep`.
# @param x A vector to potentially unbox. For `unbox_non_na`.
#
# @return
# `unbox_non_na` returns the unboxed value if `x` is a single-element vector not
# containing `NA`. Otherwise, it returns `x` unchanged.
#
# `unbox_deep` returns a list of the same structure as `l`, with all applicable
# elements unboxed.
#
# @examples
#   # Using unbox_non_na on a single-element vector
#   unbox_non_na(c(42))
#
#   # Applying unbox_deep to a nested list
#   nested_list <- list(a = 1, b = list(b1 = c(2), b2 = list(b21 = c(NA), b22 = c(3))), c = c(4))
#   unbox_deep(nested_list)
unbox_deep <- function(l) {
  lapply_deep(l, unbox_non_na)
}


# Unbox a Single-Length Non-NA Vector
#
# This function checks if `x` is a single-length vector that is not `NA`.
# If so, it applies `jsonlite::unbox()` to ensure it is treated as a scalar
# when converted to JSON. Otherwise, it returns `x` unchanged.
#
# @param x A vector of any type.
# @return If `x` has length 1 and is not `NA`, it returns an unboxed version
#   of `x` using `jsonlite::unbox()`. Otherwise, it returns `x` unchanged.
#
# @examples
# unbox_non_na(5)        # Returns an unboxed numeric value
# unbox_non_na("text")   # Returns an unboxed character string
# unbox_non_na(NA)       # Returns NA as is
# unbox_non_na(c(1, 2))  # Returns c(1, 2) unchanged
#
# @noRd
unbox_non_na <- function(x) {
  if (length(x) == 1 && !is.na(x)) {
    return(jsonlite::unbox(x))
  } else {
    return(x)
  }
}


# Convert an Object to a Double-Preserved Date
#
# This function converts an object to a numeric double representation
# while preserving the "Date" class. It first unclasses the input,
# converts it to `double`, and then reassigns the `"Date"` class.
#
# Used to ensure that dates read using `nanoparquet` are identical to
# dates read from the same file using `arrow`
#
# @param x An object that can be coerced into a date-like numeric format.
# @return A `Date` object stored as a `double`.
#
# @examples
#   d <- 10957L
#   class(d) <- "Date" # d is now "2000-01-01"
#   double_date(d)  # Returns the same date but stored as a double
double_date <- function(x) {
  x <- x |> unclass() |> as.double()
  class(x) <- "Date"
  x
}

