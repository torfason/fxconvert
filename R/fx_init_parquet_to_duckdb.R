
# Process previously downloaded parquet files and write to duckdb.
#
# Intended to provide better parallel operation than prior approaches.
#
#   - parquet_dir indicates where to look for parquet files
#   - duckdb_dir indicates where to write the duckdb files
#
# It works well (and is generally expected) for these to be the same directory.
# Previously existing duckdb files will be overwritten. Other files in the
# two directories should not be touched. No checks are performed beforehand,
# if this function is called, a completely fresh set of duckdb files will
# always be written.
#
# parquet_dir must contain:
#   ./meta_{bank}.json
#   ./{bank}/{lumpydate}.parquet
# After running, duckdb_dir shall contain for each of {banks}
#   ./{bank}.duckdb
#
# @param parquet_dir Directory where previously downloaded json and parquet
#   files can be found (according to the fxdata format)
# @param duckdb_dir Directory where duckdb files will be written
# @param banks A list of banks to process
# @param verbose Should verbose logging be shown as data is downloaded
# @return invisible(NULL)
fx_init_write_duckdb_single_bank <- function(
        parquet_dir, duckdb_dir, bank,
        verbose = FALSE,
        write_extra_tables = FALSE) {

  # Prepare known urls and directories, fs::dir_create() ensures all dirs exist
  bank_parquet_dir  <- fs::path(parquet_dir, bank)
  bank_duckdb_file  <- fs::path(duckdb_dir, glue("{bank}.duckdb"))

  # Roll-your-own log levels for now
  if (verbose) {
    xcat <- base::cat
    xprint <- base::print
  } else {
    xcat <- function(x, ...) {invisible(x)}
    xprint <- function(x, ...) {invisible(x)}
  }

# TODO: Should fx_duck_local perhaps only handle readonly connections and we hardcode the write handling
#       But also, what if we do the write in a separate process and there are open read-write connections
#       in the main process (even with the current handling, the mirai process would not correctly close
#       readwrite connections in the main pool). Also, it is now clear that a pool is overkill, only
#       one connection per db is ever needed from the main process.
# Get connection, fx_duck_local() ensures that connection is freed right after function exits


  # Read and sanity check data
  d.fxdata.org <- read_parquet_multi(bank_parquet_dir)
  if (nrow(d.fxdata.org) < 1) {
    cli::cli_abort("No rows in data read from parquet files. Clear local data and retry.")
  } else if (length(unique(d.fxdata.org$.version.)) != 1 ) {
    cli::cli_abort("More than one version in data. Clear local data and retry.")
  } else {
    xcat(glue("fxdata version: {unique(d.fxdata.org$.version.)}\n\n"))
  }

  # Prepare table, must arrange by c(currency, fxdate) so asof works correctly
  d.fxdata.long.cur.date <- d.fxdata.org |>
    dplyr::select(-".version.") |>
    tidyr::pivot_longer(-"fxdate", names_to = "currency", values_to = "rate") |>
    dplyr::filter(!is.na(.data$rate)) |>
    dplyr::arrange(.data$currency, .data$fxdate)

  # Prepare a new read-write connection and register a deferred cleanup
  if (fs::dir_exists(bank_duckdb_file)) {
    cli::cli_abort("Trying to initialize duckdb but path is a directory: \n{bank_duckdb_file}")
  } else if (fs::file_exists(bank_duckdb_file)) {
    fs::file_delete(bank_duckdb_file)
  }
  conn <- duckdb::dbConnect(duckdb::duckdb(bank_duckdb_file,
                                           read_only = FALSE,
                                           shared_home = FALSE))
  xcat("Creating read/write connection ...\n")
  withr::defer({
    duckdb::dbDisconnect(conn, shutdown = TRUE)
    xcat("Destroying read/write connection ...\n")
  })

  duckdb::dbWriteTable(conn, "fxtable_long_cur_date", d.fxdata.long.cur.date)

  if (write_extra_tables) {

    #Construct alternative formats for fxdata
    d.fxdata.wide <- d.fxdata.org |>
      dplyr::select(-".version.") |>
      dplyr::arrange(.data$fxdate)
    d.fxdata.long <- d.fxdata.wide |>
      tidyr::pivot_longer(-"fxdate", names_to = "currency", values_to = "rate") |>
      dplyr::filter(!is.na(.data$rate)) |>
      dplyr::arrange(.data$fxdate, .data$currency)
    d.fxdata.filled <- d.fxdata.wide |>
      dplyr::arrange(.data$fxdate) |>
      dplyr::mutate(dplyr::across(-"fxdate", ~ fx_vec_fill_gaps(.x)))

    # Construct alternative fxdata_long tables, each with explicit sorting
    d.fxdata.long.random   <- withr::with_seed(42, d.fxdata.long |> dplyr::slice_sample(n = nrow(d.fxdata.long)))
    d.fxdata.long.date.cur <- d.fxdata.long |> dplyr::arrange(.data$fxdate, .data$currency)
    d.fxdata.long.cur.date.old_version <- d.fxdata.long |> dplyr::arrange(.data$currency, .data$fxdate)

    # TODO: These multiple writes  (and multiple sorts above) seem like removing them could save quite a lot of time,
    # possibly making mirai even less needed

    # Write alternative versions to the database
    duckdb::dbWriteTable(conn, "fxtable", d.fxdata.wide)
    duckdb::dbWriteTable(conn, "fxtable_filled", d.fxdata.filled)
    duckdb::dbWriteTable(conn, "fxtable_long", d.fxdata.long)
    duckdb::dbWriteTable(conn, "fxtable_long_random", d.fxdata.long.random)
    duckdb::dbWriteTable(conn, "fxtable_long_date_cur", d.fxdata.long.date.cur)

    # Create table with primary key
    # (checkpoint statement is required to clear WAL)
    q.result <- db_execute(conn, glue("
          BEGIN TRANSACTION;
          CREATE TABLE fxtable_long_pk AS SELECT * FROM fxtable_long ORDER BY currency, fxdate;
          ALTER TABLE  fxtable_long_pk ADD PRIMARY KEY (currency, fxdate);
          COMMIT;
          CHECKPOINT;"))
  }

  # For now, return the long tibble for merging outside the function
  d.fxdata.long.cur.date
}


