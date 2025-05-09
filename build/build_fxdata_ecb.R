
message("╭ Retrieving and writing ECB data!")

# ECB shares historical rates every day around 16:00 CET on the following URL:
ecb_url <- "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"

# We use gitpins to get a fresh version every day, using gitpins::gp_dropper()
# to target the specific time a new version drops.

# Log info about timing
refresh_hours <- gp_dropper(16, drop_tz = "Europe/Berlin")
cat("Drop Target:  ", "16:00, Europe/Berlin\n")
cat("Current Date: ", Sys.time() |> format("%a, %d %b %Y %X %Z"), "\n")
cat("Refresh Hours:", refresh_hours, "\n")

# Do the actual fetch, targeting a drop at 16:00 Berlin
input_file_ecb <- pin(ecb_url, refresh_hours = refresh_hours) |>
  pkgcond::suppress_messages("pin\\(\\) found recent version, using it ...")

# Read ecb data into a tibble, cut last empty column, rename all variables and
# add a year column
d.org <- read_csv(input_file_ecb, na = c("", "N/A"),
                  name_repair = "unique_quiet",
                  col_types = cols( Date = col_date(format = ""),
                                    .default = col_double(),
                                    ...43 = col_logical()))
d.tidy <- d.org |>
  select(-...43) |>
  rename_with(to_snake_case) |>
  rename(fxdate = date) |>
  mutate(eur = 1, .after = fxdate)

# We need each date to be represented, regardless of whether any FX entries exist
last_date_available  <- max(d.tidy$fxdate)
first_date_available <- min(d.tidy$fxdate)
if (first_date_available == ymd("1999-01-04")) {
  # The first days of 1999 were in fact only a weekend so they should be NA-filled
  first_date_available <- ymd("1999-01-01")
}
d <- fxdata_fill(d.tidy, first_date_available, last_date_available)

# Set folder for the generated parquet files
fxdata_folder <- here("..", "fxdata")
bank <- "ecb"
ecb_parquet_files_old <- fs::dir_ls(here(fxdata_folder, bank))

# And adding a version column
ecb_parquet_files_new <- fxdata_write_lumpy_parquet(d, fxdata_folder, bank)
fxdata_write_metadata_json(d, here("..", "fxdata"), bank = bank, quotation_method = "indirect") |>
  read_lines() |> str_view() # |> cat(sep = "\n")

# Write out using new approach (in the old dir), in order to have both versions
ecb_parquet_files_new_better_lumps <- fxdata_write_lumpy_parquet_new(d, fxdata_folder, bank, version = 2L)
fxdata_write_metadata_json(d, fxdata_folder, bank = bank, quotation_method = "indirect", new_name_order = TRUE) |>
  read_lines() |> str_view() # |> cat(sep = "\n")

# Write out using new approach (in the new dir)
fxdata_folder_new <- here("..", "fxdata_new_zstd")
ecb_parquet_files_new_better_lumps <- fxdata_write_lumpy_parquet_new(d, fxdata_folder_new, bank, version = 2L, compression = "zstd")
fxdata_write_metadata_json(d, fxdata_folder_new, bank = bank, quotation_method = "indirect", new_name_order = TRUE) |>
  read_lines() |> str_view() # |> cat(sep = "\n")

# Old files that are no longer relevant must be deleted manually
for_deletion <- setdiff(ecb_parquet_files_old |> fs::path_file(),
                        ecb_parquet_files_new |> fs::path_file())
if (length(for_deletion) > 0) {
  cat("Found old parquet files that should be deleted:\n")
  cat(for_deletion, sep = "\n")
}

message("╰ Finished retrieving and writing ECB data!")
