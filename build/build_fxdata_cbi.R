
message("╭ Retrieving and writing CBI data!")

# See:
#   https://www.sedlabanki.is/hagtolur/xml-gogn/
# Approach
#   Using cbi_read_csv():
#     Pin series 7 and 9 from 1900-01-01, to most recent full year
#     Pin series 7 and 9 from start of current year
#   Row-bind all four data_frames
#   Widen using cbi_widen_mid()

# GroupID is 7 or 9
cbi_url <- function(GroupId, DagsFra, DagsTil = NULL, Type = "csv") {
  if (is.null(DagsTil)) {
    glue::glue("https://sedlabanki.is/xmltimeseries/Default.aspx?GroupID={GroupId}&Type={Type}&DagsFra={DagsFra}")
  } else {
    glue::glue("https://sedlabanki.is/xmltimeseries/Default.aspx?GroupID={GroupId}&Type={Type}&DagsFra={DagsFra}&DagsTil={DagsTil}")
  }
}

# The following CSV version is equivalent to the XML version, and much faster
cbi_read_csv <- function(filename) {
  csv_names <- c("GroupID", "SeriesName", "ID", "FameName", "Name", "Description", "Date", "Value")
  d.csv <- filename |>
    read_delim(delim  =";", col_names = csv_names, show_col_types = FALSE) |>
    select(-GroupID, -SeriesName) |>
    relocate(ID, Name, FameName)
  d.csv
}

# Read the XML version and convert to tibble
cbi_read_xml <- function(filename) {

  # Read the XML and extract the TimeSeries element list
  xml_data    <- read_xml(filename)
  time_series <- xml_find_all(xml_data, ".//TimeSeries")

  # Extract data for each element of the time_series
  d <- lapply(time_series, function(ts) {
    id <- xml_attr(ts, "ID")
    name <- xml_text(xml_find_first(ts, ".//Name"))
    description <- xml_text(xml_find_first(ts, ".//Description"))
    entries <- xml_find_all(ts, ".//Entry")

    # Extract date for each entry (i.e. date, amount, and specific descriptor)
    lapply(entries, function(entry) {
      date <- xml_text(xml_find_first(entry, ".//Date"))
      value <- xml_text(xml_find_first(entry, ".//Value"))
      fameName <- xml_text(xml_find_first(ts, ".//FameName"))
      tibble(ID = id, Name = name, FameName = fameName, Description = description, Date = date, Value = as.numeric(value))
    })
  }) |>
    unlist(recursive = FALSE) |>
    bind_rows() |>
    force()
  d
}

# Pivot a long-format CBI tibble, using only the mid-point rate in
# situations where there are more than one rate per date.
cbi_widen_mid <- function(d) {
  cur_major  =  c("isk", "eur", "usd", "gbp", "chf", "cny", "jpy", "cad", "xdr")
  # First active
  # Then, less established or not active
  # Finally, discontinued currencies when the euro started
  # But, major are relocated separately to the front
  cur_order  =  c("cad", "chf", "dkk", "eur", "gbp", "jpy", "nok", "sek", "usd",
                  "xdr", "aud", "bgn", "brl", "cny", "czk", "hkd", "huf", "ils",
                  "inr", "jmd", "krw", "kwd", "mxn", "ngn", "nzd", "pln", "sar",
                  "sgd", "srd", "thb", "try", "twd", "zar", "eek", "hrk", "ltl",
                  "lvl", "mtl", "rub", "vef", "ats", "bef", "dem", "esp", "fim",
                  "frf", "grd", "iep", "itl", "nlg", "pte")

  d |>
    mutate(fxdate   = Date |> str_remove(" .*") |> mdy(),
           currency = FameName |> str_remove("\\..*") |> tolower(),
           value    = Value,
           skmid    = str_detect(FameName, "\\.SKMI\\.S\\.D"),
           ovmid    = str_detect(FameName, "\\.OVMI\\.S\\.D") ) |>
    arrange(currency, fxdate) |>
    filter(skmid | ovmid) |>
    select(fxdate, currency, value) |>
    tidyr::pivot_wider(names_from = currency, values_from = value) |>
    arrange(fxdate) |>
    mutate(isk = 1, .after = fxdate) |>
    relocate(fxdate, any_of(cur_order)) |>
    relocate(fxdate, any_of(cur_major))
}

cbi_widen_mid_2 <- function(d) {
  cur_major  =  c("isk", "eur", "usd", "gbp", "chf", "cny", "jpy", "cad", "xdr")
  # First active
  # Then, less established or not active
  # Finally, discontinued currencies when the euro started
  # But, major are relocated separately to the front
  cur_order  =  c("cad", "chf", "dkk", "eur", "gbp", "jpy", "nok", "sek", "usd",
                  "xdr", "aud", "bgn", "brl", "cny", "czk", "hkd", "huf", "ils",
                  "inr", "jmd", "krw", "kwd", "mxn", "ngn", "nzd", "pln", "sar",
                  "sgd", "srd", "thb", "try", "twd", "zar", "eek", "hrk", "ltl",
                  "lvl", "mtl", "rub", "vef", "ats", "bef", "dem", "esp", "fim",
                  "frf", "grd", "iep", "itl", "nlg", "pte")

  d |>
    filter(RateType == "M") |> # Miðgengi / Central Rate
    mutate(fxdate   = Date |> str_remove(" .*") |> mdy(),
           currency = Symbol |> tolower(),
           value    = Value,
           .keep = "none") |>
    arrange(currency, fxdate) |>
    tidyr::pivot_wider(names_from = currency, values_from = value) |>
    arrange(fxdate) |>
    mutate(isk = 1, .after = fxdate) |>
    relocate(fxdate, any_of(c(cur_major, cur_order)))
}

d.handmade_metadata_cbi <- read_csv(here("build", "handmade_metadata_cbi.csv"),
                                  show_col_types = FALSE)

# - - - - - - - - - -
# Function definitions complete, the code below fetches from the CBI api
# - - - - - - - - - -

# Log info about timing, targeting a drop at 16:00 Reykjavik time zone
refresh_hours <- gp_dropper(16, drop_tz = "Atlantic/Reykjavik")
cat("Drop Target:  ", "16:00, Atlantic/Reykjavik\n")
cat("Current Date: ", Sys.time() |> format("%a, %d %b %Y %X %Z"), "\n")
cat("Refresh Hours:", refresh_hours, "\n")

# Fetch each of the four segments
d.7.1900_full <- cbi_url(7, "1900-01-01", "2024-12-31") |>
  pin(refresh_hours = Inf) |>
  pkgcond::suppress_messages("pin\\(\\) found recent version, using it ...") |>
  cbi_read_csv()
d.9.1900_full <- cbi_url(9, "1900-01-01", "2024-12-31") |>
  pin(refresh_hours = Inf) |>
  pkgcond::suppress_messages("pin\\(\\) found recent version, using it ...") |>
  cbi_read_csv()
d.7.current <- cbi_url(7, "2025-01-01") |>
  pin(refresh_hours = refresh_hours) |>
  pkgcond::suppress_messages("pin\\(\\) found recent version, using it ...") |>
  cbi_read_csv()
d.9.current <- cbi_url(9, "2025-01-01") |>
  pin(refresh_hours = refresh_hours) |>
  pkgcond::suppress_messages("pin\\(\\) found recent version, using it ...") |>
  cbi_read_csv()

# Bind rows together, adding GroupIDs to track where each line comes from
d.all.pre_join <- bind_rows(
  bind_rows(d.7.1900_full, d.7.current) |> mutate(GroupID = 7),
  bind_rows(d.9.1900_full, d.9.current) |> mutate(GroupID = 9) )

# Join
d.all <- inner_join(d.handmade_metadata_cbi, d.all.pre_join, by = join_by(ID), unmatched = "error")

# cbi_widen_mid_2() is an update due to changing data format at the source in spring 2025
d.all.wide <- d.all |> cbi_widen_mid_2()

# When we have a tidy (but wide) frame we use it for what comes next
d.tidy <- d.all.wide

# We need each date to be represented, regardless of whether any FX entries exist
last_date_available  <- max(d.tidy$fxdate)
first_date_available <- min(d.tidy$fxdate)

first_date_available = ymd("1980-01-01")
# last_date_available  = ymd("2024-12-31")
d <- fxdata_fill(d.tidy, first_date_available, last_date_available)
d

# Set folder for the generated parquet files
fxdata_folder <- here("..", "fxdata")
bank = "cbi"

cbi_parquet_files_old <- fs::dir_ls(here(fxdata_folder, bank))

# Write out using old approach
cbi_parquet_files_new <- fxdata_write_lumpy_parquet(d, fxdata_folder, bank = bank)
fxdata_write_metadata_json(d, fxdata_folder, bank = bank, quotation_method = "direct") |>
  read_lines() |> str_view() # |> cat(sep = "\n")

# Write out using new approach (in the new dir)
fxdata_folder_new <- here("..", "fxdata_new")
cbi_parquet_files_new_better_lumps <- fxdata_write_lumpy_parquet_new(d, fxdata_folder_new, bank, version = 2L)
fxdata_write_metadata_json(d, fxdata_folder_new, bank = bank, quotation_method = "direct", new_name_order = TRUE) |>
  read_lines() |> str_view() # |> cat(sep = "\n")

# Write out using new approach (in the old dir)
cbi_parquet_files_new_better_lumps <- fxdata_write_lumpy_parquet_new(d, fxdata_folder, bank, version = 2L)
fxdata_write_metadata_json(d, fxdata_folder, bank = bank, quotation_method = "direct", new_name_order = TRUE) |>
  read_lines() |> str_view() # |> cat(sep = "\n")

# Old files that are no longer relevant must be deleted manually
for_deletion <- setdiff(cbi_parquet_files_old |> fs::path_file(),
                        cbi_parquet_files_new |> fs::path_file())
if (length(for_deletion) > 0) {
  cat("Found old parquet files that should be deleted:\n")
  cat(for_deletion, sep = "\n")
}

message("╰ Finished retrieving and writing CBI data!")
