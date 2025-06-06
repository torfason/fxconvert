
#### Info & Examples ####

## Overview URL
# https://fred.stlouisfed.org/searchresults/?st=spot%20exchange%20rate

## Example Requests
# pin("https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXCAUS") |>
#   read_csv() |> slice(c(1:5, (n()-5):n()))
# pin("https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXMXUS&cosd=2025-01-01") |>
#   read_csv() |> slice(c(1:5, (n()-5):n()))
# pin("https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXMXUS&coed=2024-12-31") |>
#   read_csv() |> slice(c(1:5, (n()-5):n()))
# pin("https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXUSEU") |>
#   read_csv() |> slice(c(1:5, (n()-5):n()))
# pin("https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXUSEU") |>
#   system.time()
# pin("https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXUSEU&cosd=2025-01-01") |>
#   system.time()

## Example parameters
# id=DEXMXUS
# cosd=2020-03-14
# coed=2025-03-14
# fq=Daily
# fam=avg
# vintage_date=2025-03-22
# revision_date=2025-03-22

#### Retrieval Code ####

message("╭ Retrieving and writing FED data!")

fed_json_file <- here("..", "fxdata", "meta_fed.json")

if (fs::file_exists(fed_json_file)) {
  age_json <- fed_json_file |>
    jsonlite::read_json() |>
    pluck("last_date_available") |>
    ymd() |>
    {\(d) as.numeric(today() - d)}()
} else {
  age_json <- Inf
}

if (age_json < 11) {

  # If most recent FED rates are less than 11 days old, we skip retrieval,
  # because they only get updated sometime on Monday for the week before.
  glue("╰ Skipping FED retrieval, youngest rates are {age_json} days old!") |>
    message()

} else {

  # If we end up here, the rates are getting old so we retrieve the most up-to-date data
  glue("Proceeding with FED retrieval, youngest rates are {age_json} days old ...") |>
    message()

# Value to use for data that is generally not refreshed
# (set to 0 to force refresh of even such data)
refresh_inf = Inf

# List of currencies and the name of the FED series for each currency
# (This version is correct for SEK and SGD that were previously mixed up)
currency_list_as_csv <-
"currency_code,series_code,description
BRL,DEXBZUS,Brazilian Reals to USD
CAD,DEXCAUS,Canadian Dollar to USD
CNY,DEXCHUS,Chinese Yuan to USD
DKK,DEXDNUS,Danish Krone to USD
HKD,DEXHKUS,Hong Kong Dollar to USD
INR,DEXINUS,Indian Rupee to USD
JPY,DEXJPUS,Japanese Yen to USD
KRW,DEXKOUS,South Korean Won to USD
MYR,DEXMAUS,Malaysian Ringgit to USD
MXN,DEXMXUS,Mexican Peso to USD
NOK,DEXNOUS,Norwegian Krone to USD
SGD,DEXSIUS,Singapore Dollar to USD
ZAR,DEXSFUS,South African Rand to USD
SEK,DEXSDUS,Swedish Krona to USD
LKR,DEXSLUS,Sri Lankan Rupee to USD
CHF,DEXSZUS,Swiss Franc to USD
TWD,DEXTAUS,Taiwanese Dollar to USD
THB,DEXTHUS,Thai Baht to USD
AUD,DEXUSAL,USD to Australian Dollar
EUR,DEXUSEU,USD to Euro
NZD,DEXUSNZ,USD to New Zealand Dollar
GBP,DEXUSUK,USD to British Pound
VES,DEXVZUS,Venezuelan Bolivar to USD"

# Construct currency list and set direct/indirect column
d.currency_list <- read_csv(currency_list_as_csv, show_col_types = FALSE) |>
  mutate(quotation_method =
           ifelse(str_detect(description, "^USD"), "direct", "indirect")) |>
  arrange(quotation_method, currency_code)
d.currency_list

# Define lookup function for getting series code from currency code
lookup_series_code <- d.currency_list |>
  select(name = currency_code, value = series_code) |>
  lookuper()
#lookup_series_code(c("GBP","NZD"))

# Define function to retrieve, pin, and prepare a tibble for one currency,
# with the currency code as special column.
cur_tibble_long_from_pin <- function(cur) {
  Sys.sleep(runif(1, min = 0.1, max = 0.2))
  cur_series <- lookup_series_code(cur)
  d.old <- glue("https://fred.stlouisfed.org/graph/fredgraph.csv?id={cur_series}&coed=2024-12-31") |>
    pin(refresh_hours = refresh_inf) |> read_csv(show_col_types = FALSE) |>
    pkgcond::suppress_messages("pin\\(\\) found recent version, using it ...")
  d.new <- glue("https://fred.stlouisfed.org/graph/fredgraph.csv?id={cur_series}&cosd=2025-01-01") |>
    pin(refresh_hours = 12) |> read_csv(show_col_types = FALSE) |>
    pkgcond::suppress_messages("pin\\(\\) found recent version, using it ...")
  cat(".")
  bind_rows(d.old, d.new) |>
    mutate(currency = tolower(cur)) |>
    select(fxdate = observation_date, currency, rate = {{cur_series}})
}
#cur_tibble_from_pin("GBP")

# Pin all the currencies, bind into a tibble, and widen it
cat("Pinning: ")
d.tidy <- d.currency_list$currency_code |>
  lapply(cur_tibble_long_from_pin) |>
  bind_rows() |>
  tidyr::pivot_wider(id_cols = fxdate, names_from = currency, values_from = rate) |>
  mutate(usd = 1, .after = fxdate)
d.tidy
cat("\n")

# We need each date to be represented, regardless of whether any FX entries exist
last_date_available  <- max(d.tidy$fxdate)
first_date_available <- min(d.tidy$fxdate)
if (first_date_available == ymd("1971-01-04")) {
  # The first days of 1 were in fact only a weekend so they should be NA-filled
  first_date_available <- ymd("1970-01-01")
}

d <- fxdata_fill(d.tidy, first_date_available, last_date_available)
d

# Separate indirectly and directly quoted series
v.direct <- d.currency_list |>
  filter(quotation_method == "direct") |>
  pluck("currency_code") |>
  tolower()
v.indirect <- d.currency_list |>
  filter(quotation_method == "indirect") |>
  pluck("currency_code") |>
  tolower()

# Helper to divide in mutate command
div_1_x <- function(x) 1/x

# Do the actual inversion on the correct columns
d.fed.indirect <- d |>
  mutate(across(all_of(v.direct), div_1_x))
d.fed.direct <- d |>
  mutate(across(all_of(v.indirect), div_1_x))


# Write to main directory using improved lumps and automatic compression selection
fxdata_folder     <- here("..", "fxdata")
fxdata_write_lumpy_parquet_autocomp(d.fed.indirect, fxdata_folder, bank = "fed",  version = 2L)
fxdata_write_lumpy_parquet_autocomp(d.fed.direct,   fxdata_folder, bank = "xfed", version = 2L)
#
fxdata_write_metadata_json(d.fed.indirect, fxdata_folder, bank = "fed",  quotation_method = "indirect", new_name_order = TRUE)
fxdata_write_metadata_json(d.fed.direct,   fxdata_folder, bank = "xfed", quotation_method = "indirect", new_name_order = TRUE)


# Write to main directory using improved lumps and automatic compression selection
if (dev_data_generation) {
  local ({
    fxdata_folder     <- here("..", "fxdata")
    fxdata_write_lumpy_parquet_autocomp(d.fed.indirect, fxdata_folder, bank = "fed",  version = 2L)
    fxdata_write_lumpy_parquet_autocomp(d.fed.direct,   fxdata_folder, bank = "xfed", version = 2L)
    #
    fxdata_write_metadata_json(d.fed.indirect, fxdata_folder, bank = "fed",  quotation_method = "indirect", new_name_order = TRUE)
    fxdata_write_metadata_json(d.fed.direct,   fxdata_folder, bank = "xfed", quotation_method = "indirect", new_name_order = TRUE)
  })
}


# Old files that are no longer relevant must be deleted manually
for_deletion <- fxdata_list_obsolete_files(fxdata_folder, "fed")
if (length(for_deletion) > 0) {
  cat("Found old parquet files that should be deleted:\n")
  cat(for_deletion, sep = "\n")
}

# Old files that are no longer relevant must be deleted manually
for_deletion <- fxdata_list_obsolete_files(fxdata_folder, "xfed")
if (length(for_deletion) > 0) {
  cat("Found old parquet files that should be deleted:\n")
  cat(for_deletion, sep = "\n")
}

message("╰ Finished retrieving and writing FED data!")

} # End else from skipping

