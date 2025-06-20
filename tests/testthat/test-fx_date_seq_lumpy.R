library(testthat)

test_that("fx_lump_dates() works with original lump_units", {

  # Minimal range that spans all variation on each end
  dates <- fx_date_seq("2023-11-30", "2025-02-01")
  gold <- c("2023-11-30", "2023-12", "2024", "2025-01", "2025-02-01")
  fx_lump_dates(dates, lump_from = "year", lump_to = "month") |>
    unique() |>
    expect_equal(gold)

  # Reasonably complex combination, here old and new give the same result
  dates <- fx_date_seq("2020-02-28", "2024-11-04")
  gold <- c("2020-02-28", "2020-02-29", "2020-03", "2020-04", "2020-05",
            "2020-06", "2020-07", "2020-08", "2020-09", "2020-10", "2020-11",
            "2020-12", "2021", "2022", "2023", "2024-01", "2024-02", "2024-03",
            "2024-04", "2024-05", "2024-06", "2024-07", "2024-08", "2024-09",
            "2024-10", "2024-11-01", "2024-11-02", "2024-11-03", "2024-11-04")
  fx_lump_dates(dates, lump_from = "year", lump_to = "month") |>
    unique() |>
    expect_equal(gold)
  lump_dates_recursive(dates) |> unique() |>
    expect_equal(gold)

  # Month and a few days
  dates <- fx_date_seq("2020-02-28", "2020-04-01")
  gold <- c("2020-02-28", "2020-02-29", "2020-03", "2020-04-01")
  fx_lump_dates(dates) |> unique() |>
    expect_equal(gold)

  # Complete month within year
  dates <- fx_date_seq("2023-02-01", "2023-02-28")
  gold <- c("2023-02")
  fx_lump_dates(dates) |> unique() |>
    expect_equal(gold)

  # Complete year (part of failure mode below)
  dates <- fx_date_seq("2023-01-01", "2023-12-31")
  gold <- c("2023")
  fx_lump_dates(dates) |> unique() |>
    expect_equal(gold)

  # Failure mode discovered early 2025
  dates <- fx_date_seq("2022-01-01", "2024-12-31")
  gold <- c("2022", "2023", "2024")
  fx_lump_dates(dates) |> unique() |>
    expect_equal(gold)

})


test_that("fx_lump_dates() works for year+month eras", {

  # Days, months, years in output
  dates <- fx_date_seq("1980-11-29", "2024-02-03")
  gold <- c("1980-11-29", "1980-11-30",
            "1980-12",
            as.character(1981:2023),
            "2024-01",
            "2024-02-01", "2024-02-02", "2024-02-03")
  fx_lump_dates(dates, lump_from = "year", lump_to = "month") |>
    unique() |>
    expect_equal(gold)

  # Months, years in output
  dates <- fx_date_seq("1980-12-01", "2024-01-31")
  gold <- c("1980-12", as.character(1981:2023), "2024-01")
  fx_lump_dates(dates, lump_from = "year", lump_to = "month") |>
    unique() |>
    expect_equal(gold)

  # One day and then some years
  dates <- fx_date_seq("1980-12-31", "2024-12-31")
  gold <- c("1980-12-31", as.character(1981:2024) )
  fx_lump_dates(dates, lump_from = "year", lump_to = "month") |>
    unique() |>
    expect_equal(gold)

  # Minimal range that spans all variation on each end for year+month lumps
  dates <- fx_date_seq("2023-11-30", "2025-02-01")
  gold <- c("2023-11-30", "2023-12", "2024", "2025-01", "2025-02-01")
  fx_lump_dates(dates, lump_from = "year", lump_to = "month") |>
    unique() |>
    expect_equal(gold)

})


test_that("legacy fx_date_seq_lumpy() works", {

  # Minimal range that spans all variation on each end
  from_date <- lubridate::ymd("2023-11-30")
  to_date <- lubridate::ymd("2025-02-01")
  gold <- c("2023-11-30", "2023-12", "2024", "2025-01", "2025-02-01")
  fx_date_seq_lumpy(from_date, to_date) |>
    expect_equal(gold) |>
    expect_no_warning()

  # Reasonably complex combination
  from_date <- lubridate::ymd("2020-02-28")
  to_date <- lubridate::ymd("2024-11-04")
  gold <- c("2020-02-28", "2020-02-29", "2020-03", "2020-04", "2020-05",
            "2020-06", "2020-07", "2020-08", "2020-09", "2020-10", "2020-11",
            "2020-12", "2021", "2022", "2023", "2024-01", "2024-02", "2024-03",
            "2024-04", "2024-05", "2024-06", "2024-07", "2024-08", "2024-09",
            "2024-10", "2024-11-01", "2024-11-02", "2024-11-03", "2024-11-04")
  fx_date_seq_lumpy(from_date, to_date) |>
    expect_equal(gold) |>
    expect_no_warning()

  # Month and a few days
  from_date <- lubridate::ymd("2020-02-28")
  to_date <- lubridate::ymd("2020-04-01")
  gold <- c("2020-02-28", "2020-02-29", "2020-03", "2020-04-01")
  fx_date_seq_lumpy(from_date, to_date) |>
    expect_equal(gold) |>
    expect_no_warning()

  # Complete month within year
  from_date <- lubridate::ymd("2023-02-01")
  to_date <- lubridate::ymd("2023-02-28")
  gold <- c("2023-02")
  fx_date_seq_lumpy(from_date, to_date) |>
    expect_equal(gold) |>
    expect_no_warning()

  # Complete year (part of failure mode below)
  from_date <- lubridate::ymd("2023-01-01")
  to_date <- lubridate::ymd("2023-12-31")
  gold <- c("2023")
  fx_date_seq_lumpy(from_date, to_date) |>
    expect_equal(gold) |>
    expect_no_warning()

  # Failure mode discovered early 2025
  from_date <- lubridate::ymd("2022-01-01")
  to_date <- lubridate::ymd("2024-12-31")
  gold <- c("2022", "2023", "2024")
  fx_date_seq_lumpy(from_date, to_date) |>
    expect_equal(gold) |>
    expect_no_warning()

})


test_that("legacy fx_date_seq_lumpy() works even better", {

  # Example usage
  from_date <- "1980-11-29"
  to_date <- "2024-02-03"
  separated_ranges <- fx_date_seq_lumpy(from_date, to_date)

  testthat::expect_equal(separated_ranges,
                         c("1980-11-29", "1980-11-30",
                           "1980-12",
                           as.character(1981:2023),
                           "2024-01",
                           "2024-02-01", "2024-02-02", "2024-02-03") )

  separated_ranges <- fx_date_seq_lumpy("1980-12-01", "2024-01-31")
  testthat::expect_equal(separated_ranges,
                         c("1980-12",
                           as.character(1981:2023),
                           "2024-01") )

  separated_ranges <- fx_date_seq_lumpy("1980-12-31", "2024-12-31")
  testthat::expect_equal(separated_ranges,
                         c("1980-12-31",
                           as.character(1981:2023),
                           "2024") )

})


test_that("legacy fx_date_seq_lumpy() works with string inputs", {

  # Minimal range that spans all variation on each end
  gold <- c("2023-11-30", "2023-12", "2024", "2025-01", "2025-02-01")
  fx_date_seq_lumpy("2023-11-30", "2025-02-01") |>
    expect_equal(gold) |>
    expect_no_warning()

})


test_that("first_date() and last_date() work", {

  # Alchemy (creating gold for first_date)
  days <- lubridate::ymd(c("0000-01-02", "9999-12-30", "2025-02-23")) |>
    c(fx_date_seq("1999-11-16", "2001-04-16")[seq(1, 148, by = 7)])
  args <- c("millennium", "century", "decade", "year", "month", "decaday")

  # Test first_date against gold
  first_date_result <- lapply (args, \(x) {first_date(days, x)}) |> setNames(args) |>
    tibble::as_tibble() |> dplyr::mutate(day = days, .before=1)
  #first_date_result |> saveRDS(test_path("fixtures", "first_date_gold.rds"))
  expect_equal(first_date_result,
               readRDS(test_path("fixtures", "first_date_gold.rds")))

  # Test last_date against gold
  last_date_result <- lapply (args, \(x) {last_date(days, x)}) |> setNames(args) |>
    tibble::as_tibble() |> dplyr::mutate(day = days, .before=1)
  #last_date_result |> saveRDS(test_path("fixtures", "last_date_gold.rds"))
  expect_equal(last_date_result,
               readRDS(test_path("fixtures", "last_date_gold.rds")))

})
