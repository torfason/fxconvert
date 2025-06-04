test_that("same results from different banks", {

  #skip("!!! TAKES TO LONG, MUST MAKE FASTER !!!")

  # Currencies with availability from 2010 for all three banks
  curs_from_2010 = c("EUR", "USD", "CAD", "CHF", "DKK", "GBP", "JPY", "NOK", "SEK",
    "AUD", "NZD", "SGD", "ZAR", "HKD", "KRW", "CNY", "THB", "MXN",
    "BRL", "INR")

  reps <- 20
  withr::with_seed(43, {
    dates    <- random_date(reps, "2010-01-01", "2025-03-31", replace = TRUE)
    cur_from <- sample(curs_from_2010, reps, replace = TRUE)
    cur_to   <- sample(curs_from_2010, reps, replace = TRUE)
  })

  d.results <- fx_get_multibank(cur_from, cur_to, dates,
                   c("ecb", "cbi", "fed"),
                   .interpolate = TRUE) |>
    expect_s3_class("tbl_df")

  d.results_snapshot <- d.results |>
    dplyr::mutate(
      hi = pmax(ecb, cbi, fed),
      lo = pmin(ecb, cbi, fed),
      delta_perc = tibble::num((hi/lo)-1, label = "%", scale = 100)
    ) |>
    dplyr::arrange(-delta_perc)
  d.results_snapshot |> expect_snapshot()

  # This is not run by default due to the long time it takes to run
  # d.results_snapshot_10000 |>
  #   dplyr::filter(delta_perc > 0.02) |>
  #   expect_snapshot()

  # The difference between banks should be less than 5%, with mean/median less than 0.5%
  d.results_snapshot$delta_perc |> as.double() |> max()    |> expect_lt(0.05)
  d.results_snapshot$delta_perc |> as.double() |> mean()   |> expect_lt(0.005)
  d.results_snapshot$delta_perc |> as.double() |> median() |> expect_lt(0.005)

})
