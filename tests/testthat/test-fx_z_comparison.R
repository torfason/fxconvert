test_that("same results from different banks", {

  #skip("!!! TAKES TO LONG, MUST MAKE FASTER !!!")

  # Currencies with availability from 2010 for all three banks
  curs_from_2010 = c("EUR", "USD", "CAD", "CHF", "DKK", "GBP", "JPY", "NOK", "SEK",
    "AUD", "NZD", "SGD", "ZAR", "HKD", "KRW", "CNY", "THB", "MXN",
    "BRL", "INR")

  withr::with_seed(43, {
    dates    <- random_date(20, "2010-01-01", "2025-03-31")
    cur_from <- sample(curs_from_2010, 20, replace = TRUE)
    cur_to   <- sample(curs_from_2010, 20, replace = TRUE)
  })

  d.results <- fx_get_multibank(cur_from, cur_to, dates,
                   c("ecb", "cbi", "fed"),
                   .interpolate = TRUE) |>
    expect_output()

  d.results |>
    dplyr::mutate(
      hi = pmax(ecb, cbi, fed),
      lo = pmin(ecb, cbi, fed),
      delta_perc = tibble::num((hi/lo)-1, label = "%", scale = 100)
    ) |>
    dplyr::arrange(-delta_perc) |>
    expect_snapshot()

    # # Set folder for the generated parquet files
    # fxdata_folder <- here::here("..", "fxdata")
    #
    # l.d <- list()
    # l.d[["ecb"]]  <- read_parquet_multi(here::here(fxdata_folder, "ecb"))
    # l.d[["cbi"]]  <- read_parquet_multi(here::here(fxdata_folder, "cbi"))
    # l.d[["fed"]] <- read_parquet_multi(here::here(fxdata_folder, "fed"))
    # #l.d[["xfed"]] <- read_parquet_multi(here(fxdata_folder, "xfed"))
    #
    # d <- dplyr::inner_join(
    #   (l.d[["ecb"]] |> dplyr::select(fxdate, sgd_ecb = sgd)),
    #   (l.d[["fed"]] |> dplyr::select(fxdate, sgd_fed = sgd)),
    #   by = dplyr::join_by(fxdate)
    # )

   skip("FED data source has mixd singapore dollar and swedish Krona. That must be fixed: / SGD,DEXSDUS,Singapore Dollar to USD / SEK,DEXSIUS,Swedish Krona to USD. (This test also takes way too long)")

})
