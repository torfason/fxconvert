test_that("fx_convert() works", {
  skip_if(!fs::dir_exists(fx_get_fxdata_dir()),
          "Package not initialized? (workspace directory missing)")

  gold <- c(12.75, 14.03, 15.3, 16.45, 17.83, 18.97, 20.24, 21.62, 22.90,
    24.17, 25.44, 26.71, 28.04, 29.36, 30.52, 31.79, 33.06, 34.33, 35.41,
    36.62, 38.09, 39.27, 40.54, 41.80, 43.20, 44.56, 45.81, 47.19, 48.38,
    49.65, 50.92, 52.13)

  fx_convert(10:41, "gbp", "usd",
             fx_date_seq("2023-12-15", "2024-01-15"),
             .interpolate = TRUE,
             fxsource = "ecb") |>
    expect_equal(gold, tolerance = 0.01)

})

