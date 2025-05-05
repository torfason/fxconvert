
test_that("fx_get_single() works", {
  skip_if(!fs::dir_exists(fx_get_fxdata_dir()),
          "Package not initialized? (workspace directory missing)")

  fx_get_single("usd", "isk", "2024-11-11") |>
    expect_equal(139, tolerance = 1)

  fx_get_single("usd", "isk", "2025-01-01") |>
    expect_error("weekend .* .interpolate is false")

  fx_get_single("usd", "isk", "2025-01-01", .interpolate = TRUE) |>
    expect_equal(138, tolerance = 1)

  fx_get_single(character(), "isk", "2024-11-11") |>
    expect_error("length.*from")
})

test_that("fx_get() works for valid input", {
  skip_if(!fs::dir_exists(fx_get_fxdata_dir()),
          "Package not initialized? (workspace directory missing)")


  from <- c("USD", "EUR")
  to <- c("JPY", "GBP")
  fxdate <- c("2023-01-01", "2023-01-02")

  fx_get(from, to, fxdate, .interpolate = TRUE) |>
    expect_equal(c(131.88, 0.83), tolerance = 0.01)
})

test_that("fx_get() converts correctly from currency to itself", {
  skip_if(!fs::dir_exists(fx_get_fxdata_dir()),
          "Package not initialized? (workspace directory missing)")

  fx_get("ISK", "ISK", "2024-04-02") |>
    expect_equal(1) |>
    expect_warning("Looks like you ae converting to same currency.*")

})

test_that("fx_get() handles scalar or zero-length inputs correctly", {
  skip_if(!fs::dir_exists(fx_get_fxdata_dir()),
          "Package not initialized? (workspace directory missing)")

  from <- "USD"
  to <- "JPY"
  fxdate <- as.Date("2024-01-01", )

  fx_get(from, to, fxdate) |>
    expect_error("weekend .* .interpolate is false")

  fx_get(from, to, fxdate, .interpolate = TRUE) |>
    expect_equal(131, tolerance = 1)

  fx_get(character(), "isk", "2024-11-11") |>
    expect_equal(numeric())
})

test_that("fx_get throws an error for non-recyclable inputs", {
  skip_if(!fs::dir_exists(fx_get_fxdata_dir()),
          "Package not initialized? (workspace directory missing)")

  from <- c("USD", "EUR")
  to <- c("JPY", "GBP", "CAD")
  fxdate <- as.Date(c("2023-01-01", "2023-01-02"))

  fx_get(from, to, fxdate) |>
    expect_error("Can't recycle")
})

test_that("fx_get() handles input with recycling", {
  skip_if(!fs::dir_exists(fx_get_fxdata_dir()),
          "Package not initialized? (workspace directory missing)")

  from <- "USD"
  to <- c("JPY", "GBP")
  fxdate <- as.Date("2023-01-01")

  fx_get(from, to, fxdate, .interpolate = TRUE) |>
    expect_equal(c(131.88, 0.83), tolerance = 0.01)
})

test_that("fx_get() handles long missing periods in the data correctly", {
  skip("Maximum interpolation interval must be implemented")
  fx_get(c("usd", "eur"), "isk", "2016-03-05", .interpolate = TRUE) |>
    expect_error()
})

