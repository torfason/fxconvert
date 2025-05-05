
test_that("fx_init() works", {
  if (curl::has_internet()) {
    fx_init(verbose = FALSE) |>
      expect_silent()
  } else {
    fx_init(verbose = FALSE) |>
      expect_warning()
  }
})

test_that("fx_init() tests are fully implemented", {
  skip("Initialization for tests should use a setup/teardown directory")
})
