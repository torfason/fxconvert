
test_that("fx_init() works", {
  if (curl::has_internet()) {
    fx_init(action = "auto", verbose = FALSE) |>
      expect_silent()
  } else {
    fx_init(action = "offline", verbose = FALSE) |>
      expect_silent()
  }
})

test_that("fx_init() tests are fully implemented", {
  skip("Initialization for tests should use a setup/teardown directory")
})
