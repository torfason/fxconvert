test_that("fx_options() works", {

  opt <- fx_options()

  assert_fxoptions(opt) |>
    expect_no_error()

})
