test_that("fx_options() works", {

  opt <- fx_options()

  assert_fxoptions(opt) |>
    expect_no_error()

})

test_that("fx_options() handles incorrect inputs", {

  # Passing unnamed arguments should result in errors mentioning "..."
  fx_options("ecb") |>
    expect_error("\\.\\.\\.")

  # Passing incorrect names should result in errors mentioning "..."
  fx_options(wokspace = "main") |>
    expect_error("\\.\\.\\.")
})
