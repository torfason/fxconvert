
test_that("fx_get_data_dir works", {

  # Without arguments should work
  fx_get_fxdata_dir() |>
    expect_type("character") |>
    expect_length(1)

  # With a reasonable string arg should also work
  fx_get_fxdata_dir(options = fx_options(workspace = "my/stringy_string")) |>
    expect_type("character") |>
    expect_length(1)

  # But not if you try to sneak out of the app directory
  fx_get_fxdata_dir(options = fx_options(workspace = "../sneaky_folder")) |>
    expect_error("path_has_parent.*not TRUE")

})

test_that("fx_duck_local() connection is auto-closed", {

  inner_test <- function() {
    conn <- fx_duck_local("ecb")

    # Should be valid inside the function
    expect_true(duckdb::dbIsValid(conn))
    conn
  }
  conn <- inner_test()

  # After function is exidted, conn should be closed
  expect_false(duckdb::dbIsValid(conn))
})
