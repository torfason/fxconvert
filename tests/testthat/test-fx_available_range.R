test_that("fx_available_range() works", {
  skip_if(!fs::dir_exists(fx_get_fxdata_dir()),
          "Package not initialized? (workspace directory missing)")
  fx_available_range() |>
    expect_type("character") |>
    expect_named(c("date_first", "date_last"))
})
