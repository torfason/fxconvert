test_that("fx_sitrep() works", {
  skip_if(!fs::dir_exists(fx_get_fxdata_dir()),
          "Package not initialized? (workspace directory missing)")
  expect_true(FALSE)
})
