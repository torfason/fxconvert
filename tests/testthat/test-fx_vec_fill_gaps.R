

test_that("fx_vec_fill_gaps() works", {

  # Test 1: Standard fill-down behavior
  x1 <- c(1, NA, NA, 2, NA, NA, NA, NA, 3)
  expect_equal(fx_vec_fill_gaps(x1, "down"), c(1, 1, 1, 2, 2, 2, 2, 2, 3))

  # Test 2: Filling does not go beyond last non-missing value
  x2 <- c(1, NA, NA, 2, NA, NA, 3, NA, NA)
  expect_equal(fx_vec_fill_gaps(x2, "down"), c(1, 1, 1, 2, 2, 2, 3, NA, NA))

  # Test 3: Filling starts after first non-missing value
  x3 <- c(NA, NA, 1, NA, NA, 2, NA, NA, 3)
  expect_equal(fx_vec_fill_gaps(x3, "down"), c(NA, NA, 1, 1, 1, 2, 2, 2, 3))

  # Test 4: Fill stops correctly after last non-missing value
  x4 <- c(NA, NA, 1, NA, NA, 2, NA, 3, NA)
  expect_equal(fx_vec_fill_gaps(x4, "down"), c(NA, NA, 1, 1, 1, 2, 2, 3, NA))

  # Test 5: No filling when only one non-missing value exists
  x5 <- c(NA, NA, 1, NA, NA, NA, NA, NA, NA)
  expect_equal(fx_vec_fill_gaps(x5, "down"), c(NA, NA, 1, NA, NA, NA, NA, NA, NA))

  # Test 6: All NA values remain unchanged
  x6 <- c(NA_real_, NA, NA, NA, NA, NA, NA, NA, NA)
  expect_equal(fx_vec_fill_gaps(x6, "down"), x6)

  # Test 7: "up" direction fills upwards
  expect_equal(fx_vec_fill_gaps(x1, "up"), c(1, 2, 2, 2, 3, 3, 3, 3, 3))
  expect_equal(fx_vec_fill_gaps(x2, "up"), c(1, 2, 2, 2, 3, 3, 3, NA, NA))
  expect_equal(fx_vec_fill_gaps(x3, "up"), c(NA, NA, 1, 2, 2, 2, 3, 3, 3))
  expect_equal(fx_vec_fill_gaps(x4, "up"), c(NA, NA, 1, 2, 2, 2, 3, 3, NA))
  expect_equal(fx_vec_fill_gaps(x5, "up"), c(NA, NA, 1, NA, NA, NA, NA, NA, NA))
  expect_equal(fx_vec_fill_gaps(x6, "up"), x6)

  # Test 8: "downup" should always give the same result as down
  expect_equal(fx_vec_fill_gaps(x1, "downup"), fx_vec_fill_gaps(x1, "down"))
  expect_equal(fx_vec_fill_gaps(x2, "downup"), fx_vec_fill_gaps(x2, "down"))
  expect_equal(fx_vec_fill_gaps(x3, "downup"), fx_vec_fill_gaps(x3, "down"))
  expect_equal(fx_vec_fill_gaps(x4, "downup"), fx_vec_fill_gaps(x4, "down"))
  expect_equal(fx_vec_fill_gaps(x5, "downup"), fx_vec_fill_gaps(x5, "down"))
  expect_equal(fx_vec_fill_gaps(x6, "downup"), fx_vec_fill_gaps(x6, "down"))


  # Test 8: "updown" should always give the same result as up
  expect_equal(fx_vec_fill_gaps(x1, "updown"), fx_vec_fill_gaps(x1, "up"))
  expect_equal(fx_vec_fill_gaps(x2, "updown"), fx_vec_fill_gaps(x2, "up"))
  expect_equal(fx_vec_fill_gaps(x3, "updown"), fx_vec_fill_gaps(x3, "up"))
  expect_equal(fx_vec_fill_gaps(x4, "updown"), fx_vec_fill_gaps(x4, "up"))
  expect_equal(fx_vec_fill_gaps(x5, "updown"), fx_vec_fill_gaps(x5, "up"))
  expect_equal(fx_vec_fill_gaps(x6, "updown"), fx_vec_fill_gaps(x6, "up"))
})
