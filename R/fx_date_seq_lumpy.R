
#' Generate a sequence of dates
#'
#' Creates a daily date sequence between two dates, ensuring the end date is not
#' earlier than the start date.
#'
#' @param from_date A date or character string in "YYYY-MM-DD" format.
#' @param to_date A date or character string in "YYYY-MM-DD" format.
#'
#' @return A sequence of dates from `from_date` to `to_date`.
#'
#' @examples
#' fx_date_seq("2024-01-01", "2024-01-10")
#' fx_date_seq(as.Date("2023-06-01"), as.Date("2023-06-05"))
#' @keywords internal
#' @export
fx_date_seq <- function(from_date, to_date) {

  # Preprocess and verify arguments
  from_date <- lubridate::ymd(from_date)
  to_date   <- lubridate::ymd(to_date)
  stopifnot(to_date >= from_date)

  # Generate the sequence
  seq.Date(from_date, to_date, by = "day")
}


#' Generate a lumpy sequene of dates
#'
#' Generates a "lumpy" sequence of dates in that the dates are grouped into
#' individual components of full years, full months, and remaining days.
#'
#' @param from_date A date or character string in "YYYY-MM-DD" format.
#' @param to_date A date or character string in "YYYY-MM-DD" format.
#' @param lump_decades Should decades be lumped using `199X` format?
#'
#' @return A character vector of separated date ranges, sorted chronologically.
#'         The vector contains strings representing full years ("YYYY"),
#'         full months ("YYYY-MM"), and individual days ("YYYY-MM-DD") as needed.
#'
#' @examples
#' fx_date_seq_lumpy("2022-11-30", "2024-02-01")
#' @keywords internal
#' @export
fx_date_seq_lumpy <- function(from_date, to_date, lump_decades = FALSE) {

  # Create a sequence of dates from the start to the end
  date_seq <- fx_date_seq(from_date, to_date)

  # Create a data frame of these dates
  d.days <- tibble::tibble(date = date_seq) |>
    dplyr::mutate(year  = lubridate::year(.data$date),
                  month = lubridate::month(.data$date),
                  day   = lubridate::day(.data$date))

  # Yearly aggregate including first/last, and from that a full-year indicator
  d.years <- d.days |>
    dplyr::summarize(start_date = min(.data$date), end_date = max(.data$date), .by = "year") |>
    dplyr::mutate(first_date = lubridate::make_date(.data$year), last_date = lubridate::make_date(.data$year, 12, 31)) |>
    dplyr::mutate(full_year = (first_date == start_date & last_date == end_date))
  v.full_years <- d.years |>
    dplyr::filter(full_year) |>
    dplyr::pull(.data$year)
  v.full_y_chars <- as.character(v.full_years)

  # Monthly aggregate, first stripping any full years, get full-month indicator
  d.days_excluding_full_years <- d.days |>
    dplyr::filter(!(.data$year %in% v.full_years))
  if (nrow(d.days_excluding_full_years>0)) {
    # Standard case, we need to create some elements for partial years
    d.months <- d.days_excluding_full_years |>
      dplyr::summarize(start_date = min(.data$date), end_date = max(.data$date), .by = c("year", "month")) |>
      dplyr::mutate(full_month = (lubridate::day(start_date) == 1 &
                                    lubridate::day(end_date) == lubridate::days_in_month(end_date))) |>
      dplyr::mutate(ym_char = format(start_date, "%Y-%m"), .before = start_date)
    v.full_ym_chars <- d.months |>
      dplyr::filter(full_month) |>
      dplyr::pull(ym_char)
  } else {
    # Exactly whole year(s) specified. v.full_ym_chars is empty
    v.full_ym_chars <- character()
  }

  # Daily "aggregate" filter out any full years and months
  v.full_ymd_chars <- d.days |>
    dplyr::filter(!(.data$year %in% v.full_years)) |>
    dplyr::mutate(ym_char  = format(.data$date, "%Y-%m"),    .after = "date") |>
    dplyr::mutate(ymd_char = format(.data$date, "%Y-%m-%d"), .after = "date") |>
    dplyr::filter(!(ym_char %in% v.full_ym_chars)) |>
    dplyr::pull(ymd_char)

  # Combine all different range types (yearly, monthly, daily)
  # There should be no overlap, each date should only be covered by a single range
  v.separated_ranges <-
    c(v.full_y_chars, v.full_ym_chars, v.full_ymd_chars) |>
    sort()

  # Special handling to lump decades
  if (lump_decades) {

    # Varname for the following handling is result
    result <- v.separated_ranges

    # Identify full years (entries with 4 digits) and group them into decades when possible.
    is_year <- stringr::str_detect(result, "^\\d{4}$")
    full_years <- as.integer(result[is_year])
    year_indices <- which(is_year)
    remove_idx <- integer(0)
    i <- 1
    while(i <= length(year_indices)) {
      yr_val <- full_years[i]
      # Check if this full year is the start of a decade.
      if (yr_val %% 10 == 0 && i + 9 <= length(year_indices)) {
        candidate <- full_years[i:(i+9)]
        if (identical(candidate, seq(yr_val, yr_val+9))) {
          # Replace the first year's entry with the decade string and mark the rest for removal.
          result[year_indices[i]] <- paste0(substr(as.character(yr_val), 1, 3), "X")
          remove_idx <- c(remove_idx, year_indices[(i+1):(i+9)])
          i <- i + 10
          next
        }
      }
      i <- i + 1
    }
    if (length(remove_idx) > 0) result <- result[-remove_idx]

    # Reassign back to original variable
    v.separated_ranges <- result
  }

  v.separated_ranges
}


# This is not a good version
fx_date_seq_lumpy_x <- function(from_date, to_date, lump_decades = FALSE) {
  # Create the full daily sequence.
  dates <- fx_date_seq(from_date, to_date)
  result <- character(0)

  # Process each year in the sequence.
  yrs <- sort(unique(format(dates, "%Y")))
  for (yr in yrs) {
    dates_year <- dates[format(dates, "%Y") == yr]
    year_start <- as.Date(paste0(yr, "-01-01"))
    year_end   <- as.Date(paste0(yr, "-12-31"))

    if (min(dates_year) == year_start && max(dates_year) == year_end) {
      # Full year.
      result <- c(result, yr)
    } else {
      # Not a full year; process by month.
      mos <- sort(unique(format(dates_year, "%Y-%m")))
      for (mo in mos) {
        dates_mo <- dates_year[format(dates_year, "%Y-%m") == mo]
        month_start <- as.Date(paste0(mo, "-01"))
        month_end <- as.Date(seq(month_start, by = "month", length = 2)[2] - 1)
        if (min(dates_mo) == month_start && max(dates_mo) == month_end) {
          result <- c(result, mo)
        } else {
          result <- c(result, format(dates_mo, "%Y-%m-%d"))
        }
      }
    }
  }

  if (lump_decades) {

    # Identify full years (entries with 4 digits) and group them into decades when possible.
    is_year <- grepl("^\\d{4}$", result)
    full_years <- as.integer(result[is_year])
    year_indices <- which(is_year)
    remove_idx <- integer(0)
    i <- 1
    while(i <= length(year_indices)) {
      yr_val <- full_years[i]
      # Check if this full year is the start of a decade.
      if (yr_val %% 10 == 0 && i + 9 <= length(year_indices)) {
        candidate <- full_years[i:(i+9)]
        if (identical(candidate, seq(yr_val, yr_val+9))) {
          # Replace the first year's entry with the decade string and mark the rest for removal.
          result[year_indices[i]] <- paste0(substr(as.character(yr_val), 1, 3), "X")
          remove_idx <- c(remove_idx, year_indices[(i+1):(i+9)])
          i <- i + 10
          next
        }
      }
      i <- i + 1
    }
    if (length(remove_idx) > 0) result <- result[-remove_idx]
  }

  result
}


first_date <- function(x,  unit = c("millenium", "century", "decade",
                                    "year", "month", "decaday")) {

  # Verify inputs
  assert_date(x)
  assert_string(unit)
  unit <- arg_match(unit)

  if (unit != "decaday") {
    unit <- dplyr::case_match(unit, "millenium" ~ "1000 year", "century" ~ "100 year",
                       "decade" ~ "10 year", .default = unit )
    lubridate::floor_date(x, unit)
  } else {
    lubridate::day(x) <- pmax((lubridate::day(x) %/% 10)*10, 1)
    x
  }
}

last_date <- function(x,  unit = c("millenium", "century", "decade",
                                    "year", "month", "decaday")) {

  # Verify inputs
  assert_date(x)
  assert_string(unit)
  unit <- arg_match(unit)

  if (unit != "decaday") {
    unit <- dplyr::case_match(unit, "millenium" ~ "1000 year", "century" ~ "100 year",
                       "decade" ~ "10 year", .default = unit )
    lubridate::ceiling_date(x, unit) - 1
  } else {
    lubridate::day(x) <-
      pmin((lubridate::day(x) %/% 10)*10+9,
           lubridate::day(lubridate::ceiling_date(x, "month") - 1))
    x
  }
}


fmt_funs <- list(
  millenium = \(x){sprintf("%dXXX", lubridate::year(x) %/% 1000)},
  century   = \(x){sprintf("%dXX",  lubridate::year(x) %/%  100)},
  decade    = \(x){sprintf("%dX",   lubridate::year(x) %/%   10)},
  year      = \(x){format(x, "%Y")},
  month     = \(x){format(x, "%Y-%m")},
  decaday   = \(x){sprintf("%s%dX", format(x, "%Y-%m-"),
                                    lubridate::day(x) %/% 10)},
  day       = \(x){format(x, "%Y-%m-%d")})


#' Generate a lumpy sequence of dates with a recursive approach
#'
#' The function lumps dates from millenia to "decadays".
#'
#' @param dates Strictly increasing date sequence to lump
#' @param lump_unit_now Which unit to lump next
#' @param lump_unit_halt Which unit to halt on. Note that this unit is
#'   *not* lumped, the function immediately exits on this unit.
#'
#' @return A character vector of date ranges.
#'
#' @keywords internal
#' @export
fx_date_seq_lumpy_recursive <- function(dates,
                                        lump_unit_now = "millenium",
                                        lump_unit_halt = "day") {

  lump_units = c("millenium", "century", "decade", "year", "month", "decaday", "day")

  # Verify inputs
  assert_date(dates)
  arg_match(lump_unit_now, lump_units)
  arg_match(lump_unit_halt, lump_units)
  lump_unit_next = lump_units[which(lump_unit_now == lump_units) + 1]


  # Additional verification on dates
  if(!all(diff(dates) == 1))
    cli::cli_abort("dates must be strictly increasing sequence")
  if(length(dates) > 0 && lubridate::year(dates[1]) < 1000)
    cli::cli_abort("dates before '1000-01-01' are not supported")

  # Stop recursion on zero length or on completion of all lump units
  if (length(dates) == 0 || lump_unit_now == lump_unit_halt) {
    return(fmt_funs[["day"]](dates))
  }

  # Split input into three parts, format the center with fmt_funs,
  # and return the edges with a recursive call
  lump_lo <- first_date(dates, lump_unit_now)
  lump_hi <- last_date(dates, lump_unit_now)
  lump_range <- (lump_lo >= min(dates)) & (lump_hi <= max(dates))
  #tibble::tibble(dates, lump_lo, lump_hi, lump_range) |> print(n=33)
  if (!any(lump_range)) {
    return(fx_date_seq_lumpy_recursive(dates, lump_unit_next, lump_unit_halt))
  }

  # what if lump
  a <- min(which(lump_range))
  b <- max(which(lump_range))
  return(
    c(fx_date_seq_lumpy_recursive(dates[seq2(1,a-1)], lump_unit_next, lump_unit_halt),
      fmt_funs[[lump_unit_now]](dates[seq2(a,b)]),
      fx_date_seq_lumpy_recursive(dates[seq2(b+1,length(dates))], lump_unit_next, lump_unit_halt)
    )
  )
}

