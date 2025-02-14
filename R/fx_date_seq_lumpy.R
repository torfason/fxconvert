
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
#'
#' @return A character vector of separated date ranges, sorted chronologically.
#'         The vector contains strings representing full years ("YYYY"),
#'         full months ("YYYY-MM"), and individual days ("YYYY-MM-DD") as needed.
#'
#' @examples
#' fx_date_seq_lumpy("2022-11-30", "2024-02-01")
#' @keywords internal
#' @export
fx_date_seq_lumpy <- function(from_date, to_date) {

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

  v.separated_ranges
}

