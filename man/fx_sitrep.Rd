% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/fx_sitrep.R
\name{fx_sitrep}
\alias{fx_sitrep}
\title{Print debug information about the exchange rate database}
\usage{
fx_sitrep(bank = c("ecb", "cbi", "fed", "xfed"), verbose = TRUE)
}
\arguments{
\item{bank}{Character string specifying the source of exchange rate data.
Defaults to "ecb". Must match a valid source.}

\item{verbose}{Should it output results or not (only returning \code{TRUE}/\code{FALSE})}
}
\value{
\code{TRUE} if all looks well and correctly initialized. \code{FALSE} if the
database does not seem to be initialized. Throws an error if there are
errors in the data structures.
}
\description{
Provides a status report for the exchange rate database, printing key
information about the database file and available data, including
a preview of available dates and consistency checks.
}
\keyword{internal}
