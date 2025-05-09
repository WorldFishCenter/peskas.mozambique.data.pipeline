% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/validation-functions.R
\name{alert_outlier}
\alias{alert_outlier}
\title{Outlier Alert for Numeric Vectors}
\usage{
alert_outlier(
  x,
  no_alert_value = NA_real_,
  alert_if_larger = no_alert_value,
  alert_if_smaller = no_alert_value,
  ...
)
}
\arguments{
\item{x}{A numeric vector in which to detect outliers.}

\item{no_alert_value}{A numeric code (default \code{NA_real_}) to assign to non-outlier values.}

\item{alert_if_larger}{A numeric code to assign if \code{x > upper bound}.}

\item{alert_if_smaller}{A numeric code to assign if \code{x < lower bound}.}

\item{...}{Additional arguments passed to \code{\link[univOutl]{LocScaleB}}, such as
\code{logt} or \code{k}.}
}
\value{
A numeric vector of the same length as \code{x}, containing the alert codes
for each element (\code{alert_if_smaller}, \code{alert_if_larger}, or
\code{no_alert_value}).
}
\description{
This helper function identifies numeric outliers based on the bounds computed by
\code{\link[univOutl]{LocScaleB}}. It optionally applies a log transform within
\code{LocScaleB} (through the \code{...} argument) and flags values outside the computed
lower or upper bounds. Values below the lower bound receive the alert code
\code{alert_if_smaller}, values above the upper bound receive \code{alert_if_larger},
and in-range values receive \code{no_alert_value}.
}
\details{
\itemize{
\item The function checks if all \code{x} values are \code{NA} or zero, or if the MAD
(median absolute deviation) is zero. In these cases, it returns \code{NA} for
all elements, since meaningful bounds cannot be computed.
\item Otherwise, it calls \code{univOutl::LocScaleB(x, ...)} to compute \code{bounds}.
\item If a log transform was used (\code{logt = TRUE}), \code{LocScaleB} returns the
log-scale bounds; typically, you would back-transform them to compare with
the raw values.
}
}
\examples{
\dontrun{
x <- c(1, 2, 3, 100)
alert_outlier(x, no_alert_value = NA, alert_if_larger = 9, logt = TRUE, k = 3)
}

}
\seealso{
\code{\link[univOutl]{LocScaleB}} for the underlying outlier detection algorithm.
}
\keyword{validation}
