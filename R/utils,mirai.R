#' @importFrom mirai status is_error_value
#' @importFrom utils capture.output
get_mirai_daemons <- function() {
  status <- status()
  res <- status[["daemons"]]
  
  if (is.character(res)) {
    # returns number of daemons if running without dispatcher
    return(status[["connections"]])
  }
  
  if (is_error_value(res)) { # should not assume structure of an error value
    reason <- capture.output(print(res))
    stop(FutureError(sprintf("mirai::status() failed to communicate with dispatcher: %s", reason)))
  }
  
  as.data.frame(res)
  
}
