#' A Mirai-based future task queue implementation
#'
#' Set up the future parameters.
#'
#' @inheritParams future::`Future-class`
#'
#' @return An object of class `MiraiFuture`.
#'
#' @keywords internal
#' @importFrom parallelly availableCores
#' @importFrom future getGlobalsAndPackages MultiprocessFuture
#' @importFrom mirai call_mirai_ daemons make_cluster
#' @export
MiraiFuture <- function(expr = NULL,
                        substitute = TRUE,
                        envir = parent.frame(),
                        globals = TRUE,
                        packages = NULL,
                        lazy = FALSE,
                        workers = availableCores(),
                        ...)
{
  if(isTRUE(substitute)) expr <- substitute(expr)

  ## Record globals
  if(!isTRUE(attr(globals, "already-done", exact = TRUE))) {
    gp <- getGlobalsAndPackages(expr, envir = envir, persistent = FALSE, globals = globals)
    globals <- gp[["globals"]]
    packages <- c(packages, gp[["packages"]])
    expr <- gp[["expr"]]
    gp <- NULL
  }

  future <- MultiprocessFuture(
              expr = expr, substitute = substitute,
              envir = envir,
              globals = globals,
              packages = packages,
              lazy = lazy,
              ...)

  if (is.function(workers)) workers <- workers()
  if (!is.null(workers)) stop_if_not(length(workers) >= 1)

  cluster <- if (is.numeric(workers)) {
    stop_if_not(length(workers) == 1L, !is.na(workers), workers >= 1)
      
    ## Do we need to change the number of mirai workers?
    nworkers <- mirai_daemons_nworkers()
    if (is.infinite(workers) && nworkers < +Inf) {
      daemons(n = 0L)
    } else if (workers != nworkers) {
      daemons(n = 0L)  ## reset is required
      make_cluster(n = workers)
    }
  } else if (is.character(workers)) {
    stop_if_not(length(workers) >= 1L, !anyNA(workers))
    dd <- get_mirai_daemons()
    if (is.data.frame(dd)) {
      uris <- rownames(dd)
      n <- length(uris)
    } else {
      n <- -1L
    }
    if (length(workers) != n) {
      daemons(n = 0L)  ## reset is required
      make_cluster(n = length(workers))
    }
  } else if (!is.null(workers)) {
    stop("Argument 'workers' should be a numeric scalar or a character vector: ", mode(workers))
  }

  future <- structure(future, class = c("MiraiFuture", class(future)))
  future$.cluster <- cluster
  future
}


#' Check on the status of a future task.
#' @return boolean indicating the task is finished (TRUE) or not (FALSE)
#' @importFrom mirai unresolved
#' @importFrom future resolved
#' @keywords internal
#' @export
resolved.MiraiFuture <- function(x, ...) {
  debug <- getOption("future.mirai.debug", FALSE)
  if (debug) {
    mdebugf("resolved() for %s ...", class(x)[1], debug = debug)
    on.exit(mdebugf("resolved() for %s ... done", class(x)[1], debug = debug))
  }
  
  resolved <- NextMethod()
  if(resolved) {
    mdebug("- already resolved", debug = debug)
    return(TRUE)
  }
  
  if(x[["state"]] == "finished") {
    mdebug("- already resolved (state == finished)", debug = debug)
    return(TRUE)
  } else if(x[["state"]] == "created") { # Not yet submitted to queue (iff lazy)
    mdebug("- just created; launching")
    x <- run(x)
    return(FALSE)
  }

  mirai <- x[["mirai"]]
  mdebug("mirai::unresolved() ...", debug = debug)
  res <- unresolved(mirai)
  mstr(res, debug = debug)
  mdebug("mirai::unresolved() ... done", debug = debug)
  
  !res
}



#' @importFrom mirai mirai
#' @importFrom future run getExpression
#' @export
run.MiraiFuture <- function(future, ...) {
  if(isTRUE(future[["state"]] != "created")) return(invisible(future))
  
  debug <- getOption("future.mirai.debug", FALSE)
  if (debug) {
    mdebugf("run() for %s ...", class(future)[1], debug = debug)
    on.exit(mdebugf("run() for %s ... done", class(future)[1], debug = debug))
  }

  future[["state"]] <- "submitted"

  expr <- getExpression(future)
  globals <- future[["globals"]]
  mirai <- mirai(expr, .args = globals)
  future[["mirai"]] <- mirai

  future[["state"]] <- "running"

  invisible(future)
}

#' @importFrom utils packageVersion
mirai_version <- local({
  version <- NULL
  function() {
    if (is.null(version)) version <<- packageVersion("mirai")
    version
  }
})

#' @importFrom future result
#' @export
result.MiraiFuture <- function(future, ...) {
  if(isTRUE(future[["state"]] == "finished")) {
    return(future[["result"]])
  }

  debug <- getOption("future.mirai.debug", FALSE)
  if (debug) {
    mdebugf("result() for %s ...", class(future)[1], debug = debug)
    on.exit(mdebugf("result() for %s ... done", class(future)[1], debug = debug))
  }

  mirai <- future[["mirai"]]
  call_mirai_(mirai)
  
  result <- mirai[["data"]]
  future[["result"]] <- result
  future[["state"]] <- "finished"

  result
}



#' @importFrom future FutureError
#' @importFrom mirai daemons
mirai_daemons_nworkers <- function() {
  workers <- get_mirai_daemons()
  if (is.data.frame(workers)) return(nrow(workers))
  
  if (length(workers) != 1L) {
    stop(FutureError(sprintf("Length of mirai::status()$daemons is not one: %d", length(workers))))
  }
  
  if (workers == 0L) return(Inf)
  workers
}
