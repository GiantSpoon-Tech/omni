# utils_loader.R

# Absolute folder (change only if you move it)
.UTILS_DIR <- "/Users/eugenetsenter/Looker_clonedRepo/looker_personal/util/R_functions"

# Load all .R files from your personal utils folder into a private env
load_utils <- function(recursive = TRUE) {
  dir <- .UTILS_DIR
  if (!dir.exists(dir)) stop("Directory not found: ", dir)

  env <- new.env(parent = globalenv())  # use baseenv() for stricter isolation

  files <- list.files(dir, pattern = "[.][Rr]$", recursive = recursive, full.names = TRUE)
  if (length(files) == 0L) warning("No .R files found in: ", dir)

  defined <- character(0)

  for (f in sort(files)) {
    before <- ls(envir = env, all.names = TRUE)
    sys.source(f, envir = env, keep.source = FALSE)
    after  <- ls(envir = env, all.names = TRUE)
    new    <- setdiff(after, before)

    dups <- intersect(defined, new)
    if (length(dups)) {
      warning("These names were overwritten by file ", basename(f), ": ",
              paste(dups, collapse = ", "))
    }
    defined <- union(defined, new)
  }

  attr(env, "files")     <- files
  attr(env, "dir")       <- normalizePath(dir, mustWork = FALSE)
  attr(env, "loaded_at") <- Sys.time()
  env
}

# Reload the same files into an existing env (after edits)
utils_reload <- function(utils_env) {
  files <- attr(utils_env, "files")
  if (is.null(files)) stop("utils_env lacks 'files' attribute. Did it come from load_utils_personal()?")

  rm(list = ls(envir = utils_env, all.names = TRUE), envir = utils_env)
  for (f in files) sys.source(f, envir = utils_env, keep.source = FALSE)
  attr(utils_env, "loaded_at") <- Sys.time()
  invisible(utils_env)
}
