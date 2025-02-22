#!/usr/bin/env Rscript
message("\nRunning commit hooks...")

message("\n")

message("1. Checking code styling...\n")
style_output <- eval(styler::style_dir()$changed)
if (any(style_output)) {
  message("Warning: Code failed styling checks.
  \n`styler::style_dir()` has been run for you.
  \nPlease check your files and dashboard still work.
  \nThen re-stage and try committing again.")
  quit(save = "no", status = 1, runLast = FALSE)
} else {
  message("...code styling checks passed")
}

message("\n")

# End of hooks
