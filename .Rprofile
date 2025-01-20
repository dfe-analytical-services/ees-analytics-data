source("renv/activate.R")

suppressPackageStartupMessages(library(styler))
suppressPackageStartupMessages(library(lintr))

# Install commit-hooks locally
statusWriteCommit <- file.copy(".hooks/pre-commit.R", ".git/hooks/pre-commit", overwrite = TRUE)
