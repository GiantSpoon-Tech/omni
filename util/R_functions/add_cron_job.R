# Install cronR if not already installed
if (!require(cronR, quietly = TRUE)) {
  install.packages("cronR")
  library(cronR)
}

# Add the universal script runner cron job
cron_add(
  command = "/usr/local/bin/Rscript /Users/eugenetsenter/Docs/R_Studio_Projects/universal_cron_runner/universal_script_runner.R",
  frequency = "daily",
  at = "09:00",
  id = "universal_script_runner",
  description = "Universal Script Runner - runs all R scripts in sequence",
  tags = c("universal", "automation"),
  ask = FALSE
)

# Verify it was added
cat("Cron job added successfully!\n")
cat("Current cron jobs:\n")
cron_ls()
