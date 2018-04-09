# load packages -----------------------------------------------------------
pkgs <- c(
    "dplyr",
    "magrittr",
    "RMySQL",
    "DBI",
    "xgboost"
)
if(any(lapply(pkgs, require, character.only = TRUE) == 0)){
    stop("Failed to load one or many packages. Check warning messages.")
}

# set locale for date display
Sys.setlocale(locale = "en_us.UTF-8")
