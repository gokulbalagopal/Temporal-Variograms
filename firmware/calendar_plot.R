setwd("/home/teamlarylive/Desktop/data/processed_data/pm_cleaned_and_cst_tz/")
library(readr)
library(lubridate)
library(data.table)
library(dplyr)
library(openair)
library(tidyr)
library(latex2exp)

list_csv_files_pm <- list.files(path = "/home/teamlarylive/Desktop/data/processed_data/pm_cleaned_and_cst_tz/")
temp <- lapply(list_csv_files_pm , fread, sep=",")
df <- rbindlist( temp )
df_tb = tibble(df)
df_tb$date =df_tb$dateTime
calendarPlot(df_tb, pollutant = "pm2_5",main = TeX('Calendar Plot of $PM_{2.5}$ Concentration for Joppa - Dallas, TX'),
key.header = TeX("Concentration (μg/m$^{3}$)"),
key.position = "bottom",
par.settings=list(fontsize=list(text=10))
)


