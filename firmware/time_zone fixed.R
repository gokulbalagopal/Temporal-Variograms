library(openair)
library(dplyr)
library(openairmaps)
library(latex2exp)
library(lubridate)

#Convert data to Central Time

############# Converting PM Time Series Data ################
pm_raw_data = read.csv("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Joppa_raw_pm_data_combined_Jan_2023.csv")
wind_raw_data = read.csv("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Joppa_raw_wind_data_combined_Jan_2023.csv")
tph_raw_data = read.csv("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Joppa_raw_tph_data_combined_Jan_2023.csv")

time_zone_shifting = function (raw_data)
{
  # Create a POSIXct object representing a UTC time
  utc_time <- ymd_hms(raw_data$dateTime, tz = "UTC")
  
  # Convert the UTC time to Central Time
  central_time <- with_tz(utc_time, tzone = "America/Chicago")
  
  raw_data$dateTime =  central_time 
  
  return (raw_data)
}

write.csv(time_zone_shifting(pm_raw_data), "D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\R_csv\\tz_shifted_Joppa_raw_pm_data_Jan_2023.csv", row.names=FALSE)
write.csv(time_zone_shifting(wind_raw_data), "D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\R_csv\\tz_shifted_Joppa_raw_wind_data_Jan_2023.csv", row.names=FALSE)
write.csv(time_zone_shifting(tph_raw_data), "D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\R_csv\\tz_shifted_Joppa_raw_tph_data_Jan_2023.csv", row.names=FALSE)
