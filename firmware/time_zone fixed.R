library(readr)
library(lubridate)
list_csv_files_pm <- list.files(path = "/home/teamlarylive/Desktop/data/processed_data/pm/")
list_csv_files_wind <- list.files(path = "/home/teamlarylive/Desktop/data/processed_data/wind/")
list_csv_files_tph <- list.files(path = "/home/teamlarylive/Desktop/data/processed_data/tph/")
#paste(string1, string2)


time_zone_shifting = function (raw_data)
{
  # Create a POSIXct object representing a UTC time
  utc_time <- ymd_hms(raw_data$dateTime, tz = "UTC")
  
  # Convert the UTC time to Central Time
  central_time <- with_tz(utc_time, tzone = "America/Chicago")
  
  raw_data$dateTime =  central_time 
  
  return (raw_data)
}

for (i in 1:length(list_csv_files_pm)) {
  print(i)
  pm_raw_data = read_csv(paste("/home/teamlarylive/Desktop/data/processed_data/pm/",list_csv_files_pm[i],sep = ""))
  # Iterate through every file to see if there are more than 15 columns, for files with more than 15 columns run the code below:
  # Need to modify it slighlty to automate it. After running the code below create a dataframe in julia with all these files combined
  #because the csv files generated still doesnt have data for the all the time of the day as the shift was only done utc datetime so run the
  #Write a julia code for cst csv file generation.
  
  #list_csv_files_pm <- list.files(path = "/home/teamlarylive/Desktop/data/processed_data/pm_tz_changed/")
  #df = read_csv(list_csv_files_pm[218])[,1:15]
  #write.csv(df,"/home/teamlarylive/Desktop/data/processed_data/pm_tz_changed/tz_shifted_Joppa_raw_pm_data_2023-10-12.csv" ,row.names = FALSE)
  
  write.csv(time_zone_shifting(pm_raw_data),paste("/home/teamlarylive/Desktop/data/processed_data/pm_tz_changed/tz_shifted_Joppa_raw_pm_data_",substring(pm_raw_data$dateTime[1],1,10),".csv",sep=""),
            row.names = FALSE)
  wind_raw_data = read_csv(paste("/home/teamlarylive/Desktop/data/processed_data/wind/",list_csv_files_wind[i],sep = ""))
  write.csv(time_zone_shifting(wind_raw_data),paste("/home/teamlarylive/Desktop/data/processed_data/wind_tz_changed/tz_shifted_Joppa_raw_wind_data_",substring(wind_raw_data$dateTime[1],1,10),".csv",sep=""),
            row.names = FALSE)
  tph_raw_data = read_csv(paste("/home/teamlarylive/Desktop/data/processed_data/tph/",list_csv_files_tph[i],sep = ""))
  write.csv(time_zone_shifting(tph_raw_data),paste("/home/teamlarylive/Desktop/data/processed_data/tph_tz_changed/tz_shifted_Joppa_raw_tph_data_",substring(tph_raw_data$dateTime[1],1,10),".csv",sep=""),
            row.names = FALSE)
}


