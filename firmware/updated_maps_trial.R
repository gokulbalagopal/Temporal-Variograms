
library(openairmaps)
library(leaflet)
library(latex2exp)
library(dplyr)
library(tidygeocoder)
library(lubridate)
library(openair)
sources = read.csv("D:\\UTD\\UTDSpring2023\\Pollution-Sources---Self-reported-emission-data\\updated_source_locations\\Source_Locations_updated.csv")
library(tidyverse)

pm_data = read.csv("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Joppa_Jan_2_hourly_max_values.csv")
pm_data$dateTime = ymd_hms(pm_data$dateTime,tz=Sys.timezone())



traj_jan_1sthour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010207.csv")
traj_jan_2ndhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010208.csv")
traj_jan_3rdhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010209.csv")
traj_jan_4thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010210.csv")
traj_jan_5thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010211.csv")
traj_jan_6thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010212.csv")
traj_jan_7thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010213.csv")
traj_jan_8thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010214.csv")

traj_jan_9thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010215.csv")
traj_jan_10thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010216.csv")
traj_jan_11thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010217.csv")
traj_jan_12thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010218.csv")
traj_jan_13thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010219.csv")
traj_jan_14thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010220.csv")
traj_jan_15thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010221.csv")
traj_jan_16thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010222.csv")


traj_jan_17thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010223.csv")
traj_jan_18thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010300.csv")
traj_jan_19thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010301.csv")
traj_jan_20thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010302.csv")
traj_jan_21sthour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010303.csv")
traj_jan_22ndhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010304.csv")
traj_jan_23rdhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010305.csv")
traj_jan_24thhour = read.csv("D:\\UTD\\UTDSummer2023\\MintsHysplit\\processed_data\\csv\\gokuljan0006winter2023010306.csv")


data_cleaning = function(traj_jan){
  hrs= 6*60*60
  traj_jan_sample  = traj_data[1:nrow(traj_jan),]
  traj_jan_sample$date = as.POSIXct(traj_jan$DateTime_1) - hrs
  traj_jan_sample$pressure  = traj_jan$Pressure
  traj_jan_sample$height  = traj_jan$alt
  traj_jan_sample$lat = traj_jan$lat
  traj_jan_sample$lon = traj_jan$lon
  traj_jan_sample$date2 = ymd_hms(traj_jan$DateTime,tz=Sys.timezone()) - hrs
  traj_jan_sample$year =  as.numeric(format(traj_jan_sample$date2, format = "%Y"))
  traj_jan_sample$month = as.numeric(format(traj_jan_sample$date2, format = "%m"))
  traj_jan_sample$day = as.numeric(format(traj_jan_sample$date2, format = "%d"))
  traj_jan_sample$hour = as.numeric(format(traj_jan_sample$date2, format = "%H"))
  traj_jan_sample$pm2.5 = traj_jan$pm2_5
  return (traj_jan_sample)
}

traj_jan_1sthour_tb = data_cleaning(traj_jan_1sthour)
traj_jan_2ndhour_tb = data_cleaning(traj_jan_2ndhour)
traj_jan_3rdhour_tb = data_cleaning(traj_jan_3rdhour)
traj_jan_4thhour_tb = data_cleaning(traj_jan_4thhour)
traj_jan_5thhour_tb = data_cleaning(traj_jan_5thhour)
traj_jan_6thhour_tb = data_cleaning(traj_jan_6thhour)
traj_jan_7thhour_tb = data_cleaning(traj_jan_7thhour)
traj_jan_8thhour_tb = data_cleaning(traj_jan_8thhour)

traj_jan_9thhour_tb = data_cleaning(traj_jan_9thhour)
traj_jan_10thhour_tb = data_cleaning(traj_jan_10thhour)
traj_jan_11thhour_tb = data_cleaning(traj_jan_11thhour)
traj_jan_12thhour_tb = data_cleaning(traj_jan_12thhour)
traj_jan_13thhour_tb = data_cleaning(traj_jan_13thhour)
traj_jan_14thhour_tb = data_cleaning(traj_jan_14thhour)
traj_jan_15thhour_tb = data_cleaning(traj_jan_15thhour)
traj_jan_16thhour_tb = data_cleaning(traj_jan_16thhour)


traj_jan_17thhour_tb = data_cleaning(traj_jan_17thhour)
traj_jan_18thhour_tb = data_cleaning(traj_jan_18thhour)
traj_jan_19thhour_tb = data_cleaning(traj_jan_19thhour)
traj_jan_20thhour_tb = data_cleaning(traj_jan_20thhour)
traj_jan_21sthour_tb = data_cleaning(traj_jan_21sthour)
traj_jan_22ndhour_tb = data_cleaning(traj_jan_22ndhour)
traj_jan_23rdhour_tb = data_cleaning(traj_jan_23rdhour)
traj_jan_24thhour_tb = data_cleaning(traj_jan_24thhour)
shapes <- c("circle")
make_shapes <- function(shapes) {
  shapes <- gsub("circle", "50%", shapes)
  #shapes <- gsub("square", "0%", shapes)
  paste0(c("blue"), "; width:", c(10), "px; height:", c(10), "px; border:3px solid ", c("blue"), "; border-radius:", shapes)
}
leaflet(sources) %>% addTiles() %>%
  addCircleMarkers(radius = 0.25,data = sources,lng = ~LONGITUDE, lat = ~LATITUDE,
                   popup = ~ADDRESS,color = "blue",
                   label = ~COMPANY)%>%
  addLegend("topleft",colors = make_shapes(shapes),labels = c("Pollution Source"))%>%
  addTrajPaths(data = traj_jan_1sthour_tb,
               color = "#FF8080")%>%
  addTrajPaths(data = traj_jan_2ndhour_tb,
               color = "#D10000")%>%
  addTrajPaths(data = traj_jan_3rdhour_tb,
               color = "#FF8080")%>%
  addTrajPaths(data = traj_jan_4thhour_tb,
               color = "#FF8080")%>%
  addTrajPaths(data = traj_jan_5thhour_tb,
               color = "#B00000")%>%
  addTrajPaths(data = traj_jan_6thhour_tb,
               color = "#FF8080")%>%
  addTrajPaths(data = traj_jan_7thhour_tb,
               color = "#FF8080")%>%
  addTrajPaths(data = traj_jan_8thhour_tb,
             color = "#FF8080")%>%
  
  addTrajPaths(data = traj_jan_9thhour_tb,
               color = "#FF8080")%>%
  addTrajPaths(data = traj_jan_10thhour_tb,
               color = "#FF8080")%>%
  addTrajPaths(data = traj_jan_11thhour_tb,
               color = "#FFB3B3")%>%
  addTrajPaths(data = traj_jan_12thhour_tb,
               color = "#FF8080")%>%
  addTrajPaths(data = traj_jan_13thhour_tb,
               color = "#FFB3B3")%>%
  addTrajPaths(data = traj_jan_14thhour_tb,
               color = "#FFB3B3")%>%
  addTrajPaths(data = traj_jan_15thhour_tb,
               color = "#FFB3B3")%>%
  addTrajPaths(data = traj_jan_16thhour_tb,
               color = "#FFB3B3")%>%
  
  
  addTrajPaths(data = traj_jan_17thhour_tb,
               color = "#FFB3B3")%>%
  addTrajPaths(data = traj_jan_18thhour_tb,
               color = "#2B0000")%>%
  addTrajPaths(data = traj_jan_19thhour_tb,
               color = "#B00000")%>%
  addTrajPaths(data = traj_jan_20thhour_tb,
               color = "#B00000")%>%
  addTrajPaths(data = traj_jan_21sthour_tb,
               color = "#6D0000")%>%
  addTrajPaths(data = traj_jan_22ndhour_tb,
               color = "#F30000")%>%
  addTrajPaths(data = traj_jan_23rdhour_tb,
               color = "#FF4C4C")%>%
  addTrajPaths(data = traj_jan_24thhour_tb,
               color = "#FFB3B3")%>%

    
  addLegend("topright", 
            colors = c("#2B0000",
                       "#4C0000",
                       "#6D0000",
                       "#8F0000",
                       "#B00000",
                       "#D10000",
                       "#F30000",
                       "#FF4C4C",
                       "#FF8080",
                       "#FFB3B3" ),
            labels = c(">1000","800-1000","600-800","500-600", "400-500", "300-400", "200-300", "100-200","50-100","0-50"),
            title =  "PM₂.₅ Concentration (µg/m³)",
            opacity = 1)%>% addMarkers(lng = -96.748, lat = 32.715,label = "Joppa Central Node")


  
#write.csv(traj_data, "D:\\traj.csv", row.names=FALSE)

