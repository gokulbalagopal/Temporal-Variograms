
library(openairmaps)
library(leaflet)
library(dplyr)
library(tidygeocoder)
library(lubridate)
library(openair)
sources = read.csv("D:\\UTD\\UTDSpring2023\\Pollution-Sources---Self-reported-emission-data\\SourceLocations.csv")
coordinates <- sources %>%
  geocode(Address)

#traj <- importTraj(site = "london", year = 2010)
#head(traj)

wind_data = read.csv("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Wind_TPH_Range.csv")
#wind_data = wind_data %>% filter(RollingTime >= as.POSIXct('2023-01-03 00:00:00.0') & 
#                       RollingTime <= as.POSIXct('2023-01-03 23:59:59.0'))


pm_range = c(wind_data$pm0.1,wind_data$pm0.3,wind_data$pm0.5,
             wind_data$pm1.0,wind_data$pm2.5,wind_data$pm5.0,
             wind_data$pm10.0)
pm_range = na.omit(pm_range)



lim = quantile(pm_range, c(0.05,.95))
#lim = c(min(pm_range),max(pm_range))
mydata_sample = mydata[c(1:nrow(wind_data)),]
mydata_sample$pm0.1 = wind_data$pm0.1
mydata_sample$ws = wind_data$ws
mydata_sample$wd = wind_data$wd
mydata_sample$lat = c(32.715)
mydata_sample$lon = c(-96.748)
mydata_sample = subset(mydata_sample, select = -c(so2,no2,o3,nox,pm10,co,pm25))
#mydata_samp$pm0_3 = wind_data$pm0_3
#mydata_samp$pm0_5 = wind_data$pm0_5
#mydata_samp$pm1_0 = wind_data$pm1_0
#mydata_samp$pm2_5 = wind_data$pm2_5
#mydata_samp$pm5_0 = wind_data$pm5_0
#mydata_samp$pm10_0 = wind_data$pm10_0

rev_default_col = c("#9E0142","#FA8C4E","#FFFFBF","#88D1A4","#5E4FA2")

#title = "2023-01-01 - 2023-01-07"
title = "2023-01-03"
cols = c("ws","wd")
mydata_sample = mydata_sample[!rowSums(is.na(mydata_sample[cols])), ]

# make map

polarMap(mydata_sample,pollutant = "pm0.1",main = title,k =30,cols = rev_default_col,key.position = "bottom"
         ,  key.footer =NULL,
          limits = c(lim[1],lim[2]),par.settings=list(fontsize=list(text=10))) %>%
  # add markers
addCircleMarkers(data = coordinates,lng = ~long, lat = ~lat,
             popup = ~SITE,
             label = ~SITE)#%>%
  


