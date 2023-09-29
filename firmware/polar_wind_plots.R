library(openair)
library(dplyr)
library(openairmaps)
library(latex2exp)
library(lubridate)

pm_df = read.csv("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\R_csv\\tz_shifted_Joppa_raw_pm_data_Jan_2023.csv")
range_df = read.csv("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Wind_TPH_Range.csv")

# pm_df = pm_df %>%
#         mutate(dateTime = ymd_hms(dateTime))
# pm_data_tibble = as_tibble(pm_df)
# pm_data_tibble$date = pm_data_tibble$dateTime
# 
# calendarPlot(pm_data_tibble, 
#              pollutant = "pm2_5",
#              main = TeX('Calendar Plot of $PM_{2.5}$ Concentration for Joppa - Dallas, TX'),
#              key.header = TeX("Concentration (μg/m$^{3}$)"),
#              key.position = "bottom",
#              par.settings=list(fontsize=list(text=15)))




pm_range = c(range_df$pm0.1,range_df$pm0.3,range_df$pm0.5,
             range_df$pm1.0,range_df$pm2.5,range_df$pm5.0,
             range_df$pm10.0)

pm_range = round(na.omit(pm_range),digits=2)
#Specifying the upper limit and lower limit on the color bar using the pm_range aka all the time scales
lim = quantile(pm_range, c(0.05,.95))

rev_default_col = c("#9E0142","#FA8C4E","#FFFFBF","#88D1A4","#5E4FA2")
title = "01-02-2023"







# Polar plots
range_tibble = tibble(range_df)
polarPlot(range_tibble,pollutant = "pm0.1",main = title,k =30,cols = rev_default_col,key.position = "bottom",
          key.header = TeX('$PM_{0}._1$\\ Measurement Time (Minutes)'),  key.footer =NULL,
          limits = c(lim[1],lim[2]),par.settings=list(fontsize=list(text=10)))
dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pm0.1.pdf')

polarPlot(range_tibble,pollutant = "pm0.3",main = title,k =30,cols = rev_default_col,key.position = "bottom",
          key.header = TeX('$PM_{0}._3$\\ Measurement Time (Minutes)'),  key.footer =NULL,
          limits = c(lim[1],lim[2]),par.settings=list(fontsize=list(text=10)))
dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pm0.3.pdf')

polarPlot(range_tibble,pollutant = "pm0.5",main = title,k =30,cols = rev_default_col,key.position = "bottom",
          key.header = TeX('$PM_{0}._5$\\ Measurement Time (Minutes)'),  key.footer =NULL,
          limits = c(lim[1],lim[2]),par.settings=list(fontsize=list(text=10)))
dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pm0.5.pdf')


polarPlot(range_tibble,pollutant = "pm1.0",main = title,k =30,cols = rev_default_col,key.position = "bottom",
          key.header = TeX('$PM_{1}._0$\\ Measurement Time (Minutes)'),  key.footer =NULL,
          limits = c(lim[1],lim[2]),par.settings=list(fontsize=list(text=10)))
dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pm1.0.pdf')


polarPlot(range_tibble,pollutant = "pm2.5",main = title,k =30,cols = rev_default_col,key.position = "bottom",
          key.header = TeX('$PM_{2}._5$\\ Measurement Time (Minutes)'),  key.footer =NULL,
          limits = c(lim[1],lim[2]),par.settings=list(fontsize=list(text=10)))
dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pm2.5.pdf')


polarPlot(range_tibble,pollutant = "pm5.0",main = title,k =30,cols = rev_default_col,key.position = "bottom",
          key.header = TeX('$PM_{5}._0$\\ Measurement Time (Minutes)'),  key.footer =NULL,
          limits = c(lim[1],lim[2]),par.settings=list(fontsize=list(text=10)))
dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pm5.0.pdf')


polarPlot(range_tibble,pollutant = "pm10.0",main = title,k =30,cols = rev_default_col,key.position = "bottom",
          key.header = TeX('$PM_{10}._0$\\ Measurement Time (Minutes)'),  key.footer =NULL,
          limits = c(lim[1],lim[2]),par.settings=list(fontsize=list(text=10)))
dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pm10.0.pdf')


#Pollution Rose Plots
my.settings <- list( par.main.text = list(font = 7.5, # make it bold
                     just = "left", 
                     x = grid::unit(35, "mm")))

pollution_rose = pollutionRose(range_tibble,
                               pollutant = "pm2.5",
                               fontsize = 7.5,
                               cols = rev_default_col,
                               par.settings=my.settings,
                               main = title,
                               key.header = TeX('$PM_{2}._5$ Measurement Time (Minutes)'),
                               key.footer = " ",
                               key.position = "right")

dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pollution_Rose_for_pm2_5.pdf')



my.settings_tph <- list(
  par.main.text = list( # make it bold
    just = "left", 
    x = grid::unit(30, "mm")))

pollutionRose(range_tibble,
              pollutant = "pm2.5",
              type = "Temperature",
              fontsize = 7.5,
              cols = rev_default_col,
              main = title,
              layout = c(2, 2),
              key.header = TeX('$\\PM_2._5$ Measurement Time (Minutes)'),
              key.footer = " ",
              key.position = "right",
              par.settings= my.settings_tph)
dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pollution_Rose_for_Temperature_pm2_5.pdf')



pollutionRose(range_tibble,
              pollutant = "pm2.5",
              type = "Pressure",
              fontsize = 7.5,
              cols = rev_default_col,
              main = title,
              layout = c(2, 2),
              key.header = TeX('$\\PM_2._5$ Measurement Time (Minutes)'),
              key.footer = " ",
              key.position = "right",
              par.settings= my.settings_tph)
dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pollution_Rose_for_Pressure_pm2_5.pdf')


pollutionRose(range_tibble,
              pollutant = "pm2.5",
              type = "Humidity",
              fontsize = 7.5,
              cols = rev_default_col,
              main = title,
              layout = c(2, 2),
              key.header = TeX('$\\PM_2._5$ Measurement Time (Minutes)'),
              key.footer = " ",
              key.position = "right",
              par.settings= my.settings_tph)

dev.print(pdf, 'D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\pollution_Rose_for_Humidity_pm2_5.pdf')


