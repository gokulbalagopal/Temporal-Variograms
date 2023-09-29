include("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\Processing_Data.jl")
using RollingFunctions
using Plots
td= 900
n_days = 1
ts_pm =  Int(floor((n_days*24*60*60)/size(df_pm_filtered)[1]))
rolling_time_pm = collect(df_pm_filtered.dateTime[1]+Minute(15):Second(ts_pm):df_pm_filtered.dateTime[end]+Second(ts_pm))
df_pm_average = DataFrame(dateTime = rolling_time_pm)

for i in names(df_pm_filtered)
    if(i != "dateTime")
        pm_rolling_mean = RollingFunctions.rolling(mean,df_pm_filtered[!,i],Int(td/ts_pm))
        df_pm_average[!,i] = pm_rolling_mean
    end
end

td= 900
n_days = 1

ts_wind =  Int(floor((n_days*24*60*60)/size(df_wind_filtered)[1]))
wd_rolling_mean = RollingFunctions.rolling(mean,df_wind_filtered.windDirectionTrue,Int(td/ts_wind))
ws_rolling_mean = RollingFunctions.rolling(mean,df_wind_filtered.windSpeedMetersPerSecond,Int(td/ts_wind))
rolling_time_wind = collect(df_wind_filtered.dateTime[1]+Minute(15):Second(ts_wind):df_wind_filtered.dateTime[end]+Second(ts_wind))

ts_tph = Int(floor((n_days*24*60*60)/size(df_tph_filtered)[1]))
temp_rolling_mean = rolling(mean,df_tph_filtered.temperature,Int(td/ts_tph))
press_rolling_mean = rolling(mean,df_tph_filtered.pressure,Int(td/ts_tph))
hum_rolling_mean = rolling(mean,df_tph_filtered.humidity,Int(td/ts_tph))
rolling_time_tph = collect(df_tph_filtered.dateTime[1]+Minute(15):Second(ts_tph):df_tph_filtered.dateTime[end]+Second(ts_tph))


df_wind_avg = DataFrame(dateTime = rolling_time_wind,
                        ws = ws_rolling_mean,
                        wd = wd_rolling_mean)

df_tph_avg = DataFrame(dateTime = rolling_time_tph,
                       Temperature = temp_rolling_mean,
                       Pressure = press_rolling_mean,
                       Humidity = hum_rolling_mean)

range_df = CSV.read("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Range_Jan_2nd.csv",DataFrame)
sill_df = CSV.read("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Sill_Jan_2nd.csv",DataFrame)
nugget_df = CSV.read("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Nugget_Jan_2nd.csv",DataFrame)

df_range_wind_tph_var = sort!(outerjoin(outerjoin(range_df,df_wind_avg,on = :dateTime),df_tph_avg,on = :dateTime),[:dateTime])
df_sill_wind_tph_var = sort!(outerjoin(outerjoin(sill_df,df_wind_avg,on = :dateTime),df_tph_avg,on = :dateTime),[:dateTime])
df_nugget_wind_tph_var = sort!(outerjoin(outerjoin(nugget_df,df_wind_avg,on = :dateTime),df_tph_avg,on = :dateTime),[:dateTime])

CSV.write("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Wind_TPH_Range.csv",df_range_wind_tph_var)
CSV.write("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Wind_TPH_Sill.csv",df_sill_wind_tph_var)
CSV.write("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Wind_TPH_Nugget.csv",df_nugget_wind_tph_var)


using StatsBase
# Fit a linear regression model
corr = corkendall(df_pm_average[!,"pm2_5"] , Array(range_df[!,"pm2.5"]))

######################### Interesting Facts #####################################
# Minimum pm 2.5 measurement time
range_df[range_df[!,"pm2.5"] .== minimum(range_df[!,"pm2.5"]),:] 
data_frame_pm_combined_filtered = date_based_data_filtering(data_frame_pm_combined,start_date,end_date)
# Maximum pm 2.5 Conc.
data_frame_pm_combined_filtered[data_frame_pm_combined_filtered.pm2_5 .== maximum(data_frame_pm_combined_filtered.pm2_5),:]
println("Measurement Time Time Window start point",range_df[range_df[!,"pm2.5"] .== minimum(range_df[!,"pm2.5"]),:].dateTime)
println("PM Concentration Peak time",data_frame_pm_combined_filtered[data_frame_pm_combined_filtered.pm2_5 .== maximum(data_frame_pm_combined_filtered.pm2_5),:].dateTime)

println("Measurement Time Time Window start point",range_df[range_df[!,"pm10.0"] .== minimum(range_df[!,"pm10.0"]),:].dateTime)
println("PM Concentration Peak time",data_frame_pm_combined_filtered[data_frame_pm_combined_filtered.pm10_0 .== maximum(data_frame_pm_combined_filtered.pm10_0),:].dateTime)

# ######################### #####################################
# # Minimum pm 2.5 measurement time
# range_df[range_df[!,"pm2.5"] .== maximum(range_df[!,"pm2.5"]),:]
# data_frame_pm_combined_filtered = date_based_data_filtering(data_frame_pm_combined,start_date,end_date) 
# # Minimum pm 2.5 Conc.
# data_frame_pm_combined_filtered[data_frame_pm_combined_filtered.pm2_5 .== minimum(data_frame_pm_combined_filtered.pm2_5),:]
# println("Measurement Time Time Window start point",range_df[range_df[!,"pm2.5"] .== maximum(range_df[!,"pm2.5"]),:].dateTime)
# println("PM Concentration Peak time",data_frame_pm_combined_filtered[data_frame_pm_combined_filtered.pm2_5 .== minimum(data_frame_pm_combined_filtered.pm2_5),:].dateTime)
