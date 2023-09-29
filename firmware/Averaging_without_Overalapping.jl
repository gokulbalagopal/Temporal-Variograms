
include("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\Processing_Data.jl")
include("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\Resampled_Timeseries.jl")
df_pm_15_min = resampling_time_series_data("m",15,df_pm_filtered,cols_pm) 
df_wind = resampling_time_series_data("m",15,df_wind_filtered,cols_wind)
df_tph = resampling_time_series_data("m",15,df_tph_filtered,cols_tph)
df_wind_tph = leftjoin(df_wind,df_tph,on = :dateTime)