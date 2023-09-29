using CSV, DataFrames, Dates,Statistics

function resampling_time_series_data(w,tf,df,cols)

    if (w == "s")
        date_time_rounded = map((x) -> round(x, Dates.Second(tf)), df.dateTime)

    elseif (w == "m")
        date_time_rounded = map((x) -> round(x, Dates.Minute(tf)), df.dateTime)

    elseif (w == "h")
        date_time_rounded = map((x) -> round(x, Dates.Hour(tf)), df.dateTime)
  
    elseif (w== "d")
        date_time_rounded = map((x) -> round(x, Dates.Day(tf)), df.dateTime)
        
    elseif (w == "mon")
        date_time_rounded = map((x) -> round(x, Dates.Month(tf)), df.dateTime)

    elseif (w == "y")
        date_time_rounded - map((x) -> round(x, Dates.Year(tf)), df.dateTime)
    end
    df_agg = select(df,Not(:dateTime))
    df_agg.date_time_rounded  = date_time_rounded 
    gdf_date_time =  groupby(df_agg, :date_time_rounded)
    # println(gdf_date_time)
    resampled_timeseries_data = combine(gdf_date_time, valuecols(gdf_date_time) .=> mean)
    df_sensor = DataFrames.rename!(resampled_timeseries_data, cols)
    return df_sensor

end
########################### Every minute ####################################
# Example:
# r =resampling_time_series_data("m",1,data_frame)

