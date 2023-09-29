include("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\File_Search.jl")
using CSV,DataFrames,Dates,Impute, Statistics


df_pm_list = []
df_wind_list = []
df_tph_list = []
n_days = 3 # We are only filtering out first 3 days  of January 2023
for i in 1:1:n_days
    push!(df_pm_list, CSV.read(df_pm_csv.IPS7100[i],DataFrame))
    push!(df_wind_list, CSV.read(df_wind_csv.WIMDA[i],DataFrame))
    push!(df_tph_list, CSV.read(df_tph_csv.BME680[i],DataFrame))
end
data_frame_pm_combined = reduce(vcat,df_pm_list)
data_frame_wind_combined = reduce(vcat,df_wind_list)
data_frame_tph_combined = reduce(vcat,df_tph_list)

CSV.write("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Joppa_raw_pm_data_combined_Jan_2023.csv",data_frame_pm_combined)
CSV.write("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Joppa_raw_wind_data_combined_Jan_2023.csv",data_frame_wind_combined)
CSV.write("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Joppa_raw_tph_data_combined_Jan_2023.csv",data_frame_tph_combined)

data_frame_pm_combined = CSV.read("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\R_csv\\tz_shifted_Joppa_raw_pm_data_Jan_2023.csv", DataFrame)
data_frame_wind_combined = CSV.read("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\R_csv\\tz_shifted_Joppa_raw_wind_data_Jan_2023.csv", DataFrame)
data_frame_tph_combined = CSV.read("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\R_csv\\tz_shifted_Joppa_raw_tph_data_Jan_2023.csv", DataFrame)

 
#delete!(data_frame_wind_combined, [298953,321329])

function data_cleaning( data_frame,sensor_type) 
    if(sensor_type == "IPS7100")
        cols = propertynames(data_frame)
    elseif(sensor_type == "WIMDA")
        cols = [:dateTime,:windDirectionTrue,:windSpeedMetersPerSecond]
    elseif (sensor_type == "BME680")
        cols = [:dateTime,:temperature,:pressure,:humidity]
    # elseif (sensor_type == "SCD30")
    #     cols = [:dateTime,:c02]
    end 

    data_frame.dateTime = Array(data_frame.dateTime)
    k=[]
    for i in 1:1:length(data_frame.dateTime)
        #println(i)
        push!(k,DateTime(data_frame.dateTime[i],"yyyy-mm-dd HH:MM:SS")) 
    end
    data_frame.dateTime = k 
    #data_frame.dateTime = data_frame.dateTime + data_frame.ms
    #data_frame = select!(data_frame, Not(:ms))

    data_frame = data_frame[:,cols]
    col_symbols = Symbol.(names(data_frame))
    data_frame = DataFrames.combine(DataFrames.groupby(data_frame, :dateTime), col_symbols[2:end] .=> mean)
    return data_frame,col_symbols
end
data_frame_pm,cols_pm = data_cleaning(data_frame_pm_combined,"IPS7100")
data_frame_wind,cols_wind = data_cleaning(data_frame_wind_combined,"WIMDA")
data_frame_tph,cols_tph = data_cleaning(data_frame_tph_combined,"BME680")



function dataframe_updates(data_frame,cols)
    # data_frame = data_frame_wind
    #  sensor_type = "WIMDA"
    #  cols = cols_wind 
    duration = Second(data_frame.dateTime[end]-data_frame.dateTime[1]).value
    time_to_round = Int(floor(duration/size(data_frame)[1]))
    data_frame.dateTime = round.(data_frame.dateTime, Dates.Second(time_to_round))
    
    ###################  imputation logic may be fixed  ###################### 
    df = DataFrame()
    df.dateTime = collect(data_frame.dateTime[1]:Second(time_to_round):data_frame.dateTime[end]-Second(1))
    df = outerjoin( df,data_frame, on = :dateTime)
    sort!(df, (:dateTime))
    unique!(df, :dateTime)
    println(cols)
    df = DataFrames.rename!(df, cols)
    df_sensor = Impute.locf(df)|>Impute.nocb()
    
    df_sensor = DataFrames.combine(DataFrames.groupby(df_sensor, :dateTime), cols[2:end] .=> mean)
    df_sensor = DataFrames.rename!(df_sensor, cols)
    return df_sensor
end
df_pm_updated = dataframe_updates(data_frame_pm, cols_pm)
df_wind_updated = dataframe_updates(data_frame_wind, cols_wind)
df_tph_updated = dataframe_updates(data_frame_tph,cols_tph)
#df_co2 = dataframe_updates(data_frame_c02,cols_c02,"SCD30")


function date_based_data_filtering(df, start_date, end_date)
    df[DateTime(start_date) .<= df.dateTime .< DateTime(end_date), :] #filtering out data based on start and end date
end
start_date = "2023-01-02"
end_date = "2023-01-03"

df_pm_filtered = date_based_data_filtering(df_pm_updated,start_date,end_date)
df_wind_filtered = date_based_data_filtering(df_wind_updated,start_date,end_date)
df_tph_filtered = date_based_data_filtering(df_tph_updated,start_date,end_date)

