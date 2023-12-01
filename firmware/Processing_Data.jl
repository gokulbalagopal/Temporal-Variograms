include("/home/teamlarylive/Desktop/Temporal-Variograms/firmware/File_Search.jl")
using CSV,DataFrames,Dates,Impute, Statistics,Glob,OrderedCollections,BenchmarkTools



# n_days =  size(merged_df_paths)[1]
function write_csv(merged_df_paths,sensor_type,sensor_name,num_of_cols)
    df = select(merged_df_paths, ["Date", sensor_name])       
    dropmissing!(df, sensor_name)                           
    n_days = size(df)[1]
    for i in 1:1:n_days
        println(i)
        df_daily = CSV.read(df[!,sensor_name][i],DataFrame)[:,1:num_of_cols]

        CSV.write("/home/teamlarylive/Desktop/data/processed_data/"*sensor_type*"/Joppa_"*sensor_name*"_"*string(df.Date[i])*".csv",df_daily)
    end  
end

write_csv(merged_df_paths,"pm","IPS7100",15)
write_csv(merged_df_paths,"wind","WIMDA",22)
write_csv(merged_df_paths,"tph","BME680",5)
glob("*.csv", "/home/teamlarylive/Desktop/data/processed_data/pm/")
glob("*.csv", "/home/teamlarylive/Desktop/data/processed_data/wind/")
glob("*.csv", "/home/teamlarylive/Desktop/data/processed_data/tph/")
################### Run the R code for Time Zone Change ######################
# data_frame_pm_combined = reduce(vcat,df_pm_list)
# data_frame_wind_combined = reduce(vcat,df_wind_list)
# data_frame_tph_combined = reduce(vcat,df_tph_list)

# Fix this ....
pm_files = glob("*.csv", "/home/teamlarylive/Desktop/data/processed_data/pm_tz_changed/")
wind_files = glob("*.csv", "/home/teamlarylive/Desktop/data/processed_data/wind_tz_changed/")
tph_files = glob("*.csv", "/home/teamlarylive/Desktop/data/processed_data/tph_tz_changed/")




   
# df = vcat(pm_combined...)
# Dates.Date.(df.dateTime) 




function data_wrangling(path_df,n_days,sensor_files,num_of_cols,sensor_type,sensor_folder)
    function missing_values(df)
        df = ifelse.(df.== "NA", missing, df)
        df = dropmissing(df, :dateTime)
    end
    
    for i in 1:1:n_days
        println(i)
        data_frame = CSV.read(sensor_files[i], DataFrame)[:,1:num_of_cols]
        data_frame_combined = missing_values(data_frame) 
        
        function type_conversion(df,cols)
            for c in cols[2:end]
                if(eltype(df[!,c])!= Float64 && eltype(df[!,c])!= Int64)
                    # println(eltype(df[!,c]))
                    # println(c)
                    df[!,c] = passmissing(parse).(Float64, df[!,c] );
                end
            end
            return df
        end

        function data_cleaning( data_frame,sensor_type) 
            if(sensor_type == "IPS7100")
                cols = [:dateTime, :pc0_1, :pc0_3, :pc0_5, :pc1_0, :pc2_5, :pc5_0, :pc10_0, 
                        :pm0_1, :pm0_3, :pm0_5, :pm1_0, :pm2_5, :pm5_0, :pm10_0]
            elseif(sensor_type == "WIMDA")
                cols = [:dateTime,:windDirectionTrue,:windSpeedMetersPerSecond]
            elseif (sensor_type == "BME680")
                cols = [:dateTime,:temperature,:pressure,:humidity]
            # elseif (sensor_type == "SCD30")
            #     cols = [:dateTime,:c02]
            end 
            data_frame = type_conversion(data_frame,cols)
    
            data_frame.dateTime = Array(data_frame.dateTime)
            k=[]
            for i in 1:1:length(data_frame.dateTime)
                #println(data_frame.dateTime[i])
                push!(k,DateTime(data_frame.dateTime[i],"yyyy-mm-dd HH:MM:SS")) 
            end
            data_frame.dateTime = k 
            #data_frame.dateTime = data_frame.dateTime + data_frame.ms
            #data_frame = select!(data_frame, Not(:ms))
    
            data_frame = data_frame[:,cols]
            col_symbols = Symbol.(names(data_frame))
     
            #println(DataFrames.groupby(data_frame, :dateTime))
            data_frame = DataFrames.combine(DataFrames.groupby(data_frame, :dateTime), col_symbols[2:end] .=> mean)
            sort!(data_frame, :dateTime)
            return data_frame,col_symbols
        end
        data_frame,cols = data_cleaning(data_frame_combined,sensor_type)

        function dataframe_updates(data_frame,cols)
            # println("cols ",cols)
            duration = Second(sort!(unique(data_frame.dateTime))[end] - sort!(unique(data_frame.dateTime))[1]).value
    
            #println("df ",data_frame)
            time_to_round = Int(floor(duration/size(data_frame)[1]))
    
            data_frame.dateTime = round.(data_frame.dateTime, Dates.Second(time_to_round))
            
            ###################  imputation logic may be fixed  ###################### 
            df = DataFrame()
            df.dateTime = collect(data_frame.dateTime[1]:Second(time_to_round):data_frame.dateTime[end]-Second(1))
            df = outerjoin( df,data_frame, on = :dateTime)
            sort!(df, (:dateTime))
            unique!(df, :dateTime)
            # println(cols)
            df = DataFrames.rename!(df, cols)
            df_sensor = Impute.locf(df)|>Impute.nocb()
            
            df_sensor = DataFrames.combine(DataFrames.groupby(df_sensor, :dateTime), cols[2:end] .=> mean)
            df_sensor = DataFrames.rename!(df_sensor, cols)
            return df_sensor
        end
        df_updated = dataframe_updates(data_frame, cols)
        CSV.write("/home/teamlarylive/Desktop/data/processed_data/"*sensor_folder*"/Joppa_"*sensor_type*"_"*string(path_df.Date[i])*".csv",df_updated)
    end

end

data_wrangling(df_pm_csv,length(pm_files),pm_files,15,"IPS7100","pm_cleaned")
data_wrangling(df_wind_csv,length(wind_files),wind_files,22,"WIMDA","wind_cleaned")
data_wrangling(df_tph_csv,length(tph_files),tph_files,5,"BME680","tph_cleaned")







# for i in 1:1:n_days
#     println(i)
#     data_frame_pm_combined = CSV.read( [i], DataFrame)[:,1:15]
#     data_frame_wind_combined = CSV.read(wind_files[i], DataFrame)[:,1:22]
#     data_frame_tph_combined = CSV.read(tph_files[i], DataFrame)[:,1:5]

#     data_frame_pm_combined = missing_values(data_frame_pm_combined)
#     data_frame_wind_combined = missing_values(data_frame_wind_combined)
#     data_frame_tph_combined = missing_values(data_frame_tph_combined)
    
#     #delete!(data_frame_wind_combined, [298953,321329])
#     function type_conversion(df,cols)
#         for c in cols[2:end]
#             if(eltype(df[!,c])!= Float64 && eltype(df[!,c])!= Int64)
#                 # println(eltype(df[!,c]))
#                 # println(c)
#                 df[!,c] = passmissing(parse).(Float64, df[!,c] );
#             end
#         end
#         return df
#     end

#     function data_cleaning( data_frame,sensor_type) 
#         if(sensor_type == "IPS7100")
#             cols = [:dateTime, :pc0_1, :pc0_3, :pc0_5, :pc1_0, :pc2_5, :pc5_0, :pc10_0, 
#                     :pm0_1, :pm0_3, :pm0_5, :pm1_0, :pm2_5, :pm5_0, :pm10_0]
#         elseif(sensor_type == "WIMDA")
#             cols = [:dateTime,:windDirectionTrue,:windSpeedMetersPerSecond]
#         elseif (sensor_type == "BME680")
#             cols = [:dateTime,:temperature,:pressure,:humidity]
#         # elseif (sensor_type == "SCD30")
#         #     cols = [:dateTime,:c02]
#         end 
#         data_frame = type_conversion(data_frame,cols)

#         data_frame.dateTime = Array(data_frame.dateTime)
#         k=[]
#         for i in 1:1:length(data_frame.dateTime)
#             #println(data_frame.dateTime[i])
#             push!(k,DateTime(data_frame.dateTime[i],"yyyy-mm-dd HH:MM:SS")) 
#         end
#         data_frame.dateTime = k 
#         #data_frame.dateTime = data_frame.dateTime + data_frame.ms
#         #data_frame = select!(data_frame, Not(:ms))

#         data_frame = data_frame[:,cols]
#         col_symbols = Symbol.(names(data_frame))
 
#         #println(DataFrames.groupby(data_frame, :dateTime))
#         data_frame = DataFrames.combine(DataFrames.groupby(data_frame, :dateTime), col_symbols[2:end] .=> mean)
#         sort!(data_frame, :dateTime)
#         return data_frame,col_symbols
#     end

#     data_frame_pm,cols_pm = data_cleaning(data_frame_pm_combined,"IPS7100")
#     data_frame_wind,cols_wind = data_cleaning(data_frame_wind_combined,"WIMDA")
#     data_frame_tph,cols_tph = data_cleaning(data_frame_tph_combined,"BME680")



#     function dataframe_updates(data_frame,cols)
#         println("cols ",cols)
#         duration = Second(sort!(unique(data_frame.dateTime))[end] - sort!(unique(data_frame.dateTime))[1]).value

#         #println("df ",data_frame)
#         time_to_round = Int(floor(duration/size(data_frame)[1]))

#         data_frame.dateTime = round.(data_frame.dateTime, Dates.Second(time_to_round))
        
#         ###################  imputation logic may be fixed  ###################### 
#         df = DataFrame()
#         df.dateTime = collect(data_frame.dateTime[1]:Second(time_to_round):data_frame.dateTime[end]-Second(1))
#         df = outerjoin( df,data_frame, on = :dateTime)
#         sort!(df, (:dateTime))
#         unique!(df, :dateTime)
#         # println(cols)
#         df = DataFrames.rename!(df, cols)
#         df_sensor = Impute.locf(df)|>Impute.nocb()
        
#         df_sensor = DataFrames.combine(DataFrames.groupby(df_sensor, :dateTime), cols[2:end] .=> mean)
#         df_sensor = DataFrames.rename!(df_sensor, cols)
#         return df_sensor
#     end
#     df_pm_updated = dataframe_updates(data_frame_pm, cols_pm)
#     df_wind_updated = dataframe_updates(data_frame_wind, cols_wind)
#     df_tph_updated = dataframe_updates(data_frame_tph,cols_tph)


#     CSV.write("/home/teamlarylive/Desktop/data/processed_data/pm_cleaned/Joppa_IPS7100_"*string(df_pm_csv.Date[i])*".csv",df_pm_updated)
#     CSV.write("/home/teamlarylive/Desktop/data/processed_data/wind_cleaned/Joppa_WIMDA_"*string(df_wind_csv.Date[i])*".csv",df_wind_updated)
#     CSV.write("/home/teamlarylive/Desktop/data/processed_data/tph_cleaned/Joppa_BME680_"*string(df_tph_csv.Date[i])*".csv",df_tph_updated)
# end

