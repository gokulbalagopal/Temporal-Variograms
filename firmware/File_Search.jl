using Pkg
Pkg.activate("/home/teamlarylive/Desktop/Temporal-Variograms")

#using DelimitedFiles
using CSV,DataFrames,Dates

function list_csv(path_to_data,sensor_name)
    fulldirpaths = filter(isdir,readdir(path_to_data,join=true)) # Adding the subdirectory(year folders) to the above mentioned path 
    fulldirpaths_month = [] # for storing the path with months
    fulldirpaths_days = [] # for storing the path with days
    date_str = [] # adding the dates
    for i in 1:1:length(fulldirpaths)
        append!(fulldirpaths_month,filter(isdir,readdir(fulldirpaths[i],join=true)))
    end
    for i in 1:1:length(fulldirpaths_month)
        append!(fulldirpaths_days, (filter(isdir,readdir(fulldirpaths_month[i],join=true))))
    end

    # This is how the files name should end ======> IPS7100_2022_10_05.csv

    list_csv_path = readdir.(fulldirpaths_days; join=true) # find the list of files in each day folder
    index_sensor = []
    #This loop helps in finding the missing sensor path
   
    for i in list_csv_path
       if (1 in Int.(occursin.(sensor_name,i)))
        append!(index_sensor,findall(x->x==1, Int.(occursin.(sensor_name,i))))
       else
        append!(index_sensor,-1)  
       end    
    end

    list_sensor_csv = []
    for i in 1:1:length(index_sensor)
        try
        push!(list_sensor_csv,list_csv_path[i][index_sensor[i]]) 
        catch l1
            push!(list_sensor_csv,"missing") 
        end
    end
    filter!(x -> x != "missing", list_sensor_csv)

    println(length(list_sensor_csv))

    csv_date = []
    for i in 1: length(list_sensor_csv)
        # if (sensor_name == "WIMDA" && i == 28)
        #     list_sensor_csv[28] = list_sensor_csv[28][1:end-1]
        # end
        date_str = list_sensor_csv[i][end-13:end-4]
        date_str = replace.(date_str, "_" => "-")
        println(date_str)
        push!(csv_date,Date.(date_str, "yyyy-mm-dd"))# Converted date string tto date time format

    end
    df_csv_path = DataFrame()
    df_csv_path.Date = csv_date

    df_csv_path[!,sensor_name] =  list_sensor_csv
    return df_csv_path
end

# path to all csv files for gunter Central node
path_to_pm_data = "/home/teamlarylive/Desktop/data/Joppa/001e0636e547/" 
path_to_wind_data = "/home/teamlarylive/Desktop/data/Joppa/001e06430225/"
path_to_tph_data = "/home/teamlarylive/Desktop/data/Joppa/001e0636e547/"
df_pm_csv = list_csv(path_to_pm_data,"IPS7100")
df_wind_csv  = list_csv(path_to_wind_data,"WIMDA")
df_tph_csv = list_csv(path_to_tph_data,"BME680")


# Merge data frames based on the DateTime column
merged_df_paths = outerjoin(outerjoin(df_pm_csv, df_wind_csv, on = :Date), df_tph_csv, on = :Date)

# Print the merged data frame
println(merged_df_paths)
