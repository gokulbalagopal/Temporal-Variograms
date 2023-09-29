using Pkg
Pkg.activate("D:\\UTD\\UTDFall2023\\Temporal_Variograms")
#using DelimitedFiles
using CSV,DataFrames,Dates
#using DataStructures



function list_csv(path_to_data,sensor_name)
    fulldirpaths = filter(isdir,readdir(path_to_data,join=true)) # Adding the subdirectory(year folders) to the above mentioned path 
    fulldirpaths_month = [] # for storing the path with months
    fulldirpaths_days = [] # for storing the path with days
    date_str = [] # adding the dates
    append!(fulldirpaths_month,filter(isdir,readdir(fulldirpaths[1],join=true)))
    for i in 1:1:length(fulldirpaths_month)
        append!(fulldirpaths_days, (filter(isdir,readdir(fulldirpaths_month[i],join=true))))
    end
    append!(date_str,last.(fulldirpaths_days,10))# Appending the date string in yyyy\\mm\\dd format

    date_str = replace.(date_str, "\\" => "-",count=3)
    csv_date = Date.(date_str, "yyyy-mm-dd")# Converted date string tto date time format

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
    

    deleteat!(csv_date, findall(x->x == -1, index_sensor))
 
    df_csv_path = DataFrame()
    df_csv_path.Date = csv_date

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
    df_csv_path[!,sensor_name] =  list_sensor_csv
    return df_csv_path
end

# path to all csv files for gunter Central node
path_to_pm_data = "D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\001e0636e547\\" 
path_to_wind_data = "D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\001e06430225\\"
path_to_tph_data = "D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\001e0636e547\\"
df_pm_csv = list_csv(path_to_pm_data,"IPS7100")
df_wind_csv  = list_csv(path_to_wind_data,"WIMDA")
df_tph_csv = list_csv(path_to_tph_data,"BME680")

