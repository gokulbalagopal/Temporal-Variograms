include("/home/teamlarylive/Desktop/Temporal-Variograms/firmware/Processing_Data.jl")

pm_combined = []
wind_combined = []
tph_combined = []

pm_files = glob("*.csv","/home/teamlarylive/Desktop/data/processed_data/pm_cleaned/")
wind_files = glob("*.csv", "/home/teamlarylive/Desktop/data/processed_data/wind_cleaned/")
tph_files = glob("*.csv", "/home/teamlarylive/Desktop/data/processed_data/tph_cleaned/")
for i in 1:1:n_days
    println(i)
    push!(pm_combined, CSV.read(pm_files[i], DataFrame))
    push!(wind_combined, CSV.read(wind_files[i], DataFrame))
    push!(tph_combined, CSV.read(tph_files[i], DataFrame))
end

df1 = vcat(pm_combined...)
df2 = vcat(wind_combined...)
df3 = vcat(tph_combined...)

function cst_csvfiles(df,path)
    df.date = Dates.Date.(df.dateTime)
    start_date = Dates.Date(2023, 1, 1)  # Replace with your desired start date
    end_date = Dates.Date(2023, 12, 31)  # Replace with your desired end date

# Subset the DataFrame by start and end dates for 2023 ,to remove Dec 31st 20221
    subset_df = filter(row -> start_date <= row.date <= end_date, df)
    grouped_data = groupby(subset_df, :date)

    for day_data in grouped_data
        current_date = day_data[1, :date]
        csv_filename = "data_$(Dates.format(current_date, "yyyy-mm-dd")).csv"
        
        # Check if the DataFrame is not empty
        if !isempty(day_data)
            # Save the day's data as a CSV file
            # size(day_data)[2]-1 removes the date column from df
            CSV.write(path*csv_filename, day_data[:,1:size(day_data)[2]-1], writeheader=true) 
            println("Saved data for $(current_date) to $csv_filename")
        else
            println("No data for $(current_date). Skipping...")
        end
    end
end
cst_csvfiles(df1,"/home/teamlarylive/Desktop/data/processed_data/pm_cleaned_and_cst_tz/")
cst_csvfiles(df2,"/home/teamlarylive/Desktop/data/processed_data/wind_cleaned_and_cst_tz/")
cst_csvfiles(df3,"/home/teamlarylive/Desktop/data/processed_data/tph_cleaned_and_cst_tz/")