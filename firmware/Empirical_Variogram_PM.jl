#include("/home/teamlarylive/Desktop/Temporal-Variograms/firmware/tz_shifted_file_generation.jl")



using CSV,DataFrames,Dates,Impute, Statistics,Glob,OrderedCollections,BenchmarkTools


function non_overlapping_rolling_variogram(df,col) 
    df_mat = df[:,2:end]
    mat = Matrix(df_mat[1:nrow(df),:])[:,col]
    arr_mat = Array{Float64}[]

    td= 900 # sliding window size of 15 minutes = 900 seconds
    step_size = 1 #for overlapping  sliding window of 15 minutes with gaps of 1 second in between
    # step_size = 900 #for non-overlapping  sliding window of 15 minutes with gaps of 900 second in between
    for i in td:step_size:nrow(df_mat)
        append!(arr_mat, [mat[i+1-td:i]]) 
    end
    
    var_vec = []
    x = []

    for d in 1:1:length(arr_mat)

        for h in 1:1:(300)

            mat_head = arr_mat[d][1:td-h]
            mat_tail = arr_mat[d][1+h:td]
            append!(x,sum((mat_head - mat_tail).^2,dims=1)/(2*(td-h)))    
        end

            push!(var_vec,x)
        println(d)
        x=[]
    end
    return var_vec
end
dict_ips7100 = OrderedDict(1=>"pc0.1",2=>"pc0.3",3=>"pc0.5",4=>"pc1.0",5=>"pc2.5",6=>"pc5.0",7=>"pc10.0",
               8=>"pm0.1",9=>"pm0.3",10=>"pm0.5",11=>"pm1.0",12=>"pm2.5",13=>"pm5.0",14=>"pm10.0")

emp_var_dict = OrderedDict()


pm_files = glob("*.csv", "/home/teamlarylive/Desktop/data/processed_data/pm_cleaned_and_cst_tz/")
for j in 1:1:length(pm_files)
    df_pm = CSV.read(pm_files[j],DataFrame)
    for i in 8:1:14
        println("##################################  ",i,"  #######################################")
        emp_var_dict[dict_ips7100[i]] = non_overlapping_rolling_variogram(df_pm,i)
        df = DataFrame(Matrix(hcat(emp_var_dict[dict_ips7100[i]]...)'),:auto)
        CSV.write("/media/teamlarylive/loraMints1/empirical_variogram_files/"*dict_ips7100[i]*"/"*
        pm_files[j][end-13:end-4]*".csv",df)
    end
end