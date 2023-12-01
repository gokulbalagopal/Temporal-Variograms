#include("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\Empirical_Variogram_PM.jl")
using LsqFit
using Plots
# Important links 

# For Theroretical Variogram equations:# https://ml-gis-service.com/index.php/2022/04/01/geostatistics-theoretical-variogram-models/
# For Theroretical Variogram parameter limits:#https://help.seequent.com/Geothermal/5.0/en-GB/Content/estimation/variograms.htm
# For fitting Variogram : 
#γ = emp_var_dict["pm2.5"][1]
# dict_param = OrderedDict()

# function linear_variogram(h,p)
#   #p[1] => a =>  range
#   #p[2] => C =>  sill
#   #p[3] => C0 => nugget

#     #Equation γ(h) = C0 + C*(h/a)
#     return p[3] .+ p[2]*(h./p[1])

# end

function exponential_variogram(h,p)
    #p[1] => a =>  range
    #p[2] => C =>  sill
    #p[3] => C0 => nugget


    #Equation γ(h) = C0 + C*(1-exp(-h/a)))
    return p[3] .+ p[2]*(1 .- exp.(-1*(h./p[1])))
end

param_dict=OrderedDict()

function main(γ)
    h = collect(1:1:300)./60 
    # γ = emp_var_dict["pm2.5"][1]
    p0 = [0.10*maximum(h),0.7*maximum(γ),0.05*minimum(γ)] #Initial guess
    lb = [minimum(h),minimum(γ),0] #lower bounds for range, sill and nugget
    ub = [maximum(h)/3,maximum(γ),minimum(γ)] #upper bounds for range, sill and nugget
    xdata = h
    ydata = γ
    #nlin_fit(linear_variogram,xdata,ydata,p0)
    #push!(param_vec,nlin_fit(exponential_variogram,xdata,ydata,p0,key))
    return nlin_fit(exponential_variogram,xdata,ydata,p0,lb,ub)
end

function nlin_fit(model,xdata,ydata,p0,lb,ub)
    nlinfit = curve_fit(model,xdata,ydata,p0,lower=lb, upper=ub)
    pfit = nlinfit.param

    #print(pfit)
    # xlin = range(xdata[1],xdata[end],length = 3000) 
    # display(scatter(xdata,ydata, markersize = 3,legend = :outertopright,label = "Experimental Variogram"))
    # #plot(xlin,model(xlin,p0), label = "Initial fit Variogram")
    # display(plot!(xlin,model(xlin,pfit),linestyle =:dash, label = "Fitted Variogram",dpi = 200))
    # display(xaxis!("Lag Distance (seconds)"))
    # display( yaxis!("Semivariance (γ) "*key))
    return pfit
end

# for  i in 8:1:14
#     dict_param[dict_ips7100[i]]  = main(emp_var_dict[dict_ips7100[i]][1],dict_ips7100[i])
# end
pm_paths = readdir("/media/teamlarylive/loraMints1/empirical_variogram_files/",join=true)
dict_pm_id_path = OrderedDict("/media/teamlarylive/loraMints1/empirical_variogram_files/pm0.1"  => "pm0.1",
                   "/media/teamlarylive/loraMints1/empirical_variogram_files/pm0.3"  => "pm0.3",
                   "/media/teamlarylive/loraMints1/empirical_variogram_files/pm0.5"  => "pm0.5",
                   "/media/teamlarylive/loraMints1/empirical_variogram_files/pm1.0"  => "pm1.0",
                   "/media/teamlarylive/loraMints1/empirical_variogram_files/pm2.5"  => "pm2.5",
                   "/media/teamlarylive/loraMints1/empirical_variogram_files/pm5.0"  => "pm5.0",
                   "/media/teamlarylive/loraMints1/empirical_variogram_files/pm10.0" => "pm10.0")

param_dict =OrderedDict()

for p in pm_paths
    pm_id = dict_pm_id_path[p]
    pm_csv_files = glob("*.csv", p)

    for csv_path in pm_csv_files 
        df_emp_pm = CSV.read(csv_path,DataFrame)
        param_dict[pm_id] = []
        for i in 1:1:nrow(df_emp_pm)
            println(i)
            push!(param_dict[pm_id], main(Vector{Any}(df_emp_pm[i,:])))
        end
        df = DataFrame(Matrix(hcat(param_dict[pm_id]...)'),:auto)
        CSV.write("/media/teamlarylive/loraMints1/Paramters/"*pm_id*"/"*csv_path[end-13:end-4]*".csv",df)
    end
end

ts = collect(df_pm_filtered.dateTime[1]+Minute(15):Second(1):df_pm_filtered.dateTime[end] + Second(1))
range_df = DataFrame()
sill_df = DataFrame()
nugget_df = DataFrame()

range_df.dateTime = ts
sill_df.dateTime = ts
nugget_df.dateTime = ts

param_dict_df =OrderedDict()
for (key, value) in param_dict
    param_dict[key] = Matrix(hcat(param_dict[key]...)')
    # param_dict[key] = reshape(param_dict[key],96,3)
    param_dict_df[key] = DataFrame(param_dict[key], [:Range, :Sill, :Nugget])
    param_dict_df[key].dateTime = ts
    range_df[!,key] = (param_dict_df[key].Range).*3
    sill_df[!,key] = param_dict_df[key].Sill
    nugget_df[!,key] = param_dict_df[key].Nugget
end
CSV.write("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Range_Jan_2nd.csv",range_df)
CSV.write("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Sill_Jan_2nd.csv",sill_df)
CSV.write("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Nugget_Jan_2nd.csv",nugget_df)

