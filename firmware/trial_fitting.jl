include("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\Averaging_without_Overalapping.jl")
include("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\Empirical_Variogram_PM.jl")


using Optim, Random,Plots
Random.seed!(123)
# Define the spherical variogram model
function spherical_variogram(h, nugget, sill, range_param)
    if h <= range_param
        return nugget + (sill - nugget) * (1.5 * h / range_param - 0.5 * (h / range_param) ^ 3)
    else
        return nugget + (sill - nugget)
    end
end


# Define the Gaussian variogram model
function gaussian_variogram(h, nugget, sill, range_param)
    return nugget + (sill - nugget) * (1.0 - exp(-3.0 * (h / range_param)^2))
end

parameter_dict= OrderedDict()
best_params = []
lags = hcat(collect(1:1:300)./60)
for key in keys(emp_var_dict)
    for i in 1:1:length(emp_var_dict[key])
        experimental_variogram = emp_var_dict[key][i]
        # Define the spherical variogram model
        function fit_variogram_model(variogram_function, lags, experimental_variogram)
            # Objective function to minimize for range, sill, and nugget
            function objective(params, lags, experimental_variogram)
                range_param, sill, nugget = params
                model_variogram = [variogram_function(h, nugget, sill, range_param) for h in lags]
                return  sum((model_variogram .- experimental_variogram).^2)

            end

            # Number of random initial guesses
            num_initial_guesses = 100

            # Perform a random search for initial guesses
            best_params = nothing
            best_obj_value = Inf

            for _ in 1:num_initial_guesses
                # Generate random initial guess parameters within a reasonable range
                initial_guess = [rand(0.1:5.0), rand(0.01:100.0), rand(0.1:5.0)]
                
                # Find the optimal range, sill, and nugget parameters using optimization
                result = optimize(params -> objective(params, lags, experimental_variogram), initial_guess, LBFGS())

                obj_value = Optim.minimum(result)
                
                # Update best parameters if this run produced a better fit
                if obj_value < best_obj_value
                    best_params = Optim.minimizer(result)
                    best_obj_value = obj_value
                end
            end

            # # Find the optimal range, sill, and nugget parameters using optimization
            # result = optimize(params -> objective(params, lags, experimental_variogram), initial_guess, LBFGS())

            # # Extract the optimized parameters
            # optimized_params = Optim.minimizer(result)

            # Calculate the mean squared error (MSE)
            mse = best_obj_value

            return best_params, mse
        end
    
        spherical_params = fit_variogram_model(spherical_variogram, lags, experimental_variogram)
        push!(best_params,spherical_params[1])
        println(i)
    end # remove this if we are using multiple variograms   
    parameter_dict[key] = best_params
    best_params = []
end

    # gaussian_params = fit_variogram_model(gaussian_variogram, lags, experimental_variogram)

    # if spherical_params[2] < gaussian_params[2]
    #     push!(best_params,spherical_params[1])
    #     # best_mse = spherical_params[2]
    #     # best_variogram = "spherical_variogram"
    # else
    #     push!(best_params,gaussian_params[1])
    #     # best_mse = gaussian_params[2]
    #     # best_variogram = "gaussian_variogram"
    # end
   
# end 
#reshape(parameter_dict["pm2.5"],96,3)
#parameter_dict_1= parameter_dict 
var_param_dict = OrderedDict()
ts = collect(df_pm_filtered.dateTime[1]:Minute(15):df_pm_filtered.dateTime[end] )
range_df = DataFrame()
range_df.dateTime = ts

for (key, value) in parameter_dict
    #parameter_dict[key] = Matrix(hcat(parameter_dict[key]...)')
    #parameter_dict[key] = reshape(parameter_dict[key],96,3)
    var_param_dict[key] = DataFrame(parameter_dict[key], [:Range, :Sill, :Nugget])
    var_param_dict[key].dateTime = ts
    #var_param_dict[key] = var_param_dict[key][ 0 .< var_param_dict[key].Range .<= 5.0, :]
    range_df[!,key] = var_param_dict[key].Range 
end

# parameter_matrix = Matrix(hcat(best_params...)')         
#var_param_df = DataFrame(parameter_matrix, [:Range, :Sill, :Nugget])
df_range_wind_tph = leftjoin(range_df, df_wind_tph,on = :dateTime)
CSV.write("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\data\\Julia_csv\\Range_Meterological_data.csv",df_range_wind_tph)
# var_param_df = var_param_df[ 0.01 .<= var_param_df.Range .< 5.0, :]
# df_pm_15_min = df_pm_15_min[1:96,:,]
# df = leftjoin(var_param_df, df_pm_15_min, on = :dateTime)
# using Plots 
# norm(x) = (x .- minimum(x)) ./ (maximum(x) .- minimum(x))
# plot(df.dateTime,norm(df.pm2_5),label="Conc.", xlabel="Time", ylabel="Conc.", title="Conc. vs Time" , xrotation = 20,legend = :outertopright)
# plot!(df.dateTime,norm(df.Range), label="Range", xlabel="Time", ylabel="Range", title="Range vs Time", xrotation = 20)

# plot(p1, p2, layout=(2,1), legend=false)