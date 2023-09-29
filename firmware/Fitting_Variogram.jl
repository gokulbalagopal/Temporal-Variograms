# Define the spherical variogram model
include("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\Empirical_Variogram_PM.jl")
include("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\Averaging_without_Overalapping.jl")
# Define the spherical variogram model
# using Pkg
Pkg.add("NLopt")
using Optim, Random
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


paramu = []

for i in 1:1:length(emp_var_dict["pm2.5"][1])
    lags = hcat(collect(1:1:300)./60)
    experimental_variogram = emp_var_dict["pm2.5"][i]
    # Objective function to minimize for range, sill, and nugget
    function objective(params, lags, experimental_variogram)
        range_param, sill, nugget = params
        model_variogram = [spherical_variogram(h, nugget, sill, range_param) for h in lags]
        return sum((model_variogram .- experimental_variogram).^2)
    end


    # Number of random initial guesses
    num_initial_guesses = 100

    # Perform a random search for initial guesses
    best_params = nothing
    best_obj_value = Inf


    # Generate random initial guess parameters within a reasonable range
    initial_guess = [rand(0.01:5.0), rand(0.01:125.0), rand(0.001:5)]
    
    opt = Opt(:LD_MMA, length(initial_guess))
    lower_bounds = [0.0, 0.01, 0.0]  # Lower bounds for range, sill, and nugget
    upper_bounds = [5.0, 125.0, 5.0]  # Upper bounds for range, sill (replace Inf with an appropriate upper bound for nugget)
    set_bounds(opt, lower_bounds, upper_bounds)
    
    min_objective!(opt, objective)
    # Specify the stopping criteria if needed
    xtol_rel!(opt, 1e-6)

    # Solve the optimization problem
    (opt_value, opt_params, ret) = optimize(opt, initial_guess)
    # Extract optimized parameters
    optimized_params = opt_params

    push!(paramu,optimized_params)
    println(i)
    #println("Optimal Range, Sill, and Nugget Parameters:", best_params)
end
println("Optimal Range, Sill, and Nugget Parameters:", paramu)

# parameter_matrix = Matrix(hcat(paramu...)')         
# var_param_df = DataFrame(parameter_matrix, [:Range, :Sill, :Nugget])
# ts = collect(df_pm_filtered.dateTime[1]:Minute(15):df_pm_filtered.dateTime[end] )
# var_param_df.dateTime = ts
# var_param_df = var_param_df[ 0.01 .<= var_param_df.Range .< 5.0, :]
# #df_pm_15_min[df_pm_15_min.dateTime .== var_param_df.dateTime, :]
# using Plots 
# p1 = plot(df_pm_15_min.dateTime,df_pm_15_min.pm2_5,label="Conc.", xlabel="Time", ylabel="Conc.", title="Conc. vs Time" , xrotation = 20)
# p2 = plot(var_param_df.dateTime,var_param_df.Range, label="Range", xlabel="Time", ylabel="Range", title="Range vs Time", xrotation = 20)

# plot(p1, p2, layout=(2,1), legend=false)