include("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\Processing_Data.jl")
using LsqFit, Plots, OrderedCollections,LaTeXStrings

function Empirical_Variogram(df_pm_filtered,cols_pm)
    #df_mat = select!(df_pm_filtered, Not(cols_pm[1:8]))
    mat_updated =  Matrix{Float64}(undef, 0, 14)

    mat = Matrix(df_pm_filtered[:,2:end])
    m = Matrix{Float64}(undef,300,0)
    td = 900
    lag = 300
    for n in 1:1:length(cols_pm[2:end])
        x=[]
        for h in 1:1:lag
            mat_head = mat[1:td-h,n]
            mat_tail = mat[1+h:td,n]
            println("head ",1,":",td-h)
            println("tail ",1+h,":",td)
            append!(x,sum((mat_head - mat_tail).^2,dims=1)/(2*(900-h)))    
        end

        m = hcat(m,x)#To match dimensions, vcat has to be used to append a column matrix with 900 columns
    end


    m  = hcat(collect(1:1:300)./60,m)

    γ = DataFrame(m,:auto)
    n = Symbol.([["Δt"];cols_pm[2:15]])
    rename!(γ, n)
    γ = transform!(γ, names(γ) .=> ByRow(Float64), renamecols=false)

    return γ
end

γ = Empirical_Variogram(df_pm_filtered,cols_pm)




function exponential_variogram(h,p)
    #p[1] => a => range
    #p[2] => C0 => sill
    #p[3] => nugget

    #Equation γ(h) = C0(1-exp(-h/a)))
    return p[3] .+ p[2]*(1 .- exp.(-1*(h./p[1])))
end





pm_latex_strings = ["PC"*latexstring("_{0.1}"), "PC"*latexstring(" _{0.3}"),"PC"*latexstring("_{0.5}"), "PC"*latexstring("_{1.0}"),
                    "PC"*latexstring("_{2.5}"),"PC"*latexstring("_{5.0}"), "PC"*latexstring("_{10.0}"),
                    "PM"*latexstring("_{0.1}"), "PM"*latexstring(" _{0.3}"),"PM"*latexstring("_{0.5}"), "PM"*latexstring("_{1.0}"),
                    "PM"*latexstring("_{2.5}"),"PM"*latexstring("_{5.0}"), "PM"*latexstring("_{10.0}")]
dict_pm_latex = OrderedDict(zip(names(df_pm_filtered)[2:end],pm_latex_strings))

# y_unit = "(μg/m³)"
ylab = "γ(Δt) for "
xlab = "Δt (minutes)"
main_title = "Variogram for 1st window of 2023-01-02" 


for key in names(γ[:,2:end])
    # γ = emp_var_dict["pm2.5"][1]
    p0 = [0.15*maximum(γ.Δt),0.9*maximum(γ[!,key]),0.05*minimum(γ[!,key])] #Initial guess
    lb = [minimum(γ.Δt),minimum(γ[!,key]),0] #lower bounds for range, sill and nugget
    ub = [maximum((γ.Δt))/3,maximum(γ[!,key]),minimum(γ[!,key])] #upper bounds for range, sill and nugget
    xdata = γ.Δt
    ydata = γ[!,key]
    #nlin_fit(linear_variogram,xdata,ydata,p0)
    #push!(param_vec,nlin_fit(exponential_variogram,xdata,ydata,p0,key))
    nlinfit = curve_fit(exponential_variogram,xdata,ydata,p0,lower=lb, upper=ub)
    #println(nlinfit) 
# returns fit.dof: degrees of freedom 
# fit.param: best fit parameters 
# fit.resid: vector of residuals 
# fit.jacobian: estimated Jacobian at the solution
    pfit = nlinfit.param
    println(pfit)
    xlin = range(xdata[1],xdata[end],length = 3000) 
    # x_intersect = round(pfit[1]*3,digits = 2)
    # y_intersect = round(pfit[2]*0.95,digits = 2)
    # x_min, x_max = extrema(γ.Δt)
    # y_min, y_max = extrema(γ[!,key])

    # text_x = x_min + 0.18 * (x_max - x_min)
    # text_y = y_min + 0.75 * (y_max - y_min) # changed to 0.1 instead of 0.9

    scatter(xdata,ydata, markersize = 3,markerstrokewidth = 0, legend = :outertopright,
    label = "Empirical Model",title = main_title, margin=5Plots.mm)
    # plot(xlin,model(xlin,p0), label = "Initial fit Variogram")
    plot!(xlin,exponential_variogram(xlin,pfit),linestyle =:dash, 
    gridlinewidth=3, label = "Theroretical Model")
    plot!([3*pfit[1]], seriestype="vline",label= " Range",line=(:dot, 4))
    plot!([0.95*pfit[2]], seriestype="hline",label= "Sill",line=(:dot, 4))
    scatter!([0], [pfit[3]], label="Nugget", markershape=:circle, markercolor=:white,markerstrokewidth = 2, markersize=6)
    scatter!([3*pfit[1]], [0.95*pfit[2]], markershape=:xcross, markercolor=:black, markersize=6,markerstrokewidth = 4,label = "")
    #plot!([pfit[3]], seriestype="hline",label= "Nugget",line=(:dot, 4))


    # annotate!(text_x, text_y, text("($x_intersect, $y_intersect)", font(8), :left))
    # scatter!([x_intersect], [y_intersect], markershape=:xcross, markercolor=:black, markersize=6,markerstrokewidth = 4,label = "")
    xaxis!(xlab)
    yaxis!(ylab*dict_pm_latex[key])
    png("D:\\UTD\\UTDFall2023\\Temporal_Variograms\\firmware\\plots\\"*key*"_variogram.png")
end



